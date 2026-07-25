// SPDX-License-Identifier: MIT OR Apache-2.0

//! Experimental receiver-goodput Probe extension.
//!
//! This module intentionally uses a private, versioned wire protocol. It is
//! not an implementation of `draft-lcurley-moq-probe`.
//!
//! One logical Probe runs 50%, 75%, and 100% target stages in order. Each
//! stage receives the full requested active duration and follows this
//! handshake:
//!
//! `REQUEST -> START -> START_ACK -> 1-byte Padding -> paced Padding -> END`
//!
//! The sender never emits Padding payload before `START_ACK`. The receiver
//! arms on `START`, begins measurement at the first subsequent non-empty
//! Padding payload, and stops at `END`. Sender media and Padding accounting is
//! application payload only. A Padding write is polled exactly once; `Pending`
//! immediately aborts the stage and resets that stream with code `0x42`.

use std::{
    fs::File,
    future::Future,
    io::{self, BufWriter, Write},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    task::Poll,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::{pin_mut, poll};
use thiserror::Error;
use tokio::sync::Notify;

/// Private stream type used only by this experimental implementation.
pub const PROBE_STREAM_TYPE: u64 = 0xff00_0001;
pub const PROBE_PROTOCOL_VERSION: u64 = 1;

// Standard MOQT Padding stream/datagram types.
pub const PROBE_PADDING_STREAM_TYPE: u64 = 0x132B3E28;
pub const PROBE_PADDING_DATAGRAM_TYPE: u64 = 0x132B3E29;

pub const MSG_PROBE_REQUEST: u64 = 0x01;
pub const MSG_PROBE_START: u64 = 0x02;
pub const MSG_PROBE_START_ACK: u64 = 0x03;
pub const MSG_PROBE_END: u64 = 0x04;

pub const DEFAULT_STAGE_RATIOS_PERCENT: [u64; 3] = [50, 75, 100];
pub const DEFAULT_STAGE_PASS_PERCENT: u64 = 90;
pub const PADDING_PACING_TICK_MS: u64 = 10;
pub const PROBE_PADDING_ABORTED_BACKPRESSURE: u32 = 0x42;

// The native backend used by this repository passes these values to Quinn,
// where larger i32 values are scheduled first. This differs from the generic
// web_transport wrapper documentation. Keep control above media (0..=255)
// and Padding below every media stream.
pub const PROBE_CONTROL_PRIORITY: i32 = i32::MAX;
pub const PADDING_PRIORITY: i32 = i32::MIN;

#[derive(Debug, Error)]
pub enum ProbeError {
    #[error("webtransport error: {0}")]
    WebTransport(#[from] web_transport::Error),

    #[error("io error: {0}")]
    Io(#[from] io::Error),

    #[error("unexpected end of stream")]
    EndOfStream,

    #[error("invalid varint")]
    InvalidVarint,

    #[error("unsupported probe protocol version: {0}")]
    InvalidVersion(u64),

    #[error("unexpected probe message type: {0}")]
    UnexpectedMessageType(u64),

    #[error("invalid probe end status: {0}")]
    InvalidEndStatus(u64),

    #[error("invalid probe boolean: {0}")]
    InvalidBoolean(u64),

    #[error("probe protocol error: {0}")]
    Protocol(&'static str),

    #[error("probe state error: {0}")]
    State(&'static str),

    #[error("zero-length write")]
    ZeroWrite,
}

pub type ProbeResult<T> = Result<T, ProbeError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u64)]
pub enum ProbeEndStatus {
    Completed = 0,
    AbortedBackpressure = 1,
    ProtocolError = 2,
}

impl TryFrom<u64> for ProbeEndStatus {
    type Error = ProbeError;

    fn try_from(value: u64) -> ProbeResult<Self> {
        match value {
            0 => Ok(Self::Completed),
            1 => Ok(Self::AbortedBackpressure),
            2 => Ok(Self::ProtocolError),
            value => Err(ProbeError::InvalidEndStatus(value)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeRequest {
    pub request_id: u64,
    pub stage_index: u64,
    pub stage_ratio_percent: u64,
    pub stage_target_bps: u64,
    /// Active duration for this stage, not for the logical three-stage probe.
    pub active_duration_ms: u64,
    pub epoch_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeStart {
    pub request_id: u64,
    pub stage_index: u64,
    pub stage_target_bps: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeStartAck {
    pub request_id: u64,
    pub stage_index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeEnd {
    pub request_id: u64,
    pub stage_index: u64,
    pub status: ProbeEndStatus,
    pub attempted_padding_bytes: u64,
    pub accepted_padding_bytes: u64,
    pub media_accepted_payload_bytes: u64,
    pub padding_pending_seen: bool,
    pub stage_active_duration_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProbeMessage {
    Request(ProbeRequest),
    Start(ProbeStart),
    StartAck(ProbeStartAck),
    End(ProbeEnd),
}

#[derive(Debug, Default)]
pub struct SenderProbeContext {
    media_accepted_payload_bytes: AtomicU64,
}

impl SenderProbeContext {
    pub fn record_media_payload(&self, bytes: usize) {
        self.media_accepted_payload_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub fn media_accepted_payload_bytes(&self) -> u64 {
        self.media_accepted_payload_bytes.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MeasurementPhase {
    WaitingForStart,
    Armed,
    Measuring,
    Ended,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageStatus {
    Completed,
    NoPostStartPadding,
    AbortedBackpressure,
    ProtocolError,
    ConnectionClosed,
}

#[derive(Debug, Clone)]
pub struct StageResult {
    pub request_id: u64,
    pub stage_index: u64,
    pub stage_ratio_percent: u64,
    pub stage_target_bps: u64,
    pub probe_start_received_at: Instant,
    pub probe_start_received_timestamp_ms: u64,
    pub first_post_start_padding_at: Option<Instant>,
    pub first_post_start_padding_timestamp_ms: Option<u64>,
    pub probe_end_received_at: Instant,
    pub probe_end_received_timestamp_ms: u64,
    pub measurement_duration_ms: u64,
    pub arming_delay_ms: Option<u64>,
    pub pre_start_padding_bytes: u64,
    pub received_media_bytes: u64,
    pub received_padding_bytes: u64,
    pub late_padding_bytes: u64,
    pub media_goodput_bps: u64,
    pub padding_goodput_bps: u64,
    pub receiver_goodput_bps: u64,
    pub sender_attempted_padding_bytes: u64,
    pub sender_accepted_padding_bytes: u64,
    pub sender_media_accepted_payload_bytes: u64,
    pub sender_total_accepted_payload_bytes: u64,
    pub sender_app_bitrate_bps: u64,
    pub pass_percent: u64,
    pub stage_passed: bool,
    pub result_valid: bool,
    pub partial_measurement: bool,
    pub status: StageStatus,
    pub padding_stream_finished: bool,
    pub padding_stream_reset: bool,
    pub reset_error_code: Option<u32>,
}

#[derive(Debug)]
struct ReceiverStage {
    request_id: u64,
    stage_index: u64,
    stage_ratio_percent: u64,
    stage_target_bps: u64,
    pass_percent: u64,
    phase: MeasurementPhase,
    probe_start_received_at: Option<Instant>,
    probe_start_received_timestamp_ms: Option<u64>,
    measurement_start_at: Option<Instant>,
    measurement_start_timestamp_ms: Option<u64>,
    probe_end_received_at: Option<Instant>,
    probe_end_received_timestamp_ms: Option<u64>,
    pre_start_padding_bytes: u64,
    received_media_bytes: u64,
    received_padding_bytes: u64,
    late_padding_bytes: u64,
    padding_stream_open: bool,
    padding_stream_finished: bool,
    padding_stream_reset: bool,
    reset_error_code: Option<u32>,
    end_status: Option<ProbeEndStatus>,
}

#[derive(Debug, Default)]
struct ReceiverState {
    stage: Option<ReceiverStage>,
}

#[derive(Debug, Default)]
pub struct ReceiverProbeContext {
    state: Mutex<ReceiverState>,
    padding_cleanup: Notify,
}

impl ReceiverProbeContext {
    pub fn begin_stage(
        &self,
        request_id: u64,
        stage_index: u64,
        stage_ratio_percent: u64,
        stage_target_bps: u64,
        pass_percent: u64,
    ) -> ProbeResult<()> {
        let mut state = self.state.lock().unwrap();
        if let Some(previous) = &state.stage {
            if previous.phase != MeasurementPhase::Ended
                || (previous.padding_stream_open
                    && !previous.padding_stream_finished
                    && !previous.padding_stream_reset)
            {
                return Err(ProbeError::State(
                    "previous stage has not ended and cleaned up",
                ));
            }
        }

        state.stage = Some(ReceiverStage {
            request_id,
            stage_index,
            stage_ratio_percent,
            stage_target_bps,
            pass_percent,
            phase: MeasurementPhase::WaitingForStart,
            probe_start_received_at: None,
            probe_start_received_timestamp_ms: None,
            measurement_start_at: None,
            measurement_start_timestamp_ms: None,
            probe_end_received_at: None,
            probe_end_received_timestamp_ms: None,
            pre_start_padding_bytes: 0,
            received_media_bytes: 0,
            received_padding_bytes: 0,
            late_padding_bytes: 0,
            padding_stream_open: false,
            padding_stream_finished: false,
            padding_stream_reset: false,
            reset_error_code: None,
            end_status: None,
        });
        Ok(())
    }

    pub fn on_probe_start(&self, start: &ProbeStart, now: Instant) -> ProbeResult<()> {
        let mut state = self.state.lock().unwrap();
        let stage = state
            .stage
            .as_mut()
            .ok_or(ProbeError::State("START without active stage"))?;
        verify_stage(stage, start.request_id, start.stage_index)?;
        if stage.stage_target_bps != start.stage_target_bps {
            return Err(ProbeError::Protocol("START target does not match request"));
        }
        if stage.phase != MeasurementPhase::WaitingForStart {
            return Err(ProbeError::Protocol("duplicate or out-of-order START"));
        }
        stage.probe_start_received_at = Some(now);
        stage.probe_start_received_timestamp_ms = Some(now_ms());
        stage.phase = MeasurementPhase::Armed;
        Ok(())
    }

    pub fn on_padding_stream_open(&self) -> ProbeResult<()> {
        let mut state = self.state.lock().unwrap();
        let stage = state
            .stage
            .as_mut()
            .ok_or(ProbeError::State("Padding stream without active stage"))?;
        if stage.padding_stream_open
            && !stage.padding_stream_finished
            && !stage.padding_stream_reset
        {
            return Err(ProbeError::Protocol("overlapping Padding streams"));
        }
        stage.padding_stream_open = true;
        stage.padding_stream_finished = false;
        stage.padding_stream_reset = false;
        stage.reset_error_code = None;
        Ok(())
    }

    pub fn on_padding_payload(&self, bytes: usize, now: Instant) -> ProbeResult<()> {
        if bytes == 0 {
            return Ok(());
        }

        let mut state = self.state.lock().unwrap();
        let stage = state
            .stage
            .as_mut()
            .ok_or(ProbeError::State("Padding payload without active stage"))?;
        let bytes = bytes as u64;
        match stage.phase {
            MeasurementPhase::WaitingForStart => {
                stage.pre_start_padding_bytes = stage.pre_start_padding_bytes.saturating_add(bytes);
            }
            MeasurementPhase::Armed => {
                stage.measurement_start_at = Some(now);
                stage.measurement_start_timestamp_ms = Some(now_ms());
                stage.received_padding_bytes = stage.received_padding_bytes.saturating_add(bytes);
                stage.phase = MeasurementPhase::Measuring;
            }
            MeasurementPhase::Measuring => {
                stage.received_padding_bytes = stage.received_padding_bytes.saturating_add(bytes);
            }
            MeasurementPhase::Ended => {
                stage.late_padding_bytes = stage.late_padding_bytes.saturating_add(bytes);
            }
        }
        Ok(())
    }

    pub fn on_media_payload(&self, bytes: usize, _now: Instant) {
        if bytes == 0 {
            return;
        }
        let mut state = self.state.lock().unwrap();
        if let Some(stage) = state.stage.as_mut() {
            if stage.phase == MeasurementPhase::Measuring {
                stage.received_media_bytes =
                    stage.received_media_bytes.saturating_add(bytes as u64);
            }
        }
    }

    pub fn on_padding_stream_finished(&self) -> ProbeResult<()> {
        {
            let mut state = self.state.lock().unwrap();
            let stage = state
                .stage
                .as_mut()
                .ok_or(ProbeError::State("Padding FIN without active stage"))?;
            if stage.padding_stream_finished || stage.padding_stream_reset {
                return Err(ProbeError::Protocol("duplicate Padding cleanup"));
            }
            stage.padding_stream_finished = true;
        }
        self.padding_cleanup.notify_waiters();
        Ok(())
    }

    pub fn on_padding_stream_reset(&self, error_code: Option<u32>) -> ProbeResult<()> {
        {
            let mut state = self.state.lock().unwrap();
            let stage = state
                .stage
                .as_mut()
                .ok_or(ProbeError::State("Padding reset without active stage"))?;
            if stage.padding_stream_finished || stage.padding_stream_reset {
                return Err(ProbeError::Protocol("duplicate Padding cleanup"));
            }
            stage.padding_stream_reset = true;
            stage.reset_error_code = error_code;
        }
        self.padding_cleanup.notify_waiters();
        Ok(())
    }

    pub fn on_probe_end(&self, end: &ProbeEnd, now: Instant) -> ProbeResult<StageResult> {
        let mut state = self.state.lock().unwrap();
        let stage = state
            .stage
            .as_mut()
            .ok_or(ProbeError::State("END without active stage"))?;
        verify_stage(stage, end.request_id, end.stage_index)?;
        if stage.phase == MeasurementPhase::Ended {
            return Err(ProbeError::Protocol("duplicate END"));
        }

        let previous_phase = stage.phase;
        stage.phase = MeasurementPhase::Ended;
        stage.probe_end_received_at = Some(now);
        stage.probe_end_received_timestamp_ms = Some(now_ms());
        stage.end_status = Some(end.status);

        let started_at = stage.measurement_start_at;
        let elapsed_ms = started_at
            .map(|start| now.saturating_duration_since(start).as_millis() as u64)
            .unwrap_or(0);
        let media_bps = bitrate_bps(stage.received_media_bytes, elapsed_ms);
        let padding_bps = bitrate_bps(stage.received_padding_bytes, elapsed_ms);
        let receiver_bps = bitrate_bps(
            stage
                .received_media_bytes
                .saturating_add(stage.received_padding_bytes),
            elapsed_ms,
        );

        let measured = previous_phase == MeasurementPhase::Measuring && elapsed_ms > 0;
        let completed = end.status == ProbeEndStatus::Completed;
        let result_valid = measured && completed;
        let partial_measurement = measured && !completed;
        let status = match end.status {
            ProbeEndStatus::Completed if measured => StageStatus::Completed,
            ProbeEndStatus::Completed => StageStatus::NoPostStartPadding,
            ProbeEndStatus::AbortedBackpressure => StageStatus::AbortedBackpressure,
            ProbeEndStatus::ProtocolError => StageStatus::ProtocolError,
        };
        let stage_passed = result_valid
            && goodput_passes(receiver_bps, stage.stage_target_bps, stage.pass_percent);
        let sender_total_accepted_payload_bytes = end
            .media_accepted_payload_bytes
            .saturating_add(end.accepted_padding_bytes);
        let sender_app_bitrate_bps = bitrate_bps(
            sender_total_accepted_payload_bytes,
            end.stage_active_duration_ms,
        );

        let start_received = stage
            .probe_start_received_at
            .ok_or(ProbeError::Protocol("END received before START"))?;

        Ok(StageResult {
            request_id: stage.request_id,
            stage_index: stage.stage_index,
            stage_ratio_percent: stage.stage_ratio_percent,
            stage_target_bps: stage.stage_target_bps,
            probe_start_received_at: start_received,
            probe_start_received_timestamp_ms: stage
                .probe_start_received_timestamp_ms
                .ok_or(ProbeError::Protocol("missing START timestamp"))?,
            first_post_start_padding_at: started_at,
            first_post_start_padding_timestamp_ms: stage.measurement_start_timestamp_ms,
            probe_end_received_at: now,
            probe_end_received_timestamp_ms: stage
                .probe_end_received_timestamp_ms
                .ok_or(ProbeError::Protocol("missing END timestamp"))?,
            measurement_duration_ms: elapsed_ms,
            arming_delay_ms: started_at
                .map(|start| start.saturating_duration_since(start_received).as_millis() as u64),
            pre_start_padding_bytes: stage.pre_start_padding_bytes,
            received_media_bytes: stage.received_media_bytes,
            received_padding_bytes: stage.received_padding_bytes,
            late_padding_bytes: stage.late_padding_bytes,
            media_goodput_bps: media_bps,
            padding_goodput_bps: padding_bps,
            receiver_goodput_bps: receiver_bps,
            sender_attempted_padding_bytes: end.attempted_padding_bytes,
            sender_accepted_padding_bytes: end.accepted_padding_bytes,
            sender_media_accepted_payload_bytes: end.media_accepted_payload_bytes,
            sender_total_accepted_payload_bytes,
            sender_app_bitrate_bps,
            pass_percent: stage.pass_percent,
            stage_passed,
            result_valid,
            partial_measurement,
            status,
            padding_stream_finished: stage.padding_stream_finished,
            padding_stream_reset: stage.padding_stream_reset,
            reset_error_code: stage.reset_error_code,
        })
    }

    pub async fn wait_for_padding_cleanup(&self) {
        loop {
            let notified = self.padding_cleanup.notified();
            {
                let state = self.state.lock().unwrap();
                let Some(stage) = &state.stage else {
                    return;
                };
                if !stage.padding_stream_open
                    || stage.padding_stream_finished
                    || stage.padding_stream_reset
                {
                    return;
                }
            }
            notified.await;
        }
    }

    pub fn refresh_after_padding_cleanup(&self, result: &mut StageResult) -> ProbeResult<()> {
        let state = self.state.lock().unwrap();
        let stage = state
            .stage
            .as_ref()
            .ok_or(ProbeError::State("no stage result to refresh"))?;
        verify_stage(stage, result.request_id, result.stage_index)?;
        result.late_padding_bytes = stage.late_padding_bytes;
        result.padding_stream_finished = stage.padding_stream_finished;
        result.padding_stream_reset = stage.padding_stream_reset;
        result.reset_error_code = stage.reset_error_code;
        Ok(())
    }

    pub fn late_padding_bytes(&self) -> u64 {
        self.state
            .lock()
            .unwrap()
            .stage
            .as_ref()
            .map(|stage| stage.late_padding_bytes)
            .unwrap_or(0)
    }
}

fn verify_stage(stage: &ReceiverStage, request_id: u64, stage_index: u64) -> ProbeResult<()> {
    if stage.request_id != request_id {
        return Err(ProbeError::Protocol("request ID mismatch"));
    }
    if stage.stage_index != stage_index {
        return Err(ProbeError::Protocol("stage index mismatch"));
    }
    Ok(())
}

pub fn stage_target_bps(final_target_bps: u64, ratio_percent: u64) -> u64 {
    ((final_target_bps as u128).saturating_mul(ratio_percent as u128) / 100).min(u64::MAX as u128)
        as u64
}

pub fn allowed_total_bytes(target_bps: u64, elapsed: Duration) -> u64 {
    let nanos = elapsed.as_nanos();
    ((target_bps as u128).saturating_mul(nanos) / 8 / 1_000_000_000).min(u64::MAX as u128) as u64
}

pub fn padding_due_bytes(
    target_bps: u64,
    elapsed: Duration,
    accepted_media_payload_bytes: u64,
    accepted_padding_payload_bytes: u64,
) -> u64 {
    allowed_total_bytes(target_bps, elapsed)
        .saturating_sub(accepted_media_payload_bytes)
        .saturating_sub(accepted_padding_payload_bytes)
}

pub fn goodput_passes(receiver_goodput_bps: u64, target_bps: u64, pass_percent: u64) -> bool {
    (receiver_goodput_bps as u128).saturating_mul(100)
        >= (target_bps as u128).saturating_mul(pass_percent as u128)
}

pub fn bitrate_bps(bytes: u64, elapsed_ms: u64) -> u64 {
    if elapsed_ms == 0 {
        0
    } else {
        ((bytes as u128).saturating_mul(8).saturating_mul(1000) / elapsed_ms as u128)
            .min(u64::MAX as u128) as u64
    }
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub fn put_varint(out: &mut BytesMut, value: u64) {
    if value < 0x40 {
        out.put_u8(value as u8);
    } else if value < 0x4000 {
        out.put_u16((value as u16) | 0x4000);
    } else if value < 0x4000_0000 {
        out.put_u32((value as u32) | 0x8000_0000);
    } else {
        out.put_u64(value | 0xc000_0000_0000_0000);
    }
}

pub fn get_varint(buf: &mut Bytes) -> ProbeResult<u64> {
    if !buf.has_remaining() {
        return Err(ProbeError::InvalidVarint);
    }
    let needed = 1usize << (buf[0] >> 6);
    if buf.remaining() < needed {
        return Err(ProbeError::InvalidVarint);
    }
    match needed {
        1 => Ok(buf.get_u8() as u64 & 0x3f),
        2 => Ok(buf.get_u16() as u64 & 0x3fff),
        4 => Ok(buf.get_u32() as u64 & 0x3fff_ffff),
        8 => Ok(buf.get_u64() & 0x3fff_ffff_ffff_ffff),
        _ => unreachable!(),
    }
}

fn put_bool(out: &mut BytesMut, value: bool) {
    put_varint(out, value as u64);
}

fn get_bool(buf: &mut Bytes) -> ProbeResult<bool> {
    match get_varint(buf)? {
        0 => Ok(false),
        1 => Ok(true),
        value => Err(ProbeError::InvalidBoolean(value)),
    }
}

pub fn encode_probe_message(message: &ProbeMessage) -> Bytes {
    let (message_type, payload) = match message {
        ProbeMessage::Request(request) => {
            let mut payload = BytesMut::new();
            put_varint(&mut payload, request.request_id);
            put_varint(&mut payload, request.stage_index);
            put_varint(&mut payload, request.stage_ratio_percent);
            put_varint(&mut payload, request.stage_target_bps);
            put_varint(&mut payload, request.active_duration_ms);
            put_varint(&mut payload, request.epoch_ms);
            (MSG_PROBE_REQUEST, payload)
        }
        ProbeMessage::Start(start) => {
            let mut payload = BytesMut::new();
            put_varint(&mut payload, start.request_id);
            put_varint(&mut payload, start.stage_index);
            put_varint(&mut payload, start.stage_target_bps);
            (MSG_PROBE_START, payload)
        }
        ProbeMessage::StartAck(ack) => {
            let mut payload = BytesMut::new();
            put_varint(&mut payload, ack.request_id);
            put_varint(&mut payload, ack.stage_index);
            (MSG_PROBE_START_ACK, payload)
        }
        ProbeMessage::End(end) => {
            let mut payload = BytesMut::new();
            put_varint(&mut payload, end.request_id);
            put_varint(&mut payload, end.stage_index);
            put_varint(&mut payload, end.status as u64);
            put_varint(&mut payload, end.attempted_padding_bytes);
            put_varint(&mut payload, end.accepted_padding_bytes);
            put_varint(&mut payload, end.media_accepted_payload_bytes);
            put_bool(&mut payload, end.padding_pending_seen);
            put_varint(&mut payload, end.stage_active_duration_ms);
            (MSG_PROBE_END, payload)
        }
    };

    let mut out = BytesMut::new();
    put_varint(&mut out, message_type);
    put_varint(&mut out, payload.len() as u64);
    out.extend_from_slice(&payload);
    out.freeze()
}

fn decode_probe_message(message_type: u64, mut payload: Bytes) -> ProbeResult<ProbeMessage> {
    let message = match message_type {
        MSG_PROBE_REQUEST => ProbeMessage::Request(ProbeRequest {
            request_id: get_varint(&mut payload)?,
            stage_index: get_varint(&mut payload)?,
            stage_ratio_percent: get_varint(&mut payload)?,
            stage_target_bps: get_varint(&mut payload)?,
            active_duration_ms: get_varint(&mut payload)?,
            epoch_ms: get_varint(&mut payload)?,
        }),
        MSG_PROBE_START => ProbeMessage::Start(ProbeStart {
            request_id: get_varint(&mut payload)?,
            stage_index: get_varint(&mut payload)?,
            stage_target_bps: get_varint(&mut payload)?,
        }),
        MSG_PROBE_START_ACK => ProbeMessage::StartAck(ProbeStartAck {
            request_id: get_varint(&mut payload)?,
            stage_index: get_varint(&mut payload)?,
        }),
        MSG_PROBE_END => ProbeMessage::End(ProbeEnd {
            request_id: get_varint(&mut payload)?,
            stage_index: get_varint(&mut payload)?,
            status: ProbeEndStatus::try_from(get_varint(&mut payload)?)?,
            attempted_padding_bytes: get_varint(&mut payload)?,
            accepted_padding_bytes: get_varint(&mut payload)?,
            media_accepted_payload_bytes: get_varint(&mut payload)?,
            padding_pending_seen: get_bool(&mut payload)?,
            stage_active_duration_ms: get_varint(&mut payload)?,
        }),
        value => return Err(ProbeError::UnexpectedMessageType(value)),
    };
    if payload.has_remaining() {
        return Err(ProbeError::Protocol("trailing bytes in Probe message"));
    }
    Ok(message)
}

async fn read_exact_web(recv: &mut web_transport::RecvStream, len: usize) -> ProbeResult<BytesMut> {
    let mut out = BytesMut::with_capacity(len);
    while out.len() < len {
        match recv.read(len - out.len()).await? {
            Some(chunk) => out.extend_from_slice(&chunk),
            None => return Err(ProbeError::EndOfStream),
        }
    }
    Ok(out)
}

pub async fn read_varint_from_recv_stream(
    recv: &mut web_transport::RecvStream,
) -> ProbeResult<(u64, Bytes)> {
    let first = match recv.read(1).await? {
        Some(chunk) if !chunk.is_empty() => chunk[0],
        _ => return Err(ProbeError::EndOfStream),
    };
    let needed = 1usize << (first >> 6);
    let mut raw = BytesMut::with_capacity(needed);
    raw.put_u8(first);
    while raw.len() < needed {
        match recv.read(needed - raw.len()).await? {
            Some(chunk) => raw.extend_from_slice(&chunk),
            None => return Err(ProbeError::EndOfStream),
        }
    }
    let mut encoded = raw.clone().freeze();
    Ok((get_varint(&mut encoded)?, raw.freeze()))
}

pub async fn read_varint_web(recv: &mut web_transport::RecvStream) -> ProbeResult<u64> {
    read_varint_from_recv_stream(recv)
        .await
        .map(|(value, _)| value)
}

pub async fn write_all_web(send: &mut web_transport::SendStream, bytes: &[u8]) -> ProbeResult<()> {
    let mut written = 0;
    while written < bytes.len() {
        let count = send.write(&bytes[written..]).await?;
        if count == 0 {
            return Err(ProbeError::ZeroWrite);
        }
        written += count;
    }
    Ok(())
}

pub async fn write_varint_web(send: &mut web_transport::SendStream, value: u64) -> ProbeResult<()> {
    let mut encoded = BytesMut::new();
    put_varint(&mut encoded, value);
    write_all_web(send, &encoded).await
}

pub async fn write_probe_message_web(
    send: &mut web_transport::SendStream,
    message: &ProbeMessage,
) -> ProbeResult<()> {
    write_all_web(send, &encode_probe_message(message)).await
}

pub async fn read_probe_message_web(
    recv: &mut web_transport::RecvStream,
) -> ProbeResult<ProbeMessage> {
    let message_type = read_varint_web(recv).await?;
    let length = read_varint_web(recv).await? as usize;
    let payload = read_exact_web(recv, length).await?.freeze();
    decode_probe_message(message_type, payload)
}

async fn poll_write_once(
    send: &mut web_transport::SendStream,
    payload: &[u8],
) -> ProbeResult<Poll<usize>> {
    match poll_future_once(send.write(payload)).await {
        Poll::Ready(result) => Ok(Poll::Ready(result?)),
        Poll::Pending => Ok(Poll::Pending),
    }
}

async fn poll_future_once<F: Future>(future: F) -> Poll<F::Output> {
    pin_mut!(future);
    poll!(future)
}

#[derive(Debug, Clone)]
pub struct ProbeConfig {
    pub enabled: bool,
    pub log: Option<PathBuf>,
    pub max_target_bitrate_bps: u64,
}

impl Default for ProbeConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            log: None,
            max_target_bitrate_bps: 100_000_000,
        }
    }
}

#[derive(Debug, Default, Clone)]
struct SenderStageStats {
    attempted_padding_bytes: u64,
    accepted_padding_bytes: u64,
    padding_pending_seen: bool,
}

pub async fn run_relay_probe_acceptor(
    webtransport: web_transport::Session,
    config: ProbeConfig,
    context: Arc<SenderProbeContext>,
) -> ProbeResult<()> {
    if !config.enabled {
        return Ok(());
    }

    let mut csv = config.log.clone().map(RelayProbeCsv::new).transpose()?;

    loop {
        let (mut control_send, mut control_recv) = webtransport.accept_bi().await?;
        control_send.set_priority(PROBE_CONTROL_PRIORITY);

        let stream_type = read_varint_web(&mut control_recv).await?;
        if stream_type != PROBE_STREAM_TYPE {
            return Err(ProbeError::Protocol("unexpected bidirectional stream type"));
        }
        let version = read_varint_web(&mut control_recv).await?;
        if version != PROBE_PROTOCOL_VERSION {
            return Err(ProbeError::InvalidVersion(version));
        }

        let mut expected_request_id = None;
        let mut expected_stage_index = 0u64;

        loop {
            let request = match read_probe_message_web(&mut control_recv).await {
                Ok(ProbeMessage::Request(request)) => request,
                Err(ProbeError::EndOfStream) => break,
                Ok(_) => return Err(ProbeError::Protocol("expected PROBE_REQUEST")),
                Err(error) => return Err(error),
            };

            if let Err(error) = validate_request(
                &request,
                expected_request_id,
                expected_stage_index,
                config.max_target_bitrate_bps,
            ) {
                let end = ProbeEnd {
                    request_id: request.request_id,
                    stage_index: request.stage_index,
                    status: ProbeEndStatus::ProtocolError,
                    attempted_padding_bytes: 0,
                    accepted_padding_bytes: 0,
                    media_accepted_payload_bytes: 0,
                    padding_pending_seen: false,
                    stage_active_duration_ms: 0,
                };
                if let Some(csv) = &mut csv {
                    csv.row(&request, &end)?;
                }
                write_probe_message_web(&mut control_send, &ProbeMessage::End(end)).await?;
                control_send.finish()?;
                tracing::warn!(?error, "rejected invalid Probe request");
                break;
            }
            expected_request_id = Some(request.request_id);

            let end = run_sender_stage(
                &webtransport,
                &mut control_send,
                &mut control_recv,
                &request,
                context.as_ref(),
            )
            .await?;

            if let Some(csv) = &mut csv {
                csv.row(&request, &end)?;
            }
            write_probe_message_web(&mut control_send, &ProbeMessage::End(end.clone())).await?;

            if end.status != ProbeEndStatus::Completed {
                control_send.finish()?;
                break;
            }

            expected_stage_index += 1;
            if expected_stage_index == DEFAULT_STAGE_RATIOS_PERCENT.len() as u64 {
                control_send.finish()?;
                break;
            }
        }
    }
}

fn validate_request(
    request: &ProbeRequest,
    expected_request_id: Option<u64>,
    expected_stage_index: u64,
    max_target_bitrate_bps: u64,
) -> ProbeResult<()> {
    if request.stage_index != expected_stage_index {
        return Err(ProbeError::Protocol("unexpected stage index"));
    }
    if let Some(request_id) = expected_request_id {
        if request.request_id != request_id {
            return Err(ProbeError::Protocol(
                "request ID changed within logical Probe",
            ));
        }
    }
    let expected_ratio = DEFAULT_STAGE_RATIOS_PERCENT
        .get(request.stage_index as usize)
        .ok_or(ProbeError::Protocol("stage index out of range"))?;
    if request.stage_ratio_percent != *expected_ratio {
        return Err(ProbeError::Protocol("unexpected stage ratio"));
    }
    if request.stage_target_bps > max_target_bitrate_bps {
        return Err(ProbeError::Protocol(
            "stage target exceeds configured maximum",
        ));
    }
    if request.active_duration_ms == 0 || request.epoch_ms == 0 {
        return Err(ProbeError::Protocol("duration and epoch must be non-zero"));
    }
    Ok(())
}

async fn run_sender_stage(
    webtransport: &web_transport::Session,
    control_send: &mut web_transport::SendStream,
    control_recv: &mut web_transport::RecvStream,
    request: &ProbeRequest,
    context: &SenderProbeContext,
) -> ProbeResult<ProbeEnd> {
    let mut padding = webtransport.open_uni().await?;
    padding.set_priority(PADDING_PRIORITY);
    // The stream header is setup, not scheduled Padding debt. Complete it
    // before START so active duration never includes stream creation.
    write_varint_web(&mut padding, PROBE_PADDING_STREAM_TYPE).await?;

    write_probe_message_web(
        control_send,
        &ProbeMessage::Start(ProbeStart {
            request_id: request.request_id,
            stage_index: request.stage_index,
            stage_target_bps: request.stage_target_bps,
        }),
    )
    .await?;

    let mut stats = SenderStageStats::default();
    match read_probe_message_web(control_recv).await? {
        ProbeMessage::StartAck(ack)
            if ack.request_id == request.request_id && ack.stage_index == request.stage_index => {}
        _ => {
            padding.finish()?;
            return Ok(sender_end(
                request,
                ProbeEndStatus::ProtocolError,
                &stats,
                0,
                Duration::ZERO,
            ));
        }
    }

    let stage_started = Instant::now();
    let media_started = context.media_accepted_payload_bytes();
    let trigger = [0u8; 1];

    stats.attempted_padding_bytes = 1;
    match poll_write_once(&mut padding, &trigger).await? {
        Poll::Ready(0) => return Err(ProbeError::ZeroWrite),
        Poll::Ready(written) => stats.accepted_padding_bytes = written as u64,
        Poll::Pending => {
            stats.padding_pending_seen = true;
            padding.reset(PROBE_PADDING_ABORTED_BACKPRESSURE);
            return Ok(sender_end(
                request,
                ProbeEndStatus::AbortedBackpressure,
                &stats,
                context
                    .media_accepted_payload_bytes()
                    .saturating_sub(media_started),
                stage_started.elapsed(),
            ));
        }
    }

    let duration = Duration::from_millis(request.active_duration_ms);
    let tick = Duration::from_millis(PADDING_PACING_TICK_MS);
    let buffer = [0u8; 1200];

    while stage_started.elapsed() < duration {
        let remaining = duration.saturating_sub(stage_started.elapsed());
        tokio::select! {
            message = read_probe_message_web(control_recv) => {
                match message {
                    Ok(_) => {
                        padding.finish()?;
                        return Ok(sender_end(
                            request,
                            ProbeEndStatus::ProtocolError,
                            &stats,
                            context
                                .media_accepted_payload_bytes()
                                .saturating_sub(media_started),
                            stage_started.elapsed(),
                        ));
                    }
                    Err(error) => {
                        padding.finish()?;
                        return Err(error);
                    }
                }
            }
            _ = tokio::time::sleep(tick.min(remaining)) => {}
        }

        let elapsed = stage_started.elapsed().min(duration);
        let media = context
            .media_accepted_payload_bytes()
            .saturating_sub(media_started);
        let mut due = padding_due_bytes(
            request.stage_target_bps,
            elapsed,
            media,
            stats.accepted_padding_bytes,
        );

        while due > 0 {
            let length = due.min(buffer.len() as u64) as usize;
            stats.attempted_padding_bytes =
                stats.attempted_padding_bytes.saturating_add(length as u64);
            match poll_write_once(&mut padding, &buffer[..length]).await? {
                Poll::Ready(0) => return Err(ProbeError::ZeroWrite),
                Poll::Ready(written) => {
                    stats.accepted_padding_bytes =
                        stats.accepted_padding_bytes.saturating_add(written as u64);
                    due = due.saturating_sub(written as u64);
                }
                Poll::Pending => {
                    stats.padding_pending_seen = true;
                    padding.reset(PROBE_PADDING_ABORTED_BACKPRESSURE);
                    return Ok(sender_end(
                        request,
                        ProbeEndStatus::AbortedBackpressure,
                        &stats,
                        context
                            .media_accepted_payload_bytes()
                            .saturating_sub(media_started),
                        stage_started.elapsed(),
                    ));
                }
            }
        }
    }

    padding.finish()?;
    Ok(sender_end(
        request,
        ProbeEndStatus::Completed,
        &stats,
        context
            .media_accepted_payload_bytes()
            .saturating_sub(media_started),
        stage_started.elapsed(),
    ))
}

fn sender_end(
    request: &ProbeRequest,
    status: ProbeEndStatus,
    stats: &SenderStageStats,
    media_accepted_payload_bytes: u64,
    elapsed: Duration,
) -> ProbeEnd {
    ProbeEnd {
        request_id: request.request_id,
        stage_index: request.stage_index,
        status,
        attempted_padding_bytes: stats.attempted_padding_bytes,
        accepted_padding_bytes: stats.accepted_padding_bytes,
        media_accepted_payload_bytes,
        padding_pending_seen: stats.padding_pending_seen,
        stage_active_duration_ms: elapsed.as_millis() as u64,
    }
}

pub struct RelayProbeCsv {
    writer: BufWriter<File>,
}

impl RelayProbeCsv {
    pub fn new(path: PathBuf) -> ProbeResult<Self> {
        let mut writer = BufWriter::new(File::create(path)?);
        writeln!(
            writer,
            "timestamp_ms,request_id,target_bitrate_bps,paced_target_bps,cwnd_bytes,attempted_padding_bytes,write_accepted_padding_bytes,accept_ratio_ppm,write_block_time_ms,media_write_bytes,raw_sender_app_bitrate_bps,epoch_elapsed_ms,stage_index,stage_ratio_percent,stage_target_bps,padding_pending_seen,padding_generation_stopped,padding_stream_finished,padding_stream_reset,reset_error_code,probe_end_status,stage_active_duration_ms"
        )?;
        Ok(Self { writer })
    }

    pub fn row(&mut self, request: &ProbeRequest, end: &ProbeEnd) -> ProbeResult<()> {
        let ratio_ppm = end
            .accepted_padding_bytes
            .saturating_mul(1_000_000)
            .checked_div(end.attempted_padding_bytes)
            .unwrap_or(1_000_000);
        let padding_generation_stopped = true;
        let sender_total_accepted_payload_bytes = end
            .media_accepted_payload_bytes
            .saturating_add(end.accepted_padding_bytes);
        let sender_app_bitrate_bps = bitrate_bps(
            sender_total_accepted_payload_bytes,
            end.stage_active_duration_ms,
        );
        writeln!(
            self.writer,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{:?},{}",
            now_ms(),
            request.request_id,
            request.stage_target_bps,
            request.stage_target_bps,
            0,
            end.attempted_padding_bytes,
            end.accepted_padding_bytes,
            ratio_ppm,
            0,
            end.media_accepted_payload_bytes,
            sender_app_bitrate_bps,
            end.stage_active_duration_ms,
            request.stage_index,
            request.stage_ratio_percent,
            request.stage_target_bps,
            end.padding_pending_seen,
            padding_generation_stopped,
            end.status == ProbeEndStatus::Completed,
            end.status == ProbeEndStatus::AbortedBackpressure,
            if end.status == ProbeEndStatus::AbortedBackpressure {
                PROBE_PADDING_ABORTED_BACKPRESSURE
            } else {
                0
            },
            end.status,
            end.stage_active_duration_ms,
        )?;
        self.writer.flush()?;
        Ok(())
    }
}

pub struct SubscriberProbeCsv {
    writer: BufWriter<File>,
}

impl SubscriberProbeCsv {
    pub fn new(path: PathBuf) -> ProbeResult<Self> {
        let mut writer = BufWriter::new(File::create(path)?);
        writeln!(
            writer,
            "request_id,sample_index,timestamp_ms,probe_elapsed_ms,response_elapsed_ms,received_media_bytes,received_padding_stream_bytes,received_padding_datagram_bytes,total_received_bytes,receiver_goodput_bps,sender_app_written_bytes,sender_media_written_bytes,sender_padding_written_bytes,raw_sender_app_bitrate_bps,target_bitrate_bps,paced_target_bps,cwnd_bytes,logical_probe_index,stage_index,stage_ratio_percent,stage_target_bps,probe_start_received_time,first_post_start_padding_time,probe_end_received_time,measurement_duration_ms,arming_delay_ms,pre_start_padding_bytes,late_padding_bytes,media_goodput_bps,padding_goodput_bps,pass_percent,stage_passed,highest_validated_target_bps,result_valid,partial_measurement,stage_status,logical_probe_status,sender_attempted_padding_bytes,sender_accepted_padding_bytes,sender_media_accepted_payload_bytes,sender_total_accepted_payload_bytes,sender_app_bitrate_bps,padding_stream_finished,padding_stream_reset,reset_error_code"
        )?;
        Ok(Self { writer })
    }

    pub fn row(
        &mut self,
        logical_probe_index: u64,
        result: &StageResult,
        highest_validated_target_bps: u64,
        logical_status: &str,
    ) -> ProbeResult<()> {
        let total = result
            .received_media_bytes
            .saturating_add(result.received_padding_bytes);
        writeln!(
            self.writer,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{:?},{},{},{},{},{},{},{},{},{}",
            result.request_id,
            result.stage_index + 1,
            now_ms(),
            result.measurement_duration_ms,
            result.measurement_duration_ms,
            result.received_media_bytes,
            result.received_padding_bytes,
            0,
            total,
            result.receiver_goodput_bps,
            result.sender_total_accepted_payload_bytes,
            result.sender_media_accepted_payload_bytes,
            result.sender_accepted_padding_bytes,
            result.sender_app_bitrate_bps,
            result.stage_target_bps,
            result.stage_target_bps,
            0,
            logical_probe_index,
            result.stage_index,
            result.stage_ratio_percent,
            result.stage_target_bps,
            result.probe_start_received_timestamp_ms,
            result.first_post_start_padding_timestamp_ms.unwrap_or(0),
            result.probe_end_received_timestamp_ms,
            result.measurement_duration_ms,
            result.arming_delay_ms.unwrap_or(0),
            result.pre_start_padding_bytes,
            result.late_padding_bytes,
            result.media_goodput_bps,
            result.padding_goodput_bps,
            result.pass_percent,
            result.stage_passed,
            highest_validated_target_bps,
            result.result_valid,
            result.partial_measurement,
            result.status,
            logical_status,
            result.sender_attempted_padding_bytes,
            result.sender_accepted_padding_bytes,
            result.sender_media_accepted_payload_bytes,
            result.sender_total_accepted_payload_bytes,
            result.sender_app_bitrate_bps,
            result.padding_stream_finished,
            result.padding_stream_reset,
            result.reset_error_code.unwrap_or(0),
        )?;
        self.writer.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> ProbeRequest {
        ProbeRequest {
            request_id: 7,
            stage_index: 0,
            stage_ratio_percent: 50,
            stage_target_bps: 4_000_000,
            active_duration_ms: 3_000,
            epoch_ms: 500,
        }
    }

    #[test]
    fn wire_messages_round_trip() {
        let messages = [
            ProbeMessage::Request(request()),
            ProbeMessage::Start(ProbeStart {
                request_id: 7,
                stage_index: 0,
                stage_target_bps: 4_000_000,
            }),
            ProbeMessage::StartAck(ProbeStartAck {
                request_id: 7,
                stage_index: 0,
            }),
            ProbeMessage::End(ProbeEnd {
                request_id: 7,
                stage_index: 0,
                status: ProbeEndStatus::Completed,
                attempted_padding_bytes: 10,
                accepted_padding_bytes: 9,
                media_accepted_payload_bytes: 100,
                padding_pending_seen: false,
                stage_active_duration_ms: 3_000,
            }),
        ];

        for message in messages {
            let mut encoded = encode_probe_message(&message);
            let message_type = get_varint(&mut encoded).unwrap();
            let length = get_varint(&mut encoded).unwrap() as usize;
            let payload = encoded.split_to(length);
            assert_eq!(
                decode_probe_message(message_type, payload).unwrap(),
                message
            );
        }
    }

    #[test]
    fn stage_targets_are_exact() {
        assert_eq!(stage_target_bps(8_000_000, 50), 4_000_000);
        assert_eq!(stage_target_bps(8_000_000, 75), 6_000_000);
        assert_eq!(stage_target_bps(8_000_000, 100), 8_000_000);
    }

    #[test]
    fn request_validation_rejects_bad_stage_parameters() {
        let mut candidate = request();
        assert!(validate_request(&candidate, None, 0, 4_000_000).is_ok());

        candidate.stage_ratio_percent = 75;
        assert!(validate_request(&candidate, None, 0, 4_000_000).is_err());

        candidate = request();
        candidate.active_duration_ms = 0;
        assert!(validate_request(&candidate, None, 0, 4_000_000).is_err());

        candidate = request();
        candidate.epoch_ms = 0;
        assert!(validate_request(&candidate, None, 0, 4_000_000).is_err());

        candidate = request();
        assert!(validate_request(&candidate, None, 0, 3_999_999).is_err());
    }

    #[test]
    fn request_validation_enforces_logical_probe_order_and_identity() {
        let first = request();
        assert!(validate_request(&first, None, 0, 8_000_000).is_ok());

        let second = ProbeRequest {
            stage_index: 1,
            stage_ratio_percent: 75,
            stage_target_bps: 6_000_000,
            ..first.clone()
        };
        assert!(validate_request(&second, Some(first.request_id), 1, 8_000_000).is_ok());

        let wrong_id = ProbeRequest {
            request_id: first.request_id + 1,
            ..second.clone()
        };
        assert!(validate_request(&wrong_id, Some(first.request_id), 1, 8_000_000).is_err());
        assert!(validate_request(&second, Some(first.request_id), 2, 8_000_000).is_err());
    }

    #[test]
    fn sender_counters_are_isolated_per_connection() {
        let first = SenderProbeContext::default();
        let second = SenderProbeContext::default();
        first.record_media_payload(123);
        second.record_media_payload(7);
        assert_eq!(first.media_accepted_payload_bytes(), 123);
        assert_eq!(second.media_accepted_payload_bytes(), 7);
    }

    #[test]
    fn cumulative_budget_has_no_tick_rounding_drift() {
        let target = 1_000_001;
        let elapsed = Duration::from_secs(60);
        assert_eq!(
            allowed_total_bytes(target, elapsed),
            (target as u128 * 60 / 8) as u64
        );
    }

    #[tokio::test]
    async fn direct_poll_distinguishes_ready_from_pending_without_waiting() {
        assert_eq!(poll_future_once(async { 7u8 }).await, Poll::Ready(7u8));
        assert_eq!(
            poll_future_once(futures::future::pending::<u8>()).await,
            Poll::Pending
        );
    }

    #[test]
    fn media_above_target_needs_no_padding_after_trigger() {
        assert_eq!(
            padding_due_bytes(4_000_000, Duration::from_secs(1), 500_000, 1),
            0
        );
    }

    #[test]
    fn target_zero_needs_no_padding_after_trigger() {
        assert_eq!(padding_due_bytes(0, Duration::from_secs(1), 100, 1), 0);
    }

    #[test]
    fn gate_is_exact_at_ninety_percent() {
        assert!(!goodput_passes(89, 100, 90));
        assert!(goodput_passes(90, 100, 90));
        assert!(goodput_passes(91, 100, 90));
    }

    #[test]
    fn start_only_does_not_start_measurement_and_pre_start_is_excluded() {
        let context = ReceiverProbeContext::default();
        let now = Instant::now();
        context.begin_stage(7, 0, 50, 4_000_000, 90).unwrap();
        context.on_padding_stream_open().unwrap();
        context.on_padding_payload(5, now).unwrap();
        context
            .on_probe_start(
                &ProbeStart {
                    request_id: 7,
                    stage_index: 0,
                    stage_target_bps: 4_000_000,
                },
                now + Duration::from_millis(1),
            )
            .unwrap();
        context.on_media_payload(10, now + Duration::from_millis(2));
        let result = context
            .on_probe_end(
                &ProbeEnd {
                    request_id: 7,
                    stage_index: 0,
                    status: ProbeEndStatus::Completed,
                    attempted_padding_bytes: 5,
                    accepted_padding_bytes: 5,
                    media_accepted_payload_bytes: 0,
                    padding_pending_seen: false,
                    stage_active_duration_ms: 10,
                },
                now + Duration::from_millis(10),
            )
            .unwrap();
        assert_eq!(result.pre_start_padding_bytes, 5);
        assert_eq!(result.received_media_bytes, 0);
        assert_eq!(result.status, StageStatus::NoPostStartPadding);
        assert!(!result.result_valid);
    }

    #[test]
    fn first_post_start_padding_starts_and_is_counted() {
        let context = ReceiverProbeContext::default();
        let now = Instant::now();
        context.begin_stage(7, 0, 50, 1_000, 90).unwrap();
        context.on_padding_stream_open().unwrap();
        context
            .on_probe_start(
                &ProbeStart {
                    request_id: 7,
                    stage_index: 0,
                    stage_target_bps: 1_000,
                },
                now,
            )
            .unwrap();
        context.on_media_payload(10, now + Duration::from_millis(1));
        context
            .on_padding_payload(1, now + Duration::from_millis(2))
            .unwrap();
        context.on_media_payload(10, now + Duration::from_millis(3));
        context
            .on_padding_payload(2, now + Duration::from_millis(4))
            .unwrap();
        let result = context
            .on_probe_end(
                &ProbeEnd {
                    request_id: 7,
                    stage_index: 0,
                    status: ProbeEndStatus::Completed,
                    attempted_padding_bytes: 3,
                    accepted_padding_bytes: 3,
                    media_accepted_payload_bytes: 10,
                    padding_pending_seen: false,
                    stage_active_duration_ms: 10,
                },
                now + Duration::from_millis(12),
            )
            .unwrap();
        assert_eq!(result.received_padding_bytes, 3);
        assert_eq!(result.received_media_bytes, 10);
        assert_eq!(result.arming_delay_ms, Some(2));
        assert_eq!(result.sender_attempted_padding_bytes, 3);
        assert_eq!(result.sender_accepted_padding_bytes, 3);
        assert_eq!(result.sender_media_accepted_payload_bytes, 10);
        assert_eq!(result.sender_total_accepted_payload_bytes, 13);
        assert!(result.result_valid);

        context
            .on_padding_payload(7, now + Duration::from_millis(13))
            .unwrap();
        assert_eq!(context.late_padding_bytes(), 7);
    }

    #[test]
    fn abort_is_partial_and_never_passes() {
        let context = ReceiverProbeContext::default();
        let now = Instant::now();
        context.begin_stage(7, 0, 50, 1, 90).unwrap();
        context.on_padding_stream_open().unwrap();
        context
            .on_probe_start(
                &ProbeStart {
                    request_id: 7,
                    stage_index: 0,
                    stage_target_bps: 1,
                },
                now,
            )
            .unwrap();
        context
            .on_padding_payload(1, now + Duration::from_millis(1))
            .unwrap();
        let result = context
            .on_probe_end(
                &ProbeEnd {
                    request_id: 7,
                    stage_index: 0,
                    status: ProbeEndStatus::AbortedBackpressure,
                    attempted_padding_bytes: 1,
                    accepted_padding_bytes: 0,
                    media_accepted_payload_bytes: 0,
                    padding_pending_seen: true,
                    stage_active_duration_ms: 2,
                },
                now + Duration::from_millis(2),
            )
            .unwrap();
        assert!(result.partial_measurement);
        assert!(!result.result_valid);
        assert!(!result.stage_passed);
    }

    #[test]
    fn padding_reset_preserves_the_observed_error_code() {
        let context = ReceiverProbeContext::default();
        let now = Instant::now();
        context.begin_stage(7, 0, 50, 1, 90).unwrap();
        context.on_padding_stream_open().unwrap();
        context
            .on_probe_start(
                &ProbeStart {
                    request_id: 7,
                    stage_index: 0,
                    stage_target_bps: 1,
                },
                now,
            )
            .unwrap();
        context
            .on_padding_payload(1, now + Duration::from_millis(1))
            .unwrap();
        let mut result = context
            .on_probe_end(
                &ProbeEnd {
                    request_id: 7,
                    stage_index: 0,
                    status: ProbeEndStatus::Completed,
                    attempted_padding_bytes: 1,
                    accepted_padding_bytes: 1,
                    media_accepted_payload_bytes: 0,
                    padding_pending_seen: false,
                    stage_active_duration_ms: 2,
                },
                now + Duration::from_millis(2),
            )
            .unwrap();
        context.on_padding_stream_reset(Some(7)).unwrap();
        context.refresh_after_padding_cleanup(&mut result).unwrap();

        assert!(result.padding_stream_reset);
        assert_eq!(result.reset_error_code, Some(7));
    }
}
