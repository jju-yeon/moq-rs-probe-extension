// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{
    fs::File,
    io::{self, BufWriter, Write},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, OnceLock,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

/// Experimental private values.
/// Replace these after IANA / draft values are assigned.
pub const PROBE_STREAM_TYPE: u64 = 0xff00_0001;
pub const PROBE_PADDING_STREAM_TYPE: u64 = 0xff00_0002;
pub const PROBE_PADDING_DATAGRAM_TYPE: u64 = 0xff00_0003;

pub const MSG_PROBE_REQUEST: u64 = 0x01;
pub const MSG_PROBE_RESPONSE: u64 = 0x02;

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

    #[error("invalid padding_mode: {0}")]
    InvalidPaddingMode(u64),

    #[error("invalid probe mode: {0}")]
    InvalidProbeMode(u64),

    #[error("unexpected probe message type: {0}")]
    UnexpectedMessageType(u64),

    #[error("zero-length write")]
    ZeroWrite,
}

pub type ProbeResult<T> = Result<T, ProbeError>;

#[derive(Debug, Clone, Copy)]
pub enum PaddingMode {
    Stream = 0,
    Datagram = 1,
}

impl TryFrom<u64> for PaddingMode {
    type Error = ProbeError;

    fn try_from(v: u64) -> ProbeResult<Self> {
        match v {
            0 => Ok(Self::Stream),
            1 => Ok(Self::Datagram),
            _ => Err(ProbeError::InvalidPaddingMode(v)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeMode {
    Baseline = 0,
    Corrected = 1,
}

impl TryFrom<u64> for ProbeMode {
    type Error = ProbeError;

    fn try_from(v: u64) -> ProbeResult<Self> {
        match v {
            0 => Ok(Self::Baseline),
            1 => Ok(Self::Corrected),
            _ => Err(ProbeError::InvalidProbeMode(v)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorrectionReasonCode {
    None = 0,
    GrowthLimited = 1,
    LowWriteAcceptance = 2,
    WriteLag = 3,
    ReceiverFeedbackCap = 4,
    Mixed = 5,
}

impl TryFrom<u64> for CorrectionReasonCode {
    type Error = ProbeError;

    fn try_from(v: u64) -> ProbeResult<Self> {
        match v {
            0 => Ok(Self::None),
            1 => Ok(Self::GrowthLimited),
            2 => Ok(Self::LowWriteAcceptance),
            3 => Ok(Self::WriteLag),
            4 => Ok(Self::ReceiverFeedbackCap),
            5 => Ok(Self::Mixed),
            _ => Ok(Self::Mixed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ProbeRequest {
    pub request_id: u64,
    pub target_bitrate_bps: u64,
    pub probe_duration_ms: u64,
    pub epoch_ms: u64,
    pub padding_mode: PaddingMode,
    pub mode: ProbeMode,
    pub receiver_assisted: bool,
    pub previous_receiver_goodput_bps: u64,
}

#[derive(Debug, Clone)]
pub struct ProbeResponse {
    pub request_id: u64,
    pub target_bitrate_bps: u64,
    pub raw_sender_app_bitrate_bps: u64,
    pub corrected_measured_bitrate_bps: u64,
    pub elapsed_ms: u64,
    pub sender_app_written_bytes: u64,
    pub padding_written_bytes: u64,
    pub media_written_bytes: u64,
    pub correction_factor_ppm: u64,
    pub correction_reason_code: CorrectionReasonCode,
}

#[derive(Default)]
pub struct ProbeCounters {
    pub media_write_bytes: AtomicU64,
    pub media_recv_bytes: AtomicU64,
    pub padding_stream_recv_bytes: AtomicU64,
    pub padding_datagram_recv_bytes: AtomicU64,
}

static GLOBAL_COUNTERS: OnceLock<Arc<ProbeCounters>> = OnceLock::new();

pub fn install_global_counters(counters: Arc<ProbeCounters>) {
    let _ = GLOBAL_COUNTERS.set(counters);
}

pub fn counters() -> Option<&'static Arc<ProbeCounters>> {
    GLOBAL_COUNTERS.get()
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub fn bitrate_bps(bytes: u64, elapsed_ms: u64) -> u64 {
    if elapsed_ms == 0 {
        0
    } else {
        bytes.saturating_mul(8).saturating_mul(1000) / elapsed_ms
    }
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

    let first = buf[0];
    let tag = first >> 6;
    let needed = match tag {
        0 => 1,
        1 => 2,
        2 => 4,
        3 => 8,
        _ => unreachable!(),
    };

    if buf.remaining() < needed {
        return Err(ProbeError::InvalidVarint);
    }

    match tag {
        0 => Ok(buf.get_u8() as u64 & 0x3f),
        1 => Ok(buf.get_u16() as u64 & 0x3fff),
        2 => Ok(buf.get_u32() as u64 & 0x3fff_ffff),
        3 => Ok(buf.get_u64() & 0x3fff_ffff_ffff_ffff),
        _ => unreachable!(),
    }
}

async fn read_exact_web(recv: &mut web_transport::RecvStream, len: usize) -> ProbeResult<BytesMut> {
    let mut out = BytesMut::with_capacity(len);
    while out.len() < len {
        let remaining = len - out.len();
        match recv.read(remaining).await? {
            Some(chunk) => out.extend_from_slice(&chunk),
            None => return Err(ProbeError::EndOfStream),
        }
    }
    Ok(out)
}

/// Read a QUIC varint from a web_transport RecvStream and return both the value
/// and the exact encoded bytes consumed. The consumed bytes can be put back into
/// the MoQT Reader buffer for non-probe streams.
pub async fn read_varint_from_recv_stream(
    recv: &mut web_transport::RecvStream,
) -> ProbeResult<(u64, Bytes)> {
    let first = match recv.read(1).await? {
        Some(chunk) if !chunk.is_empty() => chunk[0],
        _ => return Err(ProbeError::EndOfStream),
    };

    let tag = first >> 6;
    let needed = match tag {
        0 => 1,
        1 => 2,
        2 => 4,
        3 => 8,
        _ => unreachable!(),
    };

    let mut raw = BytesMut::with_capacity(needed);
    raw.put_u8(first);

    while raw.len() < needed {
        let remaining = needed - raw.len();
        match recv.read(remaining).await? {
            Some(chunk) => raw.extend_from_slice(&chunk),
            None => return Err(ProbeError::EndOfStream),
        }
    }

    let mut copy = raw.clone().freeze();
    let value = get_varint(&mut copy)?;
    Ok((value, raw.freeze()))
}

pub async fn read_varint_web(recv: &mut web_transport::RecvStream) -> ProbeResult<u64> {
    read_varint_from_recv_stream(recv).await.map(|(value, _)| value)
}

pub async fn write_all_web(send: &mut web_transport::SendStream, bytes: &[u8]) -> ProbeResult<()> {
    let mut written = 0usize;
    while written < bytes.len() {
        let n = send.write(&bytes[written..]).await?;
        if n == 0 {
            return Err(ProbeError::ZeroWrite);
        }
        written += n;
    }
    Ok(())
}

pub async fn write_varint_web(send: &mut web_transport::SendStream, value: u64) -> ProbeResult<()> {
    let mut buf = BytesMut::new();
    put_varint(&mut buf, value);
    write_all_web(send, &buf).await
}

pub fn encode_probe_request(req: &ProbeRequest) -> Bytes {
    let mut payload = BytesMut::new();
    put_varint(&mut payload, req.request_id);
    put_varint(&mut payload, req.target_bitrate_bps);
    put_varint(&mut payload, req.probe_duration_ms);
    put_varint(&mut payload, req.epoch_ms);
    put_varint(&mut payload, req.padding_mode as u64);
    put_varint(&mut payload, req.mode as u64);
    put_varint(&mut payload, if req.receiver_assisted { 1 } else { 0 });
    put_varint(&mut payload, req.previous_receiver_goodput_bps);

    let mut out = BytesMut::new();
    put_varint(&mut out, MSG_PROBE_REQUEST);
    put_varint(&mut out, payload.len() as u64);
    out.extend_from_slice(&payload);
    out.freeze()
}

pub fn encode_probe_response(res: &ProbeResponse) -> Bytes {
    let mut payload = BytesMut::new();
    put_varint(&mut payload, res.request_id);
    put_varint(&mut payload, res.target_bitrate_bps);
    put_varint(&mut payload, res.raw_sender_app_bitrate_bps);
    put_varint(&mut payload, res.corrected_measured_bitrate_bps);
    put_varint(&mut payload, res.elapsed_ms);
    put_varint(&mut payload, res.sender_app_written_bytes);
    put_varint(&mut payload, res.padding_written_bytes);
    put_varint(&mut payload, res.media_written_bytes);
    put_varint(&mut payload, res.correction_factor_ppm);
    put_varint(&mut payload, res.correction_reason_code as u64);

    let mut out = BytesMut::new();
    put_varint(&mut out, MSG_PROBE_RESPONSE);
    put_varint(&mut out, payload.len() as u64);
    out.extend_from_slice(&payload);
    out.freeze()
}

pub async fn read_probe_request_web(
    recv: &mut web_transport::RecvStream,
) -> ProbeResult<ProbeRequest> {
    let msg_type = read_varint_web(recv).await?;
    if msg_type != MSG_PROBE_REQUEST {
        return Err(ProbeError::UnexpectedMessageType(msg_type));
    }

    let len = read_varint_web(recv).await? as usize;
    let raw = read_exact_web(recv, len).await?;
    let mut b = raw.freeze();

    let request_id = get_varint(&mut b)?;
    let target_bitrate_bps = get_varint(&mut b)?;
    let probe_duration_ms = get_varint(&mut b)?;
    let epoch_ms = get_varint(&mut b)?;
    let padding_mode = PaddingMode::try_from(get_varint(&mut b)?)?;

    let mode = if b.has_remaining() {
        ProbeMode::try_from(get_varint(&mut b)?)?
    } else {
        ProbeMode::Baseline
    };

    let receiver_assisted = if b.has_remaining() {
        get_varint(&mut b)? != 0
    } else {
        false
    };

    let previous_receiver_goodput_bps = if b.has_remaining() {
        get_varint(&mut b)?
    } else {
        0
    };

    Ok(ProbeRequest {
        request_id,
        target_bitrate_bps,
        probe_duration_ms,
        epoch_ms,
        padding_mode,
        mode,
        receiver_assisted,
        previous_receiver_goodput_bps,
    })
}

pub async fn read_probe_response_web(
    recv: &mut web_transport::RecvStream,
) -> ProbeResult<ProbeResponse> {
    let msg_type = read_varint_web(recv).await?;
    if msg_type != MSG_PROBE_RESPONSE {
        return Err(ProbeError::UnexpectedMessageType(msg_type));
    }

    let len = read_varint_web(recv).await? as usize;
    let raw = read_exact_web(recv, len).await?;
    let mut b = raw.freeze();

    let request_id = get_varint(&mut b)?;
    let target_bitrate_bps = get_varint(&mut b)?;
    let raw_sender_app_bitrate_bps = get_varint(&mut b)?;

    // Backward compatibility with the old Experiment 1 response format:
    // if only 7 fields were present, this value is actually elapsed_ms.
    let maybe_corrected_or_elapsed = get_varint(&mut b)?;

    if b.remaining() <= 3 * 8 {
        let elapsed_ms = maybe_corrected_or_elapsed;
        let sender_app_written_bytes = get_varint(&mut b)?;
        let padding_written_bytes = get_varint(&mut b)?;
        let media_written_bytes = get_varint(&mut b)?;
        return Ok(ProbeResponse {
            request_id,
            target_bitrate_bps,
            raw_sender_app_bitrate_bps,
            corrected_measured_bitrate_bps: raw_sender_app_bitrate_bps,
            elapsed_ms,
            sender_app_written_bytes,
            padding_written_bytes,
            media_written_bytes,
            correction_factor_ppm: 1_000_000,
            correction_reason_code: CorrectionReasonCode::None,
        });
    }

    Ok(ProbeResponse {
        request_id,
        target_bitrate_bps,
        raw_sender_app_bitrate_bps,
        corrected_measured_bitrate_bps: maybe_corrected_or_elapsed,
        elapsed_ms: get_varint(&mut b)?,
        sender_app_written_bytes: get_varint(&mut b)?,
        padding_written_bytes: get_varint(&mut b)?,
        media_written_bytes: get_varint(&mut b)?,
        correction_factor_ppm: if b.has_remaining() { get_varint(&mut b)? } else { 1_000_000 },
        correction_reason_code: if b.has_remaining() {
            CorrectionReasonCode::try_from(get_varint(&mut b)?)?
        } else {
            CorrectionReasonCode::None
        },
    })
}

pub struct RelayProbeCsv {
    w: BufWriter<File>,
}

impl RelayProbeCsv {
    pub fn new(path: PathBuf) -> ProbeResult<Self> {
        let mut w = BufWriter::new(File::create(path)?);
        writeln!(
            w,
            "timestamp_ms,request_id,probe_mode,target_bitrate_bps,attempted_padding_bytes,write_accepted_padding_bytes,accept_ratio_ppm,write_block_time_ms,media_write_bytes,raw_sender_app_bitrate_bps,corrected_measured_bitrate_bps,correction_factor_ppm,correction_reason,epoch_elapsed_ms"
        )?;
        Ok(Self { w })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn row(
        &mut self,
        request_id: u64,
        probe_mode: ProbeMode,
        target_bitrate_bps: u64,
        attempted_padding_bytes: u64,
        write_accepted_padding_bytes: u64,
        write_block_time_ms: u64,
        media_write_bytes: u64,
        raw_sender_app_bitrate_bps: u64,
        corrected_measured_bitrate_bps: u64,
        correction_factor_ppm: u64,
        correction_reason: CorrectionReasonCode,
        epoch_elapsed_ms: u64,
    ) -> ProbeResult<()> {
        let accept_ratio_ppm = if attempted_padding_bytes == 0 {
            1_000_000
        } else {
            write_accepted_padding_bytes.saturating_mul(1_000_000) / attempted_padding_bytes
        };

        writeln!(
            self.w,
            "{},{},{:?},{},{},{},{},{},{},{},{},{},{:?},{}",
            now_ms(),
            request_id,
            probe_mode,
            target_bitrate_bps,
            attempted_padding_bytes,
            write_accepted_padding_bytes,
            accept_ratio_ppm,
            write_block_time_ms,
            media_write_bytes,
            raw_sender_app_bitrate_bps,
            corrected_measured_bitrate_bps,
            correction_factor_ppm,
            correction_reason,
            epoch_elapsed_ms
        )?;
        self.w.flush()?;
        Ok(())
    }
}

pub struct SubscriberProbeCsv {
    w: BufWriter<File>,
}

impl SubscriberProbeCsv {
    pub fn new(path: PathBuf) -> ProbeResult<Self> {
        let mut w = BufWriter::new(File::create(path)?);
        writeln!(
            w,
            "request_id,timestamp_ms,probe_mode,received_media_bytes,received_padding_stream_bytes,received_padding_datagram_bytes,total_received_bytes,receiver_goodput_bps,raw_sender_app_bitrate_bps,corrected_measured_bitrate_bps,target_bitrate_bps,correction_factor_ppm,correction_reason"
        )?;
        Ok(Self { w })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn row(
        &mut self,
        request_id: u64,
        probe_mode: ProbeMode,
        received_media_bytes: u64,
        received_padding_stream_bytes: u64,
        received_padding_datagram_bytes: u64,
        receiver_goodput_bps: u64,
        raw_sender_app_bitrate_bps: u64,
        corrected_measured_bitrate_bps: u64,
        target_bitrate_bps: u64,
        correction_factor_ppm: u64,
        correction_reason: CorrectionReasonCode,
    ) -> ProbeResult<()> {
        let total = received_media_bytes
            .saturating_add(received_padding_stream_bytes)
            .saturating_add(received_padding_datagram_bytes);

        writeln!(
            self.w,
            "{},{},{:?},{},{},{},{},{},{},{},{},{},{:?}",
            request_id,
            now_ms(),
            probe_mode,
            received_media_bytes,
            received_padding_stream_bytes,
            received_padding_datagram_bytes,
            total,
            receiver_goodput_bps,
            raw_sender_app_bitrate_bps,
            corrected_measured_bitrate_bps,
            target_bitrate_bps,
            correction_factor_ppm,
            correction_reason,
        )?;
        self.w.flush()?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct CorrectionConfig {
    pub growth_limit_ppm: u64,
    pub min_accept_ratio_ppm: u64,
    pub write_lag_threshold_ppm: u64,
    pub receiver_feedback_cap_ppm: u64,
}

impl Default for CorrectionConfig {
    fn default() -> Self {
        Self {
            growth_limit_ppm: 1_250_000,
            min_accept_ratio_ppm: 950_000,
            write_lag_threshold_ppm: 1_100_000,
            receiver_feedback_cap_ppm: 1_200_000,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct CorrectionState {
    pub previous_stable_bitrate_bps: u64,
}

#[derive(Debug, Clone)]
pub struct CorrectionInput {
    pub raw_sender_app_bitrate_bps: u64,
    pub attempted_padding_bytes: u64,
    pub write_accepted_padding_bytes: u64,
    pub epoch_elapsed_ms: u64,
    pub epoch_target_ms: u64,
    pub previous_receiver_goodput_bps: u64,
    pub receiver_assisted: bool,
}

#[derive(Debug, Clone)]
pub struct CorrectionOutput {
    pub corrected_bitrate_bps: u64,
    pub correction_factor_ppm: u64,
    pub reason: CorrectionReasonCode,
    pub stable: bool,
}

fn merge_reason(a: CorrectionReasonCode, b: CorrectionReasonCode) -> CorrectionReasonCode {
    if a == CorrectionReasonCode::None {
        b
    } else if a == b {
        a
    } else {
        CorrectionReasonCode::Mixed
    }
}

/// Heuristic sender-side correction. This intentionally does not use cwnd,
/// bytes_in_flight, ACKed bytes, RTT, loss, congestion_events, udp_tx.bytes,
/// Quinn Connection::stats(), or recovery-layer instrumentation.
pub fn correct_sender_side_bitrate(
    state: &CorrectionState,
    cfg: &CorrectionConfig,
    input: &CorrectionInput,
) -> CorrectionOutput {
    let raw = input.raw_sender_app_bitrate_bps;
    let mut corrected = raw;
    let mut reason = CorrectionReasonCode::None;

    if state.previous_stable_bitrate_bps > 0 {
        let growth_cap = state
            .previous_stable_bitrate_bps
            .saturating_mul(cfg.growth_limit_ppm)
            / 1_000_000;
        if corrected > growth_cap {
            corrected = growth_cap;
            reason = CorrectionReasonCode::GrowthLimited;
        }
    }

    let accept_ratio_ppm = if input.attempted_padding_bytes == 0 {
        1_000_000
    } else {
        input
            .write_accepted_padding_bytes
            .saturating_mul(1_000_000)
            / input.attempted_padding_bytes
    };
    if accept_ratio_ppm < cfg.min_accept_ratio_ppm {
        let accept_capped = raw.saturating_mul(accept_ratio_ppm) / 1_000_000;
        if accept_capped < corrected {
            corrected = accept_capped;
            reason = merge_reason(reason, CorrectionReasonCode::LowWriteAcceptance);
        }
    }

    let lag_ratio_ppm = if input.epoch_target_ms == 0 {
        1_000_000
    } else {
        input.epoch_elapsed_ms.saturating_mul(1_000_000) / input.epoch_target_ms
    };
    if lag_ratio_ppm > cfg.write_lag_threshold_ppm {
        let lag_capped = corrected.saturating_mul(input.epoch_target_ms) / input.epoch_elapsed_ms.max(1);
        if lag_capped < corrected {
            corrected = lag_capped;
            reason = merge_reason(reason, CorrectionReasonCode::WriteLag);
        }
    }

    if input.receiver_assisted && input.previous_receiver_goodput_bps > 0 {
        let receiver_cap = input
            .previous_receiver_goodput_bps
            .saturating_mul(cfg.receiver_feedback_cap_ppm)
            / 1_000_000;
        if corrected > receiver_cap {
            corrected = receiver_cap;
            reason = merge_reason(reason, CorrectionReasonCode::ReceiverFeedbackCap);
        }
    }

    let stable = reason == CorrectionReasonCode::None
        && accept_ratio_ppm >= cfg.min_accept_ratio_ppm
        && lag_ratio_ppm <= cfg.write_lag_threshold_ppm;

    CorrectionOutput {
        corrected_bitrate_bps: corrected,
        correction_factor_ppm: if raw == 0 { 0 } else { corrected.saturating_mul(1_000_000) / raw },
        reason,
        stable,
    }
}

#[derive(Debug, Clone)]
pub struct ProbeConfig {
    pub enabled: bool,
    pub log: Option<PathBuf>,
    pub max_target_bitrate_bps: u64,
    pub correction: CorrectionConfig,
}

impl Default for ProbeConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            log: None,
            max_target_bitrate_bps: 100_000_000,
            correction: CorrectionConfig::default(),
        }
    }
}

pub async fn run_relay_probe_acceptor(
    webtransport: web_transport::Session,
    config: ProbeConfig,
    counters: Arc<ProbeCounters>,
) -> ProbeResult<()> {
    if !config.enabled {
        return Ok(());
    }

    let mut csv = match config.log.clone() {
        Some(path) => Some(RelayProbeCsv::new(path)?),
        None => None,
    };

    let mut correction_state = CorrectionState::default();

    loop {
        let (mut send, mut recv) = webtransport.accept_bi().await?;
        let stream_type = read_varint_web(&mut recv).await?;

        if stream_type != PROBE_STREAM_TYPE {
            tracing::warn!(stream_type, "non-PROBE bidirectional stream ignored by probe acceptor");
            continue;
        }

        let req = read_probe_request_web(&mut recv).await?;
        let target = req.target_bitrate_bps.min(config.max_target_bitrate_bps);

        let started = Instant::now();
        let media_start = counters.media_write_bytes.load(Ordering::Relaxed);
        let mut total_padding_written = 0u64;
        let mut last_corrected = 0u64;
        let mut last_factor = 1_000_000u64;
        let mut last_reason = CorrectionReasonCode::None;

        while started.elapsed() < Duration::from_millis(req.probe_duration_ms) {
            let epoch_start = Instant::now();

            let media_epoch_start = counters.media_write_bytes.load(Ordering::Relaxed);

            let epoch_budget_bytes = target
                .saturating_mul(req.epoch_ms)
                / 8
                / 1000;

            let mut attempted_padding_bytes = 0u64;
            let mut accepted_padding_bytes = 0u64;
            let mut write_block_time_ms = 0u64;

            let padding_buf = vec![0u8; 1200];

            match req.padding_mode {
                PaddingMode::Stream => {
                    let mut uni = webtransport.open_uni().await?;
                    write_varint_web(&mut uni, PROBE_PADDING_STREAM_TYPE).await?;

                    loop {
                        let epoch_elapsed_ms =
                            epoch_start.elapsed().as_millis().min(req.epoch_ms as u128) as u64;

                        if epoch_elapsed_ms >= req.epoch_ms {
                            break;
                        }

                        let allowed_total_bytes = target
                            .saturating_mul(epoch_elapsed_ms)
                            / 8
                            / 1000;

                        let media_epoch_bytes = counters
                            .media_write_bytes
                            .load(Ordering::Relaxed)
                            .saturating_sub(media_epoch_start);

                        let current_total_bytes =
                            media_epoch_bytes.saturating_add(accepted_padding_bytes);

                        if allowed_total_bytes <= current_total_bytes {
                            tokio::time::sleep(Duration::from_millis(2)).await;
                            continue;
                        }

                        let gap = allowed_total_bytes.saturating_sub(current_total_bytes);
                        let n = gap.min(padding_buf.len() as u64) as usize;

                        attempted_padding_bytes += n as u64;

                        let t0 = Instant::now();

                        // Application-layer accepted padding bytes only.
                        // This is not ACKed-byte delivery rate.
                        write_all_web(&mut uni, &padding_buf[..n]).await?;

                        write_block_time_ms += t0.elapsed().as_millis() as u64;
                        accepted_padding_bytes += n as u64;
                    }

                    uni.finish()?;
                }

                PaddingMode::Datagram => {
                    loop {
                        let epoch_elapsed_ms =
                            epoch_start.elapsed().as_millis().min(req.epoch_ms as u128) as u64;

                        if epoch_elapsed_ms >= req.epoch_ms {
                            break;
                        }

                        let allowed_total_bytes = target
                            .saturating_mul(epoch_elapsed_ms)
                            / 8
                            / 1000;

                        let media_epoch_bytes = counters
                            .media_write_bytes
                            .load(Ordering::Relaxed)
                            .saturating_sub(media_epoch_start);

                        let current_total_bytes =
                            media_epoch_bytes.saturating_add(accepted_padding_bytes);

                        if allowed_total_bytes <= current_total_bytes {
                            tokio::time::sleep(Duration::from_millis(2)).await;
                            continue;
                        }

                        let gap = allowed_total_bytes.saturating_sub(current_total_bytes);
                        let n = gap.min(1100) as usize;

                        attempted_padding_bytes += n as u64;

                        let mut d = BytesMut::new();
                        put_varint(&mut d, PROBE_PADDING_DATAGRAM_TYPE);
                        d.extend_from_slice(&padding_buf[..n]);

                        let t0 = Instant::now();

                        match webtransport.send_datagram(d.freeze()).await {
                            Ok(()) => {
                                accepted_padding_bytes += n as u64;
                            }
                            Err(err) => {
                                tracing::debug!(?err, "probe padding datagram was not accepted");
                                break;
                            }
                        }

                        write_block_time_ms += t0.elapsed().as_millis() as u64;
                    }
                }
            }

            total_padding_written += accepted_padding_bytes;

            let epoch_elapsed_ms = epoch_start.elapsed().as_millis().max(1) as u64;

            let media_written = counters
                .media_write_bytes
                .load(Ordering::Relaxed)
                .saturating_sub(media_start);

            let sender_app_written = total_padding_written.saturating_add(media_written);

            let elapsed_ms = started.elapsed().as_millis().max(1) as u64;
            let raw = bitrate_bps(sender_app_written, elapsed_ms);

            let correction = match req.mode {
                ProbeMode::Baseline => CorrectionOutput {
                    corrected_bitrate_bps: raw,
                    correction_factor_ppm: 1_000_000,
                    reason: CorrectionReasonCode::None,
                    stable: true,
                },
                ProbeMode::Corrected => correct_sender_side_bitrate(
                    &correction_state,
                    &config.correction,
                    &CorrectionInput {
                        raw_sender_app_bitrate_bps: raw,
                        attempted_padding_bytes,
                        write_accepted_padding_bytes: accepted_padding_bytes,
                        epoch_elapsed_ms,
                        epoch_target_ms: req.epoch_ms,
                        previous_receiver_goodput_bps: req.previous_receiver_goodput_bps,
                        receiver_assisted: req.receiver_assisted,
                    },
                ),
            };

            if req.mode == ProbeMode::Corrected && correction.stable {
                correction_state.previous_stable_bitrate_bps = correction.corrected_bitrate_bps;
            }

            last_corrected = correction.corrected_bitrate_bps;
            last_factor = correction.correction_factor_ppm;
            last_reason = correction.reason;

            if let Some(csv) = &mut csv {
                csv.row(
                    req.request_id,
                    req.mode,
                    target,
                    attempted_padding_bytes,
                    accepted_padding_bytes,
                    write_block_time_ms,
                    media_written,
                    raw,
                    correction.corrected_bitrate_bps,
                    correction.correction_factor_ppm,
                    correction.reason,
                    epoch_elapsed_ms,
                )?;
            }

            let sleep_ms = req.epoch_ms.saturating_sub(epoch_elapsed_ms);
            if sleep_ms > 0 {
                tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
            }
        }

        let elapsed_ms = started.elapsed().as_millis().max(1) as u64;
        let media_written = counters
            .media_write_bytes
            .load(Ordering::Relaxed)
            .saturating_sub(media_start);
        let sender_written = total_padding_written.saturating_add(media_written);
        let raw = bitrate_bps(sender_written, elapsed_ms);
        let corrected = match req.mode {
            ProbeMode::Baseline => raw,
            ProbeMode::Corrected => if last_corrected == 0 { raw } else { last_corrected },
        };

        let res = ProbeResponse {
            request_id: req.request_id,
            target_bitrate_bps: target,
            raw_sender_app_bitrate_bps: raw,
            corrected_measured_bitrate_bps: corrected,
            elapsed_ms,
            sender_app_written_bytes: sender_written,
            padding_written_bytes: total_padding_written,
            media_written_bytes: media_written,
            correction_factor_ppm: last_factor,
            correction_reason_code: last_reason,
        };

        let raw_msg = encode_probe_response(&res);
        write_all_web(&mut send, &raw_msg).await?;
        send.finish()?;

        tracing::info!(
            request_id = res.request_id,
            mode = ?req.mode,
            target_bitrate_bps = res.target_bitrate_bps,
            raw_sender_app_bitrate_bps = res.raw_sender_app_bitrate_bps,
            corrected_measured_bitrate_bps = res.corrected_measured_bitrate_bps,
            correction_factor_ppm = res.correction_factor_ppm,
            correction_reason = ?res.correction_reason_code,
            "PROBE_RESPONSE: measured bitrate is application-layer sender-side estimate"
        );
    }
}
