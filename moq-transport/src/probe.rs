// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{
    fs::File,
    io::{self, BufWriter, Write},
    ops::Deref,
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
// MOQT padding types
pub const PROBE_PADDING_STREAM_TYPE: u64 = 0x132B3E28;
pub const PROBE_PADDING_DATAGRAM_TYPE: u64 = 0x132B3E29;

pub const MSG_PROBE_REQUEST: u64 = 0x01;
pub const MSG_PROBE_RESPONSE: u64 = 0x02;

// Quinn SendStream priority: higher value means higher priority.
pub const PROBE_PRIORITY: i32 = 255;
pub const MEDIA_PRIORITY: u8 = 128;
pub const PADDING_PRIORITY: i32 = 0;

// Padding is paced inside each probe epoch at this interval.
// The epoch remains the reporting interval; this tick is only for write pacing.
pub const PADDING_PACING_TICK_MS: u64 = 10;

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

#[derive(Debug, Clone)]
pub struct ProbeRequest {
    pub request_id: u64,
    pub target_bitrate_bps: u64,
    pub probe_duration_ms: u64,
    pub epoch_ms: u64,
    pub padding_mode: PaddingMode,
}

#[derive(Debug, Clone)]
pub struct ProbeResponse {
    pub request_id: u64,
    pub target_bitrate_bps: u64,

    // Effective paced target used by the relay.
    pub paced_target_bps: u64,

    // Relay-side QUIC cwnd snapshot in bytes.
    pub cwnd_bytes: u64,

    pub raw_sender_app_bitrate_bps: u64,
    pub elapsed_ms: u64,
    pub sender_app_written_bytes: u64,
    pub padding_written_bytes: u64,
    pub media_written_bytes: u64,
}

#[derive(Default)]
pub struct ProbeCounters {
    pub media_write_bytes: AtomicU64,
    pub media_recv_bytes: AtomicU64,
    pub padding_stream_recv_bytes: AtomicU64,
    pub padding_datagram_recv_bytes: AtomicU64,

    // Relay-side QUIC congestion window, in bytes.
    // Updated by the relay WebTransport/Quinn session owner.
    pub cwnd_bytes: AtomicU64,
}

static GLOBAL_COUNTERS: OnceLock<Arc<ProbeCounters>> = OnceLock::new();

pub fn install_global_counters(counters: Arc<ProbeCounters>) {
    let _ = GLOBAL_COUNTERS.set(counters);
}

/// Periodically samples the relay-side Quinn congestion window and stores it
/// in ProbeCounters so probe CSV rows can include cwnd_bytes.
///
/// Call this from the relay while you still have the native
/// web_transport::quinn::Session, before or after converting/cloning it into
/// the generic web_transport::Session used by MoQT.
pub fn spawn_cwnd_sampler(
    session: web_transport::quinn::Session,
    counters: Arc<ProbeCounters>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let qstats = session.deref().stats();
            counters
                .cwnd_bytes
                .store(qstats.path.cwnd, Ordering::Relaxed);

            if session.close_reason().is_some() {
                break;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
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

fn paced_probe_target_bps(target_bps: u64, elapsed_ms: u64, duration_ms: u64) -> u64 {
    if target_bps == 0 || duration_ms == 0 {
        return 0;
    }

    const START_PPM: u64 = 250_000; // 25%
    const FULL_PPM: u64 = 1_000_000; // 100%
    const RAMP_PORTION_PPM: u64 = 500_000; // first 50% of probe duration

    let ramp_ms = duration_ms
        .saturating_mul(RAMP_PORTION_PPM)
        / 1_000_000;

    if ramp_ms == 0 || elapsed_ms >= ramp_ms {
        return target_bps;
    }

    let ramp_ppm = START_PPM
        + (FULL_PPM - START_PPM)
            .saturating_mul(elapsed_ms)
            / ramp_ms;

    target_bps.saturating_mul(ramp_ppm) / 1_000_000
}

fn paced_probe_allowed_bytes(target_bps: u64, elapsed_ms: u64, duration_ms: u64) -> u64 {
    if target_bps == 0 || elapsed_ms == 0 || duration_ms == 0 {
        return 0;
    }

    const START_PPM: u128 = 250_000; // 25%
    const FULL_PPM: u128 = 1_000_000; // 100%
    const RAMP_PORTION_PPM: u64 = 500_000; // first 50% of probe duration

    let ramp_ms = duration_ms
        .saturating_mul(RAMP_PORTION_PPM)
        / 1_000_000;

    let target = target_bps as u128;
    let elapsed = elapsed_ms as u128;
    let ramp = ramp_ms as u128;
    let delta_ppm = FULL_PPM - START_PPM;

    // Integral of paced_probe_target_bps(t) from 0..elapsed_ms.
    // Unit before final conversion: ppm * ms.
    let ppm_ms = if ramp == 0 {
        FULL_PPM.saturating_mul(elapsed)
    } else if elapsed <= ramp {
        START_PPM
            .saturating_mul(elapsed)
            .saturating_add(delta_ppm.saturating_mul(elapsed).saturating_mul(elapsed) / (2 * ramp))
    } else {
        let ramp_ppm_ms = START_PPM
            .saturating_mul(ramp)
            .saturating_add(delta_ppm.saturating_mul(ramp) / 2);
        ramp_ppm_ms.saturating_add(FULL_PPM.saturating_mul(elapsed.saturating_sub(ramp)))
    };

    // bytes = target_bps * (ppm_ms / 1_000_000) * (1 ms / 1000 s) / 8
    let bytes = target
        .saturating_mul(ppm_ms)
        / 1_000_000
        / 8
        / 1000;

    bytes.min(u64::MAX as u128) as u64
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
    put_varint(&mut payload, res.paced_target_bps);
    put_varint(&mut payload, res.cwnd_bytes);
    put_varint(&mut payload, res.raw_sender_app_bitrate_bps);
    put_varint(&mut payload, res.elapsed_ms);
    put_varint(&mut payload, res.sender_app_written_bytes);
    put_varint(&mut payload, res.padding_written_bytes);
    put_varint(&mut payload, res.media_written_bytes);

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

    Ok(ProbeRequest {
        request_id,
        target_bitrate_bps,
        probe_duration_ms,
        epoch_ms,
        padding_mode,
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

    // New format fields.
    // If older responses are mixed in, default to target and 0.
    let paced_target_bps = if b.has_remaining() {
        get_varint(&mut b)?
    } else {
        target_bitrate_bps
    };

    let cwnd_bytes = if b.has_remaining() {
        get_varint(&mut b)?
    } else {
        0
    };

    let raw_sender_app_bitrate_bps = get_varint(&mut b)?;
    let elapsed_ms = get_varint(&mut b)?;
    let sender_app_written_bytes = get_varint(&mut b)?;
    let padding_written_bytes = get_varint(&mut b)?;
    let media_written_bytes = get_varint(&mut b)?;

    Ok(ProbeResponse {
        request_id,
        target_bitrate_bps,
        paced_target_bps,
        cwnd_bytes,
        raw_sender_app_bitrate_bps,
        elapsed_ms,
        sender_app_written_bytes,
        padding_written_bytes,
        media_written_bytes,
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
            "timestamp_ms,request_id,target_bitrate_bps,paced_target_bps,cwnd_bytes,attempted_padding_bytes,write_accepted_padding_bytes,accept_ratio_ppm,write_block_time_ms,media_write_bytes,raw_sender_app_bitrate_bps,epoch_elapsed_ms"
        )?;
        Ok(Self { w })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn row(
        &mut self,
        request_id: u64,
        target_bitrate_bps: u64,
        paced_target_bps: u64,
        cwnd_bytes: u64,
        attempted_padding_bytes: u64,
        write_accepted_padding_bytes: u64,
        write_block_time_ms: u64,
        media_write_bytes: u64,
        raw_sender_app_bitrate_bps: u64,
        epoch_elapsed_ms: u64,
    ) -> ProbeResult<()> {
        let accept_ratio_ppm = if attempted_padding_bytes == 0 {
            1_000_000
        } else {
            write_accepted_padding_bytes.saturating_mul(1_000_000) / attempted_padding_bytes
        };

        writeln!(
            self.w,
            "{},{},{},{},{},{},{},{},{},{},{},{}",
            now_ms(),
            request_id,
            target_bitrate_bps,
            paced_target_bps,
            cwnd_bytes,
            attempted_padding_bytes,
            write_accepted_padding_bytes,
            accept_ratio_ppm,
            write_block_time_ms,
            media_write_bytes,
            raw_sender_app_bitrate_bps,
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
            "request_id,sample_index,timestamp_ms,probe_elapsed_ms,response_elapsed_ms,received_media_bytes,received_padding_stream_bytes,received_padding_datagram_bytes,total_received_bytes,receiver_goodput_bps,sender_app_written_bytes,sender_media_written_bytes,sender_padding_written_bytes,raw_sender_app_bitrate_bps,target_bitrate_bps,paced_target_bps,cwnd_bytes"
        )?;

        Ok(Self { w })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn row(
        &mut self,
        request_id: u64,
        sample_index: u64,
        probe_elapsed_ms: u64,
        response_elapsed_ms: u64,
        received_media_bytes: u64,
        received_padding_stream_bytes: u64,
        received_padding_datagram_bytes: u64,
        receiver_goodput_bps: u64,
        sender_app_written_bytes: u64,
        sender_media_written_bytes: u64,
        sender_padding_written_bytes: u64,
        raw_sender_app_bitrate_bps: u64,
        target_bitrate_bps: u64,
        paced_target_bps: u64,
        cwnd_bytes: u64,
    ) -> ProbeResult<()> {
        let total = received_media_bytes
            .saturating_add(received_padding_stream_bytes)
            .saturating_add(received_padding_datagram_bytes);

        writeln!(
            self.w,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            request_id,
            sample_index,
            now_ms(),
            probe_elapsed_ms,
            response_elapsed_ms,
            received_media_bytes,
            received_padding_stream_bytes,
            received_padding_datagram_bytes,
            total,
            receiver_goodput_bps,
            sender_app_written_bytes,
            sender_media_written_bytes,
            sender_padding_written_bytes,
            raw_sender_app_bitrate_bps,
            target_bitrate_bps,
            paced_target_bps,
            cwnd_bytes,
        )?;

        self.w.flush()?;
        Ok(())
    }
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

    // Keep the last non-zero media input rate across PROBE_REQUEST streams.
    // Padding is disabled until at least one media epoch has been observed.
    let mut remembered_media_bps: Option<u64> = None;

    loop {
        let (mut send, mut recv) = webtransport.accept_bi().await?;
        let _ = send.set_priority(PROBE_PRIORITY);
        let stream_type = read_varint_web(&mut recv).await?;

        if stream_type != PROBE_STREAM_TYPE {
            tracing::warn!(stream_type, "non-PROBE bidirectional stream ignored by probe acceptor");
            continue;
        }

        let req = read_probe_request_web(&mut recv).await?;
        let target = req.target_bitrate_bps.min(config.max_target_bitrate_bps);
        let epoch_ms = req.epoch_ms.max(1);

        // Use a logical epoch grid instead of starting the next epoch from wall-clock drift.
        // Example: duration=3000 ms, epoch=500 ms -> exactly 6 reporting epochs:
        // 0..500, 500..1000, ..., 2500..3000.
        let sample_count = if req.probe_duration_ms == 0 {
            0
        } else {
            req.probe_duration_ms
                .saturating_add(epoch_ms)
                .saturating_sub(1)
                / epoch_ms
        };

        let mut last_response_at = Instant::now();

        for sample_index in 1..=sample_count {
            let epoch_probe_start_ms = sample_index
                .saturating_sub(1)
                .saturating_mul(epoch_ms)
                .min(req.probe_duration_ms);
            let epoch_probe_end_ms = sample_index
                .saturating_mul(epoch_ms)
                .min(req.probe_duration_ms);

            if epoch_probe_end_ms <= epoch_probe_start_ms {
                break;
            }

            let epoch_budget_ms = epoch_probe_end_ms
                .saturating_sub(epoch_probe_start_ms)
                .max(1);

            let epoch_allowed_start = paced_probe_allowed_bytes(
                target,
                epoch_probe_start_ms,
                req.probe_duration_ms,
            );
            let epoch_allowed_end = paced_probe_allowed_bytes(
                target,
                epoch_probe_end_ms,
                req.probe_duration_ms,
            );

            // Report the average paced target over this fixed logical epoch.
            // This keeps paced_target_bps on the exact epoch grid and prevents
            // the final row from dropping because of wall-clock overshoot.
            let paced_target = bitrate_bps(
                epoch_allowed_end.saturating_sub(epoch_allowed_start),
                epoch_budget_ms,
            )
            .min(target);

            let epoch_start = Instant::now();
            let media_epoch_start = counters.media_write_bytes.load(Ordering::Relaxed);

            let mut attempted_padding_bytes = 0u64;
            let mut accepted_padding_bytes = 0u64;
            let mut write_block_time_ms = 0u64;

            let padding_buf = vec![0u8; 1200];

            if remembered_media_bps.is_none() {
                tokio::time::sleep(Duration::from_millis(epoch_budget_ms)).await;
            } else {
                match req.padding_mode {
                    PaddingMode::Stream => {
                        let mut uni = webtransport.open_uni().await?;
                        uni.set_priority(0);
                        write_varint_web(&mut uni, PROBE_PADDING_STREAM_TYPE).await?;

                        loop {
                            let actual_epoch_elapsed_ms = epoch_start.elapsed().as_millis() as u64;
                            if actual_epoch_elapsed_ms >= epoch_budget_ms {
                                break;
                            }

                            // Look one pacing tick ahead. Otherwise the writer is always
                            // one tick behind the target and under-fills by a few percent.
                            let decision_epoch_elapsed_ms = actual_epoch_elapsed_ms
                                .saturating_add(PADDING_PACING_TICK_MS)
                                .min(epoch_budget_ms);
                            let decision_probe_elapsed_ms = epoch_probe_start_ms
                                .saturating_add(decision_epoch_elapsed_ms)
                                .min(req.probe_duration_ms);

                            let current_paced_target = paced_probe_target_bps(
                                target,
                                decision_probe_elapsed_ms,
                                req.probe_duration_ms,
                            );

                            if remembered_media_bps.unwrap_or(0) >= current_paced_target {
                                tokio::time::sleep(Duration::from_millis(PADDING_PACING_TICK_MS)).await;
                                continue;
                            }

                            let allowed_at_decision = paced_probe_allowed_bytes(
                                target,
                                decision_probe_elapsed_ms,
                                req.probe_duration_ms,
                            );
                            let allowed_epoch_total = allowed_at_decision
                                .saturating_sub(epoch_allowed_start);

                            let media_epoch_bytes = counters
                                .media_write_bytes
                                .load(Ordering::Relaxed)
                                .saturating_sub(media_epoch_start);
                            let current_total_bytes = media_epoch_bytes
                                .saturating_add(accepted_padding_bytes);

                            if allowed_epoch_total <= current_total_bytes {
                                tokio::time::sleep(Duration::from_millis(PADDING_PACING_TICK_MS)).await;
                                continue;
                            }

                            let gap = allowed_epoch_total.saturating_sub(current_total_bytes);
                            let tick_budget = current_paced_target
                                .saturating_mul(PADDING_PACING_TICK_MS.saturating_mul(2))
                                / 8
                                / 1000;
                            let mut tick_remaining = gap.min(tick_budget.max(1200));

                            while tick_remaining > 0 {
                                let n = tick_remaining.min(padding_buf.len() as u64) as usize;
                                attempted_padding_bytes += n as u64;
                                let t0 = Instant::now();
                                write_all_web(&mut uni, &padding_buf[..n]).await?;
                                write_block_time_ms += t0.elapsed().as_millis() as u64;
                                accepted_padding_bytes += n as u64;
                                tick_remaining = tick_remaining.saturating_sub(n as u64);
                            }

                            tokio::time::sleep(Duration::from_millis(PADDING_PACING_TICK_MS)).await;
                        }

                        uni.finish()?;
                    }

                    PaddingMode::Datagram => {
                        'datagram_padding: loop {
                            let actual_epoch_elapsed_ms = epoch_start.elapsed().as_millis() as u64;
                            if actual_epoch_elapsed_ms >= epoch_budget_ms {
                                break;
                            }

                            let decision_epoch_elapsed_ms = actual_epoch_elapsed_ms
                                .saturating_add(PADDING_PACING_TICK_MS)
                                .min(epoch_budget_ms);
                            let decision_probe_elapsed_ms = epoch_probe_start_ms
                                .saturating_add(decision_epoch_elapsed_ms)
                                .min(req.probe_duration_ms);

                            let current_paced_target = paced_probe_target_bps(
                                target,
                                decision_probe_elapsed_ms,
                                req.probe_duration_ms,
                            );

                            let allowed_at_decision = paced_probe_allowed_bytes(
                                target,
                                decision_probe_elapsed_ms,
                                req.probe_duration_ms,
                            );
                            let allowed_epoch_total = allowed_at_decision
                                .saturating_sub(epoch_allowed_start);

                            let media_epoch_bytes = counters
                                .media_write_bytes
                                .load(Ordering::Relaxed)
                                .saturating_sub(media_epoch_start);
                            let current_total_bytes = media_epoch_bytes
                                .saturating_add(accepted_padding_bytes);

                            if allowed_epoch_total <= current_total_bytes {
                                tokio::time::sleep(Duration::from_millis(PADDING_PACING_TICK_MS)).await;
                                continue;
                            }

                            let gap = allowed_epoch_total.saturating_sub(current_total_bytes);
                            let tick_budget = current_paced_target
                                .saturating_mul(PADDING_PACING_TICK_MS.saturating_mul(2))
                                / 8
                                / 1000;
                            let mut tick_remaining = gap.min(tick_budget.max(1100));

                            while tick_remaining > 0 {
                                let n = tick_remaining.min(1100) as usize;
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
                                        break 'datagram_padding;
                                    }
                                }
                                write_block_time_ms += t0.elapsed().as_millis() as u64;
                                tick_remaining = tick_remaining.saturating_sub(n as u64);
                            }

                            tokio::time::sleep(Duration::from_millis(PADDING_PACING_TICK_MS)).await;
                        }
                    }
                }

                let actual_epoch_elapsed_ms = epoch_start.elapsed().as_millis() as u64;
                let sleep_ms = epoch_budget_ms.saturating_sub(actual_epoch_elapsed_ms);
                if sleep_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                }
            }

            // Use actual wall-clock elapsed time for measured sender rate.
            // The logical epoch grid is used only for pacing decisions and paced_target_bps reporting.
            let epoch_elapsed_ms = epoch_start.elapsed().as_millis().max(1) as u64;
            let response_elapsed_ms = last_response_at.elapsed().as_millis().max(1) as u64;
            last_response_at = Instant::now();

            let cwnd_bytes = counters.cwnd_bytes.load(Ordering::Relaxed);

            let media_epoch_bytes = counters
                .media_write_bytes
                .load(Ordering::Relaxed)
                .saturating_sub(media_epoch_start);

            let media_epoch_bps = bitrate_bps(media_epoch_bytes, epoch_elapsed_ms);
            if media_epoch_bytes > 0 && media_epoch_bps > 0 {
                remembered_media_bps = Some(match remembered_media_bps {
                    Some(prev) => prev
                        .saturating_mul(3)
                        .saturating_add(media_epoch_bps)
                        / 4,
                    None => media_epoch_bps,
                });
            }

            let sender_app_written =
                media_epoch_bytes.saturating_add(accepted_padding_bytes);

            let raw = bitrate_bps(sender_app_written, epoch_elapsed_ms);

            if let Some(csv) = &mut csv {
                csv.row(
                    req.request_id,
                    target,
                    paced_target,
                    cwnd_bytes,
                    attempted_padding_bytes,
                    accepted_padding_bytes,
                    write_block_time_ms,
                    media_epoch_bytes,
                    raw,
                    epoch_elapsed_ms,
                )?;
            }

            let res = ProbeResponse {
                request_id: req.request_id,
                target_bitrate_bps: target,
                paced_target_bps: paced_target,
                cwnd_bytes,
                raw_sender_app_bitrate_bps: raw,
                elapsed_ms: response_elapsed_ms,
                sender_app_written_bytes: sender_app_written,
                padding_written_bytes: accepted_padding_bytes,
                media_written_bytes: media_epoch_bytes,
            };

            let raw_msg = encode_probe_response(&res);
            write_all_web(&mut send, &raw_msg).await?;

            tracing::info!(
                request_id = res.request_id,
                sample_index,
                target_bitrate_bps = res.target_bitrate_bps,
                paced_target_bps = res.paced_target_bps,
                raw_sender_app_bitrate_bps = res.raw_sender_app_bitrate_bps,
                remembered_media_bps = remembered_media_bps.unwrap_or(0),
                "PROBE_RESPONSE sample sent"
            );
        }

        send.finish()?;
    }
}
