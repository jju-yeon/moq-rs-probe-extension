// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{
    net,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use anyhow::Context;
use clap::Parser;
use url::Url;

use moq_native_ietf::quic;
use moq_sub::media::Media;
use moq_transport::{coding::TrackNamespace, probe, serve::Tracks};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing with env filter (respects RUST_LOG environment variable)
    // Default to info level, but suppress quinn's verbose output
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,quinn=warn")),
        )
        .init();

    let out = tokio::io::stdout();

    let config = Config::parse();
    let tls = config.tls.load()?;
    let quic = quic::Endpoint::new(quic::Config::new(config.bind, None, tls)?)?;

    let (session, connection_id, transport) = quic.client.connect(&config.url, None).await?;
    let probe_wt = session.clone();

    tracing::info!(
        "connected with CID: {} (use this to look up qlog/mlog on server)",
        connection_id
    );

    let counters = Arc::new(probe::ProbeCounters::default());
    probe::install_global_counters(counters.clone());

    let (session, subscriber) = moq_transport::session::Subscriber::connect(session, transport)
        .await
        .context("failed to create MoQ Transport session")?;

    if config.probe_enable {
        let probe_config = config.clone();
        let probe_counters = counters.clone();
        tokio::spawn(async move {
            if let Err(err) = run_probe_client(probe_wt, probe_config, probe_counters).await {
                tracing::warn!(?err, "probe client stopped");
            }
        });
    }

    // Associate empty set of Tracks with provided namespace
    let tracks = Tracks::new(TrackNamespace::from_utf8_path(&config.name));

    let mut media = Media::new(subscriber, tracks, out, config.catalog).await?;

    tokio::select! {
        res = session.run() => res.context("session error")?,
        res = media.run() => res.context("media error")?,
    }

    Ok(())
}

#[derive(Parser, Clone)]
//수정
pub struct Config {
    /// Listen for UDP packets on the given address.
    #[arg(long, default_value = "[::]:0")]
    pub bind: net::SocketAddr,

    #[arg(long, default_value = "1")]
    pub probe_count: u64,

    #[arg(long, default_value = "1000")]
    pub probe_interval_ms: u64,

    /// Connect to the given URL starting with https://
    #[arg(value_parser = moq_url)]
    pub url: Url,

    /// The name of the broadcast
    #[arg(long)]
    pub name: String,

    /// The TLS configuration.
    #[command(flatten)]
    pub tls: moq_native_ietf::tls::Args,

    /// Request the catalog track.
    #[arg(long)]
    pub catalog: bool,

    // 여기부터 추가

    /// Enable MoQ Probe experiment.
    #[arg(long)]
    pub probe_enable: bool,

    /// Probe target bitrate.
    #[arg(long, default_value = "8000000")]
    pub probe_target_bitrate: u64,

    /// Probe duration in milliseconds.
    #[arg(long, default_value = "3000")]
    pub probe_duration_ms: u64,

    /// Probe epoch duration in milliseconds.
    #[arg(long, default_value = "500")]
    pub probe_epoch_ms: u64,

    /// Probe mode: baseline or corrected.
    #[arg(long, default_value = "corrected")]
    pub probe_mode: String,

    /// Probe padding mode: stream or datagram.
    #[arg(long, default_value = "stream")]
    pub probe_padding_mode: String,

    /// Subscriber-side probe CSV log path.
    #[arg(long)]
    pub probe_log: Option<std::path::PathBuf>,

    /// Include previous receiver goodput feedback in next probe request.
    #[arg(long)]
    pub probe_receiver_assisted: bool,
}


async fn run_probe_client(
    wt: web_transport::Session,
    config: Config,
    counters: Arc<probe::ProbeCounters>,
) -> anyhow::Result<()> {
    let padding_mode = match config.probe_padding_mode.as_str() {
        "stream" => probe::PaddingMode::Stream,
        "datagram" => probe::PaddingMode::Datagram,
        other => anyhow::bail!("invalid --probe-padding-mode: {other}"),
    };

    let mode = match config.probe_mode.as_str() {
        "baseline" => probe::ProbeMode::Baseline,
        "corrected" => probe::ProbeMode::Corrected,
        other => anyhow::bail!("invalid --probe-mode: {other}"),
    };

    let log = config
        .probe_log
        .clone()
        .unwrap_or_else(|| std::path::PathBuf::from("subscriber_probe.csv"));

    let mut csv = probe::SubscriberProbeCsv::new(log)?;

    let previous_receiver_goodput_bps = AtomicU64::new(0);

    for i in 0..config.probe_count {
        let request_id = probe::now_ms();

        let media_start = counters.media_recv_bytes.load(Ordering::Relaxed);
        let ps_start = counters.padding_stream_recv_bytes.load(Ordering::Relaxed);
        let pd_start = counters.padding_datagram_recv_bytes.load(Ordering::Relaxed);

        let started = Instant::now();

        let (mut send, mut recv) = wt.open_bi().await?;

        probe::write_varint_web(&mut send, probe::PROBE_STREAM_TYPE).await?;

        let req = probe::ProbeRequest {
            request_id,
            target_bitrate_bps: config.probe_target_bitrate,
            probe_duration_ms: config.probe_duration_ms,
            epoch_ms: config.probe_epoch_ms,
            padding_mode,
            mode,
            receiver_assisted: config.probe_receiver_assisted,
            previous_receiver_goodput_bps: if config.probe_receiver_assisted {
                previous_receiver_goodput_bps.load(Ordering::Relaxed)
            } else {
                0
            },
        };

        let raw_req = probe::encode_probe_request(&req);
        probe::write_all_web(&mut send, &raw_req).await?;

        send.finish()?;

        let mut sample_index = 0u64;

        let mut media_last = media_start;
        let mut ps_last = ps_start;
        let mut pd_last = pd_start;

        let mut last_sample_at = Instant::now();

        loop {
            let res = match probe::read_probe_response_web(&mut recv).await {
                Ok(res) => res,
                Err(probe::ProbeError::EndOfStream) => break,
                Err(err) => return Err(err.into()),
            };

            sample_index += 1;

            let sample_elapsed_ms = last_sample_at.elapsed().as_millis().max(1) as u64;
            last_sample_at = Instant::now();

            let probe_elapsed_ms = started.elapsed().as_millis().max(1) as u64;

            let media_now = counters.media_recv_bytes.load(Ordering::Relaxed);
            let ps_now = counters.padding_stream_recv_bytes.load(Ordering::Relaxed);
            let pd_now = counters.padding_datagram_recv_bytes.load(Ordering::Relaxed);

            let media = media_now.saturating_sub(media_last);
            let padding_stream = ps_now.saturating_sub(ps_last);
            let padding_datagram = pd_now.saturating_sub(pd_last);

            media_last = media_now;
            ps_last = ps_now;
            pd_last = pd_now;

            let total_received = media
                .saturating_add(padding_stream)
                .saturating_add(padding_datagram);

            let receiver_goodput_bps = probe::bitrate_bps(total_received, sample_elapsed_ms);
            previous_receiver_goodput_bps.store(receiver_goodput_bps, Ordering::Relaxed);

            csv.row(
                res.request_id,
                sample_index,
                mode,
                probe_elapsed_ms,
                res.elapsed_ms,
                media,
                padding_stream,
                padding_datagram,
                receiver_goodput_bps,
                res.sender_app_written_bytes,
                res.media_written_bytes,
                res.padding_written_bytes,
                res.raw_sender_app_bitrate_bps,
                res.corrected_measured_bitrate_bps,
                res.target_bitrate_bps,
                res.paced_target_bps,
                res.cwnd_bytes,
                res.correction_factor_ppm,
                res.correction_reason_code,
            )?;

            tracing::info!(
                request_id = res.request_id,
                sample_index,
                probe_index = i + 1,
                probe_count = config.probe_count,
                target_bitrate_bps = res.target_bitrate_bps,
                paced_target_bps = res.paced_target_bps,
                raw_sender_app_bitrate_bps = res.raw_sender_app_bitrate_bps,
                corrected_measured_bitrate_bps = res.corrected_measured_bitrate_bps,
                receiver_goodput_bps,
                "probe sample received"
            );
        }

        tracing::info!(
            request_id,
            probe_index = i + 1,
            probe_count = config.probe_count,
            samples = sample_index,
            "probe completed"
        );

        if i + 1 < config.probe_count {
            tokio::time::sleep(Duration::from_millis(config.probe_interval_ms)).await;
        }
    }

    Ok(())
}

fn moq_url(s: &str) -> Result<Url, String> {
    let url = Url::try_from(s).map_err(|e| e.to_string())?;

    // Make sure the scheme is moq
    if url.scheme() != "https" && url.scheme() != "moqt" {
        return Err("url scheme must be https:// for WebTransport & moqt:// for QUIC".to_string());
    }

    Ok(url)
}
