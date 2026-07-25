// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{
    net,
    sync::Arc,
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

    let (session, subscriber) = moq_transport::session::Subscriber::connect(session, transport)
        .await
        .context("failed to create MoQ Transport session")?;
    let probe_context = subscriber.probe_context();

    // Associate empty set of Tracks with provided namespace
    let tracks = Tracks::new(TrackNamespace::from_utf8_path(&config.name));

    let mut media = Media::new(subscriber, tracks, out, config.catalog).await?;
    let session = session.run();
    tokio::pin!(session);

    let media_tracks = tokio::select! {
        res = &mut session => return res.context("session error"),
        res = media.initialize() => res.context("media initialization error")?,
    };

    let probe_task = if config.probe_enable && config.probe_count > 0 {
        let probe_config = config.clone();
        Some(tokio::spawn(async move {
            if let Err(err) = run_probe_client(probe_wt, probe_config, probe_context).await {
                tracing::warn!(?err, "probe client stopped");
            }
        }))
    } else {
        None
    };

    tokio::select! {
        res = &mut session => res.context("session error")?,
        res = media.run(media_tracks) => res.context("media error")?,
    }

    if let Some(task) = probe_task {
        task.abort();
        let _ = task.await;
    }

    Ok(())
}

#[derive(Parser, Clone)]
pub struct Config {
    /// Listen for UDP packets on the given address.
    #[arg(long, default_value = "[::]:0")]
    pub bind: net::SocketAddr,

    #[arg(
        long,
        default_value = "1",
        value_parser = clap::value_parser!(u64).range(1..)
    )]
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

    /// Enable MoQ Probe experiment.
    #[arg(long)]
    pub probe_enable: bool,

    /// Probe target bitrate.
    #[arg(long, default_value = "8000000")]
    pub probe_target_bitrate: u64,

    /// Probe duration in milliseconds.
    #[arg(
        long,
        default_value = "3000",
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub probe_duration_ms: u64,

    /// Probe epoch duration in milliseconds.
    #[arg(
        long,
        default_value = "500",
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub probe_epoch_ms: u64,

    /// Probe padding mode. The staged protocol supports only streams.
    #[arg(long, default_value = "stream", value_parser = ["stream"])]
    pub probe_padding_mode: String,

    /// Subscriber-side probe CSV log path.
    #[arg(long)]
    pub probe_log: Option<std::path::PathBuf>,
}

async fn run_probe_client(
    wt: web_transport::Session,
    config: Config,
    context: Arc<probe::ReceiverProbeContext>,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        config.probe_padding_mode == "stream",
        "--probe-padding-mode must be stream for staged Probe"
    );
    anyhow::ensure!(
        config.probe_duration_ms > 0,
        "--probe-duration-ms must be greater than zero"
    );
    anyhow::ensure!(
        config.probe_epoch_ms > 0,
        "--probe-epoch-ms must be greater than zero"
    );

    let log = config
        .probe_log
        .clone()
        .unwrap_or_else(|| std::path::PathBuf::from("subscriber_probe.csv"));

    let mut csv = probe::SubscriberProbeCsv::new(log)?;

    for logical_index in 0..config.probe_count {
        let request_id = probe::now_ms().saturating_add(logical_index);
        let logical_started = Instant::now();
        let (mut send, mut recv) = wt.open_bi().await?;
        send.set_priority(probe::PROBE_CONTROL_PRIORITY);

        probe::write_varint_web(&mut send, probe::PROBE_STREAM_TYPE).await?;
        probe::write_varint_web(&mut send, probe::PROBE_PROTOCOL_VERSION).await?;

        let mut highest_validated_target_bps = 0u64;
        let mut logical_status = "COMPLETED";

        for (stage_index, ratio) in probe::DEFAULT_STAGE_RATIOS_PERCENT.iter().enumerate() {
            let stage_target = probe::stage_target_bps(config.probe_target_bitrate, *ratio);
            context.begin_stage(
                request_id,
                stage_index as u64,
                *ratio,
                stage_target,
                probe::DEFAULT_STAGE_PASS_PERCENT,
            )?;

            let request = probe::ProbeRequest {
                request_id,
                stage_index: stage_index as u64,
                stage_ratio_percent: *ratio,
                stage_target_bps: stage_target,
                active_duration_ms: config.probe_duration_ms,
                epoch_ms: config.probe_epoch_ms,
            };
            probe::write_probe_message_web(
                &mut send,
                &probe::ProbeMessage::Request(request.clone()),
            )
            .await?;

            let start = match probe::read_probe_message_web(&mut recv).await? {
                probe::ProbeMessage::Start(start) => start,
                _ => anyhow::bail!("expected PROBE_START"),
            };
            context.on_probe_start(&start, Instant::now())?;
            probe::write_probe_message_web(
                &mut send,
                &probe::ProbeMessage::StartAck(probe::ProbeStartAck {
                    request_id,
                    stage_index: stage_index as u64,
                }),
            )
            .await?;

            let end = match probe::read_probe_message_web(&mut recv).await? {
                probe::ProbeMessage::End(end) => end,
                _ => anyhow::bail!("expected PROBE_END"),
            };
            let mut result = context.on_probe_end(&end, Instant::now())?;
            context.wait_for_padding_cleanup().await;
            context.refresh_after_padding_cleanup(&mut result)?;

            if result.stage_passed {
                highest_validated_target_bps = stage_target;
                tracing::info!(
                    request_id,
                    stage_index,
                    stage_target_bps = stage_target,
                    receiver_goodput_bps = result.receiver_goodput_bps,
                    "STAGE_PASSED"
                );
            } else {
                logical_status = match result.status {
                    probe::StageStatus::AbortedBackpressure => "ABORTED_BACKPRESSURE",
                    _ if stage_index == 0 => "VALIDATION_FAILED",
                    _ if stage_index == 1 => "VALIDATION_FAILED_AT_75",
                    _ => "VALIDATION_FAILED_AT_100",
                };
                tracing::info!(
                    request_id,
                    stage_index,
                    stage_target_bps = stage_target,
                    receiver_goodput_bps = result.receiver_goodput_bps,
                    ?result.status,
                    "STAGE_FAILED"
                );
            }

            csv.row(
                logical_index + 1,
                &result,
                highest_validated_target_bps,
                logical_status,
            )?;

            if !result.stage_passed {
                break;
            }
        }

        send.finish()?;
        tracing::info!(
            request_id,
            probe_index = logical_index + 1,
            probe_count = config.probe_count,
            highest_validated_target_bps,
            logical_probe_wall_clock_ms = logical_started.elapsed().as_millis() as u64,
            logical_status,
            "LOGICAL_PROBE_COMPLETED"
        );

        if logical_index + 1 < config.probe_count {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_probe_args(extra: &[&str]) -> Result<Config, clap::Error> {
        let mut args = vec!["moq-sub", "--name", "test", "https://localhost/"];
        args.extend_from_slice(extra);
        Config::try_parse_from(args)
    }

    #[test]
    fn staged_probe_rejects_datagram_padding() {
        let error = match parse_probe_args(&["--probe-padding-mode", "datagram"]) {
            Ok(_) => panic!("datagram padding unexpectedly accepted"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("stream"));
    }

    #[test]
    fn staged_probe_rejects_zero_durations() {
        assert!(parse_probe_args(&["--probe-count", "0"]).is_err());
        assert!(parse_probe_args(&["--probe-duration-ms", "0"]).is_err());
        assert!(parse_probe_args(&["--probe-epoch-ms", "0"]).is_err());
    }

    #[test]
    fn staged_probe_allows_target_zero() {
        let config = parse_probe_args(&["--probe-target-bitrate", "0"]).unwrap();
        assert_eq!(config.probe_target_bitrate, 0);
    }
}
