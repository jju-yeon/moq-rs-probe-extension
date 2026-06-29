#!/usr/bin/env python3
"""
Local patch for MoQ probe padding pacing, v3.

This script patches moq-transport/src/probe.rs so that:
  - probe_epoch_ms remains the CSV/reporting interval;
  - padding is paced at a short tick interval inside each epoch;
  - total padding is NOT capped, so high targets remain reachable;
  - the budget is cumulative from probe start, so media bursts reduce later padding.

Usage:
  python patch_probe_token_bucket_pacing_v3.py
  python patch_probe_token_bucket_pacing_v3.py C:\\Users\\Laptop\\Desktop\\moq-rs
  python patch_probe_token_bucket_pacing_v3.py --dry-run
"""

from __future__ import annotations

import argparse
import difflib
from pathlib import Path
import re
import shutil
import sys


class PatchError(RuntimeError):
    pass


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8", newline="")


def backup(path: Path) -> None:
    bak = path.with_suffix(path.suffix + ".bak")
    if not bak.exists():
        shutil.copy2(path, bak)


def find_matching_brace(text: str, open_brace: int) -> int:
    depth = 0
    i = open_brace
    in_line_comment = False
    in_block_comment = False
    in_string = False
    in_char = False
    escaped = False

    while i < len(text):
        c = text[i]
        nxt = text[i + 1] if i + 1 < len(text) else ""

        if in_line_comment:
            if c == "\n":
                in_line_comment = False
            i += 1
            continue
        if in_block_comment:
            if c == "*" and nxt == "/":
                in_block_comment = False
                i += 2
            else:
                i += 1
            continue
        if in_string:
            if escaped:
                escaped = False
            elif c == "\\":
                escaped = True
            elif c == '"':
                in_string = False
            i += 1
            continue
        if in_char:
            if escaped:
                escaped = False
            elif c == "\\":
                escaped = True
            elif c == "'":
                in_char = False
            i += 1
            continue

        if c == "/" and nxt == "/":
            in_line_comment = True
            i += 2
            continue
        if c == "/" and nxt == "*":
            in_block_comment = True
            i += 2
            continue
        if c == '"':
            in_string = True
            i += 1
            continue
        if c == "'":
            in_char = True
            i += 1
            continue

        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return i
        i += 1

    raise PatchError("matching brace not found")


def find_function(text: str, fn_name: str) -> tuple[int, int]:
    # v3: allow async fn, pub async fn, pub(crate) async fn, unsafe async fn, etc.
    pat = re.compile(
        r"(?m)^\s*"
        r"(?:pub(?:\([^)]*\))?\s+)?"
        r"(?:unsafe\s+)?"
        r"(?:async\s+)?"
        r"fn\s+" + re.escape(fn_name) + r"\s*\(",
    )
    m = pat.search(text)
    if not m:
        raise PatchError(f"function not found: {fn_name}")
    open_brace = text.find("{", m.end())
    if open_brace < 0:
        raise PatchError(f"opening brace not found for function: {fn_name}")
    close = find_matching_brace(text, open_brace)
    return m.start(), close + 1


def find_container_around(text: str, marker_pos: int) -> tuple[int, int]:
    """Find the smallest enclosing Rust fn-like block around marker_pos."""
    fn_pat = re.compile(
        r"(?m)^\s*"
        r"(?:pub(?:\([^)]*\))?\s+)?"
        r"(?:unsafe\s+)?"
        r"(?:async\s+)?"
        r"fn\s+[A-Za-z_][A-Za-z0-9_]*\s*\(",
    )
    candidates: list[tuple[int, int]] = []
    for m in fn_pat.finditer(text):
        open_brace = text.find("{", m.end())
        if open_brace < 0:
            continue
        try:
            close = find_matching_brace(text, open_brace) + 1
        except PatchError:
            continue
        if m.start() <= marker_pos < close:
            candidates.append((m.start(), close))
    if not candidates:
        raise PatchError("could not find enclosing function around `match req.padding_mode`")
    return max(candidates, key=lambda x: x[0])


def add_constant(text: str) -> str:
    if "PADDING_PACING_TICK_MS" in text:
        return text

    m = re.search(r"(?m)^(?P<indent>\s*)pub\s+const\s+PADDING_PRIORITY\s*:\s*i32\s*=\s*0\s*;\s*$", text)
    if not m:
        raise PatchError("could not find PADDING_PRIORITY constant")

    insert = (
        f"{m.group('indent')}// Padding is paced inside each probe epoch at this interval.\n"
        f"{m.group('indent')}// The epoch remains the reporting interval; this tick is only for write pacing.\n"
        f"{m.group('indent')}pub const PADDING_PACING_TICK_MS: u64 = 10;\n"
    )
    return text[: m.end()] + "\n" + insert + text[m.end():]


def add_allowed_bytes_helper(text: str) -> str:
    if "fn paced_probe_allowed_bytes" in text:
        return text

    try:
        _, end = find_function(text, "paced_probe_target_bps")
    except PatchError:
        # Fallback: insert before the first function that handles probe acceptor.
        marker = text.find("paced_probe_target_bps")
        if marker < 0:
            raise
        end = marker

    helper = r'''

fn paced_probe_allowed_bytes(target_bps: u64, elapsed_ms: u64, duration_ms: u64) -> u64 {
    if target_bps == 0 || elapsed_ms == 0 || duration_ms == 0 {
        return 0;
    }

    const START_PPM: u128 = 250_000; // 25%
    const FULL_PPM: u128 = 1_000_000; // 100%
    const RAMP_PORTION_PPM: u64 = 600_000; // first 60% of probe duration

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
'''
    return text[:end] + helper + text[end:]


def locate_padding_match(text: str) -> tuple[int, int, str, tuple[int, int]]:
    m = re.search(r"(?m)^(?P<indent>\s*)match\s+req\.padding_mode\s*\{", text)
    if not m:
        raise PatchError("match req.padding_mode block not found")
    open_brace = text.find("{", m.start())
    close = find_matching_brace(text, open_brace) + 1
    fn_start, fn_end = find_container_around(text, m.start())
    return m.start(), close, m.group("indent"), (fn_start, fn_end)


def add_probe_start_counters(text: str, fn_range: tuple[int, int]) -> str:
    if "media_probe_start" in text and "accepted_padding_total" in text:
        return text

    fn_start, fn_end = fn_range
    body = text[fn_start:fn_end]
    m = re.search(r"(?m)^(?P<indent>\s*)let\s+started\s*=\s*Instant::now\s*\(\s*\)\s*;\s*$", body)
    if not m:
        raise PatchError("could not find `let started = Instant::now();` inside probe function")

    insert = (
        f"{m.group('indent')}let media_probe_start = counters.media_write_bytes.load(Ordering::Relaxed);\n"
        f"{m.group('indent')}let mut accepted_padding_total = 0u64;\n"
    )
    body2 = body[: m.end()] + "\n" + insert + body[m.end():]
    return text[:fn_start] + body2 + text[fn_end:]


def indent_block(block: str, indent: str) -> str:
    # block is written with 0-indented first line and 4-space internal indentation.
    return "\n".join((indent + line if line else line) for line in block.split("\n"))


def build_new_match_block(indent: str) -> str:
    block = r'''match req.padding_mode {
    PaddingMode::Stream => {
        let mut uni = webtransport.open_uni().await?;
        uni.set_priority(0);
        write_varint_web(&mut uni, PROBE_PADDING_STREAM_TYPE).await?;

        loop {
            let epoch_elapsed_ms = epoch_start
                .elapsed()
                .as_millis()
                .min(req.epoch_ms as u128) as u64;
            if epoch_elapsed_ms >= req.epoch_ms {
                break;
            }

            let probe_elapsed_ms = started
                .elapsed()
                .as_millis()
                .min(req.probe_duration_ms as u128) as u64;
            let current_paced_target = paced_probe_target_bps(
                target,
                probe_elapsed_ms,
                req.probe_duration_ms,
            );
            let allowed_total_bytes = paced_probe_allowed_bytes(
                target,
                probe_elapsed_ms,
                req.probe_duration_ms,
            );
            let media_probe_bytes = counters
                .media_write_bytes
                .load(Ordering::Relaxed)
                .saturating_sub(media_probe_start);
            let current_total_bytes = media_probe_bytes.saturating_add(accepted_padding_total);

            if allowed_total_bytes <= current_total_bytes {
                tokio::time::sleep(Duration::from_millis(PADDING_PACING_TICK_MS)).await;
                continue;
            }

            let gap = allowed_total_bytes.saturating_sub(current_total_bytes);
            let tick_budget = current_paced_target
                .saturating_mul(PADDING_PACING_TICK_MS)
                / 8
                / 1000;
            let mut tick_remaining = gap.min(tick_budget.max(1));

            while tick_remaining > 0 {
                let n = tick_remaining.min(padding_buf.len() as u64) as usize;
                attempted_padding_bytes += n as u64;
                let t0 = Instant::now();
                write_all_web(&mut uni, &padding_buf[..n]).await?;
                write_block_time_ms += t0.elapsed().as_millis() as u64;
                accepted_padding_bytes += n as u64;
                accepted_padding_total += n as u64;
                tick_remaining = tick_remaining.saturating_sub(n as u64);
            }

            tokio::time::sleep(Duration::from_millis(PADDING_PACING_TICK_MS)).await;
        }
        uni.finish()?;
    }
    PaddingMode::Datagram => {
        'datagram_padding: loop {
            let epoch_elapsed_ms = epoch_start
                .elapsed()
                .as_millis()
                .min(req.epoch_ms as u128) as u64;
            if epoch_elapsed_ms >= req.epoch_ms {
                break;
            }

            let probe_elapsed_ms = started
                .elapsed()
                .as_millis()
                .min(req.probe_duration_ms as u128) as u64;
            let current_paced_target = paced_probe_target_bps(
                target,
                probe_elapsed_ms,
                req.probe_duration_ms,
            );
            let allowed_total_bytes = paced_probe_allowed_bytes(
                target,
                probe_elapsed_ms,
                req.probe_duration_ms,
            );
            let media_probe_bytes = counters
                .media_write_bytes
                .load(Ordering::Relaxed)
                .saturating_sub(media_probe_start);
            let current_total_bytes = media_probe_bytes.saturating_add(accepted_padding_total);

            if allowed_total_bytes <= current_total_bytes {
                tokio::time::sleep(Duration::from_millis(PADDING_PACING_TICK_MS)).await;
                continue;
            }

            let gap = allowed_total_bytes.saturating_sub(current_total_bytes);
            let tick_budget = current_paced_target
                .saturating_mul(PADDING_PACING_TICK_MS)
                / 8
                / 1000;
            let mut tick_remaining = gap.min(tick_budget.max(1));

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
                        accepted_padding_total += n as u64;
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
}'''
    return indent_block(block, indent)


def patch_probe_rs(text: str) -> str:
    original = text
    text = add_constant(text)
    text = add_allowed_bytes_helper(text)
    match_start, match_end, indent, fn_range = locate_padding_match(text)
    text = add_probe_start_counters(text, fn_range)
    # Re-locate after insertion shifted offsets.
    match_start, match_end, indent, _ = locate_padding_match(text)
    new_block = build_new_match_block(indent)
    text = text[:match_start] + new_block + text[match_end:]

    required = [
        "PADDING_PACING_TICK_MS",
        "fn paced_probe_allowed_bytes",
        "let media_probe_start = counters.media_write_bytes.load(Ordering::Relaxed);",
        "let mut accepted_padding_total = 0u64;",
        "let current_paced_target = paced_probe_target_bps",
        "accepted_padding_total += n as u64;",
    ]
    missing = [s for s in required if s not in text]
    if missing:
        raise PatchError("patch result is missing required text: " + ", ".join(missing))
    if text == original:
        raise PatchError("no changes produced; the file may already be patched")
    return text


def unified_diff(path: Path, old: str, new: str) -> str:
    return "".join(
        difflib.unified_diff(
            old.splitlines(keepends=True),
            new.splitlines(keepends=True),
            fromfile=str(path) + ".before",
            tofile=str(path) + ".after",
        )
    )


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("repo", nargs="?", default=".", help="repository root")
    parser.add_argument("--dry-run", action="store_true", help="print diff without writing")
    parser.add_argument("--no-backup", action="store_true", help="do not create .bak file")
    args = parser.parse_args(argv)

    repo = Path(args.repo).resolve()
    probe_rs = repo / "moq-transport" / "src" / "probe.rs"
    if not probe_rs.exists():
        raise PatchError(f"not found: {probe_rs}")

    old = read_text(probe_rs)
    new = patch_probe_rs(old)
    diff = unified_diff(probe_rs, old, new)

    if args.dry_run:
        print(diff)
        print(f"DRY-RUN OK: {probe_rs}", file=sys.stderr)
        return 0

    if not args.no_backup:
        backup(probe_rs)
    write_text(probe_rs, new)
    print(f"PATCHED   {probe_rs}")
    print("Next: cargo check -p moq-transport -p moq-sub -p moq-relay-ietf")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except PatchError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
