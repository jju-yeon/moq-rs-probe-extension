#!/usr/bin/env python3
"""
Remove the leftover `correction: moq_transport::probe::CorrectionConfig { ... },`
field from moq-relay-ietf after ProbeConfig.correction was removed.

Usage:
  python fix_relay_correction_config.py
  python fix_relay_correction_config.py C:\\Users\\Laptop\\Desktop\\moq-rs
  python fix_relay_correction_config.py --dry-run
"""
from __future__ import annotations

import argparse
import difflib
from pathlib import Path

REMOVED_TERMS = [
    "CorrectionConfig",
]


def strip_comments_and_strings_context(_text: str) -> None:
    # Not needed for the current generated Rust block. Kept intentionally empty.
    return None


def find_matching_brace(text: str, open_idx: int) -> int:
    """Return index just after the matching closing brace for text[open_idx] == '{'."""
    assert text[open_idx] == "{"
    depth = 0
    i = open_idx
    n = len(text)
    in_str = False
    in_char = False
    escape = False
    line_comment = False
    block_comment = 0

    while i < n:
        ch = text[i]
        nxt = text[i + 1] if i + 1 < n else ""

        if line_comment:
            if ch == "\n":
                line_comment = False
            i += 1
            continue

        if block_comment:
            if ch == "/" and nxt == "*":
                block_comment += 1
                i += 2
                continue
            if ch == "*" and nxt == "/":
                block_comment -= 1
                i += 2
                continue
            i += 1
            continue

        if in_str:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_str = False
            i += 1
            continue

        if in_char:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == "'":
                in_char = False
            i += 1
            continue

        if ch == "/" and nxt == "/":
            line_comment = True
            i += 2
            continue
        if ch == "/" and nxt == "*":
            block_comment += 1
            i += 2
            continue
        if ch == '"':
            in_str = True
            i += 1
            continue
        if ch == "'":
            in_char = True
            i += 1
            continue

        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return i + 1
        i += 1

    raise ValueError("matching brace not found")


def remove_correction_field_blocks(text: str) -> tuple[str, int]:
    """
    Remove Rust struct-literal fields named `correction:` whose value contains
    `CorrectionConfig { ... }`.
    """
    count = 0
    pos = 0
    out = text

    while True:
        idx = out.find("correction:", pos)
        if idx == -1:
            break

        # Identifier boundary check.
        before = out[idx - 1] if idx > 0 else "\n"
        after = out[idx + len("correction:")] if idx + len("correction:") < len(out) else ""
        if (before.isalnum() or before == "_") or after == ":":
            pos = idx + len("correction:")
            continue

        # Only remove the ProbeConfig field that instantiates CorrectionConfig.
        next_chunk = out[idx : idx + 500]
        if "CorrectionConfig" not in next_chunk:
            pos = idx + len("correction:")
            continue

        line_start = out.rfind("\n", 0, idx) + 1
        open_brace = out.find("{", idx)
        if open_brace == -1:
            pos = idx + len("correction:")
            continue

        try:
            end = find_matching_brace(out, open_brace)
        except ValueError:
            pos = idx + len("correction:")
            continue

        # Consume trailing whitespace, comma, and at most one newline after the field.
        j = end
        while j < len(out) and out[j] in " \t\r":
            j += 1
        if j < len(out) and out[j] == ",":
            j += 1
        while j < len(out) and out[j] in " \t\r":
            j += 1
        if j < len(out) and out[j] == "\n":
            j += 1

        out = out[:line_start] + out[j:]
        count += 1
        pos = line_start

    return out, count


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", nargs="?", default=".", help="repository root; default: current directory")
    parser.add_argument("--dry-run", action="store_true", help="show diff without writing")
    args = parser.parse_args()

    root = Path(args.root).resolve()
    target = root / "moq-relay-ietf" / "src" / "bin" / "moq-relay-ietf" / "main.rs"

    if not target.exists():
        raise SystemExit(f"ERROR: file not found: {target}")

    old = target.read_text(encoding="utf-8")
    new, count = remove_correction_field_blocks(old)

    if new == old:
        print(f"UNCHANGED {target}")
        if "CorrectionConfig" in old or "correction:" in old:
            print("WARNING: correction-related text may still remain; inspect the file manually.")
        return 0

    diff = "".join(
        difflib.unified_diff(
            old.splitlines(True),
            new.splitlines(True),
            fromfile=str(target) + ".before",
            tofile=str(target) + ".after",
        )
    )

    if args.dry_run:
        print(diff)
        print(f"DRY-RUN would remove {count} correction block(s) from {target}")
        return 0

    backup = target.with_suffix(target.suffix + ".bak-correction-config")
    if not backup.exists():
        backup.write_text(old, encoding="utf-8")
    target.write_text(new, encoding="utf-8")
    print(f"PATCHED   {target}")
    print(f"REMOVED   {count} correction block(s)")
    print(f"BACKUP    {backup}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
