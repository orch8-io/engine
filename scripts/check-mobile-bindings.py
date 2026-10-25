#!/usr/bin/env python3
"""Check committed mobile bindings against deterministic current-library output."""
from __future__ import annotations

import argparse
import difflib
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
PAIRS = (
    ("swift/Orch8Mobile.swift", "orch8-mobile/bindings/swift/Orch8Mobile.swift"),
    ("swift/Orch8Mobile.swift", "packages/swift/Sources/Orch8Mobile/Orch8MobileBindings.swift"),
    ("swift/Orch8MobileFFI.h", "orch8-mobile/bindings/swift/Orch8MobileFFI.h"),
    ("swift/Orch8MobileFFI.modulemap", "orch8-mobile/bindings/swift/Orch8MobileFFI.modulemap"),
    ("kotlin/io/orch8/mobile/orch8_mobile.kt", "orch8-mobile/bindings/kotlin/io/orch8/mobile/orch8_mobile.kt"),
)


def lines(path: Path) -> list[str]:
    # Existing generated files differ in trailing whitespace. Keep every code,
    # documentation and checksum line significant; normalize only line endings
    # and trailing/EOF whitespace rather than hiding signature/contract changes.
    normalized = [line.rstrip() for line in path.read_text(encoding="utf-8").splitlines()]
    while normalized and not normalized[-1]:
        normalized.pop()
    return normalized


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("generated_root", nargs="?", type=Path, default=ROOT / "bindings")
    args = parser.parse_args()
    failed = False
    for generated_name, committed_name in PAIRS:
        generated = args.generated_root / generated_name
        committed = ROOT / committed_name
        if not generated.is_file() or not committed.is_file():
            print(f"Missing binding: {generated} or {committed}", file=sys.stderr)
            failed = True
            continue
        actual, expected = lines(committed), lines(generated)
        if actual != expected:
            failed = True
            print(f"Outdated mobile binding: {committed_name}", file=sys.stderr)
            diff = list(difflib.unified_diff(actual, expected, fromfile=committed_name,
                                           tofile=f"generated/{generated_name}", lineterm=""))
            print("\n".join(diff[:80]), file=sys.stderr)
            if len(diff) > 80:
                print(f"... {len(diff) - 80} more diff lines", file=sys.stderr)
    if failed:
        print("Regenerate Swift and Kotlin bindings from the current orch8-mobile library with --no-format, "
              "then synchronize all committed copies before packaging.", file=sys.stderr)
        return 1
    print(f"All {len(PAIRS)} committed mobile binding copies match current generated contracts.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
