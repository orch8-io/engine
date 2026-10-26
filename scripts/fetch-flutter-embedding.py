#!/usr/bin/env python3
"""Fetch a pinned, real Flutter Android embedding for native bridge compilation."""
import hashlib
import io
from pathlib import Path
import sys
import urllib.request
import zipfile

# Flutter stable engine, recorded from bin/internal/engine.version. Update the engine and jar hash together.
ENGINE = "cf636b1dfb818dd6a3ff16e390e93b1a036969c8"
SHA256 = "4f2bb7a3038508cfc12f7b4902ac0323d5f9f8119825270d9580aeab68781c0f"


def main():
    target = Path(sys.argv[1])
    if target.is_file() and hashlib.sha256(target.read_bytes()).hexdigest() == SHA256:
        return
    url = f"https://storage.googleapis.com/flutter_infra_release/flutter/{ENGINE}/android-x64/artifacts.zip"
    with urllib.request.urlopen(url, timeout=120) as response:
        archive = response.read()
    with zipfile.ZipFile(io.BytesIO(archive)) as zipped:
        jar = zipped.read("flutter.jar")
    if hashlib.sha256(jar).hexdigest() != SHA256:
        raise SystemExit("Flutter embedding checksum mismatch")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(jar)


if __name__ == "__main__":
    main()
