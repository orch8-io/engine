#!/usr/bin/env python3
"""Fetch checksum-pinned official Apple frameworks for native adapter checks."""
import hashlib
import json
from pathlib import Path
import sys
import tarfile
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[1]
REACT = "0.88.0"
FLUTTER_ENGINE = "cf636b1dfb818dd6a3ff16e390e93b1a036969c8"
ARTIFACTS = (
    ("react-core.tar.gz", "react", f"https://repo1.maven.org/maven2/com/facebook/react/react-native-artifacts/{REACT}/react-native-artifacts-{REACT}-reactnative-core-debug.tar.gz", "c33c04dca2c14be698877c381c8916888e55eb81ea704563aed99294f18871ff"),
    ("react-dependencies.tar.gz", "react-dependencies", f"https://repo1.maven.org/maven2/com/facebook/react/react-native-artifacts/{REACT}/react-native-artifacts-{REACT}-reactnative-dependencies-debug.tar.gz", "e2ff39ab5e7c3d45e7eb9fc092b40eae0c0f34f52b54fdcda816a5c24d3bb5b1"),
    ("flutter-ios.zip", "flutter", f"https://storage.googleapis.com/flutter_infra_release/flutter/{FLUTTER_ENGINE}/ios/artifacts.zip", "638d629f791904477c932e73e4a4b5849435a24ff5dc9eabfde583cb4a1a59a9"),
)


def main():
    version = json.loads((ROOT / "packages/react-native/package.json").read_text())["devDependencies"]["react-native"]
    if version != REACT:
        raise SystemExit("Update the pinned Apple React artifacts alongside the React Native dependency")
    destination = Path(sys.argv[1]).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    manifest = {name: digest for name, _, _, digest in ARTIFACTS}
    ready = destination / "frameworks-ready.json"
    if ready.exists() and json.loads(ready.read_text()) == manifest:
        return
    downloads = destination / "downloads"
    downloads.mkdir(exist_ok=True)
    for name, folder, url, expected in ARTIFACTS:
        archive = downloads / name
        if not archive.exists():
            temporary = archive.with_suffix(".download")
            request = urllib.request.Request(url, headers={"User-Agent": "curl/8"})
            try:
                with urllib.request.urlopen(request, timeout=120) as response, temporary.open("wb") as output:
                    while chunk := response.read(1024 * 1024):
                        output.write(chunk)
                temporary.replace(archive)
            finally:
                temporary.unlink(missing_ok=True)
        with archive.open("rb") as source:
            actual = hashlib.file_digest(source, "sha256").hexdigest()
        if actual != expected:
            raise SystemExit(f"Checksum mismatch for {name}")
        target = destination / folder
        target.mkdir(exist_ok=True)
        if name.endswith(".zip"):
            with zipfile.ZipFile(archive) as zipped:
                zipped.extractall(target)
        else:
            with tarfile.open(archive) as packed:
                packed.extractall(target, filter="data")
        archive.unlink()  # Completed installer archives are not build inputs.
    ready.write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"Pinned Apple frameworks ready: React Native {REACT}, Flutter engine {FLUTTER_ENGINE}")


if __name__ == "__main__":
    main()
