"""Verified launcher for the native Orch8 CLI."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.request
import zipfile

REPOSITORY = os.environ.get("ORCH8_REPOSITORY", "orch8-io/engine")


def _target() -> str:
    key = (platform.system().lower(), platform.machine().lower())
    targets = {
        ("linux", "x86_64"): "x86_64-unknown-linux-gnu",
        ("linux", "aarch64"): "aarch64-unknown-linux-gnu",
        ("darwin", "x86_64"): "x86_64-apple-darwin",
        ("darwin", "arm64"): "aarch64-apple-darwin",
        ("windows", "amd64"): "x86_64-pc-windows-msvc",
    }
    try:
        return targets[key]
    except KeyError as error:
        raise RuntimeError(f"unsupported platform: {key[0]}-{key[1]}") from error


def _tag() -> str:
    requested = os.environ.get("ORCH8_VERSION", "latest")
    if requested != "latest":
        return requested if requested.startswith("v") else f"v{requested}"
    request = urllib.request.Request(
        f"https://api.github.com/repos/{REPOSITORY}/releases/latest",
        headers={"User-Agent": "orch8-cli pipx installer"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)["tag_name"]


def _binary_path(tag: str) -> Path:
    suffix = ".exe" if platform.system() == "Windows" else ""
    return Path.home() / ".cache" / "orch8" / tag / f"orch8{suffix}"


def _safe_extract(package: tarfile.TarFile, destination: Path) -> None:
    """Extract without allowing archive members to escape the destination."""
    root = destination.resolve()
    for member in package.getmembers():
        if member.issym() or member.islnk() or member.isdev():
            raise RuntimeError(f"unsafe archive member type: {member.name}")
        target = (destination / member.name).resolve()
        if root not in target.parents and target != root:
            raise RuntimeError(f"unsafe archive member: {member.name}")
    package.extractall(destination)


def _safe_extract_zip(package: zipfile.ZipFile, destination: Path) -> None:
    """Extract a zip only when every member remains below destination."""
    root = destination.resolve()
    for member in package.infolist():
        target = (destination / member.filename).resolve()
        if root not in target.parents and target != root:
            raise RuntimeError(f"unsafe archive member: {member.filename}")
    package.extractall(destination)


def _install(destination: Path, tag: str) -> None:
    target = _target()
    extension = "zip" if platform.system() == "Windows" else "tar.gz"
    archive = f"orch8-{tag}-{target}.{extension}"
    base = f"https://github.com/{REPOSITORY}/releases/download/{tag}"
    with tempfile.TemporaryDirectory(prefix="orch8-") as temporary:
        root = Path(temporary)
        urllib.request.urlretrieve(f"{base}/{archive}", root / archive)
        urllib.request.urlretrieve(f"{base}/{archive}.sha256", root / f"{archive}.sha256")
        expected = (root / f"{archive}.sha256").read_text().split()[0].lower()
        actual = hashlib.sha256((root / archive).read_bytes()).hexdigest()
        if actual != expected:
            raise RuntimeError(f"checksum mismatch for {archive}")
        if extension == "zip":
            with zipfile.ZipFile(root / archive) as package:
                _safe_extract_zip(package, root / "unpack")
        else:
            with tarfile.open(root / archive, "r:gz") as package:
                _safe_extract(package, root / "unpack")
        suffix = ".exe" if platform.system() == "Windows" else ""
        source = root / "unpack" / f"orch8-{tag}-{target}" / f"orch8{suffix}"
        destination.parent.mkdir(parents=True, exist_ok=True)
        staged = destination.with_suffix(destination.suffix + ".new")
        shutil.copy2(source, staged)
        staged.chmod(0o755)
        staged.replace(destination)


def main() -> None:
    tag = _tag()
    binary = _binary_path(tag)
    if not binary.exists():
        _install(binary, tag)
    completed = subprocess.run([str(binary), *sys.argv[1:]], check=False)
    raise SystemExit(completed.returncode)
