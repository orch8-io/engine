#!/usr/bin/env python3
"""Publish the orch8-mobile AAR into a checkout of the orch8-io/maven repository.

orch8-io/maven is a static Maven layout served from
https://raw.githubusercontent.com/orch8-io/maven/main. Consumers resolve
io.orch8:orch8-mobile:<version> from it (the Expo, React Native and Flutter
wrappers and packages/kmp all do). This script writes

    io/orch8/orch8-mobile/<version>/orch8-mobile-<version>.aar
    io/orch8/orch8-mobile/<version>/orch8-mobile-<version>.pom
    io/orch8/orch8-mobile/maven-metadata.xml

plus .md5/.sha1/.sha256 sidecars. The POM's runtime dependencies are read from
the `implementation("group:artifact:version[@type]")` lines of
packages/android/orch8-mobile/build.gradle.kts, so they cannot drift from the
AAR's build.

Published versions are immutable: re-running with byte-identical inputs is a
no-op; different bytes for an existing version are an error.

Usage:
    scripts/publish-maven-aar.py --repo ../maven --version 0.7.2 \
        --aar orch8-mobile-v0.7.2.aar [--gradle packages/android/orch8-mobile/build.gradle.kts]
    scripts/publish-maven-aar.py --print-pom --version 0.7.2
"""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

GROUP_ID = "io.orch8"
ARTIFACT_ID = "orch8-mobile"
REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_GRADLE = REPO_ROOT / "packages/android/orch8-mobile/build.gradle.kts"

DEPENDENCY_RE = re.compile(
    r'^\s*implementation\("(?P<group>[^:"]+):(?P<artifact>[^:"]+):(?P<version>[^@"]+)(?:@(?P<type>[^"]+))?"\)',
    re.MULTILINE,
)
VERSION_RE = re.compile(r"^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$")


def runtime_dependencies(gradle_file: Path) -> list[dict[str, str | None]]:
    deps = [m.groupdict() for m in DEPENDENCY_RE.finditer(gradle_file.read_text())]
    if not deps:
        sys.exit(f"error: no implementation(...) dependencies found in {gradle_file}")
    return deps


def render_pom(version: str, deps: list[dict[str, str | None]]) -> str:
    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<project xmlns="http://maven.apache.org/POM/4.0.0"',
        '         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"',
        '         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">',
        "  <modelVersion>4.0.0</modelVersion>",
        f"  <groupId>{GROUP_ID}</groupId>",
        f"  <artifactId>{ARTIFACT_ID}</artifactId>",
        f"  <version>{version}</version>",
        "  <packaging>aar</packaging>",
        "  <name>Orch8 Mobile</name>",
        "  <description>Embedded durable workflow runtime for Android</description>",
        "  <url>https://github.com/orch8-io/engine</url>",
        "  <licenses>",
        "    <license>",
        "      <name>Business Source License 1.1</name>",
        "      <url>https://github.com/orch8-io/engine/blob/main/LICENSE</url>",
        "      <distribution>repo</distribution>",
        "    </license>",
        "  </licenses>",
        "  <dependencies>",
    ]
    for dep in deps:
        lines += [
            "    <dependency>",
            f"      <groupId>{dep['group']}</groupId>",
            f"      <artifactId>{dep['artifact']}</artifactId>",
            f"      <version>{dep['version']}</version>",
        ]
        if dep["type"]:
            lines.append(f"      <type>{dep['type']}</type>")
        lines += ["      <scope>runtime</scope>", "    </dependency>"]
    lines += ["  </dependencies>", "</project>", ""]
    return "\n".join(lines)


def write_with_checksums(path: Path, data: bytes) -> None:
    path.write_bytes(data)
    for algo in ("md5", "sha1", "sha256"):
        Path(f"{path}.{algo}").write_text(hashlib.new(algo, data).hexdigest())


def version_key(version: str) -> tuple:
    core, _, pre = version.partition("-")
    nums = tuple(int(p) for p in core.split("."))
    # A release sorts after its prereleases.
    return nums + ((1, "") if not pre else (0, pre))


def update_metadata(artifact_dir: Path) -> None:
    versions = sorted(
        (p.name for p in artifact_dir.iterdir() if p.is_dir() and VERSION_RE.match(p.name)),
        key=version_key,
    )
    releases = [v for v in versions if "-" not in v]
    root = ET.Element("metadata")
    ET.SubElement(root, "groupId").text = GROUP_ID
    ET.SubElement(root, "artifactId").text = ARTIFACT_ID
    versioning = ET.SubElement(root, "versioning")
    ET.SubElement(versioning, "latest").text = versions[-1]
    if releases:
        ET.SubElement(versioning, "release").text = releases[-1]
    versions_el = ET.SubElement(versioning, "versions")
    for v in versions:
        ET.SubElement(versions_el, "version").text = v
    ET.SubElement(versioning, "lastUpdated").text = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    ET.indent(root, space="  ")
    body = '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(root, encoding="unicode") + "\n"
    write_with_checksums(artifact_dir / "maven-metadata.xml", body.encode())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--version", required=True, help="version without a leading v, e.g. 0.7.2")
    parser.add_argument("--repo", type=Path, help="checkout of orch8-io/maven")
    parser.add_argument("--aar", type=Path, help="orch8-mobile release AAR (from the engine GitHub release)")
    parser.add_argument("--gradle", type=Path, default=DEFAULT_GRADLE)
    parser.add_argument("--print-pom", action="store_true", help="print the POM and exit")
    args = parser.parse_args()

    version = args.version.removeprefix("v")
    if not VERSION_RE.match(version):
        sys.exit(f"error: {version!r} is not a semantic version")
    pom = render_pom(version, runtime_dependencies(args.gradle)).encode()
    if args.print_pom:
        sys.stdout.write(pom.decode())
        return 0
    if not args.repo or not args.aar:
        parser.error("--repo and --aar are required unless --print-pom is given")

    aar = args.aar.read_bytes()
    if aar[:4] != b"PK\x03\x04":
        sys.exit(f"error: {args.aar} is not an AAR (zip) file")

    artifact_dir = args.repo / GROUP_ID.replace(".", "/") / ARTIFACT_ID
    version_dir = artifact_dir / version
    aar_path = version_dir / f"{ARTIFACT_ID}-{version}.aar"
    pom_path = version_dir / f"{ARTIFACT_ID}-{version}.pom"

    if version_dir.exists():
        existing_aar = aar_path.read_bytes() if aar_path.exists() else None
        if existing_aar == aar:
            print(f"{GROUP_ID}:{ARTIFACT_ID}:{version} is already published with identical bytes; nothing to do.")
            return 0
        sys.exit(
            f"error: {GROUP_ID}:{ARTIFACT_ID}:{version} is already published with different bytes. "
            "Published versions are immutable; release a new version instead."
        )

    version_dir.mkdir(parents=True)
    write_with_checksums(aar_path, aar)
    write_with_checksums(pom_path, pom)
    update_metadata(artifact_dir)
    print(f"Published {GROUP_ID}:{ARTIFACT_ID}:{version} to {args.repo}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
