#!/usr/bin/env python3
"""Check build prerequisites and optional offline mobile runtime declarations."""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys

from doctor_runtime import load_runtime_config, runtime_checks


ROOT = Path(__file__).resolve().parent


def output(*command: str) -> str:
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=20)
    except (OSError, subprocess.TimeoutExpired):
        return ""
    return result.stdout + result.stderr if result.returncode == 0 else ""


def release_pins() -> tuple[str | None, str | None]:
    ios = re.search(r"(?m)^    exactVersion: '([^']+)'$", (ROOT / "project.yml").read_text())
    android = re.search(
        r'getOrElse\("([^"]+)"\)', (ROOT / "android/app/build.gradle.kts").read_text()
    )
    return ios[1] if ios else None, android[1] if android else None


def ios_checks(test: bool) -> list[tuple[bool, str]]:
    xcode = re.search(r"(?m)^Xcode (\d+)", output("xcodebuild", "-version"))
    checks = [
        (bool(xcode and int(xcode[1]) >= 16), "Select Xcode 16 or later with xcode-select."),
        (bool(output("xcodegen", "--version")), "Install XcodeGen with brew install xcodegen."),
    ]
    if test:
        try:
            devices = json.loads(output("xcrun", "simctl", "list", "devices", "available", "-j"))["devices"]
        except (ValueError, KeyError):
            devices = {}
        checks.append((any(
            item["name"].startswith("iPhone")
            for runtime, items in devices.items() if ".iOS-" in runtime
            for item in items
        ), "Install an iOS simulator runtime and allow access to CoreSimulator."))
    return checks


def android_checks(test: bool) -> list[tuple[bool, str]]:
    default_sdk = Path.home() / ("Library/Android/sdk" if sys.platform == "darwin" else "Android/Sdk")
    sdk = Path(os.getenv("ANDROID_HOME") or os.getenv("ANDROID_SDK_ROOT") or default_sdk)
    java_home = os.environ.get("JAVA_HOME")
    java = str(Path(java_home) / "bin/java") if java_home else "java"
    checks = [
        (bool(re.search(r'version "17(?:\.|"|\+)', output(java, "-version"))), "Set JAVA_HOME to JDK 17."),
        (bool(re.search(r"(?m)^Gradle 8\.9(?:\.|$)", output("gradle", "--version"))),
         "Install Gradle 8.14.8 and put gradle on PATH."),
        ((sdk / "platforms/android-35/android.jar").is_file(), "Set ANDROID_HOME and install Android SDK Platform 35."),
    ]
    if test:
        adb = sdk / "platform-tools/adb"
        checks.append((any(line.endswith("\tdevice") for line in output(str(adb), "devices").splitlines()),
                       "Boot an emulator or connect an authorized Android device."))
    return checks


class DoctorParser(argparse.ArgumentParser):
    def error(self, _message: str) -> None:
        super().error("Invalid arguments. Run with --help for usage.")


def main() -> int:
    parser = DoctorParser(description=__doc__)
    parser.add_argument("platform", choices=["ios", "android", "all"])
    parser.add_argument("--test", action="store_true", help="also check simulator or device availability")
    parser.add_argument("--runtime-config", type=Path, help="report on a local, credential-free runtime configuration JSON file")
    parser.add_argument("--runtime-only", action="store_true", help="skip native build checks and run the runtime report only")
    args = parser.parse_args()
    if args.runtime_only and (args.runtime_config is None or args.test):
        parser.error("--runtime-only requires --runtime-config and cannot be combined with --test")

    build_ok = True if args.runtime_only else check_build(args.platform, args.test)
    runtime_ok = True if args.runtime_config is None else check_runtime(args.runtime_config, args.platform)
    return 0 if build_ok and runtime_ok else 1


def check_runtime(config_path: Path, platform: str) -> bool:
    print("Runtime integration (offline)")
    try:
        checks = runtime_checks(load_runtime_config(config_path), platform)
    except ValueError as error:
        print(f"  FAIL  {error}")
        return False
    for check in checks:
        print(f"  {check.status:<12} {check.label} · {check.summary}")
    return all(check.status != "FAIL" for check in checks)


def check_build(platform: str, test: bool) -> bool:
    ios, android = release_pins()
    aligned = bool(ios and ios == android and re.fullmatch(r"\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?", ios))
    if aligned:
        print(f"Pinned Orch8 release {ios}")
    else:
        print("Align the exact SDK release pins in project.yml and android/app/build.gradle.kts.")

    checks = []
    if platform in {"ios", "all"}:
        checks.extend(ios_checks(test))
    if platform in {"android", "all"}:
        checks.extend(android_checks(test))
    missing = [fix for ready, fix in checks if not ready]
    for fix in missing:
        print(f"  {fix}")
    if aligned and not missing:
        print("Local build prerequisites are available.")
        return True
    return False


if __name__ == "__main__":
    raise SystemExit(main())
