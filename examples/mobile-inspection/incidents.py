#!/usr/bin/env python3
"""Run a repeatable mobile incident suite against a compiled SDK test harness."""

import argparse
import subprocess

SCENARIOS = {
    "process-death": "distributed_tests::reopen_after_kill_mid_native_step_never_replays_the_effect",
    "expired-session": "sync::catalog::tests::expired_identity_cancels_a_private_retry_wait_without_redispatch",
    "low-power": "worker::worker_tests::backgrounded_or_critical_battery_worker_does_not_claim",
    "missed-wake": "tick_controller::tests::quiescent_timer_wake_services_sync_without_scheduler_work",
    "offline-lease": "worker::worker_tests::renewal_outage_quarantines_the_live_native_callback_at_lease_expiry",
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("harness", help="orch8-mobile lib test executable printed by cargo test --no-run")
    parser.add_argument("scenario", nargs="*", choices=SCENARIOS)
    args = parser.parse_args()
    listed = subprocess.run([args.harness, "--list"], check=True, capture_output=True, text=True)
    available = {line.removesuffix(": test") for line in listed.stdout.splitlines() if line.endswith(": test")}
    selected = args.scenario or list(SCENARIOS)
    missing = [name for name in selected if SCENARIOS[name] not in available]
    if missing:
        parser.error("test fixture missing from the harness: " + ", ".join(missing))
    for name in selected:
        print(f"Running {name}", flush=True)
        subprocess.run([args.harness, "--exact", SCENARIOS[name]], check=True)


if __name__ == "__main__":
    main()
