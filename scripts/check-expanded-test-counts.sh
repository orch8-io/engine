#!/usr/bin/env bash
set -euo pipefail

# Expanded-coverage gate.
#
# Verifies the two coverage waves stay intact:
#   - Rust unit tests: every file matching the coverage/boundary test naming
#     convention, counted as UNIQUE `coverage_<area>_NNN_*` test names
#     (macro-generated names appear exactly once at the invocation site, so
#     unique-name count == test count; verified against `cargo test -- --list`).
#   - Node E2E tests: `it(` cases in the expanded feature suites.
#
# Counts are exact on purpose: a silently dropped file or macro case fails CI.
# When you add more tests, bump the expected numbers in the same commit.

shopt -s nullglob

unit_files=(
  orch8-api/src/*_boundary_tests.rs
  orch8-api/src/*_coverage_tests.rs
  orch8-api/src/*_contract_tests.rs
  orch8-api/src/*_admission_tests.rs
  orch8-api/src/*_structure_tests.rs
  orch8-engine/src/*_boundary_tests.rs
  orch8-engine/src/*_coverage_tests.rs
  orch8-engine/src/evaluator/coverage_tests.rs
  orch8-engine/src/handlers/*_coverage_tests.rs
  orch8-grpc/src/service/*_coverage_tests.rs
  orch8-mobile/src/*_boundary_tests.rs
  orch8-mobile/src/*_coverage_tests.rs
  orch8-publisher/src/*_boundary_tests.rs
  orch8-publisher/src/*_coverage_tests.rs
  orch8-push/src/*_boundary_tests.rs
  orch8-push/src/*_coverage_tests.rs
  orch8-storage/src/*_coverage_tests.rs
  orch8-storage/src/sqlite/*_coverage_tests.rs
  orch8-storage/src/postgres/*_coverage_tests.rs
  orch8-server/src/*_coverage_tests.rs
  orch8-cli/src/commands/*_coverage_tests.rs
  orch8-types/src/*_coverage_tests.rs
  orch8/src/*_coverage_tests.rs
)

e2e_files=(
  tests/e2e/features/compiled_plan_input_types.test.ts
  tests/e2e/features/compiled_plan_input_failures.test.ts
  tests/e2e/features/compiled_plan_producer_failures.test.ts
  tests/e2e/features/compiled_plan_output_types.test.ts
  tests/e2e/features/compiled_plan_unknown_roots.test.ts
  tests/e2e/features/entitlements_admission.test.ts
  tests/e2e/features/entitlements_batch_limits.test.ts
  tests/e2e/features/principals_key_management.test.ts
  tests/e2e/features/principals_worker.test.ts
  tests/e2e/features/principals_device.test.ts
  tests/e2e/features/principals_publisher.test.ts
  tests/e2e/features/principals_approver.test.ts
  tests/e2e/features/principals_auditor.test.ts
  tests/e2e/features/principals_operator.test.ts
  tests/e2e/features/changefeed_basics.test.ts
  tests/e2e/features/changefeed_tenancy.test.ts
  tests/e2e/features/changefeed_stream.test.ts
  tests/e2e/features/doctor_diagnosis.test.ts
  tests/e2e/features/doctor_remediations_preview.test.ts
  tests/e2e/features/doctor_remediations_apply.test.ts
  tests/e2e/features/pushwake_outbox_persistence.test.ts
  tests/e2e/features/pushwake_outbox_lifecycle.test.ts
  tests/e2e/features/pushwake_tenant_isolation.test.ts
  tests/e2e/features/mobilesync_commands_sync.test.ts
  tests/e2e/features/mobilesync_devices_runtime.test.ts
  tests/e2e/features/mobilesync_telemetry.test.ts
  tests/e2e/features/plansem_composite_blocks.test.ts
  tests/e2e/features/plansem_nesting_budgets.test.ts
  tests/e2e/features/plansem_equivalence.test.ts
  tests/e2e/features/plansem_plugin_dispatch.test.ts
  tests/e2e/features/plansem_multi_root.test.ts
)

unit_count="$(grep -ohE 'coverage_[a-z0-9_]+' "${unit_files[@]}" | sort -u | wc -l | tr -d ' ')"
e2e_count="$(grep -hEc '^[[:space:]]*it\(' "${e2e_files[@]}" | awk '{ total += $1 } END { print total + 0 }')"

if [[ "$unit_count" -ne 1936 ]]; then
  echo "expected exactly 1936 expanded Rust unit tests, found $unit_count" >&2
  exit 1
fi

if [[ "$e2e_count" -ne 412 ]]; then
  echo "expected exactly 412 expanded Node E2E tests, found $e2e_count" >&2
  exit 1
fi

echo "expanded test counts verified: $unit_count Rust unit tests, $e2e_count Node E2E tests"
