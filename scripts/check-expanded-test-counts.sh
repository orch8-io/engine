#!/usr/bin/env bash
set -euo pipefail

unit_files=(
  orch8-api/src/client_contract_boundary_tests.rs
  orch8-api/src/entitlements_boundary_tests.rs
  orch8-engine/src/memory_governance_boundary_tests.rs
  orch8-engine/src/optimizer_boundary_tests.rs
  orch8-mobile/src/capabilities_boundary_tests.rs
  orch8-mobile/src/privacy_boundary_tests.rs
  orch8-publisher/src/distribution_boundary_tests.rs
  orch8-push/src/governance_boundary_tests.rs
)

e2e_files=(
  tests/e2e/features/compiled_plan_input_types.test.ts
  tests/e2e/features/compiled_plan_input_failures.test.ts
  tests/e2e/features/compiled_plan_producer_failures.test.ts
  tests/e2e/features/compiled_plan_output_types.test.ts
  tests/e2e/features/compiled_plan_unknown_roots.test.ts
)

unit_pattern='^[[:space:]]*(validation_case|catalog_case|fallback_case|escape_case|generated_contains_case|namespace_case|residency_case|policy_case|guard_case|identity_case|handler_case|descriptor_case|execute_case|rejected_case|sanitize_case|route_hit_case|route_miss_case|invalid_route_case|wake_verify_case|collapse_key_difference_case|path_case|hash_case|requirement_case)!|^[[:space:]]*(async )?fn coverage_'
unit_count="$(grep -hEc "$unit_pattern" "${unit_files[@]}" | awk '{ total += $1 } END { print total + 0 }')"
e2e_count="$(grep -hEc '^[[:space:]]*it\(' "${e2e_files[@]}" | awk '{ total += $1 } END { print total + 0 }')"

if [[ "$unit_count" -ne 400 ]]; then
  echo "expected exactly 400 added Rust unit tests, found $unit_count" >&2
  exit 1
fi

if [[ "$e2e_count" -ne 150 ]]; then
  echo "expected exactly 150 added Node E2E tests, found $e2e_count" >&2
  exit 1
fi

echo "expanded test counts verified: $unit_count Rust unit tests, $e2e_count Node E2E tests"
