# Engine adoption changeset technical review

The Deep review sampled mobile recovery and delegation, worker admission and rollouts, replay export, and the dashboard controls that use those APIs. It used the current worktree and sampled Rust, SQL, TypeScript, Swift, Kotlin, and CI entry points. The review covered code quality, type and API contracts, Rust ownership and concurrency, architecture and event ordering, release behavior, operator UX, and dashboard evidence. BigQuery review was outside this changeset.

## Findings and fixes

| Priority | Finding | Resolution |
|---|---|---|
| High | A successful fleet preview could remain visible after the operator changed its inputs or staged a new version. A late response could place old evidence next to the new controls. | Bind the result to the exact request and observed rollout revisions. Invalidate after a successful rollout mutation. Abort superseded requests and identify which pin mode was evaluated. |
| Medium | Enrollment trimmed the tenant sent to the API but displayed the original form value beside the key. Copying that value could configure a different tenant string. | Render the normalized tenant and the API endpoint captured at issuance. Clear the displayed receipt when the operator finishes. |
| Medium | The npm audit reported seven moderate entries associated with one transitive advisory in dashboard lint tooling. The automated remediation proposes changing the parent tooling version, which requires compatibility validation. | Keep the advisory open for a tested tooling upgrade. [Minimatch advisory](https://github.com/advisories/GHSA-g3hv-75wg-m249) |

The highest-leverage change is to keep decision evidence tied to the inputs and rollout revision that produced it. Durable rollout compare-and-swap protects writes. The dashboard now keeps the read-side evidence consistent with the same operator action.

## Verification

- The worker browser regression delays an active-pin response, checks a staged-pin result, invalidates a ready result after a rollout write, and verifies the tenant shown at enrollment. Chromium intercepts API requests with deterministic fixtures.
- The replay browser regression downloads both files and invokes the real CLI. The release and worker API regressions run separately on real loopback HTTP.
- The dashboard production build reported a Workers JavaScript chunk of approximately 21 kB before compression. This is an asset measurement. Runtime latency and workspace-wide line coverage were outside the checks run here.
- The cached Rust advisory database reported zero vulnerabilities and one maintenance warning for a lockfile dependency. [RustSec maintenance advisory](https://rustsec.org/advisories/RUSTSEC-2026-0374.html)

Re-run the worker browser and API cases when rollout approval, pin enforcement, or readiness inputs change. Re-run the dependency audit after changing the dashboard lint-tool chain.
