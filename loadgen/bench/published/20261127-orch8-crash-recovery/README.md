# Orch8 0.7.1 crash-recovery trials

The three trials in [GitHub Actions](https://github.com/orch8-io/web/actions/runs/46016924591)
used PostgreSQL 16 on one hosted runner. Each measured 500 workflows with up
to 50 in flight, after 50 warmup workflows. The runner sent SIGKILL to the
engine five seconds into measured work and waited five seconds before issuing
the restart. The workflow driver had a 600-second measured deadline.

| Trial | Completed | In flight at kill | Pre-kill submissions completed after restart | Duplicate activity executions | First observed completion after restart |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 500 | 50 | 48 | 21 | 863 ms |
| 2 | 497 | 50 | 47 | 0 | 961 ms |
| 3 | 500 | 50 | 50 | 22 | 1,086 ms |

Two other workflows in trial 2 reported terminal failure and one hit the
driver deadline. The time to first observed completion measures the first
completion after the restart command, across all measured submissions.
It does not measure the time to recover every interrupted workflow.
Decompress each trial's logs to read the filenames referenced by its result.
