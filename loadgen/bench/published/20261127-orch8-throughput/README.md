# Orch8 0.7.1 and PostgreSQL 16 throughput trials

These three single-node trials ran in sequence on the same GitHub-hosted Ubuntu runner on November 27, 2026. The [source workflow run](https://github.com/orch8-io/web/actions/runs/46013519151) timed out after all throughput trials had finished, during a separate crash-recovery series.

Each throughput trial used 1,000 measured workflows, 50 warmup workflows, up to 100 workflows in flight, and three sequential 50 ms activities per workflow. Each result records the harness commit, image digests, host details, and timing source. Decompress the two JSONL files in each trial directory to open the filenames referenced by its result.

| Trial | Completed | Workflows/sec | Server p50 | Server p95 | Server p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 1,000 | 35.12 | 2,569 ms | 3,775 ms | 4,134 ms |
| 2 | 1,000 | 36.00 | 2,596 ms | 3,834 ms | 3,954 ms |
| 3 | 1,000 | 36.05 | 2,632 ms | 3,280 ms | 3,851 ms |

The median throughput was 36.00 workflows per second on this runner and workload. All workflows finished. The raw sample files contain measured workflows. The activity logs also include the warmup steps.
