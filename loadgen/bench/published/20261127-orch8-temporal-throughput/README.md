# Orch8 0.7.1 and Temporal 1.25.2 throughput

Six trials ran sequentially on the same 2-vCPU GitHub-hosted runner, with
PostgreSQL 16 and fresh state for each trial. Each trial ran 1,000 measured
workflows, 50 warmup workflows, and at most 100 workflows in flight. Each
workflow executes three sequential 50 ms activities. Temporal used its
all-in-one auto-setup server image and Node SDK 1.27.0.

| System | Trial | Completed | Workflows/sec | Server p99 |
| --- | ---: | ---: | ---: | ---: |
| Orch8 | 1 | 1,000 | 35.56 | 3,830 ms |
| Orch8 | 2 | 1,000 | 37.33 | 3,438 ms |
| Orch8 | 3 | 1,000 | 35.78 | 3,503 ms |
| Temporal | 1 | 1,000 | 15.20 | 11,757 ms |
| Temporal | 2 | 1,000 | 16.18 | 9,523 ms |
| Temporal | 3 | 1,000 | 15.17 | 11,760 ms |

The test measures these exact configurations, versions, and workload.
Orch8 ran first, so the test does not control for ordering. The drivers use
each system's native interfaces. The server timestamps supply latency and
the driver-observed completion window supplies throughput. This series
covers throughput on one host.

The summaries record the harness, hardware, image identities, and SDK
versions. Decompress the JSONL files in each directory to read the raw
samples and activity events. The [source Actions run](https://github.com/orch8-io/web/actions/runs/46031662072)
also records the stack setup.
