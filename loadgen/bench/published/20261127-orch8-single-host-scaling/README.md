# Orch8 0.7.1 single-host executor scaling

All six trials ran sequentially on one 2-vCPU GitHub-hosted runner with
PostgreSQL 16, one control node, and four REST workers. Each trial measured
1,000 workflows with up to 100 in flight after 50 warmup workflows. Each
workflow ran three sequential activities with a 50 ms delay per activity.
The measured driver deadline was 300 seconds. This tests executor scaling
under resource contention on one host.

| Executors | Trial | Completed of 1,000 | Workflows/sec | Server p99 |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 989 | 8.66 | 65,413 ms |
| 1 | 2 | 976 | 9.45 | 12,371 ms |
| 1 | 3 | 980 | 31.55 | 11,801 ms |
| 3 | 1 | 939 | 14.23 | 31,010 ms |
| 3 | 2 | 920 | 7.22 | 48,538 ms |
| 3 | 3 | 903 | 5.19 | 61,993 ms |

Median throughput was 9.45 workflows/sec with one executor and 7.22 with
three. Latency is reported for completed workflows. Throughput uses the last
observed completion, so it excludes any further time spent waiting for
unfinished workflows. The configurations ran in sequence in a fixed order.
Use the completion counts and trial variation when interpreting the medians.

Each trial directory contains a summary with image digests and topology,
plus compressed raw workflow samples and a combined activity log.
See the [source GitHub Actions run](https://github.com/orch8-io/web/actions/runs/46021722658)
for the original artifacts, including per-worker logs.
