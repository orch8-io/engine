# Multi-region active-passive failover

> **Stability: beta**, shipped and tested; may change in a minor release with a changelog note.

Orch8 supports **active-passive** operation across regions: exactly one region's engines run the scheduler, and a standby region takes over when you promote it. The engine provides the **fence**: who is allowed to run, and a procedure that retires the old region before the new one starts. It does **not** replicate data.

> **Replication is your PostgreSQL responsibility.** Orch8 has no cross-region data path. Use streaming replication, a managed cross-region replica (Aurora Global Database, Cloud SQL cross-region replica, Azure geo-replica), or equivalent. Your replication setup determines your RPO. The fence determines who is allowed to write after a failover.

---

## Mechanism

- The database holds a singleton **region fence**: `{active_region, epoch}`. Every promotion is a compare-and-swap that increases `epoch` by exactly one. Two operators, or a script retry, cannot both win.
- Every engine node sets `ORCH8_FAILOVER_REGION=<region>`. `ORCH8_FAILOVER_POLL_SECS` is optional (default 5, range 1–60).
  - **Standby:** a node whose region is not active serves `/health/live`, reports `/health/ready` = 503, and **does not run the scheduler**, triggers, cron, or the federation poller. It re-reads the fence every poll interval. With no fence installed yet, every node stays in standby.
  - **Active:** once its region holds the fence, the node starts the engine at that epoch.
  - **Self-fencing:** an active node re-reads the fence every poll interval. If the fence names another region or a newer epoch, the node cancels its shutdown token and the whole process exits. Your orchestrator restarts it, and it comes back as a standby. The node also exits if it **cannot read the fence for 3 poll intervals** (fail closed: a node that cannot prove it is active stops claiming work).
- In-flight side effects are guarded by the existing **effect ledger**. A step whose effect receipt was `dispatched` when the old region stopped is not blindly re-run by the new region: its receipt resolves to `unknown`, and the step's retry policy or an operator decides (see [CONTINUITY_OPERATIONS](CONTINUITY_OPERATIONS.md)). Worker-task leases are fenced by claim epoch and continuity ownership epoch in the same way.

Nodes without `ORCH8_FAILOVER_REGION` ignore the fence. Set it on **every** node of a failover deployment, or a node without it will run in any region.

## Procedure

Run the CLI against databases directly, because the API may be unavailable during a failover.

```bash
# 0. One-time install (primary region "eu"), before starting the fleet:
orch8 failover promote --database-url "$EU_PRIMARY_URL" --region eu --reason "initial"

# Inspect at any time:
orch8 failover status --database-url "$URL"
```

**Planned switchover (eu → us), zero data loss:**

1. `orch8 failover promote --database-url "$EU_PRIMARY_URL" --region fenced --expect-epoch N --reason "planned switchover"`
   `fenced` is a placeholder region that no node uses. The eu engines see epoch N+1 within one poll interval and exit. us engines keep waiting in standby.
2. Wait until replication lag is 0 (the fence row and all work have reached the us replica).
3. Promote the us replica to primary (a PostgreSQL / provider operation).
4. `orch8 failover promote --database-url "$US_PRIMARY_URL" --region us --expect-epoch N+1 --reason "planned"`
   us engines start within one poll interval. Instances that were `running` in eu are reclaimed after `stale_instance_threshold_secs`.

Promote the fence to `us` only **after** the database promotion. Otherwise us engines would start against a read-only replica.

**Unplanned failover (eu lost):**

1. **Fence the old region first.**
   - If the eu primary is still reachable, run `orch8 failover promote --database-url "$EU_PRIMARY_URL" --region fenced --expect-epoch N`. The eu engines stop within one poll interval.
   - If it is not reachable, make sure the eu engines **cannot reach any primary**: scale them to zero, cut their network, or apply STONITH at your provider. If the eu engines can still write to the eu database and a later reconciliation merges the two, work will be duplicated. The fence is cooperative: a node honors it only when it can read it.
2. Promote the us replica to primary (a PostgreSQL / provider operation).
3. `orch8 failover promote --database-url "$US_PRIMARY_URL" --region us --expect-epoch M --reason "eu outage"`, where `M` is the epoch `status` prints on the promoted database. This is `N`, or `N+1` if the step 1 fence replicated before the link broke.
4. us engines start within one poll interval.
5. Review effect receipts in the `unknown` state (`orch8 execution …` / continuity operations), and resolve ambiguous side effects with the provider's idempotency keys.
6. **Before bringing eu back:** rebuild the eu database as a replica of us. Never restart eu engines against the old eu primary. If they start against it, the `fenced` stamp from step 1 keeps them in standby; without that stamp, they would run.

## RPO and RTO

These figures follow from the actual mechanism with default settings. Your replication setup can change RPO, and the timeouts below bound RTO.

| | Value | Why |
|---|---|---|
| **RPO (planned switchover)** | 0 | You wait for replication lag to reach 0 before promoting. |
| **RPO (unplanned, async replication)** | Replication lag at the moment of loss, typically seconds | Orch8 commits scheduling state and effect receipts transactionally in PostgreSQL. What the replica never received is lost: steps completed in that window re-run on the new region, **except** where a replicated receipt records a dispatched effect. |
| **RPO (unplanned, synchronous replication)** | 0 for committed transactions | Depends on your PostgreSQL `synchronous_commit` / provider settings. |
| **RTO (engine part)** | ≈ detection + DB promotion + ≤ 1 poll interval (5 s) + engine start | Standby engines are already running and polling the fence. Scheduled and waiting instances resume on the first tick after promotion. |
| **RTO for instances that were mid-step** | + up to `stale_instance_threshold_secs` (default 300 s) | A `running` instance is re-queued by the stale-instance reaper once its heartbeat is older than the threshold. Lower the threshold to shorten this, at the cost of more false positives for slow steps. |
| **RTO for external worker tasks** | + up to `worker_reaper_stale_secs` (default 60 s) | Leases expire, then a side-effecting task becomes `unknown` rather than being re-dispatched. |
| **Split-brain window (reachable old primary)** | ≤ 1 poll interval + in-flight step drain | Old-region nodes exit within one poll after the fence moves. Handlers already executing may finish their current step during graceful shutdown. |
| **Split-brain window (unreachable old primary)** | Unbounded unless you fence at the infrastructure level | See step 1 of the unplanned procedure. |

Detection is not automated. Orch8 does not decide that a region is dead, because an automated decision made during a partition is how split-brain happens. Wire promotion to your own health checks or on-call runbook.

## What is not included

- No built-in data replication and no multi-primary (active-active) mode.
- No automatic failure detection or promotion.
- A standby node does not accept writes through its API, because its database is a read-only replica. Point clients at the active region (DNS / global load balancer), and use `/health/ready` for routing: it is 503 on standby nodes.
- Artifacts (`[artifacts]`) and BYOK vault objects live in object storage. Replicate those buckets with your provider's cross-region replication; the fence does not cover them.
