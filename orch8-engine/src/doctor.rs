//! Stuck Instance Doctor: pure diagnostic rules over a collected
//! [`InstanceDiagnosticContext`].
//!
//! The API layer collects evidence (instance, tree, signals, worker
//! tasks, registrations, breakers, children) and these rules turn it
//! into a ranked [`InstanceDiagnosisReport`]. Rules never touch storage
//! and never mutate anything — recovery actions are described as
//! remediations and require their own authorized calls.
//!
//! Evidence sections that were not collected (`None`) degrade the
//! affected rules to lower confidence or produce an explicit
//! `EVIDENCE_INCOMPLETE` warning; they never silently pass.

use chrono::{DateTime, Duration, Utc};

use orch8_types::circuit_breaker::{BreakerState, CircuitBreakerState};
use orch8_types::diagnosis::{
    Diagnosis, DiagnosisCategory, DiagnosisHealth, InstanceDiagnosisReport, rank_diagnoses,
};
use orch8_types::finding::{Confidence, Evidence, Finding, FindingSeverity, Remediation};
use orch8_types::instance::{InstanceState, TaskInstance};
use orch8_types::signal::Signal;
use orch8_types::worker::{
    WorkerRegistration, WorkerTask, WorkerTaskState, WorkerVersionPin, version_satisfies,
};

/// A claimed worker task whose heartbeat is older than this is stale.
pub const STALE_HEARTBEAT_SECS: i64 = 120;
/// A pending worker task older than this suggests no worker is polling.
pub const PENDING_TASK_ALARM_SECS: i64 = 120;
/// A `Running` instance not updated for this long is presumed crashed.
pub const STALE_RUNNING_SECS: i64 = 600;
/// A `Scheduled` instance overdue by this much suggests scheduler lag.
pub const SCHEDULER_LAG_SECS: i64 = 300;

/// Everything the rules may look at. Each `Option` section is `None`
/// when the collector could not (or chose not to) gather it.
#[derive(Debug, Clone)]
pub struct InstanceDiagnosticContext {
    pub instance: TaskInstance,
    /// Whether the instance's sequence still exists.
    pub sequence_exists: Option<bool>,
    /// Pending (undelivered) signals for the instance.
    pub pending_signals: Option<Vec<Signal>>,
    /// Worker tasks belonging to this instance.
    pub worker_tasks: Option<Vec<WorkerTask>>,
    /// Live worker registrations (collector applies the liveness window).
    pub worker_registrations: Option<Vec<WorkerRegistration>>,
    /// Worker version pins for the tenant.
    pub version_pins: Option<Vec<WorkerVersionPin>>,
    /// Currently open circuit breakers (all tenants; rules filter).
    pub open_breakers: Option<Vec<CircuitBreakerState>>,
    /// Child instances spawned by this instance.
    pub children: Option<Vec<TaskInstance>>,
    /// Block ids currently waiting for human input.
    pub pending_approval_blocks: Option<Vec<String>>,
}

impl InstanceDiagnosticContext {
    /// A context with only the instance; everything else uncollected.
    #[must_use]
    pub fn new(instance: TaskInstance) -> Self {
        Self {
            instance,
            sequence_exists: None,
            pending_signals: None,
            worker_tasks: None,
            worker_registrations: None,
            version_pins: None,
            open_breakers: None,
            children: None,
            pending_approval_blocks: None,
        }
    }
}

/// Run every diagnostic rule and return the ranked report.
#[must_use]
pub fn diagnose(ctx: &InstanceDiagnosticContext, now: DateTime<Utc>) -> InstanceDiagnosisReport {
    let mut diagnoses: Vec<Diagnosis> = Vec::new();
    let instance = &ctx.instance;

    if instance.state.is_terminal() {
        diagnoses.push(terminal_diagnosis(instance, now));
    } else {
        rule_sequence_missing(ctx, now, &mut diagnoses);
        rule_waiting_until(instance, now, &mut diagnoses);
        rule_pending_approval(ctx, now, &mut diagnoses);
        rule_paused(instance, now, &mut diagnoses);
        rule_worker_tasks(ctx, now, &mut diagnoses);
        rule_open_breaker(ctx, now, &mut diagnoses);
        rule_children(ctx, now, &mut diagnoses);
        rule_undelivered_signals(ctx, now, &mut diagnoses);
        rule_state_inconsistencies(instance, now, &mut diagnoses);
        rule_waiting_external(ctx, now, &mut diagnoses);

        if diagnoses.is_empty() {
            diagnoses.push(Diagnosis {
                category: DiagnosisCategory::HealthWarning,
                health: DiagnosisHealth::Expected,
                finding: Finding::new(
                    "NO_BLOCKER_FOUND",
                    FindingSeverity::Info,
                    "no blocking condition identified — the instance appears to be \
                     progressing normally",
                    Confidence::Low,
                    now,
                ),
            });
        }
        note_incomplete_evidence(ctx, now, &mut diagnoses);
    }

    rank_diagnoses(&mut diagnoses);
    InstanceDiagnosisReport {
        instance_id: instance.id.into_uuid(),
        state: instance.state.to_string(),
        diagnoses,
        generated_at: now,
    }
}

fn terminal_diagnosis(instance: &TaskInstance, now: DateTime<Utc>) -> Diagnosis {
    let mut finding = Finding::new(
        "TERMINAL_STATE",
        FindingSeverity::Info,
        format!(
            "instance is {} — terminal states do not progress",
            instance.state
        ),
        Confidence::Certain,
        now,
    );
    if instance.state == InstanceState::Failed {
        finding = finding.with_remediation(
            Remediation::new("retry the instance from the DLQ")
                .with_command(format!("orch8 instance retry {}", instance.id))
                .with_side_effect_risk(),
        );
    }
    Diagnosis {
        category: DiagnosisCategory::DirectEvidence,
        health: DiagnosisHealth::Expected,
        finding,
    }
}

fn rule_sequence_missing(
    ctx: &InstanceDiagnosticContext,
    now: DateTime<Utc>,
    out: &mut Vec<Diagnosis>,
) {
    if ctx.sequence_exists == Some(false) {
        out.push(Diagnosis {
            category: DiagnosisCategory::DirectEvidence,
            health: DiagnosisHealth::Inconsistent,
            finding: Finding::new(
                "SEQUENCE_MISSING",
                FindingSeverity::Critical,
                format!(
                    "the instance references sequence {}, which no longer exists — \
                     it can never progress",
                    ctx.instance.sequence_id.into_uuid()
                ),
                Confidence::Certain,
                now,
            ),
        });
    }
}

fn rule_waiting_until(instance: &TaskInstance, now: DateTime<Utc>, out: &mut Vec<Diagnosis>) {
    if instance.state == InstanceState::Paused {
        return; // paused instances hold regardless of timers
    }
    if let Some(fire_at) = instance.next_fire_at
        && fire_at > now
    {
        let wait = fire_at - now;
        out.push(Diagnosis {
            category: DiagnosisCategory::DirectEvidence,
            health: DiagnosisHealth::Expected,
            finding: Finding::new(
                "WAITING_UNTIL",
                FindingSeverity::Info,
                format!(
                    "the instance is waiting for a timer that fires at {fire_at} \
                     (in {})",
                    humanize(wait)
                ),
                Confidence::Certain,
                now,
            )
            .with_evidence(
                Evidence::new("next_fire_at", fire_at.to_rfc3339()).observed_at(now),
            ),
        });
    }
}

fn rule_pending_approval(
    ctx: &InstanceDiagnosticContext,
    now: DateTime<Utc>,
    out: &mut Vec<Diagnosis>,
) {
    let Some(blocks) = &ctx.pending_approval_blocks else {
        return;
    };
    for block in blocks {
        out.push(Diagnosis {
            category: DiagnosisCategory::DirectEvidence,
            health: DiagnosisHealth::Expected,
            finding: Finding::new(
                "PENDING_APPROVAL",
                FindingSeverity::Info,
                format!("block '{block}' is waiting for human input"),
                Confidence::Certain,
                now,
            )
            .with_remediation(
                Remediation::new("resolve the approval").with_command(format!(
                    "orch8 signal {} custom:human_input:{block} '{{\"value\": \"yes\"}}'",
                    ctx.instance.id
                )),
            ),
        });
    }
}

fn rule_paused(instance: &TaskInstance, now: DateTime<Utc>, out: &mut Vec<Diagnosis>) {
    if instance.state != InstanceState::Paused {
        return;
    }
    let paused_reason = instance
        .metadata
        .get("paused_reason")
        .and_then(serde_json::Value::as_str);
    if paused_reason == Some("budget_exceeded") {
        let mut finding = Finding::new(
            "BUDGET_PAUSED",
            FindingSeverity::Warning,
            "the instance exhausted its resource budget and was paused by policy",
            Confidence::Certain,
            now,
        )
        .with_remediation(
            Remediation::new("raise the budget, then resume").with_command(format!(
                "orch8 signal {} resume",
                instance.id
            )),
        );
        if let Some(breach) = instance.metadata.get("budget_breach") {
            finding = finding
                .with_evidence(Evidence::new("budget_breach", breach.to_string()).observed_at(now));
        }
        out.push(Diagnosis {
            category: DiagnosisCategory::DirectEvidence,
            health: DiagnosisHealth::Expected,
            finding,
        });
    } else {
        out.push(Diagnosis {
            category: DiagnosisCategory::DirectEvidence,
            health: DiagnosisHealth::Expected,
            finding: Finding::new(
                "PAUSED",
                FindingSeverity::Info,
                "the instance was paused by an operator or signal and will not \
                 progress until resumed",
                Confidence::Certain,
                now,
            )
            .with_remediation(
                Remediation::new("resume the instance")
                    .with_command(format!("orch8 signal {} resume", instance.id)),
            ),
        });
    }
}

fn rule_worker_tasks(
    ctx: &InstanceDiagnosticContext,
    now: DateTime<Utc>,
    out: &mut Vec<Diagnosis>,
) {
    let Some(tasks) = &ctx.worker_tasks else {
        return;
    };
    let tenant = ctx.instance.tenant_id.as_str();

    for task in tasks {
        match task.state {
            WorkerTaskState::Pending => {
                let Some(registrations) = &ctx.worker_registrations else {
                    out.push(Diagnosis {
                        category: DiagnosisCategory::ProbableCause,
                        health: DiagnosisHealth::Degraded,
                        finding: Finding::new(
                            "WORKER_TASK_PENDING",
                            FindingSeverity::Warning,
                            format!(
                                "block '{}' is queued for external handler '{}' but \
                                 worker liveness could not be checked",
                                task.block_id.as_str(),
                                task.handler_name
                            ),
                            Confidence::Low,
                            now,
                        ),
                    });
                    continue;
                };
                let matching: Vec<&WorkerRegistration> = registrations
                    .iter()
                    .filter(|r| {
                        r.handler_name == task.handler_name
                            && r.tenant_id
                                .as_deref()
                                .is_none_or(|t| t.is_empty() || t == tenant)
                    })
                    .collect();
                if matching.is_empty() {
                    out.push(no_compatible_worker(task, now));
                    continue;
                }
                let pin = ctx.version_pins.as_ref().and_then(|pins| {
                    pins.iter()
                        .find(|p| p.handler_name == task.handler_name && p.tenant_id == tenant)
                });
                if let Some(pin) = pin
                    && !matching
                        .iter()
                        .any(|r| version_satisfies(r.version.as_deref(), &pin.min_version))
                {
                    let seen: Vec<String> = matching
                        .iter()
                        .map(|r| {
                            format!(
                                "{}@{}",
                                r.worker_id,
                                r.version.as_deref().unwrap_or("unversioned")
                            )
                        })
                        .collect();
                    out.push(Diagnosis {
                        category: DiagnosisCategory::DirectEvidence,
                        health: DiagnosisHealth::Degraded,
                        finding: Finding::new(
                            "WORKER_BELOW_VERSION_PIN",
                            FindingSeverity::Error,
                            format!(
                                "block '{}' needs handler '{}' at version >= {}, but no \
                                 live worker satisfies the pin",
                                task.block_id.as_str(),
                                task.handler_name,
                                pin.min_version
                            ),
                            Confidence::High,
                            now,
                        )
                        .with_evidence(
                            Evidence::new("live_workers", seen.join(", ")).observed_at(now),
                        )
                        .with_remediation(Remediation::new(
                            "upgrade the worker or relax the version pin",
                        )),
                    });
                    continue;
                }
                // Worker exists; how long has the task waited?
                let age = now - task.created_at;
                if age > Duration::seconds(PENDING_TASK_ALARM_SECS) {
                    out.push(Diagnosis {
                        category: DiagnosisCategory::DirectEvidence,
                        health: DiagnosisHealth::Degraded,
                        finding: Finding::new(
                            "WORKER_NOT_CLAIMING",
                            FindingSeverity::Warning,
                            format!(
                                "block '{}' has been queued for handler '{}' for {} even \
                                 though a live worker is registered — the worker may be \
                                 polling a different queue ({})",
                                task.block_id.as_str(),
                                task.handler_name,
                                humanize(age),
                                task.queue_name.as_deref().unwrap_or("default")
                            ),
                            Confidence::Medium,
                            now,
                        )
                        .with_evidence(
                            Evidence::new("task_created_at", task.created_at.to_rfc3339())
                                .observed_at(now),
                        ),
                    });
                } else {
                    out.push(Diagnosis {
                        category: DiagnosisCategory::ProbableCause,
                        health: DiagnosisHealth::Expected,
                        finding: Finding::new(
                            "WAITING_WORKER_PICKUP",
                            FindingSeverity::Info,
                            format!(
                                "block '{}' is queued for handler '{}' and a live worker \
                                 is registered — pickup expected shortly",
                                task.block_id.as_str(),
                                task.handler_name
                            ),
                            Confidence::Medium,
                            now,
                        ),
                    });
                }
            }
            WorkerTaskState::Claimed => {
                let last_beat = task.heartbeat_at.or(task.claimed_at);
                if let Some(beat) = last_beat
                    && now - beat > Duration::seconds(STALE_HEARTBEAT_SECS)
                {
                    out.push(Diagnosis {
                        category: DiagnosisCategory::DirectEvidence,
                        health: DiagnosisHealth::Degraded,
                        finding: Finding::new(
                            "STALE_WORKER_CLAIM",
                            FindingSeverity::Error,
                            format!(
                                "block '{}' was claimed by worker '{}' but its last \
                                 heartbeat was {} ago — the worker likely died",
                                task.block_id.as_str(),
                                task.worker_id.as_deref().unwrap_or("?"),
                                humanize(now - beat)
                            ),
                            Confidence::High,
                            now,
                        )
                        .with_evidence(
                            Evidence::new("last_heartbeat", beat.to_rfc3339()).observed_at(now),
                        )
                        .with_remediation(
                            Remediation::new(
                                "requeue stale tasks so another worker can claim them",
                            )
                            .with_side_effect_risk(),
                        ),
                    });
                }
            }
            // Completed / Failed tasks (and future states) don't block.
            _ => {}
        }
    }
}

fn no_compatible_worker(task: &WorkerTask, now: DateTime<Utc>) -> Diagnosis {
    Diagnosis {
        category: DiagnosisCategory::DirectEvidence,
        health: DiagnosisHealth::Degraded,
        finding: Finding::new(
            "NO_COMPATIBLE_WORKER",
            FindingSeverity::Error,
            format!(
                "block '{}' is queued for external handler '{}' (queue: {}) but no \
                 live worker is registered for it",
                task.block_id.as_str(),
                task.handler_name,
                task.queue_name.as_deref().unwrap_or("default")
            ),
            Confidence::High,
            now,
        )
        .with_remediation(Remediation::new(
            "start a worker that registers this handler",
        )),
    }
}

fn rule_open_breaker(
    ctx: &InstanceDiagnosticContext,
    now: DateTime<Utc>,
    out: &mut Vec<Diagnosis>,
) {
    let Some(breakers) = &ctx.open_breakers else {
        return;
    };
    let task_handlers: Vec<&str> = ctx
        .worker_tasks
        .as_ref()
        .map(|ts| ts.iter().map(|t| t.handler_name.as_str()).collect())
        .unwrap_or_default();

    for breaker in breakers {
        if breaker.state != BreakerState::Open || breaker.tenant_id != ctx.instance.tenant_id {
            continue;
        }
        let directly_involved = task_handlers.contains(&breaker.handler.as_str());
        let cooldown_left = breaker.opened_at.map(|opened| {
            let elapsed = now - opened;
            Duration::seconds(i64::try_from(breaker.cooldown_secs).unwrap_or(i64::MAX)) - elapsed
        });
        let mut finding = Finding::new(
            "OPEN_CIRCUIT_BREAKER",
            FindingSeverity::Warning,
            format!(
                "circuit breaker for handler '{}' is open after {} consecutive \
                 failures{}",
                breaker.handler,
                breaker.failure_count,
                cooldown_left
                    .filter(|d| *d > Duration::zero())
                    .map(|d| format!(" — retries resume in {}", humanize(d)))
                    .unwrap_or_default()
            ),
            if directly_involved {
                Confidence::High
            } else {
                Confidence::Low
            },
            now,
        );
        if let Some(opened) = breaker.opened_at {
            finding = finding
                .with_evidence(Evidence::new("opened_at", opened.to_rfc3339()).observed_at(now));
        }
        out.push(Diagnosis {
            category: if directly_involved {
                DiagnosisCategory::DirectEvidence
            } else {
                DiagnosisCategory::HealthWarning
            },
            health: DiagnosisHealth::Degraded,
            finding,
        });
    }
}

fn rule_children(ctx: &InstanceDiagnosticContext, now: DateTime<Utc>, out: &mut Vec<Diagnosis>) {
    let Some(children) = &ctx.children else {
        return;
    };
    if children.is_empty() {
        return;
    }
    let live: Vec<&TaskInstance> = children.iter().filter(|c| !c.state.is_terminal()).collect();
    if !live.is_empty() && ctx.instance.state == InstanceState::Waiting {
        let ids: Vec<String> = live.iter().map(|c| c.id.to_string()).collect();
        out.push(Diagnosis {
            category: DiagnosisCategory::DirectEvidence,
            health: DiagnosisHealth::Expected,
            finding: Finding::new(
                "WAITING_CHILD",
                FindingSeverity::Info,
                format!(
                    "the instance is waiting for {} child workflow(s) to finish",
                    live.len()
                ),
                Confidence::High,
                now,
            )
            .with_evidence(Evidence::new("children", ids.join(", ")).observed_at(now)),
        });
    } else if live.is_empty() && ctx.instance.state == InstanceState::Waiting {
        out.push(Diagnosis {
            category: DiagnosisCategory::ProbableCause,
            health: DiagnosisHealth::Inconsistent,
            finding: Finding::new(
                "CHILDREN_DONE_PARENT_WAITING",
                FindingSeverity::Warning,
                "every child workflow is terminal but the parent is still waiting — \
                 the completion callback may have been lost",
                Confidence::Medium,
                now,
            ),
        });
    }
}

fn rule_undelivered_signals(
    ctx: &InstanceDiagnosticContext,
    now: DateTime<Utc>,
    out: &mut Vec<Diagnosis>,
) {
    let Some(signals) = &ctx.pending_signals else {
        return;
    };
    let stale: Vec<&Signal> = signals
        .iter()
        .filter(|s| now - s.created_at > Duration::seconds(60))
        .collect();
    if !stale.is_empty() {
        out.push(Diagnosis {
            category: DiagnosisCategory::ProbableCause,
            health: DiagnosisHealth::Degraded,
            finding: Finding::new(
                "SIGNALS_NOT_CONSUMED",
                FindingSeverity::Warning,
                format!(
                    "{} signal(s) have been enqueued for over a minute without being \
                     processed — the scheduler may be down or the instance never wakes",
                    stale.len()
                ),
                Confidence::Medium,
                now,
            ),
        });
    }
}

fn rule_state_inconsistencies(
    instance: &TaskInstance,
    now: DateTime<Utc>,
    out: &mut Vec<Diagnosis>,
) {
    if instance.state == InstanceState::Running
        && now - instance.updated_at > Duration::seconds(STALE_RUNNING_SECS)
    {
        out.push(Diagnosis {
            category: DiagnosisCategory::ProbableCause,
            health: DiagnosisHealth::Inconsistent,
            finding: Finding::new(
                "STALE_RUNNING_STATE",
                FindingSeverity::Error,
                format!(
                    "the instance has been 'running' without any update for {} — the \
                     executing process likely crashed; crash recovery should \
                     reschedule it",
                    humanize(now - instance.updated_at)
                ),
                Confidence::Medium,
                now,
            )
            .with_evidence(
                Evidence::new("updated_at", instance.updated_at.to_rfc3339()).observed_at(now),
            ),
        });
    }
    if instance.state == InstanceState::Scheduled
        && let Some(fire_at) = instance.next_fire_at
        && now - fire_at > Duration::seconds(SCHEDULER_LAG_SECS)
    {
        out.push(Diagnosis {
            category: DiagnosisCategory::ProbableCause,
            health: DiagnosisHealth::Degraded,
            finding: Finding::new(
                "SCHEDULER_LAG",
                FindingSeverity::Error,
                format!(
                    "the instance became due {} ago but has not been claimed — the \
                     scheduler may be down, overloaded, or the tenant's concurrency \
                     limit is saturated",
                    humanize(now - fire_at)
                ),
                Confidence::Medium,
                now,
            )
            .with_evidence(Evidence::new("next_fire_at", fire_at.to_rfc3339()).observed_at(now)),
        });
    }
}

/// Waiting with no timer, approval, live child, or queued worker task:
/// most likely an external signal/callback that never arrived.
fn rule_waiting_external(
    ctx: &InstanceDiagnosticContext,
    now: DateTime<Utc>,
    out: &mut Vec<Diagnosis>,
) {
    if ctx.instance.state != InstanceState::Waiting {
        return;
    }
    let timer_in_future = ctx.instance.next_fire_at.is_some_and(|t| t > now);
    let has_approval = ctx
        .pending_approval_blocks
        .as_ref()
        .is_some_and(|b| !b.is_empty());
    let has_live_child = ctx
        .children
        .as_ref()
        .is_some_and(|cs| cs.iter().any(|c| !c.state.is_terminal()));
    let has_open_task = ctx.worker_tasks.as_ref().is_some_and(|ts| {
        ts.iter()
            .any(|t| matches!(t.state, WorkerTaskState::Pending | WorkerTaskState::Claimed))
    });
    if timer_in_future || has_approval || has_live_child || has_open_task {
        return;
    }
    out.push(Diagnosis {
        category: DiagnosisCategory::ProbableCause,
        health: DiagnosisHealth::Expected,
        finding: Finding::new(
            "WAITING_EXTERNAL_EVENT",
            FindingSeverity::Info,
            "the instance is waiting and no timer, approval, child, or worker task \
             explains it — it is most likely waiting for an external signal or \
             callback that has not arrived",
            Confidence::Medium,
            now,
        )
        .with_remediation(
            Remediation::new("deliver the expected signal")
                .with_command(format!("orch8 signal {} custom:<name> '<payload>'", ctx.instance.id)),
        ),
    });
}

fn note_incomplete_evidence(
    ctx: &InstanceDiagnosticContext,
    now: DateTime<Utc>,
    out: &mut Vec<Diagnosis>,
) {
    let mut missing: Vec<&str> = Vec::new();
    if ctx.sequence_exists.is_none() {
        missing.push("sequence");
    }
    if ctx.pending_signals.is_none() {
        missing.push("signals");
    }
    if ctx.worker_tasks.is_none() {
        missing.push("worker_tasks");
    }
    if ctx.worker_registrations.is_none() {
        missing.push("worker_registrations");
    }
    if ctx.open_breakers.is_none() {
        missing.push("circuit_breakers");
    }
    if ctx.children.is_none() {
        missing.push("children");
    }
    if ctx.pending_approval_blocks.is_none() {
        missing.push("approvals");
    }
    if missing.is_empty() {
        return;
    }
    out.push(Diagnosis {
        category: DiagnosisCategory::HealthWarning,
        health: DiagnosisHealth::Expected,
        finding: Finding::new(
            "EVIDENCE_INCOMPLETE",
            FindingSeverity::Info,
            format!(
                "some evidence could not be collected ({}) — diagnoses may be \
                 incomplete",
                missing.join(", ")
            ),
            Confidence::Certain,
            now,
        ),
    });
}

/// Render a duration for humans ("42s", "3m 10s", "2h 5m", "3d 4h").
fn humanize(d: Duration) -> String {
    let secs = d.num_seconds().max(0);
    let (days, rem) = (secs / 86_400, secs % 86_400);
    let (hours, rem) = (rem / 3_600, rem % 3_600);
    let (mins, secs) = (rem / 60, rem % 60);
    match (days, hours, mins) {
        (0, 0, 0) => format!("{secs}s"),
        (0, 0, m) => format!("{m}m {secs}s"),
        (0, h, m) => format!("{h}h {m}m"),
        (d2, h, _) => format!("{d2}d {h}h"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use orch8_types::context::ExecutionContext;
    use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};
    use orch8_types::instance::Priority;
    use serde_json::json;

    fn t0() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 11, 12, 0, 0).unwrap()
    }

    fn instance(state: InstanceState) -> TaskInstance {
        TaskInstance {
            id: InstanceId::new(),
            sequence_id: SequenceId::new(),
            tenant_id: TenantId::unchecked("t1"),
            namespace: Namespace::new("default"),
            state,
            next_fire_at: None,
            priority: Priority::Normal,
            timezone: "UTC".into(),
            metadata: json!({}),
            context: ExecutionContext::default(),
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: None,
            session_id: None,
            parent_instance_id: None,
            budget: None,
            created_at: t0() - Duration::minutes(5),
            updated_at: t0() - Duration::seconds(5),
        }
    }

    fn full_ctx(inst: TaskInstance) -> InstanceDiagnosticContext {
        InstanceDiagnosticContext {
            instance: inst,
            sequence_exists: Some(true),
            pending_signals: Some(vec![]),
            worker_tasks: Some(vec![]),
            worker_registrations: Some(vec![]),
            version_pins: Some(vec![]),
            open_breakers: Some(vec![]),
            children: Some(vec![]),
            pending_approval_blocks: Some(vec![]),
        }
    }

    fn task(state: WorkerTaskState, handler: &str, age_secs: i64) -> WorkerTask {
        WorkerTask {
            id: uuid::Uuid::now_v7(),
            instance_id: InstanceId::new(),
            block_id: BlockId::new("blk"),
            handler_name: handler.to_string(),
            queue_name: None,
            params: json!({}),
            context: json!({}),
            attempt: 1,
            timeout_ms: None,
            state,
            worker_id: Some("w-1".into()),
            claimed_at: Some(t0() - Duration::seconds(age_secs)),
            heartbeat_at: Some(t0() - Duration::seconds(age_secs)),
            completed_at: None,
            output: None,
            error_message: None,
            error_retryable: None,
            created_at: t0() - Duration::seconds(age_secs),
        }
    }

    fn registration(handler: &str, version: Option<&str>) -> WorkerRegistration {
        WorkerRegistration {
            worker_id: "w-1".into(),
            handler_name: handler.to_string(),
            queue_name: None,
            version: version.map(ToString::to_string),
            tenant_id: None,
            last_seen_at: t0(),
        }
    }

    fn codes(report: &InstanceDiagnosisReport) -> Vec<&str> {
        report
            .diagnoses
            .iter()
            .map(|d| d.finding.code.as_str())
            .collect()
    }

    #[test]
    fn terminal_instance_reports_terminal_state_only() {
        let report = diagnose(&full_ctx(instance(InstanceState::Completed)), t0());
        assert_eq!(codes(&report), vec!["TERMINAL_STATE"]);
        assert_eq!(report.state, "completed");
    }

    #[test]
    fn failed_instance_offers_retry_with_side_effect_flag() {
        let report = diagnose(&full_ctx(instance(InstanceState::Failed)), t0());
        let d = &report.diagnoses[0];
        assert_eq!(d.finding.code, "TERMINAL_STATE");
        assert!(d.finding.remediation[0].side_effect_risk);
    }

    #[test]
    fn future_timer_is_waiting_until_certain() {
        let mut inst = instance(InstanceState::Scheduled);
        inst.next_fire_at = Some(t0() + Duration::hours(3));
        let report = diagnose(&full_ctx(inst), t0());
        let d = &report.diagnoses[0];
        assert_eq!(d.finding.code, "WAITING_UNTIL");
        assert_eq!(d.category, DiagnosisCategory::DirectEvidence);
        assert_eq!(d.health, DiagnosisHealth::Expected);
        assert_eq!(d.finding.confidence, Confidence::Certain);
        assert!(d.finding.summary.contains("3h"), "{}", d.finding.summary);
    }

    #[test]
    fn budget_paused_reads_metadata() {
        let mut inst = instance(InstanceState::Paused);
        inst.metadata = json!({
            "paused_reason": "budget_exceeded",
            "budget_breach": {"limit": "max_steps", "limit_value": 10, "actual": 11}
        });
        let report = diagnose(&full_ctx(inst), t0());
        let d = &report.diagnoses[0];
        assert_eq!(d.finding.code, "BUDGET_PAUSED");
        assert!(d.finding.evidence[0].summary.contains("max_steps"));
        assert!(d.finding.remediation[0].command.as_deref().unwrap().contains("resume"));
    }

    #[test]
    fn operator_pause_is_reported_plainly() {
        let report = diagnose(&full_ctx(instance(InstanceState::Paused)), t0());
        assert_eq!(report.diagnoses[0].finding.code, "PAUSED");
    }

    #[test]
    fn paused_with_future_timer_reports_pause_not_timer() {
        let mut inst = instance(InstanceState::Paused);
        inst.next_fire_at = Some(t0() + Duration::hours(1));
        let report = diagnose(&full_ctx(inst), t0());
        assert!(!codes(&report).contains(&"WAITING_UNTIL"));
        assert_eq!(report.diagnoses[0].finding.code, "PAUSED");
    }

    #[test]
    fn pending_task_without_worker_is_no_compatible_worker() {
        let mut ctx = full_ctx(instance(InstanceState::Waiting));
        ctx.worker_tasks = Some(vec![task(WorkerTaskState::Pending, "charge_card", 10)]);
        let report = diagnose(&ctx, t0());
        let d = &report.diagnoses[0];
        assert_eq!(d.finding.code, "NO_COMPATIBLE_WORKER");
        assert_eq!(d.health, DiagnosisHealth::Degraded);
        assert!(d.finding.summary.contains("charge_card"));
    }

    #[test]
    fn pending_task_with_worker_expects_pickup() {
        let mut ctx = full_ctx(instance(InstanceState::Waiting));
        ctx.worker_tasks = Some(vec![task(WorkerTaskState::Pending, "charge_card", 10)]);
        ctx.worker_registrations = Some(vec![registration("charge_card", None)]);
        let report = diagnose(&ctx, t0());
        assert!(codes(&report).contains(&"WAITING_WORKER_PICKUP"));
    }

    #[test]
    fn old_pending_task_with_worker_flags_not_claiming() {
        let mut ctx = full_ctx(instance(InstanceState::Waiting));
        ctx.worker_tasks = Some(vec![task(WorkerTaskState::Pending, "charge_card", 600)]);
        ctx.worker_registrations = Some(vec![registration("charge_card", None)]);
        let report = diagnose(&ctx, t0());
        assert!(codes(&report).contains(&"WORKER_NOT_CLAIMING"));
    }

    #[test]
    fn version_pin_blocks_pickup() {
        let mut ctx = full_ctx(instance(InstanceState::Waiting));
        ctx.worker_tasks = Some(vec![task(WorkerTaskState::Pending, "charge_card", 10)]);
        ctx.worker_registrations = Some(vec![registration("charge_card", Some("1.0.0"))]);
        ctx.version_pins = Some(vec![WorkerVersionPin {
            tenant_id: "t1".into(),
            handler_name: "charge_card".into(),
            min_version: "2.0.0".into(),
            created_at: t0(),
            updated_at: t0(),
        }]);
        let report = diagnose(&ctx, t0());
        let d = report
            .diagnoses
            .iter()
            .find(|d| d.finding.code == "WORKER_BELOW_VERSION_PIN")
            .expect("pin diagnosis");
        assert!(d.finding.evidence[0].summary.contains("1.0.0"));
    }

    #[test]
    fn stale_claim_detected_via_heartbeat() {
        let mut ctx = full_ctx(instance(InstanceState::Waiting));
        ctx.worker_tasks = Some(vec![task(WorkerTaskState::Claimed, "charge_card", 900)]);
        let report = diagnose(&ctx, t0());
        let d = report
            .diagnoses
            .iter()
            .find(|d| d.finding.code == "STALE_WORKER_CLAIM")
            .expect("stale claim");
        assert_eq!(d.health, DiagnosisHealth::Degraded);
        assert!(d.finding.remediation[0].side_effect_risk);
    }

    #[test]
    fn fresh_claim_is_not_stale() {
        let mut ctx = full_ctx(instance(InstanceState::Waiting));
        ctx.worker_tasks = Some(vec![task(WorkerTaskState::Claimed, "charge_card", 10)]);
        let report = diagnose(&ctx, t0());
        assert!(!codes(&report).contains(&"STALE_WORKER_CLAIM"));
    }

    #[test]
    fn open_breaker_for_involved_handler_is_direct_evidence() {
        let mut ctx = full_ctx(instance(InstanceState::Waiting));
        ctx.worker_tasks = Some(vec![task(WorkerTaskState::Pending, "flaky_api", 10)]);
        ctx.worker_registrations = Some(vec![registration("flaky_api", None)]);
        ctx.open_breakers = Some(vec![CircuitBreakerState {
            tenant_id: TenantId::unchecked("t1"),
            handler: "flaky_api".into(),
            state: BreakerState::Open,
            failure_count: 7,
            failure_threshold: 5,
            cooldown_secs: 300,
            opened_at: Some(t0() - Duration::seconds(60)),
        }]);
        let report = diagnose(&ctx, t0());
        let d = report
            .diagnoses
            .iter()
            .find(|d| d.finding.code == "OPEN_CIRCUIT_BREAKER")
            .expect("breaker diagnosis");
        assert_eq!(d.category, DiagnosisCategory::DirectEvidence);
        assert_eq!(d.finding.confidence, Confidence::High);
        assert!(d.finding.summary.contains("resume in"), "{}", d.finding.summary);
    }

    #[test]
    fn open_breaker_for_other_tenant_is_ignored() {
        let mut ctx = full_ctx(instance(InstanceState::Waiting));
        ctx.open_breakers = Some(vec![CircuitBreakerState {
            tenant_id: TenantId::unchecked("someone-else"),
            handler: "x".into(),
            state: BreakerState::Open,
            failure_count: 5,
            failure_threshold: 5,
            cooldown_secs: 60,
            opened_at: Some(t0()),
        }]);
        let report = diagnose(&ctx, t0());
        assert!(!codes(&report).contains(&"OPEN_CIRCUIT_BREAKER"));
    }

    #[test]
    fn waiting_on_live_children() {
        let mut ctx = full_ctx(instance(InstanceState::Waiting));
        ctx.children = Some(vec![instance(InstanceState::Running)]);
        let report = diagnose(&ctx, t0());
        assert_eq!(report.diagnoses[0].finding.code, "WAITING_CHILD");
        assert_eq!(report.diagnoses[0].health, DiagnosisHealth::Expected);
    }

    #[test]
    fn children_done_but_parent_waiting_is_inconsistent() {
        let mut ctx = full_ctx(instance(InstanceState::Waiting));
        ctx.children = Some(vec![instance(InstanceState::Completed)]);
        let report = diagnose(&ctx, t0());
        let d = report
            .diagnoses
            .iter()
            .find(|d| d.finding.code == "CHILDREN_DONE_PARENT_WAITING")
            .expect("inconsistency");
        assert_eq!(d.health, DiagnosisHealth::Inconsistent);
    }

    #[test]
    fn pending_approval_is_direct_evidence_with_signal_command() {
        let mut ctx = full_ctx(instance(InstanceState::Waiting));
        ctx.pending_approval_blocks = Some(vec!["approve_payment".into()]);
        let report = diagnose(&ctx, t0());
        let d = &report.diagnoses[0];
        assert_eq!(d.finding.code, "PENDING_APPROVAL");
        assert!(
            d.finding.remediation[0]
                .command
                .as_deref()
                .unwrap()
                .contains("human_input:approve_payment")
        );
    }

    #[test]
    fn stale_signals_flag_scheduler_problem() {
        let mut ctx = full_ctx(instance(InstanceState::Waiting));
        ctx.pending_signals = Some(vec![Signal {
            id: uuid::Uuid::now_v7(),
            instance_id: ctx.instance.id,
            signal_type: orch8_types::signal::SignalType::Resume,
            payload: json!({}),
            delivered: false,
            created_at: t0() - Duration::minutes(10),
            delivered_at: None,
        }]);
        let report = diagnose(&ctx, t0());
        assert!(codes(&report).contains(&"SIGNALS_NOT_CONSUMED"));
    }

    #[test]
    fn stale_running_instance_is_inconsistent() {
        let mut inst = instance(InstanceState::Running);
        inst.updated_at = t0() - Duration::minutes(30);
        let report = diagnose(&full_ctx(inst), t0());
        let d = report
            .diagnoses
            .iter()
            .find(|d| d.finding.code == "STALE_RUNNING_STATE")
            .expect("stale running");
        assert_eq!(d.health, DiagnosisHealth::Inconsistent);
        assert!(d.finding.summary.contains("30m"), "{}", d.finding.summary);
    }

    #[test]
    fn overdue_scheduled_instance_flags_scheduler_lag() {
        let mut inst = instance(InstanceState::Scheduled);
        inst.next_fire_at = Some(t0() - Duration::minutes(10));
        let report = diagnose(&full_ctx(inst), t0());
        assert!(codes(&report).contains(&"SCHEDULER_LAG"));
    }

    #[test]
    fn recently_due_scheduled_instance_is_not_lag() {
        let mut inst = instance(InstanceState::Scheduled);
        inst.next_fire_at = Some(t0() - Duration::seconds(10));
        let report = diagnose(&full_ctx(inst), t0());
        assert!(!codes(&report).contains(&"SCHEDULER_LAG"));
    }

    #[test]
    fn waiting_with_no_explanation_suggests_external_event() {
        let report = diagnose(&full_ctx(instance(InstanceState::Waiting)), t0());
        let d = &report.diagnoses[0];
        assert_eq!(d.finding.code, "WAITING_EXTERNAL_EVENT");
        // The doctor must not claim certainty here.
        assert!(d.finding.confidence < Confidence::High);
    }

    #[test]
    fn missing_sequence_is_critical_inconsistency() {
        let mut ctx = full_ctx(instance(InstanceState::Scheduled));
        ctx.sequence_exists = Some(false);
        let report = diagnose(&ctx, t0());
        let d = &report.diagnoses[0];
        assert_eq!(d.finding.code, "SEQUENCE_MISSING");
        assert_eq!(d.health, DiagnosisHealth::Inconsistent);
        assert_eq!(d.finding.severity, FindingSeverity::Critical);
    }

    #[test]
    fn uncollected_evidence_is_reported_not_ignored() {
        let inst = instance(InstanceState::Waiting);
        let report = diagnose(&InstanceDiagnosticContext::new(inst), t0());
        let d = report
            .diagnoses
            .iter()
            .find(|d| d.finding.code == "EVIDENCE_INCOMPLETE")
            .expect("evidence warning");
        assert!(d.finding.summary.contains("worker_tasks"));
        assert!(d.finding.summary.contains("circuit_breakers"));
    }

    #[test]
    fn healthy_scheduled_instance_reports_no_blockers() {
        let mut inst = instance(InstanceState::Scheduled);
        inst.next_fire_at = Some(t0() - Duration::seconds(1)); // just became due
        let report = diagnose(&full_ctx(inst), t0());
        assert_eq!(codes(&report), vec!["NO_BLOCKER_FOUND"]);
    }

    #[test]
    fn direct_evidence_outranks_probable_and_health() {
        // Stale claim (direct) + stale signals (probable) + other-tenant
        // breaker filtered; ordering must put the claim first.
        let mut ctx = full_ctx(instance(InstanceState::Waiting));
        ctx.worker_tasks = Some(vec![task(WorkerTaskState::Claimed, "h", 900)]);
        ctx.pending_signals = Some(vec![Signal {
            id: uuid::Uuid::now_v7(),
            instance_id: ctx.instance.id,
            signal_type: orch8_types::signal::SignalType::Resume,
            payload: json!({}),
            delivered: false,
            created_at: t0() - Duration::minutes(5),
            delivered_at: None,
        }]);
        let report = diagnose(&ctx, t0());
        assert_eq!(report.diagnoses[0].finding.code, "STALE_WORKER_CLAIM");
        assert_eq!(report.diagnoses[0].category, DiagnosisCategory::DirectEvidence);
    }

    #[test]
    fn humanize_formats_ranges() {
        assert_eq!(humanize(Duration::seconds(42)), "42s");
        assert_eq!(humanize(Duration::seconds(190)), "3m 10s");
        assert_eq!(humanize(Duration::seconds(7500)), "2h 5m");
        assert_eq!(humanize(Duration::days(3) + Duration::hours(4)), "3d 4h");
        assert_eq!(humanize(Duration::seconds(-5)), "0s");
    }
}
