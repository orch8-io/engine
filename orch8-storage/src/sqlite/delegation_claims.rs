//! Publication-certified decoder/resource admission, shared by scan and locked mutation.
use orch8_types::continuity::RuntimeCapabilities;
use sqlx::{QueryBuilder, Sqlite};

pub(super) fn append(
    query: &mut QueryBuilder<Sqlite>,
    task: &'static str,
    capabilities: &RuntimeCapabilities,
) {
    append_protocol(query, task, capabilities.delegation_protocol.as_ref());
}

pub(super) fn append_protocol(
    query: &mut QueryBuilder<Sqlite>,
    task: &'static str,
    protocol: Option<&orch8_types::delegation_definition::protocol::DelegationProtocolCapabilities>,
) {
    // All aliases are fixed by the calling SQL; no user data enters SQL text.
    debug_assert!(matches!(task, "wt" | "worker_tasks"));
    query.push(format!(" AND (({task}.handler_name<>'orch8.delegation' AND NOT EXISTS(SELECT 1 FROM delegation_publications dp WHERE dp.task_id={task}.id))"));
    let Some(protocol) = protocol.filter(|p| p.validate().is_ok()) else {
        query.push(")");
        return;
    };
    query.push(format!(" OR ({task}.handler_name='orch8.delegation' AND EXISTS(SELECT 1 FROM delegation_publications p JOIN delegation_definition_manifests m ON m.tenant_id=p.tenant_id AND m.task_id=p.task_id JOIN delegation_publication_inputs i ON i.tenant_id=p.tenant_id AND i.task_id=p.task_id WHERE p.task_id={task}.id AND p.instance_id={task}.instance_id AND p.tenant_id=(SELECT owner.tenant_id FROM task_instances owner WHERE owner.id={task}.instance_id) AND i.version=1 AND i.request_sha256=json_extract(p.record,'$.request_sha256') AND julianday(json_extract(p.record,'$.delegation.expires_at')) > julianday('now')"));
    query
        .push(" AND m.wire_version=")
        .push_bind(i64::from(protocol.definition_wire_version));
    query
        .push(" AND i.input_wire_version=")
        .push_bind(i64::from(protocol.input_wire_version));
    query
        .push(" AND m.wire_bytes<=")
        .push_bind(i64::from(protocol.max_definition_payload_bytes));
    query
        .push(" AND m.wire_definitions<=")
        .push_bind(i64::from(protocol.max_definitions));
    query
        .push(" AND m.wire_selectors<=")
        .push_bind(i64::from(protocol.max_selectors));
    query
        .push(" AND i.definition_expanded_bytes<=")
        .push_bind(i64::from(protocol.max_definition_expanded_bytes));
    query
        .push(" AND i.input_bytes<=")
        .push_bind(i64::from(protocol.max_input_payload_bytes));
    query
        .push(" AND i.input_expanded_bytes<=")
        .push_bind(i64::from(protocol.max_input_expanded_bytes));
    query.push(")))");
}
