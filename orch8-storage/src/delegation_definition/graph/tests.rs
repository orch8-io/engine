use super::*;
use crate::testing::fixtures::SequenceBuilder;
use orch8_types::sequence::BlockDefinition;
use serde_json::{Value, json};
use std::collections::BTreeSet;

fn sub(name: &str, version: Option<i32>) -> Value {
    json!({"type":"sub_sequence","id":name,"sequence_name":name,"version":version})
}
fn sequence(name: &str, blocks: Value) -> SequenceDefinition {
    SequenceBuilder::new("graph-tenant")
        .namespace("private-catalog")
        .name(name)
        .blocks(serde_json::from_value::<Vec<BlockDefinition>>(blocks).unwrap())
        .build()
}
#[derive(Default)]
struct Catalog {
    documents: Vec<SequenceDefinition>,
    calls: usize,
}

struct UnstableCatalog(Vec<SequenceDefinition>);
#[async_trait]
impl DefinitionResolver for UnstableCatalog {
    async fn resolve(
        &mut self,
        _: &TenantId,
        _: &Namespace,
        _: &DefinitionSelector,
    ) -> Result<Option<SequenceDefinition>, StorageError> {
        Ok(self.0.pop())
    }
}

#[tokio::test]
async fn incoherent_catalog_versions_cannot_be_certified_as_one_cut() {
    let root = sequence(
        "root",
        json!([
            sub("child", None),
            {"type":"sub_sequence","id":"pinned","sequence_name":"child","version":1}
        ]),
    );
    let child = sequence(
        "child",
        json!([{"type":"step","id":"work","handler":"noop"}]),
    );
    for same_id in [true, false] {
        let mut changed = child.clone();
        if !same_id {
            changed.id = orch8_types::ids::SequenceId::new();
        }
        if let BlockDefinition::Step(step) = &mut changed.blocks[0] {
            step.params = json!({"replaced_between_reads":true});
        }
        let mut catalog = UnstableCatalog(vec![child.clone(), changed]);
        assert!(
            crate::delegation_definition::graph::capture(
                root.clone(),
                &mut catalog,
                DefinitionGraphLimits::default()
            )
            .await
            .is_err()
        );
    }
}
#[async_trait]
impl DefinitionResolver for Catalog {
    async fn resolve(
        &mut self,
        tenant: &TenantId,
        namespace: &Namespace,
        selector: &DefinitionSelector,
    ) -> Result<Option<SequenceDefinition>, StorageError> {
        self.calls += 1;
        assert_eq!(tenant.as_str(), "graph-tenant");
        assert_eq!(namespace.as_str(), "private-catalog");
        Ok(self
            .documents
            .iter()
            .filter(|d| matches_selector(d, selector))
            .max_by_key(|d| d.version)
            .cloned())
    }
}

#[test]
fn typed_references_cover_every_branch_and_cleanup_without_scanning_json() {
    let mut root = sequence(
        "root",
        json!([
            {"type":"step","id":"work","handler":"noop","params":sub("fake",None)},
            sub("direct",Some(7)),
            {"type":"parallel","id":"parallel","branches":[[sub("parallel-a",None)],[sub("parallel-b",None)]]},
            {"type":"race","id":"race","branches":[[sub("race-a",None)],[sub("race-b",None)]]},
            {"type":"loop","id":"loop","condition":"false","body":[sub("loop-body",None)]},
            {"type":"for_each","id":"each","collection":"[]","body":[sub("each-body",None)]},
            {"type":"router","id":"router","routes":[{"condition":"true","blocks":[sub("route",None)]}],"default":[sub("default-route",None)]},
            {"type":"try_catch","id":"try","try_block":[sub("try-body",None)],"catch_block":[sub("catch",None)],"finally_block":[sub("finally",None)]},
            {"type":"ab_split","id":"ab","variants":[{"name":"a","weight":1,"blocks":[sub("variant-a",None)]},{"name":"b","weight":1,"blocks":[sub("variant-b",None)]}]},
            {"type":"cancellation_scope","id":"scope","blocks":[sub("protected",None)]},
            {"type":"saga","id":"saga","steps":[{"id":"action","action":sub("forward",None),"compensation":sub("compensate",None)}]}
        ]),
    );
    root.on_failure = Some(serde_json::from_value(json!([sub("failure", None)])).unwrap());
    root.on_cancel = Some(serde_json::from_value(json!([sub("cancel", None)])).unwrap());
    let actual = references(&root).unwrap();
    let names: BTreeSet<_> = actual.iter().map(|s| s.name.as_str()).collect();
    assert_eq!(
        names,
        BTreeSet::from([
            "direct",
            "parallel-a",
            "parallel-b",
            "race-a",
            "race-b",
            "loop-body",
            "each-body",
            "route",
            "default-route",
            "try-body",
            "catch",
            "finally",
            "variant-a",
            "variant-b",
            "protected",
            "forward",
            "compensate",
            "failure",
            "cancel",
        ])
    );
    assert_eq!(
        actual.iter().find(|s| s.name == "direct").unwrap().version,
        Some(7)
    );
}

#[tokio::test]
async fn cycles_deduplicate_documents_and_preserve_pinned_and_latest_selections() {
    let root = sequence(
        "root",
        json!([sub("child",None),{"type":"sub_sequence","id":"pinned","sequence_name":"child","version":1}]),
    );
    let child1 = sequence("child", json!([sub("root", Some(1))]));
    let mut child2 = child1.clone();
    child2.id = orch8_types::ids::SequenceId::new();
    child2.version = 2;
    let mut catalog = Catalog {
        documents: vec![root.clone(), child1, child2],
        calls: 0,
    };
    let graph = crate::delegation_definition::graph::capture(
        root,
        &mut catalog,
        DefinitionGraphLimits::default(),
    )
    .await
    .unwrap();
    assert_eq!(catalog.calls, 3);
    assert_eq!(graph.definitions.len(), 3);
    assert_eq!(graph.manifest.bindings.len(), 3);
    graph
        .verify(&graph.sha256().unwrap(), DefinitionGraphLimits::default())
        .unwrap();
    let child_versions: BTreeSet<_> = graph
        .definitions
        .iter()
        .filter(|d| d.definition.name == "child")
        .map(|d| d.definition.version)
        .collect();
    assert_eq!(child_versions, BTreeSet::from([1, 2]));
}

#[tokio::test]
async fn identical_root_can_have_distinct_immutable_bundle_identities() {
    let root = sequence("root", json!([sub("child", None)]));
    let first = sequence(
        "child",
        json!([{"type":"step","id":"work","handler":"noop"}]),
    );
    let mut catalog = Catalog {
        documents: vec![first.clone()],
        calls: 0,
    };
    let a = crate::delegation_definition::graph::capture(
        root.clone(),
        &mut catalog,
        DefinitionGraphLimits::default(),
    )
    .await
    .unwrap();
    let mut newer = first;
    newer.id = orch8_types::ids::SequenceId::new();
    newer.version = 2;
    catalog.documents.push(newer);
    let b = crate::delegation_definition::graph::capture(
        root,
        &mut catalog,
        DefinitionGraphLimits::default(),
    )
    .await
    .unwrap();
    assert_eq!(a.manifest.root_sha256, b.manifest.root_sha256);
    assert_ne!(a.sha256().unwrap(), b.sha256().unwrap());
    assert!(
        b.verify(&a.sha256().unwrap(), DefinitionGraphLimits::default())
            .is_err()
    );
}

#[tokio::test]
async fn resolver_cannot_substitute_scope_or_deprecated_latest_and_missing_fails_closed() {
    let root = sequence("root", json!([sub("child", None)]));
    let child = sequence(
        "child",
        json!([{"type":"step","id":"work","handler":"noop"}]),
    );
    for mismatch in 0..4 {
        let mut wrong = child.clone();
        match mismatch {
            0 => wrong.tenant_id = TenantId::unchecked("another-account"),
            1 => wrong.namespace = Namespace::new("another-catalog"),
            2 => wrong.deprecated = true,
            _ => wrong.name = "another-child".into(),
        }
        let mut catalog = UnstableCatalog(vec![wrong]);
        assert!(
            crate::delegation_definition::graph::capture(
                root.clone(),
                &mut catalog,
                DefinitionGraphLimits::default()
            )
            .await
            .is_err()
        );
    }
    let mut missing = UnstableCatalog(vec![]);
    assert!(
        crate::delegation_definition::graph::capture(
            root,
            &mut missing,
            DefinitionGraphLimits::default()
        )
        .await
        .is_err()
    );
    let pinned_wrong = sequence("root", json!([sub("child", Some(2))]));
    assert!(
        crate::delegation_definition::graph::capture(
            pinned_wrong,
            &mut UnstableCatalog(vec![child.clone()]),
            DefinitionGraphLimits::default()
        )
        .await
        .is_err()
    );
    let mut deprecated = child;
    deprecated.deprecated = true;
    let mut catalog = Catalog {
        documents: vec![deprecated],
        calls: 0,
    };
    let pinned = sequence("root", json!([sub("child", Some(1))]));
    assert!(
        crate::delegation_definition::graph::capture(
            pinned,
            &mut catalog,
            DefinitionGraphLimits::default()
        )
        .await
        .is_ok()
    );
}

#[tokio::test]
async fn graph_limits_bound_documents_selectors_and_final_wire_bytes() {
    let root = sequence("root", json!([sub("child", None)]));
    let child = sequence(
        "child",
        json!([{"type":"step","id":"work","handler":"noop"}]),
    );
    let mut catalog = Catalog {
        documents: vec![child],
        calls: 0,
    };
    let graph = crate::delegation_definition::graph::capture(
        root.clone(),
        &mut catalog,
        DefinitionGraphLimits::default(),
    )
    .await
    .unwrap();
    let bytes = serde_json::to_vec(&graph).unwrap().len();
    let exact = DefinitionGraphLimits {
        serialized_bytes: bytes,
        ..DefinitionGraphLimits::default()
    };
    assert!(
        crate::delegation_definition::graph::capture(root.clone(), &mut catalog, exact)
            .await
            .is_ok()
    );
    for limits in [
        DefinitionGraphLimits {
            definitions: 1,
            ..exact
        },
        DefinitionGraphLimits {
            serialized_bytes: bytes - 1,
            ..exact
        },
        DefinitionGraphLimits {
            selectors: 0,
            ..exact
        },
        DefinitionGraphLimits {
            definitions: 257,
            ..exact
        },
    ] {
        assert!(
            crate::delegation_definition::graph::capture(root.clone(), &mut catalog, limits)
                .await
                .is_err()
        );
    }
    let two = sequence(
        "root",
        json!([sub("child",None),{"type":"sub_sequence","id":"pinned","sequence_name":"child","version":1}]),
    );
    assert!(
        crate::delegation_definition::graph::capture(
            two,
            &mut catalog,
            DefinitionGraphLimits {
                selectors: 1,
                ..exact
            }
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn verification_rejects_tampering_extra_documents_and_incomplete_closure() {
    let root = sequence("root", json!([sub("child", None)]));
    let child = sequence(
        "child",
        json!([{"type":"step","id":"work","handler":"noop"}]),
    );
    let mut catalog = Catalog {
        documents: vec![child.clone()],
        calls: 0,
    };
    let good = crate::delegation_definition::graph::capture(
        root,
        &mut catalog,
        DefinitionGraphLimits::default(),
    )
    .await
    .unwrap();
    for corruption in 0..7 {
        let mut graph = good.clone();
        match corruption {
            0 => graph.definitions[0].definition.name = "tampered".into(),
            1 => {
                graph.definitions.pop();
            }
            2 => graph.definitions.push(
                FrozenDelegationDefinition::new(sequence(
                    "unreachable",
                    json!([{"type":"step","id":"work","handler":"noop"}]),
                ))
                .unwrap(),
            ),
            3 => graph.manifest.bindings.clear(),
            4 => graph
                .manifest
                .bindings
                .push(graph.manifest.bindings[0].clone()),
            5 => graph.definitions.push(graph.definitions[0].clone()),
            _ => graph.manifest.version = 2,
        }
        // Even a self-consistent replacement manifest must be structurally safe.
        assert!(
            graph
                .verify(&graph.sha256().unwrap(), DefinitionGraphLimits::default())
                .is_err(),
            "corruption {corruption}"
        );
    }
}
