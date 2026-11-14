use super::*;
use crate::delegation_definition::{
    DefinitionBinding, DefinitionGraphLimits, DefinitionManifest, DefinitionSelector,
    FrozenDelegationDefinition,
    wire::{EncodedDefinitionBundle, RetainedDefinitionDocument},
};
use crate::sequence::BlockDefinition;
use serde_json::{Value, json};

fn sub(id: &str, version: Option<i32>) -> Value {
    json!({"type":"sub_sequence","id":id,"sequence_name":"child","version":version})
}
fn source() -> VerifiedDefinitionGraph {
    let vector: Value = serde_json::from_str(include_str!(
        "../../../../contracts/vectors/delegation-definition-v1.json"
    ))
    .unwrap();
    let mut root: SequenceDefinition =
        serde_json::from_str(vector["retained_definition_json"].as_str().unwrap()).unwrap();
    let fake = sub("opaque", None);
    root.blocks = serde_json::from_value(json!([
        {"type":"step","id":"work","handler":"noop","params":fake},
        sub("direct",None),
        {"type":"parallel","id":"parallel","branches":[[sub("pa",None)],[sub("pb",Some(7))]]},
        {"type":"race","id":"race","branches":[[sub("ra",None)],[sub("rb",None)]]},
        {"type":"loop","id":"loop","condition":"false","body":[sub("lb",None)]},
        {"type":"for_each","id":"each","collection":"[]","body":[sub("eb",None)]},
        {"type":"router","id":"router","routes":[{"condition":"true","blocks":[sub("route",None)]}],"default":[sub("default",None)]},
        {"type":"try_catch","id":"try","try_block":[sub("try-body",None)],"catch_block":[sub("catch",None)],"finally_block":[sub("finally",None)]},
        {"type":"ab_split","id":"ab","variants":[{"name":"a","weight":1,"blocks":[sub("va",None)]},{"name":"b","weight":1,"blocks":[sub("vb",None)]}]},
        {"type":"cancellation_scope","id":"scope","blocks":[sub("protected",None)]},
        {"type":"saga","id":"saga","steps":[{"id":"action","action":sub("forward",None),"compensation":sub("undo",None)}]}
    ])).unwrap();
    root.on_failure = Some(serde_json::from_value(json!([sub("failure", None)])).unwrap());
    root.on_cancel = Some(serde_json::from_value(json!([sub("cancel", None)])).unwrap());
    root.sub_tenant = Some("foreign-embed-owner".into());
    root.embed = Some(crate::sub_tenant::SequenceEmbed {
        gallery: true,
        visible_outputs: vec!["work".into()],
        ..Default::default()
    });
    let mut child = root.clone();
    child.id = SequenceId::from_uuid(uuid::Uuid::from_u128(23456));
    child.name = "child".into();
    child.version = 7;
    child.blocks =
        serde_json::from_value(json!([{"type":"step","id":"child-work","handler":"noop"}]))
            .unwrap();
    child.on_failure = None;
    child.on_cancel = None;
    let documents = [root, child].map(|d| FrozenDelegationDefinition::new(d).unwrap());
    let manifest = DefinitionManifest {
        version: 1,
        root_sha256: documents[0].sequence_sha256.clone(),
        bindings: [None, Some(7)]
            .into_iter()
            .map(|version| DefinitionBinding {
                selector: DefinitionSelector {
                    name: "child".into(),
                    version,
                },
                sequence_sha256: documents[1].sequence_sha256.clone(),
            })
            .collect(),
    };
    EncodedDefinitionBundle::from_retained(
        &serde_json::to_string(&manifest).unwrap(),
        documents
            .into_iter()
            .map(|d| RetainedDefinitionDocument {
                sequence_sha256: d.sequence_sha256,
                definition_json: serde_json::to_string(&d.definition).unwrap(),
            })
            .collect(),
        DefinitionGraphLimits::default(),
    )
    .unwrap()
    .1
}
#[test]
fn local_preparation_remaps_ownership_and_pins_every_typed_dependency() {
    let graph = source();
    let source_ids: Vec<_> = graph
        .graph()
        .definitions
        .iter()
        .map(|d| d.definition.id)
        .collect();
    let manifest_hash = graph.graph().sha256().unwrap();
    let expected_sources: std::collections::BTreeSet<_> = graph
        .graph()
        .definitions
        .iter()
        .map(|d| d.sequence_sha256.clone())
        .collect();
    let plan = graph
        .prepare_local(&TenantId::new("local-alice").unwrap())
        .unwrap();
    assert_eq!(plan.bundle_sha256(), manifest_hash);
    assert_eq!(plan.entries().len(), 2);
    assert_eq!(
        plan.entries()
            .iter()
            .map(|e| e.source_sha256().to_owned())
            .collect::<std::collections::BTreeSet<_>>(),
        expected_sources
    );
    let root = plan
        .entries()
        .iter()
        .find(|e| e.definition().id == plan.root_id())
        .unwrap()
        .definition();
    let child = plan
        .entries()
        .iter()
        .find(|e| e.definition().name == "child")
        .unwrap()
        .definition();
    assert_ne!(root.id, child.id);
    for e in plan.entries() {
        let d = e.definition();
        assert!(!source_ids.contains(&d.id));
        assert_eq!(d.tenant_id.as_str(), "local-alice");
        assert_eq!(&d.namespace, plan.namespace());
        assert!(d.sub_tenant.is_none() && d.embed.is_none());
    }
    assert_eq!(
        super::super::definition_references(root).unwrap(),
        std::collections::BTreeSet::from([DefinitionSelector {
            name: "child".into(),
            version: Some(7)
        }])
    );
    let BlockDefinition::Step(step) = &root.blocks[0] else {
        panic!("step")
    };
    assert_eq!(step.params, sub("opaque", None));
}
#[test]
fn independent_preparations_cannot_share_local_rows_or_catalog_namespace() {
    let a = source()
        .prepare_local(&TenantId::new("alice").unwrap())
        .unwrap();
    let b = source()
        .prepare_local(&TenantId::new("bob").unwrap())
        .unwrap();
    assert_ne!(a.namespace(), b.namespace());
    assert_ne!(a.root_id(), b.root_id());
    assert_eq!(a.bundle_sha256(), b.bundle_sha256());
}

#[test]
fn explicit_older_child_version_does_not_follow_the_unversioned_binding() {
    let mut graph = source().into_graph();
    let root_index = graph
        .definitions
        .iter()
        .position(|d| d.sequence_sha256 == graph.manifest.root_sha256)
        .unwrap();
    let mut older = graph
        .definitions
        .iter()
        .find(|d| d.definition.name == "child")
        .unwrap()
        .definition
        .clone();
    older.id = SequenceId::new();
    older.version = 3;
    let older = FrozenDelegationDefinition::new(older).unwrap();
    let mut root = graph.definitions[root_index].definition.clone();
    root.blocks
        .push(serde_json::from_value(sub("older", Some(3))).unwrap());
    let root = FrozenDelegationDefinition::new(root).unwrap();
    graph.manifest.root_sha256 = root.sequence_sha256.clone();
    graph.definitions[root_index] = root;
    graph.manifest.bindings.push(DefinitionBinding {
        selector: DefinitionSelector {
            name: "child".into(),
            version: Some(3),
        },
        sequence_sha256: older.sequence_sha256.clone(),
    });
    graph.definitions.push(older);
    let verified = EncodedDefinitionBundle::from_retained(
        &serde_json::to_string(&graph.manifest).unwrap(),
        graph
            .definitions
            .into_iter()
            .map(|d| RetainedDefinitionDocument {
                sequence_sha256: d.sequence_sha256,
                definition_json: serde_json::to_string(&d.definition).unwrap(),
            })
            .collect(),
        DefinitionGraphLimits::default(),
    )
    .unwrap()
    .1;
    let plan = verified
        .prepare_local(&TenantId::new("alice").unwrap())
        .unwrap();
    let root = plan
        .entries()
        .iter()
        .find(|e| e.definition().id == plan.root_id())
        .unwrap()
        .definition();
    assert_eq!(
        super::super::definition_references(root).unwrap(),
        std::collections::BTreeSet::from([
            DefinitionSelector {
                name: "child".into(),
                version: Some(3)
            },
            DefinitionSelector {
                name: "child".into(),
                version: Some(7)
            }
        ])
    );
    assert_eq!(
        plan.entries()
            .iter()
            .filter(|e| e.definition().name == "child")
            .count(),
        2
    );
}

#[test]
fn unchecked_empty_local_tenant_cannot_prepare_a_graph() {
    assert!(source().prepare_local(&TenantId::unchecked("  ")).is_err());
}
