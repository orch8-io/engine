#![no_main]

use libfuzzer_sys::fuzz_target;
use orch8_types::delegation_definition::{DefinitionGraphLimits, wire::DefinitionWireEnvelope};

fuzz_target!(|input: &[u8]| {
    if input.len() > 64 * 1024 {
        return;
    }
    let Ok(envelope) = serde_json::from_slice::<DefinitionWireEnvelope>(input) else {
        return;
    };
    let limits = DefinitionGraphLimits::default();
    // Use the envelope identity to reach the codec in this harness. Workers
    // provide their expected scope and reference from the trusted claim.
    let Ok(graph) = envelope.verify(&envelope.scope, &envelope.reference, limits) else {
        return;
    };
    let mut wrong_scope = envelope.scope.clone();
    wrong_scope.claim_epoch ^= 1;
    assert!(
        envelope
            .verify(&wrong_scope, &envelope.reference, limits)
            .is_err()
    );

    let mut wrong_reference = envelope.reference.clone();
    wrong_reference.payload_sha256.replace_range(
        ..1,
        if envelope.reference.payload_sha256.starts_with('0') {
            "1"
        } else {
            "0"
        },
    );
    assert!(
        envelope
            .verify(&envelope.scope, &wrong_reference, limits)
            .is_err()
    );

    let bytes = serde_json::to_vec(&envelope).expect("decoded envelope serializes");
    let round_trip: DefinitionWireEnvelope =
        serde_json::from_slice(&bytes).expect("canonical envelope decodes");
    let verified = round_trip
        .verify(&envelope.scope, &envelope.reference, limits)
        .expect("canonical envelope verifies with its original identity");
    assert_eq!(graph.expanded_bytes(), verified.expanded_bytes());
});
