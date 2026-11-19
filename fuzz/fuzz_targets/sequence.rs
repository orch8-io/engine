#![no_main]

use libfuzzer_sys::fuzz_target;
use orch8_types::sequence::deserialize_sequence_strict;

fuzz_target!(|input: &[u8]| {
    if input.len() > 32 * 1024 {
        return;
    }
    let Ok(value) = serde_json::from_slice(input) else {
        return;
    };
    let Ok(sequence) = deserialize_sequence_strict(&value) else {
        return;
    };
    let valid = sequence.validate().is_ok();
    let serialized = serde_json::to_value(&sequence).expect("decoded sequence serializes");
    let round_trip = deserialize_sequence_strict(&serialized).expect("canonical sequence decodes");
    assert_eq!(sequence.id, round_trip.id);
    assert_eq!(valid, round_trip.validate().is_ok());
});
