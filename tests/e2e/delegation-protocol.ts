/** Explicit decoder support for delegation publication fixtures. */
export function readyDelegationProtocol() {
  return {
    version: 1,
    definition_wire_version: 1,
    max_definition_payload_bytes: 4 * 1024 * 1024,
    max_definitions: 256,
    max_selectors: 1024,
    max_definition_expanded_bytes: 16 * 1024 * 1024,
    input_wire_version: 1,
    max_input_payload_bytes: 4 * 1024 * 1024,
    max_input_expanded_bytes: 16 * 1024 * 1024,
  };
}
