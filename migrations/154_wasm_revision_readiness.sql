CREATE TABLE wasm_revision_readiness (
    node_id UUID NOT NULL REFERENCES cluster_nodes(id) ON DELETE CASCADE,
    sequence_id UUID NOT NULL REFERENCES sequences(id) ON DELETE CASCADE,
    ready_until TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (sequence_id, node_id)
);
