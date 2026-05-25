#!/bin/bash
sed -i '/pub use checkpoints::{PruneCheckpointsRequest, SaveCheckpointRequest};/d' orch8-api/src/instances.rs
#!/bin/bash
sed -i '/pub(crate) use checkpoints::{/i \pub use checkpoints::{PruneCheckpointsRequest, SaveCheckpointRequest};' orch8-api/src/instances.rs
