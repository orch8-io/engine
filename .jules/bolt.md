## 2024-11-20 - Removed synchronous eprintln! from async executor
**Learning:** Found leftover debugging `eprintln!` statements inside core engine loops in `signals.rs` and `handlers/builtin.rs`. In a high-throughput async executor like Tokio, synchronous console I/O can block executor threads and degrade performance.
**Action:** Replaced `eprintln!` with `tracing::debug!` which integrates cleanly with the existing tracing subscriber, honors logging levels in production, and avoids unexpected synchronous stalls in async functions.
