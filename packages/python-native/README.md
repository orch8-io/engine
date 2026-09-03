# orch8-engine-native

Zero-server Python bindings built with PyO3/maturin from the same Rust engine
as the server. In addition to strict validation, `run_sequence_json` executes
built-in handlers in an isolated in-memory engine, with dry-run effects and
virtual-time delay advancement. It supports Python 3.10+ through abi3.
