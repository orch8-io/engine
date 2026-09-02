# @orch8/engine-native

Zero-server Node bindings for strict sequence decoding, validation, and
in-memory execution through the same Rust engine as `orch8-server`. The
`runSequenceJson` export runs built-in handlers in dry-run mode and advances
virtual time across delays. Build with `npm install` and `npm run build`;
release automation can publish the napi platform packages.
