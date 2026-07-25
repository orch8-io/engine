# Mobile protected fields and device tools

The mobile SDK exposes four standard capability descriptors: camera, scoped
file access, biometric verification, and secure storage. `DeviceToolBridge`
allows only each descriptor's listed operations, bounds request/response size,
and requires foreground execution for permission-sensitive tools. Reference
handlers retain protected bytes in the host and return opaque handles.

`ProtectedFieldBoundary` seals handoff values with tenant-and-instance AAD.
Only a trusted runtime holding the key can open them. Logs, traces, artifacts,
and sync values pass through the same recursive policy redactor. Executable
leakage assertions prove a reference eligibility workflow never emits its raw
protected identifier through any of those surfaces or its encrypted capsule.

For rotation, construct the primary `FieldEncryptor` with `with_old_key`.
Existing ciphertext remains readable through the old key while every new seal
uses the primary key. Remove the old key only after all retained ciphertext has
been rewritten or expired.

The SDK does not request OS permissions automatically, export camera/file/key
bytes, redact unlabelled semantic secrets, or rotate host keys on its own.

