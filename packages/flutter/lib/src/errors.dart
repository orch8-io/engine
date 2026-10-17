/// Error categories reported by the native engine. Mirrors the Rust
/// `MobileError` and `SyncError` variants (and `Orch8ErrorKind` in the KMP and
/// React Native SDKs).
///
/// Native failures surface as a `PlatformException` whose `code` is
/// `ORCH8_<KIND>` (for example `ORCH8_NOT_FOUND`) and whose `details` map holds
/// `kind` plus `legacyCode`, the pre-0.8 code (`ORCH8_ERROR` / `INIT_ERROR`).
/// Use [orch8ErrorKindOf] to classify one.
enum Orch8ErrorKind {
  engine('engine'),
  storage('storage'),
  invalidInput('invalid_input'),
  notFound('not_found'),
  resourceLimit('resource_limit'),
  alreadyExists('already_exists'),
  shutdown('shutdown'),
  network('network'),
  signatureInvalid('signature_invalid'),
  invalidManifest('invalid_manifest'),

  /// Not an engine error, or one this wrapper version does not know.
  unknown('unknown');

  const Orch8ErrorKind(this.wire);

  /// snake_case name used on the platform channel.
  final String wire;

  /// Parses a wire name (`not_found`), tolerating `NOT_FOUND` / `NotFound`.
  static Orch8ErrorKind fromWire(String value) {
    final normalized = value
        .trim()
        .replaceAllMapped(RegExp(r'([a-z0-9])([A-Z])'), (m) => '${m[1]}_${m[2]}')
        .toLowerCase();
    for (final kind in values) {
      if (kind.wire == normalized) return kind;
    }
    return Orch8ErrorKind.unknown;
  }

  /// Whether retrying the same call later can plausibly succeed.
  bool get isRetryable =>
      this == network || this == resourceLimit || this == storage;
}

/// Classifies a platform-channel failure from its `code` and `details`
/// (`PlatformException.code` / `.details`). Legacy `ORCH8_ERROR`, `INIT_ERROR`
/// and non-engine codes map to [Orch8ErrorKind.unknown].
Orch8ErrorKind orch8ErrorKindFromChannel(String code, Object? details) {
  if (details is Map) {
    final kind = details['kind'];
    if (kind is String) {
      final parsed = Orch8ErrorKind.fromWire(kind);
      if (parsed != Orch8ErrorKind.unknown) return parsed;
    }
  }
  const prefix = 'ORCH8_';
  if (code.startsWith(prefix)) {
    return Orch8ErrorKind.fromWire(code.substring(prefix.length));
  }
  return Orch8ErrorKind.unknown;
}

/// Classifies any thrown object; works with `PlatformException` without
/// importing Flutter by reading its `code` / `details` dynamically.
Orch8ErrorKind orch8ErrorKindOf(Object error) {
  try {
    final dynamic e = error;
    final Object? code = e.code;
    final Object? details = e.details;
    if (code is String) return orch8ErrorKindFromChannel(code, details);
  } on NoSuchMethodError {
    // Not a PlatformException.
  }
  return Orch8ErrorKind.unknown;
}
