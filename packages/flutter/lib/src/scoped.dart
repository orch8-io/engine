part of '../orch8_flutter.dart';

/// Trusted identity from the host's sign-in flow, never inferred from a token.
final class AccountIdentity {
  final String accountKey, issuer, subject;
  const AccountIdentity({
    required this.accountKey,
    required this.issuer,
    required this.subject,
  });
  Map<String, Object?> toMap() => {
    'accountKey': accountKey,
    'issuer': issuer,
    'subject': subject,
  };
}

final class CatalogConfiguration {
  final String catalogKey, source, signingAuthority, manifestUrl, identityUrl;
  const CatalogConfiguration({
    required this.catalogKey,
    required this.source,
    required this.signingAuthority,
    required this.manifestUrl,
    required this.identityUrl,
  });
  Map<String, Object?> toMap() => {
    'catalogKey': catalogKey,
    'source': source,
    'signingAuthority': signingAuthority,
    'manifestUrl': manifestUrl,
    'identityUrl': identityUrl,
  };
}

final class CatalogRunRequest {
  final String workflow, operationKey, inputJson;
  final int? version;
  const CatalogRunRequest({
    required this.workflow,
    required this.operationKey,
    this.inputJson = '{}',
    this.version,
  });
  Map<String, Object?> toMap() => {
    'workflow': workflow,
    'operationKey': operationKey,
    'inputJson': inputJson,
    'version': version,
  };
}

final class CatalogCallbackContext {
  final String accountId, catalogId, ownerEpoch, runId, blockId, effectId;
  final int attempt;
  CatalogCallbackContext._(Map<String, Object?> value)
    : accountId = _scopeString(value['accountId']),
      catalogId = _scopeString(value['catalogId']),
      ownerEpoch = _scopeEpoch(value['ownerEpoch']),
      runId = _scopeString(value['runId']),
      blockId = _scopeString(value['blockId']),
      effectId = _scopeString(value['effectId']),
      attempt = _scopeU32(value['attempt']);
}

typedef CatalogStepHandler =
    FutureOr<String> Function(
      CatalogCallbackContext context,
      String stepName,
      String inputJson,
    );

abstract interface class CatalogTokenSource {
  String currentToken();
  Future<String> refreshToken();
  factory CatalogTokenSource.callbacks({
    required String Function() currentToken,
    required Future<String> Function() refreshToken,
  }) = _CatalogTokenCallbacks;
}

final class _CatalogTokenCallbacks implements CatalogTokenSource {
  final String Function() _current;
  final Future<String> Function() _refresh;
  _CatalogTokenCallbacks({
    required String Function() currentToken,
    required Future<String> Function() refreshToken,
  }) : _current = currentToken,
       _refresh = refreshToken;
  @override
  String currentToken() => _current();
  @override
  Future<String> refreshToken() => _refresh();
}

/// Closed SDK failure type; handling it never invokes arbitrary error getters.
final class CatalogHandlerFailure implements Exception {
  final String message;
  final bool permanent;
  const CatalogHandlerFailure.permanent(this.message) : permanent = true;
  const CatalogHandlerFailure.retryable(this.message) : permanent = false;
}

final class CatalogRunSnapshot {
  final String instanceId,
      accountId,
      catalogId,
      sessionEpoch,
      state,
      dataJson,
      createdAt,
      updatedAt;
  final List<String> pendingSteps;
  CatalogRunSnapshot._(Map<String, Object?> v)
    : instanceId = _scopeString(v['instanceId']),
      accountId = _scopeString(v['accountId']),
      catalogId = _scopeString(v['catalogId']),
      sessionEpoch = _scopeEpoch(v['sessionEpoch']),
      state = _scopeState(v['state']),
      dataJson = _scopeString(v['dataJson']),
      createdAt = _scopeString(v['createdAt']),
      updatedAt = _scopeString(v['updatedAt']),
      pendingSteps = List.unmodifiable(
        (v['pendingSteps'] as List).map(_scopeString),
      );
}

final class CatalogArchiveReceipt {
  final String instanceId,
      accountId,
      catalogId,
      acknowledgementDigest,
      archivedAt;
  final String? outcome;
  CatalogArchiveReceipt._(Map<String, Object?> v)
    : instanceId = _scopeString(v['instanceId']),
      accountId = _scopeString(v['accountId']),
      catalogId = _scopeString(v['catalogId']),
      acknowledgementDigest = _scopeString(v['acknowledgementDigest']),
      archivedAt = _scopeString(v['archivedAt']),
      outcome = v['outcome'] == null
          ? null
          : _scopeState(v['outcome'], terminal: true) {
    if (!RegExp(r'^[0-9a-f]{64}$').hasMatch(acknowledgementDigest))
      throw const FormatException('Invalid acknowledgement digest');
  }
}

final class CatalogNativeState {
  final String kind;
  final String? state;
  CatalogNativeState._(Map<String, Object?> v)
    : kind = _scopeString(v['kind']),
      state = v['state'] == null
          ? null
          : _scopeState(v['state'], terminal: v['kind'] == 'recorded') {
    if (![
          'not_materialized',
          'unavailable',
          'present',
          'recorded',
        ].contains(kind) ||
        (['present', 'recorded'].contains(kind) != (state != null)))
      throw const FormatException('Invalid native state');
  }
}

final class CatalogRunSummary {
  final String instanceId,
      accountId,
      catalogId,
      ownerEpoch,
      workflow,
      sequenceId,
      createdAt,
      requestState;
  final int version;
  final CatalogNativeState nativeState;
  CatalogRunSummary._(Map<String, Object?> v)
    : instanceId = _scopeString(v['instanceId']),
      accountId = _scopeString(v['accountId']),
      catalogId = _scopeString(v['catalogId']),
      ownerEpoch = _scopeEpoch(v['ownerEpoch']),
      workflow = _scopeString(v['workflow']),
      sequenceId = _scopeString(v['sequenceId']),
      createdAt = _scopeString(v['createdAt']),
      requestState = _scopeString(v['requestState']),
      version = _scopeU32(v['version']),
      nativeState = CatalogNativeState._(_scopeObject(v['nativeState'])) {
    if (!['prepared', 'cancelled', 'archived'].contains(requestState) ||
        (requestState == 'archived' &&
            !['recorded', 'unavailable'].contains(nativeState.kind)) ||
        (requestState != 'archived' && nativeState.kind == 'recorded'))
      throw const FormatException('Inconsistent archive history');
  }
}

final class CatalogRunPage {
  final List<CatalogRunSummary> items;
  final CatalogHistoryCursor? next;
  CatalogRunPage._(List<CatalogRunSummary> items, this.next)
    : items = List.unmodifiable(items);
}

Map<String, Object?> _scopeObject(Object? value) {
  if (value is! Map)
    throw const FormatException('Invalid scoped native object');
  return Map<String, Object?>.from(value);
}

String _scopeString(Object? value) {
  if (value is! String || value.isEmpty)
    throw const FormatException('Invalid scoped native string');
  return value;
}

String _scopeEpoch(Object? value) {
  final v = _scopeString(value);
  if (!RegExp(r'^[1-9][0-9]*$').hasMatch(v) ||
      v.length > 19 ||
      (v.length == 19 && v.compareTo('9223372036854775807') > 0))
    throw const FormatException('Invalid scoped native epoch');
  return v;
}

int _scopeU32(Object? value) {
  if (value is! num ||
      !value.isFinite ||
      value < 0 ||
      value > 4294967295 ||
      value != value.truncateToDouble())
    throw const FormatException('Invalid scoped native integer');
  return value.toInt();
}

String _scopeState(Object? value, {bool terminal = false}) {
  final all = [
    'scheduled',
    'running',
    'waiting',
    'paused',
    'completed',
    'failed',
    'cancelled',
  ];
  if (value is! String || !(terminal ? all.sublist(4) : all).contains(value))
    throw const FormatException('Invalid scoped native state');
  return value;
}

bool _scopeBoolean(Object? value) {
  if (value is! bool)
    throw const FormatException('Invalid scoped native boolean');
  return value;
}

PlatformException _scopeRetired() =>
    PlatformException(code: 'ORCH8_SHUTDOWN', message: 'scope view retired');

final class _ScopeFence {
  bool closed = false;
  final _ScopeFence? parent;
  _ScopeFence([this.parent]);
  void check() {
    if (closed) throw _scopeRetired();
    parent?.check();
  }
}

final class _ScopeDescriptor {
  final String handle, kind, accountId, sessionEpoch;
  final String? catalogId, instanceId;
  _ScopeDescriptor(Object? value, String expected, [_ScopeDescriptor? parent])
    : this._(_scopeObject(value), expected, parent);
  _ScopeDescriptor._(
    Map<String, Object?> v,
    String expected,
    _ScopeDescriptor? parent,
  ) : handle = _scopeString(v['handle']),
      kind = _scopeString(v['kind']),
      accountId = _scopeString(v['accountId']),
      sessionEpoch = _scopeEpoch(v['sessionEpoch']),
      catalogId = v['catalogId'] == null ? null : _scopeString(v['catalogId']),
      instanceId = v['instanceId'] == null
          ? null
          : _scopeString(v['instanceId']) {
    if (kind != expected ||
        (kind != 'account' && catalogId == null) ||
        (kind == 'run' && instanceId == null) ||
        (parent != null &&
            (accountId != parent.accountId ||
                sessionEpoch != parent.sessionEpoch ||
                (parent.catalogId != null && catalogId != parent.catalogId))))
      throw const FormatException('Scoped native owner mismatch');
  }
  Map<String, Object?> owned(Object? value, {bool run = false}) {
    final v = _scopeObject(value);
    if (v['accountId'] != accountId ||
        v['catalogId'] != catalogId ||
        (run && v['instanceId'] != instanceId))
      throw const FormatException('Scoped native owner mismatch');
    return v;
  }
}

final class _ScopeHostBudget {
  int active = 0;
  final Map<String, int> accounts = {};
  void Function() enter(String account) {
    final used = accounts[account] ?? 0;
    if (active >= 32 || used >= 2)
      throw const CatalogHandlerFailure.retryable(
        'Scoped host capacity exhausted',
      );
    active++;
    accounts[account] = used + 1;
    return () {
      active--;
      final remaining = accounts[account]! - 1;
      if (remaining == 0) {
        accounts.remove(account);
      } else {
        accounts[account] = remaining;
      }
    };
  }
}

final class _RegisteredCatalogHandler {
  final _ScopeDescriptor owner;
  final _ScopeFence fence;
  final CatalogStepHandler handler;
  _RegisteredCatalogHandler(this.owner, this.fence, this.handler);
}

final class _RegisteredCatalogTokens {
  final _ScopeDescriptor owner;
  final _ScopeFence fence;
  final CatalogTokenSource source;
  _RegisteredCatalogTokens(this.owner, this.fence, this.source);
}

final class _ScopedClient {
  static int _nextCallback = 0;
  static final _handlerBudget = _ScopeHostBudget(),
      _tokenBudget = _ScopeHostBudget();
  static _ScopeFence _channelRoot = _ScopeFence();
  _ScopeFence _root = _ScopeFence()..closed = true;
  final MethodChannel channel;
  final Map<String, _RegisteredCatalogHandler> _handlers = {};
  final Map<String, _RegisteredCatalogTokens> _tokens = {};
  final Map<String, String> _handlerNames = {};
  final Map<String, Future<void>> _registrationTails = {};
  final Map<String, ({String epoch, _ScopeFence fence})> _accounts = {};
  int _pendingRegistrations = 0, _pendingSyncs = 0;
  _ScopedClient(this.channel);
  void reset() {
    _channelRoot.closed = true;
    _root.closed = true;
    _channelRoot = _ScopeFence();
    _root = _channelRoot;
    _handlers.clear();
    _tokens.clear();
    _handlerNames.clear();
    _registrationTails.clear();
    _accounts.clear();
  }

  String _key() {
    if (_nextCallback >= 9007199254740991)
      throw StateError('Scoped callback identity space exhausted');
    return 'scope-${++_nextCallback}';
  }

  Future<Object?> _native(
    String handle,
    String method,
    Map<String, Object?> args,
  ) async {
    final encoded = jsonEncode(args);
    final reply = await channel.invokeMethod<String>('scopedCall', {
      'handle': handle,
      'method': method,
      'argsJson': encoded,
    });
    if (reply == null)
      throw const FormatException('Missing scoped native reply');
    return jsonDecode(reply);
  }

  Future<void> _releaseOrphan(String handle) async {
    try {
      await _native(handle, 'dispose', {});
    } catch (_) {
      /* Retired engine has already pruned its table. */
    }
  }

  Future<Object?> call(
    _ScopeFence fence,
    String handle,
    String method, [
    Map<String, Object?> args = const {},
  ]) async {
    fence.check();
    final encoded = jsonEncode(args);
    fence.check();
    final reply = await channel.invokeMethod<String>('scopedCall', {
      'handle': handle,
      'method': method,
      'argsJson': encoded,
    });
    if (reply == null)
      throw const FormatException('Missing scoped native reply');
    final value = jsonDecode(reply);
    try {
      fence.check();
    } catch (_) {
      if (value is Map) {
        final next = value['next'];
        final orphan = value['handle'] ?? (next is Map ? next['handle'] : null);
        if (orphan is String) unawaited(_releaseOrphan(orphan));
      }
      rethrow;
    }
    return value;
  }

  Future<Orch8Account> openAccount(AccountIdentity identity) async {
    final root = _root;
    final value = _ScopeDescriptor(
      await call(root, '', 'openAccount', identity.toMap()),
      'account',
    );
    var session = _accounts[value.accountId];
    if (session != null && session.epoch != value.sessionEpoch) {
      session.fence.closed = true;
      releaseHandlers(session.fence);
      session = null;
    }
    session ??= (epoch: value.sessionEpoch, fence: _ScopeFence(root));
    _accounts[value.accountId] = session;
    return Orch8Account._(this, value, _ScopeFence(session.fence));
  }

  void retiredAccount(_ScopeDescriptor owner) {
    final session = _accounts[owner.accountId];
    if (session?.epoch == owner.sessionEpoch) {
      session!.fence.closed = true;
      releaseHandlers(session.fence);
      _accounts.remove(owner.accountId);
    }
  }

  void releaseHandlers(_ScopeFence fence) {
    _handlers.removeWhere((_, value) {
      for (
        _ScopeFence? current = value.fence;
        current != null;
        current = current.parent
      ) {
        if (identical(current, fence)) return true;
      }
      return false;
    });
    _handlerNames.removeWhere((_, key) => !_handlers.containsKey(key));
  }

  Future<void> register(
    _ScopeDescriptor owner,
    _ScopeFence fence,
    String name,
    CatalogStepHandler handler,
    Duration timeout,
  ) async {
    if (_pendingRegistrations >= 1024)
      throw PlatformException(
        code: 'ORCH8_RESOURCE_LIMIT',
        message: 'Scoped registration queue exhausted',
      );
    if (timeout <= Duration.zero || timeout.inMilliseconds > 9007199254740991)
      throw ArgumentError.value(timeout, 'timeout');
    final binding = jsonEncode([
      owner.accountId,
      owner.catalogId,
      owner.sessionEpoch,
      name,
    ]);
    final previous = _registrationTails[binding];
    _pendingRegistrations++;
    final operation = () async {
      if (previous != null) {
        try {
          await previous;
        } catch (_) {}
      }
      fence.check();
      if (_handlers.length >= 1024)
        throw PlatformException(
          code: 'ORCH8_RESOURCE_LIMIT',
          message: 'Scoped handler table exhausted',
        );
      final key = _key();
      _handlers[key] = _RegisteredCatalogHandler(owner, fence, handler);
      try {
        await call(fence, owner.handle, 'registerHandler', {
          'name': name,
          'callbackKey': key,
          'timeoutMs': timeout.inMilliseconds,
        });
        final old = _handlerNames[binding];
        if (old != null) _handlers.remove(old);
        _handlerNames[binding] = key;
      } catch (_) {
        _handlers.remove(key);
        rethrow;
      }
    }();
    _registrationTails[binding] = operation;
    try {
      await operation;
    } finally {
      _pendingRegistrations--;
      if (identical(_registrationTails[binding], operation))
        _registrationTails.remove(binding);
    }
  }

  Future<SyncResult> sync(
    _ScopeDescriptor owner,
    _ScopeFence fence,
    CatalogTokenSource source,
  ) async {
    fence.check();
    if (_pendingSyncs >= 32)
      throw PlatformException(
        code: 'ORCH8_RESOURCE_LIMIT',
        message: 'Scoped sync capacity exhausted',
      );
    final token = source.currentToken(), key = _key();
    _pendingSyncs++;
    _tokens[key] = _RegisteredCatalogTokens(owner, fence, source);
    try {
      final value = _scopeObject(
        await call(fence, owner.handle, 'sync', {
          'providerKey': key,
          'initialToken': token,
        }),
      );
      final counts = {
        for (final name in [
          'added',
          'updated',
          'removed',
          'skipped',
          'signatureFailures',
        ])
          name: _scopeU32(value[name]),
      };
      return SyncResult.fromMap(counts);
    } finally {
      _tokens.remove(key);
      _pendingSyncs--;
    }
  }

  Future<String> callback(MethodCall call) async {
    void Function()? release;
    try {
      final body = _scopeObject(call.arguments);
      if (call.method == 'scopedExecuteStep') {
        final registration = _handlers[_scopeString(body['callbackKey'])];
        if (registration == null) throw _scopeRetired();
        registration.fence.check();
        final context = CatalogCallbackContext._(
          registration.owner.owned(body['context']),
        );
        if (context.ownerEpoch != registration.owner.sessionEpoch)
          throw const FormatException('Callback session mismatch');
        release = _handlerBudget.enter(registration.owner.accountId);
        final output = await registration.handler(
          context,
          _scopeString(body['stepName']),
          _scopeString(body['inputJson']),
        );
        registration.fence.check();
        return output;
      }
      final registration = _tokens[_scopeString(body['providerKey'])];
      if (registration == null) throw _scopeRetired();
      registration.fence.check();
      release = _tokenBudget.enter(registration.owner.accountId);
      final token = await registration.source.refreshToken();
      registration.fence.check();
      return _scopeString(token);
    } catch (error) {
      final known = error is CatalogHandlerFailure ? error : null;
      throw PlatformException(
        code: known?.permanent == true
            ? 'ORCH8_CATALOG_PERMANENT'
            : 'ORCH8_CATALOG_RETRYABLE',
        message: known?.message ?? 'scoped host callback failed',
      );
    } finally {
      release?.call();
    }
  }
}

abstract base class _CatalogView {
  final _ScopedClient _client;
  final _ScopeDescriptor _owner;
  final _ScopeFence _fence;
  bool _disposed = false;
  _CatalogView(this._client, this._owner, this._fence);
  String get accountId => _owner.accountId;
  String get sessionEpoch => _owner.sessionEpoch;
  void _check() {
    if (_disposed) throw _scopeRetired();
    _fence.check();
  }

  Future<Object?> _call(
    String method, [
    Map<String, Object?> args = const {},
  ]) async {
    _check();
    return _client.call(_fence, _owner.handle, method, args);
  }

  Future<void> closeView() async {
    if (_fence.closed) return;
    try {
      await _call('closeView');
    } finally {
      _fence.closed = true;
      _client.releaseHandlers(_fence);
    }
  }

  Future<void> dispose() async {
    if (_disposed) return;
    await _client._native(_owner.handle, 'dispose', {});
    _disposed = true;
  }
}

final class Orch8Account extends _CatalogView {
  Orch8Account._(super.client, super.owner, super.fence);
  Future<Orch8Catalog> openCatalog(CatalogConfiguration configuration) async =>
      Orch8Catalog._(
        _client,
        _ScopeDescriptor(
          await _call('openCatalog', configuration.toMap()),
          'catalog',
          _owner,
        ),
        _ScopeFence(_fence),
      );
  Future<void> retire() async {
    try {
      await _call('retire');
    } finally {
      _fence.closed = true;
      _client.retiredAccount(_owner);
    }
  }
}

final class Orch8Catalog extends _CatalogView {
  Orch8Catalog._(super.client, super.owner, super.fence);
  String get catalogId => _owner.catalogId!;
  Future<void> registerHandler(
    String name,
    CatalogStepHandler handler, {
    Duration timeout = const Duration(seconds: 30),
  }) async {
    _check();
    return _client.register(_owner, _fence, name, handler, timeout);
  }

  Future<SyncResult> sync(CatalogTokenSource source) async {
    _check();
    return _client.sync(_owner, _fence, source);
  }

  Future<Orch8CatalogRun> prepareRun(CatalogRunRequest request) async =>
      Orch8CatalogRun._(
        _client,
        _ScopeDescriptor(
          await _call('prepareRun', request.toMap()),
          'run',
          _owner,
        ),
        _ScopeFence(_fence),
      );
  Future<Orch8CatalogRun> run(String instanceId) async => Orch8CatalogRun._(
    _client,
    _ScopeDescriptor(
      await _call('run', {'instanceId': instanceId}),
      'run',
      _owner,
    ),
    _ScopeFence(_fence),
  );
  Future<CatalogRunPage> history({
    CatalogHistoryCursor? cursor,
    int limit = 64,
  }) async {
    if (limit < 1 || limit > 128) throw ArgumentError.value(limit, 'limit');
    final value = _scopeObject(
      await _call('history', {
        'limit': limit,
        'cursor': cursor?._reference(_client, _owner),
      }),
    );
    final items = (value['items'] as List)
        .map((item) => CatalogRunSummary._(_owner.owned(item)))
        .toList();
    final next = value['next'] == null
        ? null
        : CatalogHistoryCursor._(
            _client,
            _ScopeDescriptor(value['next'], 'cursor', _owner),
            _ScopeFence(_fence),
          );
    return CatalogRunPage._(items, next);
  }
}

final class CatalogHistoryCursor extends _CatalogView {
  CatalogHistoryCursor._(super.client, super.owner, super.fence);
  String _reference(_ScopedClient client, _ScopeDescriptor owner) {
    _check();
    if (!identical(client, _client) ||
        owner.accountId != accountId ||
        owner.catalogId != _owner.catalogId ||
        owner.sessionEpoch != sessionEpoch)
      throw ArgumentError('Cursor belongs to another catalog session');
    return _owner.handle;
  }
}

final class Orch8CatalogRun extends _CatalogView {
  Orch8CatalogRun._(super.client, super.owner, super.fence);
  String get catalogId => _owner.catalogId!;
  String get instanceId => _owner.instanceId!;
  Future<void> start() async {
    await _call('start');
  }

  Future<CatalogRunSnapshot?> snapshot() async {
    final value = await _call('snapshot');
    if (value == null) return null;
    final snapshot = CatalogRunSnapshot._(_owner.owned(value, run: true));
    if (snapshot.sessionEpoch != sessionEpoch)
      throw const FormatException('Snapshot session mismatch');
    return snapshot;
  }

  Future<String?> cancel() async {
    final value = await _call('cancel');
    return value == null ? null : _scopeState(value);
  }

  Future<bool> completeStep(
    String expectedUpdatedAt,
    String stepName,
    String outputJson,
  ) async => _scopeBoolean(
    await _call('completeStep', {
      'expectedUpdatedAt': expectedUpdatedAt,
      'stepName': stepName,
      'outputJson': outputJson,
    }),
  );
  Future<bool> answer(
    CatalogRunSnapshot snapshot,
    String stepName,
    String choice, {
    Map<String, Object?> data = const {},
  }) async {
    if (snapshot.instanceId != instanceId ||
        snapshot.accountId != accountId ||
        snapshot.catalogId != catalogId ||
        snapshot.sessionEpoch != sessionEpoch ||
        choice.trim().isEmpty ||
        data.containsKey('value'))
      throw ArgumentError('Invalid answer or snapshot owner');
    return completeStep(
      snapshot.updatedAt,
      stepName,
      jsonEncode({...data, 'value': choice}),
    );
  }

  Future<CatalogArchiveReceipt?> archiveAfterAcknowledgement(
    String acknowledgement,
  ) async {
    final value = await _call('archiveAfterAcknowledgement', {
      'acknowledgement': acknowledgement,
    });
    return value == null
        ? null
        : CatalogArchiveReceipt._(_owner.owned(value, run: true));
  }

  Future<bool> forgetCancelledRequest() async =>
      _scopeBoolean(await _call('forgetCancelledRequest'));
}
