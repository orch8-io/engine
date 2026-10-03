library orch8_flutter;

import 'dart:async';
import 'dart:convert';
import 'package:flutter/services.dart';

class Orch8Config {
  final String? dbPath;
  final int tickIntervalMs;
  final int maxConcurrentSteps;
  final int maxStepsPerInstance;
  final int maxConcurrentInstances;
  final int maxTickDurationMs;
  final int maxInstanceLifetimeSecs;
  final int maxStoredSequences;
  final int maxSequenceSizeBytes;
  final int handlerTimeoutMs;
  final int operationTimeoutMs;
  final bool telemetryEnabled;
  final String environment;
  final String rootPublicKey;
  final String sdkVersion;

  /// HTTPS telemetry ingest endpoint. Empty disables delivery.
  final String telemetryUrl;

  /// Skip ticks while process RSS exceeds this many bytes (0 = unlimited).
  final int memoryBudgetBytes;

  /// Endpoint returning a JSON array of sequences for [Orch8.loadSequencesFromUrl].
  final String sequencesUrl;

  /// Server sync endpoint (`…/api/v1/mobile/sync`). Required for [Orch8.registerNode].
  final String syncUrl;
  final String deviceId;

  /// Static API key for sync and runtime-node calls. **Legacy, not for
  /// production apps**: a key stored in the app is extractable from the
  /// binary. Use [Orch8.setTokenProvider] with device sessions minted by your
  /// backend instead, and never put an operator key here.
  final String syncApiKey;

  const Orch8Config({
    this.dbPath,
    this.tickIntervalMs = 100,
    this.maxConcurrentSteps = 4,
    this.maxStepsPerInstance = 1000,
    this.maxConcurrentInstances = 10,
    this.maxTickDurationMs = 5000,
    this.maxInstanceLifetimeSecs = 86400,
    this.maxStoredSequences = 50,
    this.maxSequenceSizeBytes = 1048576,
    this.handlerTimeoutMs = 30000,
    this.operationTimeoutMs = 10000,
    this.telemetryEnabled = true,
    this.environment = 'production',
    this.rootPublicKey = '',
    this.sdkVersion = '0.7.1',
    this.telemetryUrl = '',
    this.memoryBudgetBytes = 0,
    this.sequencesUrl = '',
    this.syncUrl = '',
    this.deviceId = '',
    this.syncApiKey = '',
  });

  Map<String, dynamic> toMap() => {
        if (dbPath != null) 'dbPath': dbPath,
        'tickIntervalMs': tickIntervalMs,
        'maxConcurrentSteps': maxConcurrentSteps,
        'maxStepsPerInstance': maxStepsPerInstance,
        'maxConcurrentInstances': maxConcurrentInstances,
        'maxTickDurationMs': maxTickDurationMs,
        'maxInstanceLifetimeSecs': maxInstanceLifetimeSecs,
        'maxStoredSequences': maxStoredSequences,
        'maxSequenceSizeBytes': maxSequenceSizeBytes,
        'handlerTimeoutMs': handlerTimeoutMs,
        'operationTimeoutMs': operationTimeoutMs,
        'telemetryEnabled': telemetryEnabled,
        'environment': environment,
        'rootPublicKey': rootPublicKey,
        'sdkVersion': sdkVersion,
        'telemetryUrl': telemetryUrl,
        'memoryBudgetBytes': memoryBudgetBytes,
        'sequencesUrl': sequencesUrl,
        'syncUrl': syncUrl,
        'deviceId': deviceId,
        'syncApiKey': syncApiKey,
      };
}

class TickResult {
  final int instancesAdvanced;
  final int stepsExecuted;
  final bool hasPendingWork;

  TickResult({
    required this.instancesAdvanced,
    required this.stepsExecuted,
    required this.hasPendingWork,
  });

  factory TickResult.fromMap(Map<String, dynamic> map) => TickResult(
        instancesAdvanced: map['instancesAdvanced'] as int,
        stepsExecuted: map['stepsExecuted'] as int,
        hasPendingWork: map['hasPendingWork'] as bool,
      );
}

class SyncResult {
  final int added;
  final int updated;
  final int removed;
  final int skipped;
  final int signatureFailures;

  SyncResult({
    required this.added,
    required this.updated,
    required this.removed,
    required this.skipped,
    this.signatureFailures = 0,
  });

  factory SyncResult.fromMap(Map<String, dynamic> map) => SyncResult(
        added: map['added'] as int,
        updated: map['updated'] as int,
        removed: map['removed'] as int,
        skipped: map['skipped'] as int,
        signatureFailures: (map['signatureFailures'] as int?) ?? 0,
      );
}

class SequenceInfo {
  final String name;
  final int version;

  SequenceInfo({required this.name, required this.version});

  factory SequenceInfo.fromMap(Map<String, dynamic> map) => SequenceInfo(
        name: map['name'] as String,
        version: map['version'] as int,
      );
}

class InstanceSummary {
  final String instanceId;
  final String sequenceName;
  final String state;
  final String createdAt;

  InstanceSummary({
    required this.instanceId,
    required this.sequenceName,
    required this.state,
    required this.createdAt,
  });

  factory InstanceSummary.fromMap(Map<String, dynamic> map) => InstanceSummary(
        instanceId: map['instanceId'] as String,
        sequenceName: map['sequenceName'] as String,
        state: map['state'] as String,
        createdAt: map['createdAt'] as String,
      );
}

class BackgroundRunResult {
  final int ticksExecuted;
  final int instancesAdvanced;
  final int stepsExecuted;
  final bool hasPendingWork;

  /// Work remains; schedule another OS background opportunity.
  final bool budgetExhausted;

  BackgroundRunResult({
    required this.ticksExecuted,
    required this.instancesAdvanced,
    required this.stepsExecuted,
    required this.hasPendingWork,
    required this.budgetExhausted,
  });

  factory BackgroundRunResult.fromMap(Map<String, dynamic> map) =>
      BackgroundRunResult(
        ticksExecuted: map['ticksExecuted'] as int,
        instancesAdvanced: map['instancesAdvanced'] as int,
        stepsExecuted: map['stepsExecuted'] as int,
        hasPendingWork: map['hasPendingWork'] as bool,
        budgetExhausted: map['budgetExhausted'] as bool,
      );
}

// -- Delegation from phone-local workflows (engine release after 0.7.1) ----

/// Maximum lifetime of a delegation and its grant accepted by the control plane.
const Duration maxDelegationTtl = Duration(seconds: 86400);

/// Options for [Orch8.startDelegation].
class DelegationOptions {
  /// Tenant of the node credential; every continuity call is scoped to it.
  final String tenantId;

  /// How often pending delegations are advanced and polled. Push wakes
  /// advance them immediately.
  final Duration pollInterval;

  /// Lifetime of each grant and delegation (whole seconds, at most one day).
  /// A destination that has not reported by then fails the delegation and
  /// the parked step follows its retry policy.
  final Duration ttl;

  const DelegationOptions({
    required this.tenantId,
    this.pollInterval = const Duration(seconds: 2),
    this.ttl = const Duration(seconds: 600),
  });

  void _validate() {
    if (tenantId.trim().isEmpty) {
      throw ArgumentError.value(tenantId, 'tenantId', 'must not be empty');
    }
    if (pollInterval <= Duration.zero) {
      throw ArgumentError.value(pollInterval, 'pollInterval', 'must be positive');
    }
    if (ttl < const Duration(seconds: 1) || ttl > maxDelegationTtl) {
      throw ArgumentError.value(ttl, 'ttl', 'must be between 1s and 86400s');
    }
  }

  Map<String, dynamic> toMap() => {
        'tenantId': tenantId,
        'pollIntervalMs': pollInterval.inMilliseconds,
        'ttlSecs': ttl.inSeconds,
      };
}

/// An explicit delegation of a server-side sub-sequence ([Orch8.delegate]).
/// No local step is parked.
class DelegateRequest {
  /// Local parent instance the delegation belongs to (must exist).
  final String instanceId;

  /// Destination runtime id (a live registration of the same tenant).
  final String destinationRuntimeId;

  /// Server-side sequence the destination runs.
  final String subSequenceId;

  /// Explicit input handed to the sub-sequence.
  final Map<String, dynamic> input;

  const DelegateRequest({
    required this.instanceId,
    required this.destinationRuntimeId,
    required this.subSequenceId,
    this.input = const {},
  });

  void _validate() {
    for (final entry in {
      'instanceId': instanceId,
      'destinationRuntimeId': destinationRuntimeId,
      'subSequenceId': subSequenceId,
    }.entries) {
      if (entry.value.trim().isEmpty) {
        throw ArgumentError.value(entry.value, entry.key, 'must not be empty');
      }
    }
  }

  Map<String, dynamic> toMap() => {
        'instanceId': instanceId,
        'destinationRuntimeId': destinationRuntimeId,
        'subSequenceId': subSequenceId,
        'inputJson': jsonEncode(input),
      };
}

/// Where a delegation stands. `preparing`: not yet accepted by the control
/// plane; `delegated`: in the destination's mailbox or running there;
/// `abandoned`: never placed before its deadline.
enum DelegationState {
  preparing,
  delegated,
  completed,
  failed,
  abandoned;

  bool get isTerminal => this == completed || this == failed || this == abandoned;

  static DelegationState fromWire(String value) => DelegationState.values.firstWhere(
        (s) => s.name == value,
        orElse: () => throw FormatException('unknown delegation state: $value'),
      );
}

/// A delegation as journaled on this device.
class DelegationStatus {
  final String delegationId;
  final DelegationState state;
  final String localInstanceId;

  /// The parked local step, for delegations made by a sequence.
  final String? blockId;
  final String? destinationRuntimeId;

  /// The destination's reported output (JSON), once completed.
  final String? outputJson;
  final String? error;

  DelegationStatus({
    required this.delegationId,
    required this.state,
    required this.localInstanceId,
    this.blockId,
    this.destinationRuntimeId,
    this.outputJson,
    this.error,
  });

  /// [outputJson] decoded, or null.
  dynamic get output => outputJson == null ? null : jsonDecode(outputJson!);

  factory DelegationStatus.fromMap(Map<String, dynamic> map) => DelegationStatus(
        delegationId: map['delegationId'] as String,
        state: DelegationState.fromWire(map['state'] as String),
        localInstanceId: map['localInstanceId'] as String,
        blockId: map['blockId'] as String?,
        destinationRuntimeId: map['destinationRuntimeId'] as String?,
        outputJson: map['outputJson'] as String?,
        error: map['error'] as String?,
      );
}

/// Counters of the delegation pump (zeros while it is not running).
class DelegationStats {
  final bool running;

  /// Delegations accepted by the control plane.
  final int delegated;
  final int completed;
  final int failed;
  final int abandoned;

  /// Parked local steps resumed with an outcome (exactly once each).
  final int resumed;

  DelegationStats({
    required this.running,
    required this.delegated,
    required this.completed,
    required this.failed,
    required this.abandoned,
    required this.resumed,
  });

  factory DelegationStats.fromMap(Map<String, dynamic> map) => DelegationStats(
        running: map['running'] as bool,
        delegated: map['delegated'] as int,
        completed: map['completed'] as int,
        failed: map['failed'] as int,
        abandoned: map['abandoned'] as int,
        resumed: map['resumed'] as int,
      );
}

class FlushResult {
  final int sent;
  final int dropped;

  FlushResult({required this.sent, required this.dropped});

  factory FlushResult.fromMap(Map<String, dynamic> map) =>
      FlushResult(sent: map['sent'] as int, dropped: map['dropped'] as int);
}

class InstanceState {
  final String instanceId;
  final String sequenceName;

  /// `scheduled`, `running`, `waiting`, `paused`, `completed`, `failed` or `cancelled`.
  final String state;
  final String context;
  final String createdAt;
  final String updatedAt;

  InstanceState({
    required this.instanceId,
    required this.sequenceName,
    required this.state,
    required this.context,
    required this.createdAt,
    required this.updatedAt,
  });

  factory InstanceState.fromMap(Map<String, dynamic> map) => InstanceState(
        instanceId: map['instanceId'] as String,
        sequenceName: map['sequenceName'] as String,
        state: map['state'] as String,
        context: map['context'] as String,
        createdAt: map['createdAt'] as String,
        updatedAt: map['updatedAt'] as String,
      );
}

enum PowerState { charging, unplugged, lowBattery, criticalBattery }

// -- Runtime node / worker (engine release after 0.7.1) ----------------------

enum NodeConnectivity { offline, metered, wifi, ethernet }

/// What this device offers the distributed-execution mesh. `handlers` empty
/// means every handler registered with [Orch8.registerHandler].
class NodeCapabilities {
  final List<String> handlers;
  final List<String> regions;

  /// Free-form hardware facts (`camera`, `nfc`, …).
  final List<String> hardware;
  final List<String> plugins;

  /// Credential binding *names* available on the device (never secrets).
  final List<String> credentials;
  final bool offlineCapable;
  final NodeConnectivity? connectivity;

  /// 0..100.
  final int? batteryPercent;
  final String? platform;

  /// APNs/FCM token used for id-only wake-up hints.
  final String? pushToken;
  final String? appVersion;

  /// Overrides the API base derived from [Orch8Config.syncUrl].
  final String? apiBaseUrl;
  final String? capsuleSigningPublicKey;

  const NodeCapabilities({
    this.handlers = const [],
    this.regions = const [],
    this.hardware = const [],
    this.plugins = const [],
    this.credentials = const [],
    this.offlineCapable = true,
    this.connectivity,
    this.batteryPercent,
    this.platform,
    this.pushToken,
    this.appVersion,
    this.apiBaseUrl,
    this.capsuleSigningPublicKey,
  }) : assert(batteryPercent == null ||
            (batteryPercent >= 0 && batteryPercent <= 100));

  Map<String, dynamic> toMap() => {
        'handlers': handlers,
        'regions': regions,
        'hardware': hardware,
        'plugins': plugins,
        'credentials': credentials,
        'offlineCapable': offlineCapable,
        'connectivity': connectivity?.name,
        'batteryPercent': batteryPercent,
        'platform': platform,
        'pushToken': pushToken,
        'appVersion': appVersion,
        'apiBaseUrl': apiBaseUrl,
        'capsuleSigningPublicKey': capsuleSigningPublicKey,
      };
}

class NodeRegistration {
  final String runtimeId;
  final String deviceId;
  final List<String> handlers;
  final String expiresAt;

  NodeRegistration({
    required this.runtimeId,
    required this.deviceId,
    required this.handlers,
    required this.expiresAt,
  });

  factory NodeRegistration.fromMap(Map<String, dynamic> map) =>
      NodeRegistration(
        runtimeId: map['runtimeId'] as String,
        deviceId: map['deviceId'] as String,
        handlers: List<String>.from(map['handlers'] as List),
        expiresAt: map['expiresAt'] as String,
      );
}

class WorkerOptions {
  final int maxConcurrentTasks;
  final Duration idlePollInterval;
  final String? version;

  const WorkerOptions({
    this.maxConcurrentTasks = 1,
    this.idlePollInterval = const Duration(seconds: 15),
    this.version,
  });

  Map<String, dynamic> toMap() => {
        'maxConcurrentTasks': maxConcurrentTasks,
        'idlePollIntervalMs': idlePollInterval.inMilliseconds,
        'version': version,
      };
}

class WorkerStats {
  final bool running;
  final int inFlight;
  final int claimed;
  final int completed;
  final int failed;
  final int released;
  final int lost;

  WorkerStats({
    required this.running,
    required this.inFlight,
    required this.claimed,
    required this.completed,
    required this.failed,
    required this.released,
    required this.lost,
  });

  factory WorkerStats.fromMap(Map<String, dynamic> map) => WorkerStats(
        running: map['running'] as bool,
        inFlight: map['inFlight'] as int,
        claimed: map['claimed'] as int,
        completed: map['completed'] as int,
        failed: map['failed'] as int,
        released: map['released'] as int,
        lost: map['lost'] as int,
      );
}

class WorkerWindowResult {
  final int claimed;
  final int completed;
  final int failed;
  final int stillRunning;
  final bool budgetExhausted;

  WorkerWindowResult({
    required this.claimed,
    required this.completed,
    required this.failed,
    required this.stillRunning,
    required this.budgetExhausted,
  });

  factory WorkerWindowResult.fromMap(Map<String, dynamic> map) =>
      WorkerWindowResult(
        claimed: map['claimed'] as int,
        completed: map['completed'] as int,
        failed: map['failed'] as int,
        stillRunning: map['stillRunning'] as int,
        budgetExhausted: map['budgetExhausted'] as bool,
      );
}

/// Remote-task metadata the worker loop adds to handler params as `__orch8`.
class Orch8TaskContext {
  /// The server's idempotency key for the step's effect; send it to
  /// downstream APIs. Null against servers that predate the contract.
  final String? effectId;
  final String? taskId;
  final String? instanceId;
  final String? blockId;
  final int? attempt;
  final String? runtimeId;
  final int? continuityEpoch;
  final Object? resumeCheckpoint;

  const Orch8TaskContext({
    this.effectId,
    this.taskId,
    this.instanceId,
    this.blockId,
    this.attempt,
    this.runtimeId,
    this.continuityEpoch,
    this.resumeCheckpoint,
  });

  /// Parse `__orch8` from a handler's `input`; null for local steps.
  static Orch8TaskContext? fromInput(String input) {
    Object? root;
    try {
      root = jsonDecode(input);
    } on FormatException {
      return null;
    }
    if (root is! Map) return null;
    final meta = root['__orch8'];
    if (meta is! Map) return null;
    String? str(String k) => meta[k] is String ? meta[k] as String : null;
    int? num(String k) => meta[k] is int ? meta[k] as int : null;
    return Orch8TaskContext(
      effectId: str('effect_id'),
      taskId: str('task_id'),
      instanceId: str('instance_id'),
      blockId: str('block_id'),
      attempt: num('attempt'),
      runtimeId: str('runtime_id'),
      continuityEpoch: num('continuity_epoch'),
      resumeCheckpoint: meta['resume_checkpoint'],
    );
  }
}

/// Throw from a [StepHandler] to fail the step without retry.
class PermanentHandlerException implements Exception {
  final String message;
  PermanentHandlerException(this.message);
  @override
  String toString() => 'PermanentHandlerException: $message';
}

/// Native step handler. [input] is the params JSON; remote tasks carry a
/// reserved `__orch8` member (see [Orch8TaskContext.fromInput]).
/// Return output JSON. Throw [PermanentHandlerException] to fail without
/// retry; any other error is retryable.
typedef StepHandler = FutureOr<String> Function(String stepName, String input);

class Orch8 {
  static const MethodChannel _channel = MethodChannel('io.orch8/mobile');
  static const EventChannel _eventChannel = EventChannel('io.orch8/events');

  final Map<String, StepHandler> _handlers = {};
  Future<String> Function()? _fetchToken;
  StreamSubscription? _eventSubscription;

  final StreamController<({String instanceId, String output})>
      _completedController = StreamController.broadcast();
  final StreamController<({String instanceId, String error})>
      _failedController = StreamController.broadcast();
  final StreamController<
          ({String instanceId, String stepName, String handler})>
      _pendingController = StreamController.broadcast();

  Stream<({String instanceId, String output})> get onInstanceCompleted =>
      _completedController.stream;
  Stream<({String instanceId, String error})> get onInstanceFailed =>
      _failedController.stream;
  Stream<({String instanceId, String stepName, String handler})>
      get onStepPending => _pendingController.stream;

  Future<void> initialize([Orch8Config config = const Orch8Config()]) async {
    _channel.setMethodCallHandler(_handleMethodCall);
    _setupEventChannel();
    await _channel.invokeMethod('initialize', config.toMap());
  }

  Future<void> registerHandler(String name, StepHandler handler) async {
    _handlers[name] = handler;
    await _channel.invokeMethod('registerHandler', {'name': name});
  }

  Future<void> resume() => _channel.invokeMethod('resume');

  Future<void> pause() => _channel.invokeMethod('pause');

  Future<TickResult> tickOnce() async {
    final map = await _channel.invokeMapMethod<String, dynamic>('tickOnce');
    return TickResult.fromMap(map!);
  }

  Future<String> start(
    String sequenceName, {
    String input = '{}',
    String? dedupKey,
  }) async {
    final result = await _channel.invokeMethod<String>('start', {
      'sequenceName': sequenceName,
      'input': input,
      'dedupKey': dedupKey,
    });
    return result!;
  }

  Future<void> cancelInstance(String instanceId) =>
      _channel.invokeMethod('cancelInstance', {'instanceId': instanceId});

  Future<void> completeStep(
    String instanceId,
    String stepName,
    String output,
  ) =>
      _channel.invokeMethod('completeStep', {
        'instanceId': instanceId,
        'stepName': stepName,
        'output': output,
      });

  Future<List<SequenceInfo>> loadedSequences() async {
    final list = await _channel.invokeListMethod<Map>('loadedSequences');
    return list
            ?.map(
                (m) => SequenceInfo.fromMap(Map<String, dynamic>.from(m)))
            .toList() ??
        [];
  }

  Future<SyncResult> sync(String manifestUrl, {String? token}) async {
    final map = await _channel.invokeMapMethod<String, dynamic>('sync', {
      'manifestUrl': manifestUrl,
      'token': token,
    });
    return SyncResult.fromMap(map!);
  }

  Future<FlushResult> flushTelemetry(String endpointUrl) async {
    final map = await _channel.invokeMapMethod<String, dynamic>(
        'flushTelemetry', {'endpointUrl': endpointUrl});
    return FlushResult.fromMap(map!);
  }

  /// Drain work inside an OS-granted background window.
  Future<BackgroundRunResult> runUntilIdle(int maxTicks, Duration timeBudget) async {
    if (maxTicks <= 0 || timeBudget <= Duration.zero) {
      throw ArgumentError('maxTicks and timeBudget must be positive');
    }
    final map = await _channel.invokeMapMethod<String, dynamic>('runUntilIdle', {
      'maxTicks': maxTicks,
      'timeBudgetMs': timeBudget.inMilliseconds,
    });
    return BackgroundRunResult.fromMap(map!);
  }

  Future<void> reportPowerState(PowerState state) =>
      _channel.invokeMethod('reportPowerState', {'state': state.name});

  Future<InstanceState> getInstance(String instanceId) async {
    final map = await _channel.invokeMapMethod<String, dynamic>(
        'getInstance', {'instanceId': instanceId});
    return InstanceState.fromMap(map!);
  }

  Future<List<InstanceSummary>> activeInstances() async {
    final list = await _channel.invokeListMethod<Map>('activeInstances');
    return list
            ?.map((m) => InstanceSummary.fromMap(Map<String, dynamic>.from(m)))
            .toList() ??
        [];
  }

  Future<void> loadSequenceFromJson(String json) =>
      _channel.invokeMethod('loadSequenceFromJson', {'json': json});

  /// Empty [url] uses [Orch8Config.sequencesUrl]. Returns the number loaded.
  Future<int> loadSequencesFromUrl([String url = '']) async =>
      (await _channel.invokeMethod<int>('loadSequencesFromUrl', {'url': url}))!;

  /// Trigger an immediate sync and worker poll after a push notification.
  Future<void> onPushReceived() => _channel.invokeMethod('onPushReceived');

  // -- Runtime node / worker (engine release after 0.7.1) ------------------

  /// Stable runtime UUID of this installation (the lease `worker_id`).
  Future<String> nodeRuntimeId() async =>
      (await _channel.invokeMethod<String>('nodeRuntimeId'))!;

  /// Authenticate every control-plane call (node registration, worker
  /// leases, delegation, sync reporting) with short-lived **device sessions**
  /// instead of the legacy static `syncApiKey`.
  ///
  /// [fetchToken] asks your app backend for a fresh `dst_…` token; the backend
  /// holds the operator key and mints it with `POST /runtimes/device-sessions`
  /// for this device id and [nodeRuntimeId]. It is awaited once here for the
  /// initial token and again whenever the control plane answers `401` (the
  /// request is then retried once); the engine waits at most [refreshTimeout]
  /// for each refresh. Call it after [initialize] and before [registerNode].
  /// Never ship an operator key in an app. Needs the engine release after 0.7.1.
  Future<void> setTokenProvider(
    Future<String> Function() fetchToken, {
    Duration refreshTimeout = const Duration(seconds: 30),
  }) async {
    if (refreshTimeout <= Duration.zero) {
      throw ArgumentError.value(
          refreshTimeout, 'refreshTimeout', 'must be greater than zero');
    }
    final token = await fetchToken();
    if (token.trim().isEmpty) {
      throw ArgumentError('fetchToken returned an empty token');
    }
    _fetchToken = fetchToken;
    await _channel.invokeMethod('setTokenProvider', {
      'token': token,
      'refreshTimeoutMs': refreshTimeout.inMilliseconds,
    });
  }

  /// Join the runtime mesh: registers device + capabilities using `syncUrl`
  /// and the node credential ([setTokenProvider], or the legacy
  /// `syncApiKey`), then re-advertises before the 5-minute TTL.
  Future<NodeRegistration> registerNode(
      [NodeCapabilities capabilities = const NodeCapabilities()]) async {
    final map = await _channel.invokeMapMethod<String, dynamic>(
        'registerNode', capabilities.toMap());
    return NodeRegistration.fromMap(map!);
  }

  Future<void> updateNodeStatus({NodeConnectivity? connectivity, int? batteryPercent}) {
    if (batteryPercent != null && (batteryPercent < 0 || batteryPercent > 100)) {
      throw ArgumentError.value(batteryPercent, 'batteryPercent', 'must be 0..100');
    }
    return _channel.invokeMethod('updateNodeStatus', {
      'connectivity': connectivity?.name,
      'batteryPercent': batteryPercent,
    });
  }

  /// Stop the worker, advertise `draining`, stop re-advertising.
  Future<void> unregisterNode() => _channel.invokeMethod('unregisterNode');

  /// Start the remote worker loop. Register handlers first.
  Future<void> startWorker([WorkerOptions options = const WorkerOptions()]) {
    if (options.maxConcurrentTasks <= 0 || options.idlePollInterval <= Duration.zero) {
      throw ArgumentError('maxConcurrentTasks and idlePollInterval must be positive');
    }
    return _channel.invokeMethod('startWorker', options.toMap());
  }

  Future<void> stopWorker() => _channel.invokeMethod('stopWorker');

  /// Claim and run remote tasks inside a bounded background window.
  Future<WorkerWindowResult> runWorkerWindow(Duration timeBudget) async {
    if (timeBudget <= Duration.zero) {
      throw ArgumentError.value(timeBudget, 'timeBudget', 'must be positive');
    }
    final map = await _channel.invokeMapMethod<String, dynamic>(
        'runWorkerWindow', {'timeBudgetMs': timeBudget.inMilliseconds});
    return WorkerWindowResult.fromMap(map!);
  }

  Future<WorkerStats> workerStats() async {
    final map = await _channel.invokeMapMethod<String, dynamic>('workerStats');
    return WorkerStats.fromMap(map!);
  }

  /// Forward an id-only wake push (`task_id` / `runtime_id` / `reason`,
  /// optionally nested under `orch8`). Returns false when it is not for this
  /// runtime or carries no Orch8 fields.
  Future<bool> onPushWake(Map<String, dynamic> data) async {
    final source = data['orch8'] is Map ? Map<String, dynamic>.from(data['orch8'] as Map) : data;
    final envelope = <String, String>{
      for (final k in const ['task_id', 'runtime_id', 'reason'])
        if (source[k] is String) k: source[k] as String,
    };
    if (envelope.isEmpty) return false;
    return (await _channel.invokeMethod<bool>(
            'onPushWake', {'envelopeJson': jsonEncode(envelope)})) ??
        false;
  }

  /// Enable an opt-in builtin handler (`http_request`) before [resume].
  Future<void> enableBuiltin(String name) =>
      _channel.invokeMethod('enableBuiltin', {'name': name});

  // -- Delegation from phone-local workflows (engine release after 0.7.1) --

  /// Start the delegation pump: a step of a workflow running on this engine
  /// whose `$runtime` places it on another runtime is handed to that runtime
  /// through the server mailbox; the local instance parks and resumes
  /// exactly once with the result. Requires [registerNode]. Delegations are
  /// journaled and survive app kills: call again after every launch.
  Future<void> startDelegation(DelegationOptions options) {
    options._validate();
    return _channel.invokeMethod('startDelegation', options.toMap());
  }

  /// Pause the pump. Journaled delegations resume with the next
  /// [startDelegation].
  Future<void> stopDelegation() => _channel.invokeMethod('stopDelegation');

  /// Delegate a server-side sub-sequence on behalf of a local instance
  /// without parking a step. Returns the delegation id; read the outcome with
  /// [delegationStatus]. Requires [startDelegation].
  Future<String> delegate(DelegateRequest request) async {
    request._validate();
    return (await _channel.invokeMethod<String>('delegate', request.toMap()))!;
  }

  /// The locally journaled state of a delegation (a `PlatformException` when
  /// unknown).
  Future<DelegationStatus> delegationStatus(String delegationId) async {
    if (delegationId.trim().isEmpty) {
      throw ArgumentError.value(delegationId, 'delegationId', 'must not be empty');
    }
    final map = await _channel.invokeMapMethod<String, dynamic>(
        'delegationStatus', {'delegationId': delegationId});
    return DelegationStatus.fromMap(map!);
  }

  /// Every journaled delegation, oldest first.
  Future<List<DelegationStatus>> listDelegations() async {
    final list = await _channel.invokeListMethod<Map>('listDelegations') ?? const [];
    return list.map((m) => DelegationStatus.fromMap(Map<String, dynamic>.from(m))).toList();
  }

  /// Pump counters (zeros while it is not running).
  Future<DelegationStats> delegationStats() async {
    final map = await _channel.invokeMapMethod<String, dynamic>('delegationStats');
    return DelegationStats.fromMap(map!);
  }

  Future<void> shutdown() async {
    await _channel.invokeMethod('shutdown');
    _eventSubscription?.cancel();
    _completedController.close();
    _failedController.close();
    _pendingController.close();
  }

  Future<dynamic> _handleMethodCall(MethodCall call) async {
    if (call.method == 'refreshToken') {
      final fetchToken = _fetchToken;
      if (fetchToken == null) {
        throw PlatformException(
            code: 'NO_TOKEN_PROVIDER', message: 'setTokenProvider not called');
      }
      final token = await fetchToken();
      if (token.trim().isEmpty) {
        throw PlatformException(
            code: 'EMPTY_TOKEN', message: 'fetchToken returned an empty token');
      }
      return token;
    }
    if (call.method == 'executeStep') {
      final args = call.arguments as Map;
      final stepName = args['stepName'] as String;
      final input = args['input'] as String;
      final handler = _handlers[stepName];
      if (handler == null) {
        throw PlatformException(
          code: 'NO_HANDLER',
          message: "No handler registered for step '$stepName'",
        );
      }
      try {
        return await handler(stepName, input);
      } on PermanentHandlerException catch (e) {
        throw PlatformException(code: 'PERMANENT', message: e.message);
      } catch (e) {
        throw PlatformException(code: 'RETRYABLE', message: e.toString());
      }
    }
    return null;
  }

  void _setupEventChannel() {
    _eventSubscription = _eventChannel.receiveBroadcastStream().listen((event) {
      final map = Map<String, dynamic>.from(event as Map);
      final type = map['type'] as String;
      switch (type) {
        case 'instanceCompleted':
          _completedController.add((
            instanceId: map['instanceId'] as String,
            output: map['output'] as String,
          ));
        case 'instanceFailed':
          _failedController.add((
            instanceId: map['instanceId'] as String,
            error: map['error'] as String,
          ));
        case 'stepPending':
          _pendingController.add((
            instanceId: map['instanceId'] as String,
            stepName: map['stepName'] as String,
            handler: map['handler'] as String,
          ));
      }
    });
  }
}
