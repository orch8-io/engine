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
    this.sdkVersion = '0.1.0',
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

  SyncResult({
    required this.added,
    required this.updated,
    required this.removed,
    required this.skipped,
  });

  factory SyncResult.fromMap(Map<String, dynamic> map) => SyncResult(
        added: map['added'] as int,
        updated: map['updated'] as int,
        removed: map['removed'] as int,
        skipped: map['skipped'] as int,
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

typedef StepHandler = FutureOr<String> Function(String stepName, String input);

class Orch8 {
  static const MethodChannel _channel = MethodChannel('io.orch8/mobile');
  static const EventChannel _eventChannel = EventChannel('io.orch8/events');

  final Map<String, StepHandler> _handlers = {};
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

  Future<void> flushTelemetry(String endpointUrl) =>
      _channel.invokeMethod('flushTelemetry', {'endpointUrl': endpointUrl});

  Future<void> shutdown() async {
    await _channel.invokeMethod('shutdown');
    _eventSubscription?.cancel();
    _completedController.close();
    _failedController.close();
    _pendingController.close();
  }

  Future<dynamic> _handleMethodCall(MethodCall call) async {
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
      return await handler(stepName, input);
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
