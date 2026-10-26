import 'dart:async';
import 'dart:convert';
import 'package:flutter/services.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:orch8_flutter/orch8_flutter.dart';

const configuration = CatalogConfiguration(
  catalogKey: 'one',
  source: 'source',
  signingAuthority: 'key',
  manifestUrl: 'https://catalog.example/manifest',
  identityUrl: 'https://catalog.example/identity',
);

final class Fixture {
  static const channel = MethodChannel('io.orch8/mobile');
  static const eventChannel = MethodChannel('io.orch8/events');
  final root = Orch8();
  final views = <String, Map<String, Object?>>{};
  final requests = <Map<String, Object?>>[],
      registrations = <Map<String, Object?>>[];
  final hostGates = <Completer<String>>[];
  final callbacks = <Future<Object?>>[];
  Future<Object?>? Function(String, Map<String, Object?>)? intercept;
  String epoch = '9007199254740993';
  int next = 0;
  Future<void> start() async {
    final messenger =
        TestDefaultBinaryMessengerBinding.instance.defaultBinaryMessenger;
    messenger.setMockMethodCallHandler(eventChannel, (_) async => null);
    messenger.setMockMethodCallHandler(channel, (call) async {
      if (call.method != 'scopedCall') return null;
      final envelope = Map<String, Object?>.from(call.arguments as Map),
          method = envelope['method'] as String;
      final args = Map<String, Object?>.from(
        jsonDecode(envelope['argsJson'] as String) as Map,
      );
      final handle = envelope['handle'] as String, parent = views[handle];
      requests.add({'handle': handle, 'method': method, 'args': args});
      final custom = intercept?.call(method, args);
      if (custom != null) {
        final value = await custom;
        return jsonEncode(method == 'sync' && value == null ? counts : value);
      }
      Object? value;
      if ([
        'openAccount',
        'openCatalog',
        'prepareRun',
        'run',
      ].contains(method)) {
        final kind = method == 'openAccount'
            ? 'account'
            : method == 'openCatalog'
            ? 'catalog'
            : 'run';
        final descriptor = <String, Object?>{
          'handle': 'h${++next}',
          'kind': kind,
          'accountId': parent?['accountId'] ?? args['accountKey'],
          'sessionEpoch': parent?['sessionEpoch'] ?? epoch,
        };
        if (kind != 'account')
          descriptor['catalogId'] = kind == 'catalog'
              ? '${parent!['accountId']}:${args['catalogKey']}'
              : parent!['catalogId'];
        if (kind == 'run')
          descriptor['instanceId'] = args['instanceId'] ?? 'run$next';
        views[descriptor['handle'] as String] = descriptor;
        value = descriptor;
      } else if (method == 'registerHandler') {
        registrations.add({...args, 'owner': parent});
      } else if (method == 'sync') {
        value = counts;
      } else if (method == 'history') {
        value = {'items': [], 'next': null};
      } else if (method == 'completeStep' ||
          method == 'forgetCancelledRequest') {
        value = true;
      }
      return jsonEncode(value);
    });
    await root.initialize();
  }

  static const counts = {
    'added': 0,
    'updated': 0,
    'removed': 0,
    'skipped': 0,
    'signatureFailures': 0,
  };
  Future<Orch8Account> account([String name = 'alice']) => root.openAccount(
    AccountIdentity(accountKey: name, issuer: 'issuer', subject: name),
  );
  Completer<String> gate() {
    final value = Completer<String>();
    hostGates.add(value);
    return value;
  }

  Map<String, Object?> event(
    Map<String, Object?> registration, [
    Map<String, Object?> overrides = const {},
  ]) {
    final owner = registration['owner'] as Map;
    return {
      'callbackKey': registration['callbackKey'],
      'requestId': 'request',
      'stepName': 'step',
      'inputJson': '{}',
      'context': {
        'accountId': owner['accountId'],
        'catalogId': owner['catalogId'],
        'ownerEpoch': owner['sessionEpoch'],
        'runId': 'run',
        'blockId': 'block',
        'attempt': 0,
        'effectId': 'effect',
        ...overrides,
      },
    };
  }

  Future<Object?> deliver(String method, Map<String, Object?> args) {
    final operation = () async {
      const codec = StandardMethodCodec();
      final reply = await TestDefaultBinaryMessengerBinding
          .instance
          .defaultBinaryMessenger
          .handlePlatformMessage(
            channel.name,
            codec.encodeMethodCall(MethodCall(method, args)),
            null,
          );
      try {
        return codec.decodeEnvelope(reply!);
      } catch (error) {
        return error;
      }
    }();
    callbacks.add(operation);
    return operation;
  }

  Future<void> close() async {
    for (final gate in hostGates) {
      if (!gate.isCompleted) gate.complete('{}');
    }
    await Future.wait(callbacks);
    await root.shutdown();
    final messenger =
        TestDefaultBinaryMessengerBinding.instance.defaultBinaryMessenger;
    messenger.setMockMethodCallHandler(channel, null);
    messenger.setMockMethodCallHandler(eventChannel, null);
  }
}

Future<void> flush() async {
  for (var i = 0; i < 12; i++) {
    await Future<void>.value();
  }
}

void main() {
  TestWidgetsFlutterBinding.ensureInitialized();
  late Fixture f;
  setUp(() async {
    f = Fixture();
    await f.start();
  });
  tearDown(() => f.close());
  test(
    'preserves large epochs and routes same-named handlers by explicit catalog',
    () async {
      final a = await (await f.account()).openCatalog(configuration),
          b = await (await f.account('bob')).openCatalog(configuration);
      final calls = <CatalogCallbackContext>[];
      for (final catalog in [a, b]) {
        await catalog.registerHandler('same', (context, _, _) {
          calls.add(context);
          return '{}';
        });
      }
      expect(
        await f.deliver('scopedExecuteStep', f.event(f.registrations[0])),
        '{}',
      );
      expect(
        await f.deliver('scopedExecuteStep', f.event(f.registrations[1])),
        '{}',
      );
      expect(calls.map((c) => c.accountId), ['alice', 'bob']);
      expect(calls.first.ownerEpoch, '9007199254740993');
    },
  );
  test(
    'rejects foreign owners and lossy numeric epochs before host entry',
    () async {
      final catalog = await (await f.account()).openCatalog(configuration);
      var entered = 0;
      await catalog.registerHandler('handler', (_, _, _) {
        entered++;
        return '{}';
      });
      expect(
        await f.deliver(
          'scopedExecuteStep',
          f.event(f.registrations[0], {'accountId': 'bob'}),
        ),
        isA<PlatformException>(),
      );
      expect(
        await f.deliver(
          'scopedExecuteStep',
          f.event(f.registrations[0], {'ownerEpoch': 9007199254740992}),
        ),
        isA<PlatformException>(),
      );
      expect(entered, 0);
    },
  );
  test(
    'retirement through a sibling fences all views and late replies',
    () async {
      final first = await f.account(), sibling = await f.account();
      final catalog = await sibling.openCatalog(configuration),
          waiting = f.gate();
      await catalog.registerHandler('handler', (_, _, _) => waiting.future);
      final reply = f.deliver('scopedExecuteStep', f.event(f.registrations[0]));
      await flush();
      await first.retire();
      waiting.complete('{}');
      expect(await reply, isA<PlatformException>());
      await expectLater(catalog.run('run'), throwsA(isA<PlatformException>()));
      f.epoch = '9007199254740994';
      expect((await f.account()).sessionEpoch, f.epoch);
    },
  );
  test(
    'late retirement cannot close a freshly reopened account epoch',
    () async {
      final old = await f.account(), gate = Completer<Object?>();
      f.intercept = (method, _) => method == 'retire' ? gate.future : null;
      final retiring = old.retire();
      final retired = expectLater(retiring, throwsA(isA<PlatformException>()));
      await flush();
      f.epoch = '9007199254740994';
      final fresh = await f.account();
      gate.complete(null);
      await retired;
      final catalog = await fresh.openCatalog(configuration);
      await (await catalog.run('fresh')).start();
    },
  );
  test(
    'closing a parent disposes a child returned by a late native reply',
    () async {
      final account = await f.account(), gate = Completer<Object?>();
      f.intercept = (method, _) => method == 'openCatalog' ? gate.future : null;
      final opening = account.openCatalog(configuration);
      final rejected = expectLater(opening, throwsA(isA<PlatformException>()));
      await flush();
      await account.closeView();
      gate.complete({
        'handle': 'orphan',
        'kind': 'catalog',
        'accountId': 'alice',
        'catalogId': 'alice:one',
        'sessionEpoch': account.sessionEpoch,
      });
      await rejected;
      await flush();
      expect(
        f.requests.where(
          (r) => r['handle'] == 'orphan' && r['method'] == 'dispose',
        ),
        hasLength(1),
      );
    },
  );
  test(
    'temporary parent disposal preserves children; closeView fences descendants',
    () async {
      final account = await f.account(),
          catalog = await account.openCatalog(configuration),
          run = await catalog.run('run');
      await account.dispose();
      await catalog.dispose();
      await run.start();
      final other = await (await f.account()).openCatalog(configuration),
          child = await other.run('child');
      await other.closeView();
      await expectLater(child.start(), throwsA(isA<PlatformException>()));
      var getter = 0;
      await expectLater(
        catalog.sync(
          CatalogTokenSource.callbacks(
            currentToken: () {
              getter++;
              return 'token';
            },
            refreshToken: () async => 'fresh',
          ),
        ),
        throwsA(isA<PlatformException>()),
      );
      expect(getter, 0);
    },
  );
  test(
    'pending registration admission is bounded before native settlement',
    () async {
      final catalog = await (await f.account()).openCatalog(configuration);
      final gate = Completer<Object?>();
      var entered = 0;
      f.intercept = (method, _) {
        if (method != 'registerHandler') return null;
        entered++;
        return entered == 1 ? gate.future : Future.value(null);
      };
      final admitted = List.generate(
        1024,
        (_) => catalog.registerHandler('same', (_, _, _) => '{}'),
      );
      await expectLater(
        catalog.registerHandler('overflow', (_, _, _) => '{}'),
        throwsA(
          isA<PlatformException>().having(
            (e) => e.code,
            'code',
            'ORCH8_RESOURCE_LIMIT',
          ),
        ),
      );
      await flush();
      expect(entered, 1);
      gate.complete(null);
      await Future.wait(admitted);
      expect(entered, 1024);
      await catalog.registerHandler('after-settlement', (_, _, _) => '{}');
      expect(entered, 1025);
    },
  );
  test(
    'serialized replacement rejects queued deliveries to superseded handlers',
    () async {
      final catalog = await (await f.account()).openCatalog(configuration);
      var first = 0, second = 0;
      await Future.wait([
        catalog.registerHandler('same', (_, _, _) {
          first++;
          return '{}';
        }),
        catalog.registerHandler('same', (_, _, _) {
          second++;
          return '{}';
        }),
      ]);
      expect(
        await f.deliver('scopedExecuteStep', f.event(f.registrations[0])),
        isA<PlatformException>(),
      );
      expect(
        await f.deliver('scopedExecuteStep', f.event(f.registrations[1])),
        '{}',
      );
      expect(first, 0);
      expect(second, 1);
    },
  );
  test(
    'host handler admission survives root reset and another account recovers',
    () async {
      final catalog = await (await f.account()).openCatalog(configuration),
          gate = f.gate();
      var entered = 0;
      await catalog.registerHandler('hung', (_, _, _) {
        entered++;
        return gate.future;
      });
      final old = [
        f.deliver('scopedExecuteStep', f.event(f.registrations[0])),
        f.deliver('scopedExecuteStep', f.event(f.registrations[0])),
      ];
      await flush();
      await f.root.initialize();
      final fresh = await (await f.account()).openCatalog(configuration);
      var healthy = 0;
      await fresh.registerHandler('new', (_, _, _) {
        healthy++;
        return '{}';
      });
      expect(
        await f.deliver('scopedExecuteStep', f.event(f.registrations[1])),
        isA<PlatformException>(),
      );
      expect(entered, 2);
      expect(healthy, 0);
      final bob = await (await f.account('bob')).openCatalog(configuration);
      await bob.registerHandler('healthy', (_, _, _) {
        healthy++;
        return '{}';
      });
      expect(
        await f.deliver('scopedExecuteStep', f.event(f.registrations[2])),
        '{}',
      );
      gate.complete('{}');
      await Future.wait(old);
      expect(
        await f.deliver('scopedExecuteStep', f.event(f.registrations[1])),
        '{}',
      );
      expect(healthy, 2);
    },
  );
  test(
    'each loaded module enforces the default 32 unsettled handler ceiling',
    () async {
      final gate = f.gate(), old = <Future<Object?>>[];
      for (var index = 0; index < 16; index++) {
        final catalog = await (await f.account(
          'capacity-$index',
        )).openCatalog(configuration);
        await catalog.registerHandler('hung', (_, _, _) => gate.future);
        old.add(
          f.deliver('scopedExecuteStep', f.event(f.registrations[index])),
        );
        old.add(
          f.deliver('scopedExecuteStep', f.event(f.registrations[index])),
        );
      }
      await flush();
      final extra = await (await f.account('extra')).openCatalog(configuration);
      var entered = 0;
      await extra.registerHandler('extra', (_, _, _) {
        entered++;
        return '{}';
      });
      expect(
        await f.deliver('scopedExecuteStep', f.event(f.registrations.last)),
        isA<PlatformException>(),
      );
      expect(entered, 0);
      gate.complete('{}');
      await Future.wait(old);
      expect(
        await f.deliver('scopedExecuteStep', f.event(f.registrations.last)),
        '{}',
      );
      expect(entered, 1);
    },
  );
  test(
    'token refresh sources remain per-operation and are fenced on reset',
    () async {
      final catalog = await (await f.account()).openCatalog(configuration),
          operation = Completer<Object?>(),
          token = f.gate();
      final keys = <String>[];
      f.intercept = (method, args) {
        if (method != 'sync') return null;
        keys.add(args['providerKey'] as String);
        return operation.future;
      };
      final first = catalog.sync(
        CatalogTokenSource.callbacks(
          currentToken: () => 'alice',
          refreshToken: () => token.future,
        ),
      );
      final second = catalog.sync(
        CatalogTokenSource.callbacks(
          currentToken: () => 'bob',
          refreshToken: () async => 'bob-fresh',
        ),
      );
      final operations = [
        first.then<Object?>((v) => v, onError: (Object e) => e),
        second.then<Object?>((v) => v, onError: (Object e) => e),
      ];
      await flush();
      final late = f.deliver('scopedRefreshToken', {
        'providerKey': keys[0],
        'requestId': 'a',
      });
      expect(
        await f.deliver('scopedRefreshToken', {
          'providerKey': keys[1],
          'requestId': 'b',
        }),
        'bob-fresh',
      );
      await f.root.initialize();
      token.complete('alice-late');
      operation.complete(null);
      expect(await late, isA<PlatformException>());
      await Future.wait(operations);
    },
  );
  test(
    'native sync completion cannot release an unsettled token host charge',
    () async {
      final catalog = await (await f.account()).openCatalog(configuration),
          operation = Completer<Object?>(),
          token = f.gate();
      final keys = <String>[];
      var entered = 0;
      f.intercept = (method, args) {
        if (method != 'sync') return null;
        keys.add(args['providerKey'] as String);
        return operation.future;
      };
      final source = CatalogTokenSource.callbacks(
        currentToken: () => 'token',
        refreshToken: () {
          entered++;
          return token.future;
        },
      );
      final pending = [catalog.sync(source), catalog.sync(source)];
      await flush();
      final old = [
        f.deliver('scopedRefreshToken', {
          'providerKey': keys[0],
          'requestId': 'a',
        }),
        f.deliver('scopedRefreshToken', {
          'providerKey': keys[1],
          'requestId': 'b',
        }),
      ];
      await flush();
      operation.complete(null);
      await Future.wait(pending);
      final next = Completer<Object?>();
      f.intercept = (method, args) {
        if (method != 'sync') return null;
        keys.add(args['providerKey'] as String);
        return next.future;
      };
      final third = catalog.sync(source);
      await flush();
      expect(
        await f.deliver('scopedRefreshToken', {
          'providerKey': keys.last,
          'requestId': 'c',
        }),
        isA<PlatformException>(),
      );
      expect(entered, 2);
      token.complete('fresh');
      next.complete(null);
      await Future.wait(old);
      await third;
    },
  );
  test(
    'cursor reuse across catalogs is refused and exact answer/archive arguments are sent',
    () async {
      final a = await (await f.account()).openCatalog(configuration),
          b = await (await f.account('bob')).openCatalog(configuration);
      f.intercept = (method, _) => method == 'history'
          ? Future.value({
              'items': [],
              'next': {
                'handle': 'cursor',
                'kind': 'cursor',
                'accountId': 'alice',
                'catalogId': 'alice:one',
                'sessionEpoch': a.sessionEpoch,
              },
            })
          : null;
      final page = await a.history();
      await expectLater(b.history(cursor: page.next), throwsArgumentError);
      final run = await a.run('run');
      f.intercept = (method, _) => method == 'snapshot'
          ? Future.value({
              'instanceId': 'run',
              'accountId': 'alice',
              'catalogId': 'alice:one',
              'sessionEpoch': a.sessionEpoch,
              'state': 'waiting',
              'dataJson': '{}',
              'pendingSteps': ['approval'],
              'createdAt': 'created',
              'updatedAt': 'exact-version',
            })
          : null;
      final snapshot = (await run.snapshot())!;
      expect(
        await run.answer(snapshot, 'approval', 'yes', data: {'note': 'ok'}),
        isTrue,
      );
      expect(f.requests.last['args'], {
        'expectedUpdatedAt': 'exact-version',
        'stepName': 'approval',
        'outputJson': '{"note":"ok","value":"yes"}',
      });
      await run.archiveAfterAcknowledgement('backend-receipt');
      expect(f.requests.last['args'], {'acknowledgement': 'backend-receipt'});
    },
  );
  test(
    'permanent SDK errors retain classification and unknown errors stay bounded',
    () async {
      final catalog = await (await f.account()).openCatalog(configuration);
      await catalog.registerHandler(
        'known',
        (_, _, _) =>
            throw const CatalogHandlerFailure.permanent('known failure'),
      );
      final known =
          await f.deliver('scopedExecuteStep', f.event(f.registrations[0]))
              as PlatformException;
      expect(known.code, 'ORCH8_CATALOG_PERMANENT');
      expect(known.message, 'known failure');
      await catalog.registerHandler('unknown', (_, _, _) => throw Object());
      final unknown =
          await f.deliver('scopedExecuteStep', f.event(f.registrations[1]))
              as PlatformException;
      expect(unknown.code, 'ORCH8_CATALOG_RETRYABLE');
      expect(unknown.message, 'scoped host callback failed');
    },
  );
}
