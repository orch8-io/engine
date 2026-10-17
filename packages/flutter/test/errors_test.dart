import 'package:flutter_test/flutter_test.dart';
import 'package:orch8_flutter/src/errors.dart';

class _Fake {
  _Fake(this.code, this.details);
  final String code;
  final Object? details;
}

void main() {
  test('details.kind wins over the code', () {
    expect(
      orch8ErrorKindFromChannel('ORCH8_ERROR', {'kind': 'not_found'}),
      Orch8ErrorKind.notFound,
    );
  });

  test('falls back to the ORCH8_<KIND> code', () {
    expect(orch8ErrorKindFromChannel('ORCH8_RESOURCE_LIMIT', null),
        Orch8ErrorKind.resourceLimit);
    expect(orch8ErrorKindFromChannel('ORCH8_SIGNATURE_INVALID', null),
        Orch8ErrorKind.signatureInvalid);
  });

  test('legacy and foreign codes are unknown', () {
    expect(orch8ErrorKindFromChannel('ORCH8_ERROR', null), Orch8ErrorKind.unknown);
    expect(orch8ErrorKindFromChannel('INIT_ERROR', null), Orch8ErrorKind.unknown);
    expect(orch8ErrorKindFromChannel('NO_HANDLER', null), Orch8ErrorKind.unknown);
  });

  test('fromWire accepts snake, upper and camel case', () {
    expect(Orch8ErrorKind.fromWire('invalid_input'), Orch8ErrorKind.invalidInput);
    expect(Orch8ErrorKind.fromWire('INVALID_INPUT'), Orch8ErrorKind.invalidInput);
    expect(Orch8ErrorKind.fromWire('InvalidInput'), Orch8ErrorKind.invalidInput);
    expect(Orch8ErrorKind.fromWire('bogus'), Orch8ErrorKind.unknown);
  });

  test('orch8ErrorKindOf reads code/details duck-typed', () {
    expect(orch8ErrorKindOf(_Fake('ORCH8_NETWORK', null)), Orch8ErrorKind.network);
    expect(orch8ErrorKindOf(Exception('x')), Orch8ErrorKind.unknown);
  });

  test('retryable kinds', () {
    expect(Orch8ErrorKind.network.isRetryable, isTrue);
    expect(Orch8ErrorKind.invalidInput.isRetryable, isFalse);
  });
}
