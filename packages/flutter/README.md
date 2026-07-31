# orch8_flutter

Flutter bridge for the Orch8 `0.7.1` embedded durable workflow engine.

```yaml
dependencies:
  orch8_flutter: ^0.7.1
```

```dart
import 'package:orch8_flutter/orch8_flutter.dart';

final orch8 = Orch8();
await orch8.initialize();
```

The plugin supports Swift Package Manager on current Flutter releases and
CocoaPods as a compatibility fallback. Android resolves the native AAR from
Orch8's public, read-only Maven repository.

Requires Dart 3.12+, Flutter 3.44+, iOS 16+, Xcode 16+, and Android API 24+.
