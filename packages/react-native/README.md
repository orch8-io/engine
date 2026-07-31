# @orch8.io/react-native-orch8

React Native bridge for the Orch8 `0.7.1` embedded durable workflow engine.

```bash
npm install @orch8.io/react-native-orch8@0.7.1
```

On iOS, run `pod install` after installing the package. On Android, the
package resolves the native AAR from Orch8's public, read-only Maven
repository.

```ts
import { orch8 } from "@orch8.io/react-native-orch8";

await orch8.initialize();
```

Requires React Native 0.71+, iOS 16+, Xcode 16+, and Android API 24+.
