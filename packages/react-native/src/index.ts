import { NativeModules, NativeEventEmitter } from "react-native";
import { Orch8Client, type NativeOrch8 } from "./core";

export * from "./types";
export {
  Orch8Client,
  PermanentHandlerError,
  parseTaskContext,
  EVENTS,
  MAX_DELEGATION_TTL_SECS,
} from "./core";
export type { NativeOrch8, NativeDelegateRequest, EventSource, Subscription, TokenFetcher } from "./core";

const { Orch8Module } = NativeModules as { Orch8Module?: NativeOrch8 };

if (!Orch8Module) {
  throw new Error(
    "@orch8.io/react-native-orch8: NativeModule not found. " +
      "Rebuild the app after installing (pod install on iOS; on Android add " +
      "Orch8's Maven repository, see the package README)."
  );
}

const emitter = new NativeEventEmitter(Orch8Module as never);

export const orch8 = new Orch8Client(Orch8Module, emitter);
export default orch8;
