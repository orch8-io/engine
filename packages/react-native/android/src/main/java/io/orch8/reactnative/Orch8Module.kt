package io.orch8.reactnative

import com.facebook.react.bridge.*
import com.facebook.react.modules.core.DeviceEventManagerModule
import io.orch8.mobile.*

class Orch8Module(reactContext: ReactApplicationContext) :
    ReactContextBaseJavaModule(reactContext) {

    private var engine: MobileEngine? = null

    override fun getName(): String = "Orch8Module"

    @ReactMethod
    fun initialize(config: ReadableMap, promise: Promise) {
        try {
            val dbPath = config.getString("dbPath")
                ?: reactApplicationContext.getDatabasePath("orch8.db").absolutePath

            val cfg = MobileEngineConfig(
                tickIntervalMs = config.getDoubleOr("tickIntervalMs", 100.0).toULong(),
                maxConcurrentSteps = config.getDoubleOr("maxConcurrentSteps", 4.0).toUInt(),
                maxStepsPerInstance = config.getDoubleOr("maxStepsPerInstance", 1000.0).toUInt(),
                maxConcurrentInstances = config.getDoubleOr("maxConcurrentInstances", 10.0).toUInt(),
                maxTickDurationMs = config.getDoubleOr("maxTickDurationMs", 5000.0).toULong(),
                maxInstanceLifetimeSecs = config.getDoubleOr("maxInstanceLifetimeSecs", 86400.0).toULong(),
                maxStoredSequences = config.getDoubleOr("maxStoredSequences", 50.0).toUInt(),
                maxSequenceSizeBytes = config.getDoubleOr("maxSequenceSizeBytes", 1_048_576.0).toULong(),
                handlerTimeoutMs = config.getDoubleOr("handlerTimeoutMs", 30000.0).toULong(),
                operationTimeoutMs = config.getDoubleOr("operationTimeoutMs", 10000.0).toULong(),
                telemetryEnabled = if (config.hasKey("telemetryEnabled")) config.getBoolean("telemetryEnabled") else true,
                environment = config.getString("environment") ?: "production",
                rootPublicKey = config.getString("rootPublicKey") ?: "",
                sdkVersion = config.getString("sdkVersion") ?: "0.1.0",
            )

            engine = MobileEngine(dbPath, cfg)

            engine?.setListener(object : EngineListener {
                override fun onInstanceCompleted(instanceId: String, output: String) {
                    sendEvent("orch8:instanceCompleted", Arguments.createMap().apply {
                        putString("instanceId", instanceId)
                        putString("output", output)
                    })
                }

                override fun onInstanceFailed(instanceId: String, error: String) {
                    sendEvent("orch8:instanceFailed", Arguments.createMap().apply {
                        putString("instanceId", instanceId)
                        putString("error", error)
                    })
                }

                override fun onStepPending(instanceId: String, stepName: String, handler: String) {
                    sendEvent("orch8:stepPending", Arguments.createMap().apply {
                        putString("instanceId", instanceId)
                        putString("stepName", stepName)
                        putString("handler", handler)
                    })
                }
            })

            promise.resolve(null)
        } catch (e: Exception) {
            promise.reject("INIT_ERROR", e.message, e)
        }
    }

    @ReactMethod
    fun resume(promise: Promise) {
        engine?.resume()
        promise.resolve(null)
    }

    @ReactMethod
    fun pause(promise: Promise) {
        engine?.pause()
        promise.resolve(null)
    }

    @ReactMethod
    fun start(sequenceName: String, input: String, dedupKey: String?, promise: Promise) {
        try {
            val instanceId = engine?.start(sequenceName, input, dedupKey)
            promise.resolve(instanceId)
        } catch (e: Exception) {
            promise.reject("START_ERROR", e.message, e)
        }
    }

    @ReactMethod
    fun sync(manifestUrl: String, token: String?, promise: Promise) {
        try {
            val result = engine?.sync(manifestUrl, null)
            val map = Arguments.createMap().apply {
                putInt("added", result?.added?.toInt() ?: 0)
                putInt("updated", result?.updated?.toInt() ?: 0)
                putInt("removed", result?.removed?.toInt() ?: 0)
                putInt("skipped", result?.skipped?.toInt() ?: 0)
            }
            promise.resolve(map)
        } catch (e: Exception) {
            promise.reject("SYNC_ERROR", e.message, e)
        }
    }

    @ReactMethod
    fun shutdown(promise: Promise) {
        engine?.shutdown()
        engine = null
        promise.resolve(null)
    }

    private fun sendEvent(name: String, params: WritableMap) {
        reactApplicationContext
            .getJSModule(DeviceEventManagerModule.RCTDeviceEventEmitter::class.java)
            .emit(name, params)
    }

    private fun ReadableMap.getDoubleOr(key: String, default: Double): Double {
        return if (hasKey(key)) getDouble(key) else default
    }
}
