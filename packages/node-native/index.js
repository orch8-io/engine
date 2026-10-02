'use strict'

const native = require('./binding.js')

/**
 * Throw (or reject with) a `PermanentError` from a handler to fail the step
 * without retrying. Any other thrown value is retryable.
 */
class PermanentError extends Error {
  constructor(message, options) {
    super(message, options)
    this.name = 'PermanentError'
    this.permanent = true
  }
}

function toJson(value, what) {
  if (typeof value === 'string') return value
  if (value === undefined) return undefined
  try {
    return JSON.stringify(value)
  } catch (error) {
    throw new TypeError(`${what} must be JSON-serializable: ${error.message}`)
  }
}

function wrapHandler(name, fn) {
  if (typeof fn !== 'function') throw new TypeError(`handler '${name}' must be a function`)
  // The native side always receives a resolved JSON envelope, so neither a
  // synchronous throw nor a rejection can escape into the addon.
  return async (requestJson) => {
    try {
      const req = JSON.parse(requestJson)
      const output = await fn({
        params: req.params,
        data: req.data,
        outputs: req.outputs,
        instanceId: req.instance_id,
        stepId: req.step_id,
        attempt: req.attempt,
      })
      return JSON.stringify({ ok: output === undefined ? null : output })
    } catch (error) {
      const permanent = Boolean(error && (error instanceof PermanentError || error.permanent === true))
      const message = error instanceof Error ? error.message : String(error)
      return JSON.stringify({ error: { message, permanent } })
    }
  }
}

/** A durable workflow engine persisted in one SQLite file, in-process. */
class Engine {
  /** Open (or create) the database at `path`. */
  static async open(path) {
    if (typeof path !== 'string' || path.length === 0) {
      throw new TypeError('Engine.open(path) needs a SQLite file path')
    }
    return new Engine(new native.NativeDurableEngine(path))
  }

  constructor(inner) {
    this._inner = inner
  }

  /** Register a step handler. Register all handlers before the first other call. */
  handler(name, fn) {
    this._inner.register(name, wrapHandler(name, fn))
    return this
  }

  /** Store a sequence definition (object or JSON). Idempotent per name + version. */
  deploy(sequence) {
    return this._inner.deploy(toJson(sequence, 'sequence'))
  }

  /** Start an instance of a deployed sequence; returns the instance id. */
  start(name, input = {}, options = {}) {
    return this._inner.start(
      name,
      toJson(input, 'input'),
      options.idempotencyKey ?? undefined,
      options.version ?? undefined,
    )
  }

  /** Drive the engine until the instance settles (or `timeoutMs` elapses). */
  async run(id, options = {}) {
    return JSON.parse(await this._inner.run(id, options.timeoutMs ?? undefined))
  }

  /** Current snapshot `{ id, state, data, outputs }`. */
  async get(id) {
    return JSON.parse(await this._inner.get(id))
  }

  /** Send `pause` | `resume` | `cancel` | `update_context` or a custom signal. */
  signal(id, signal, payload) {
    return this._inner.signal(id, signal, toJson(payload, 'payload'))
  }

  /** Stop the engine and release the database. */
  close() {
    return this._inner.close()
  }
}

module.exports = {
  Engine,
  PermanentError,
  validateSequenceJson: native.validateSequenceJson,
  sequenceSchemaVersion: native.sequenceSchemaVersion,
  runSequenceJson: native.runSequenceJson,
}
