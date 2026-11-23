'use strict'

const native = require('./binding.js')
const { hostError, markPermanentError } = require('./host-errors.js')

/**
 * Throw (or reject with) a `PermanentError` from a handler to fail the step
 * without retrying. Any other thrown value is retryable.
 */
class PermanentError extends Error {
  constructor(message, options) {
    super(message, options)
    this.name = 'PermanentError'
    this.permanent = true
    markPermanentError(this)
  }
}

function toJson(value, what) {
  if (typeof value === 'string') return value
  if (value === undefined) return undefined
  try {
    return JSON.stringify(value)
  } catch (error) {
    throw new TypeError(`${what} must be JSON-serializable: ${hostError(error).message}`)
  }
}

function wrapHandler(name, fn, activeHandlers) {
  if (typeof fn !== 'function') throw new TypeError(`handler '${name}' must be a function`)
  // The native side always receives a resolved JSON envelope, so neither a
  // synchronous throw nor a rejection can escape into the addon.
  return async (requestJson) => {
    try {
      const req = JSON.parse(requestJson)
      const controller = new AbortController()
      const instanceId = req.instance_id
      const active = activeHandlers.get(instanceId) ?? new Set()
      activeHandlers.set(instanceId, active)
      active.add(controller)
      try {
        const output = await fn({
          params: req.params,
          data: req.data,
          outputs: req.outputs,
          instanceId,
          stepId: req.step_id,
          attempt: req.attempt,
          signal: controller.signal,
        })
        controller.signal.throwIfAborted()
        return JSON.stringify({ ok: output === undefined ? null : output })
      } finally {
        active.delete(controller)
        if (active.size === 0) activeHandlers.delete(instanceId)
      }
    } catch (error) {
      return JSON.stringify({ error: hostError(error) })
    }
  }
}

/** A durable workflow engine persisted in one SQLite file, in-process. */
class Engine {
  #activeHandlers = new Map()
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
    this._inner.register(name, wrapHandler(name, fn, this.#activeHandlers))
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

  /** Start a transactional batch and return one instance ID per input position. */
  async startBatch(items) {
    if (!Array.isArray(items)) throw new TypeError('startBatch expects an array')
    const request = items.map(({ name, input, version, idempotencyKey }) => ({
      name, input, version, idempotency_key: idempotencyKey,
    }))
    return JSON.parse(await this._inner.startBatch(toJson(request, 'batch')))
  }

  /** Drive the engine until the instance settles or the scheduling budget elapses. Storage reads can continue past `timeoutMs`. */
  async run(id, options = {}) {
    return JSON.parse(await this._inner.run(
      id, options.timeoutMs ?? undefined, options.observerTimeoutMs ?? undefined,
    ))
  }

  /** Current snapshot `{ id, state, data, outputs }`. */
  async get(id) {
    return JSON.parse(await this._inner.get(id))
  }

  /** Read one output by block and persisted attempt number. */
  async getOutput(id, block, attempt) {
    return JSON.parse(await this._inner.getOutput(id, block, attempt))
  }

  /** Iterate newest-first by instance ID. `sequenceId` is returned by deploy. */
  async *iterInstances({ state, sequenceId, pageSize = 100 } = {}) {
    if (!Number.isInteger(pageSize) || pageSize < 1 || pageSize > 1000) {
      throw new RangeError('pageSize must be an integer between 1 and 1000')
    }
    let before
    while (true) {
      const page = JSON.parse(await this._inner.listInstancePage(state, sequenceId, before, pageSize))
      before = page.at(-1)?.id
      yield* page
      if (page.length < pageSize) return
    }
  }

  /** Send `pause` | `resume` | `cancel` | `update_context` or a custom signal. */
  async signal(id, signal, payload) {
    await this._inner.signal(id, signal, toJson(payload, 'payload'))
    const instanceId = id.toLowerCase()
    if (signal === 'cancel' && this.#activeHandlers.has(instanceId)
      && await this._inner.interruptRunningInstance(id)) {
      for (const controller of this.#activeHandlers.get(instanceId) ?? []) controller.abort()
    }
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
