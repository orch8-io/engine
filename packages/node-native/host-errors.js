'use strict'

const { isProxy } = require('node:util').types
const { Buffer } = require('node:buffer')
const { getOwnPropertyDescriptor } = Object
const fallback = 'handler failed'
const permanentErrors = new WeakSet()

function markPermanentError(error) {
  permanentErrors.add(error)
}

function boundedMessage(message) {
  let end = Math.min(message.length, 1024)
  if (end < message.length && message.charCodeAt(end - 1) >= 0xD800 &&
      message.charCodeAt(end - 1) <= 0xDBFF && message.charCodeAt(end) >= 0xDC00 &&
      message.charCodeAt(end) <= 0xDFFF) end--
  // Rust JSON strings cannot contain lone UTF-16 surrogates. Bound the slice
  // before converting so sanitization itself cannot allocate an unbounded copy.
  return Buffer.from(message.slice(0, end), 'utf8').toString('utf8')
}

// Error reporting must not execute another piece of host code. Even reflection
// can invoke a Proxy trap, so reject proxies before inspecting data properties.
function hostError(error) {
  let message = fallback
  let permanent = false
  if (typeof error === 'string') {
    message = error
  } else if (error !== null && (typeof error === 'object' || typeof error === 'function')) {
    if (isProxy(error)) return { message, permanent }
    try {
      const text = getOwnPropertyDescriptor(error, 'message')
      const marker = getOwnPropertyDescriptor(error, 'permanent')
      if (text && typeof text.value === 'string') message = text.value
      permanent = permanentErrors.has(error) || marker?.value === true
    } catch (_) {
      return { message: fallback, permanent: false }
    }
  }
  return { message: boundedMessage(message), permanent }
}

module.exports = { hostError, markPermanentError }
