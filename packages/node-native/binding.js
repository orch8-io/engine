'use strict'

// Loads the compiled addon. `napi build --platform` emits
// `orch8_engine_native.<platform>-<arch>[-<libc>].node`. A plain native
// Cargo build can be installed as `orch8_engine_native.node`.
const { existsSync } = require('node:fs')
const { join } = require('node:path')

const platform = `${process.platform}-${process.arch}`
const libc = process.platform === 'linux'
  ? (process.report.getReport().header.glibcVersionRuntime ? 'gnu' : 'musl')
  : null
const candidates = ['orch8_engine_native.node']
if (libc !== 'musl') candidates.unshift(`orch8_engine_native.${platform}.node`)
if (libc) candidates.unshift(`orch8_engine_native.${platform}-${libc}.node`)
const local = candidates.map((file) => join(__dirname, file)).find((file) => existsSync(file))

module.exports = local
  ? require(local)
  : require(`@orch8/engine-native-${platform}${libc === 'musl' ? '-musl' : ''}`)
