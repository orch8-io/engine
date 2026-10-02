'use strict'

// Loads the compiled addon. `napi build --platform` emits
// `orch8_engine_native.<platform>-<arch>.node`; a plain `cargo build` copied
// by `scripts/build-debug.sh` emits `orch8_engine_native.node`.
const { existsSync } = require('node:fs')
const { join } = require('node:path')

const candidates = [
  `orch8_engine_native.${process.platform}-${process.arch}.node`,
  'orch8_engine_native.node',
]
const local = candidates.map((file) => join(__dirname, file)).find((file) => existsSync(file))

module.exports = local
  ? require(local)
  : require(`@orch8/engine-native-${process.platform}-${process.arch}`)
