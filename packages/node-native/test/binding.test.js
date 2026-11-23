'use strict'

const assert = require('node:assert/strict')
const { readFileSync } = require('node:fs')
const path = require('node:path')
const test = require('node:test')
const { runInNewContext } = require('node:vm')

const source = readFileSync(path.join(__dirname, '../binding.js'), 'utf8')

function selects({ platform, arch, glibc, local = [] }) {
  const module = { exports: {} }
  const directory = '/native-package'
  runInNewContext(source, {
    __dirname: directory,
    module,
    process: {
      platform, arch,
      report: { getReport: () => ({ header: { glibcVersionRuntime: glibc } }) },
    },
    require(name) {
      if (name === 'node:fs') return { existsSync: (file) => local.includes(path.basename(file)) }
      if (name === 'node:path') return path.posix
      return name
    },
  })
  return module.exports
}

test('picks the Alpine binary for each architecture without probing a GNU addon', () => {
  for (const arch of ['x64', 'arm64']) {
    assert.equal(selects({
      platform: 'linux', arch, local: [`orch8_engine_native.linux-${arch}.node`],
    }), `@orch8/engine-native-linux-${arch}-musl`)
    const file = `orch8_engine_native.linux-${arch}-musl.node`
    assert.equal(selects({ platform: 'linux', arch, local: [file] }), `/native-package/${file}`)
  }
})

test('preserves existing Linux glibc and non-Linux distribution names', () => {
  assert.equal(selects({ platform: 'linux', arch: 'x64', glibc: '2.39' }),
    '@orch8/engine-native-linux-x64')
  assert.equal(selects({ platform: 'darwin', arch: 'arm64' }),
    '@orch8/engine-native-darwin-arm64')
})
