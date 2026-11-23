'use strict'

const { closeSync, cpSync, mkdirSync, openSync, readSync, writeFileSync } = require('node:fs')
const path = require('node:path')

const [binary, outputDir, releaseVersion] = process.argv.slice(2)
if (!binary || !outputDir || !['x64', 'arm64'].includes(process.arch)) {
  throw new Error('usage on x64 or arm64: node pack-node-native-musl.cjs ADDON OUT_DIR [VERSION]')
}
const root = path.resolve(__dirname, '..')
const source = path.join(root, 'packages/node-native')
const wrapper = require(path.join(source, 'package.json'))
const version = releaseVersion || wrapper.version
if (!/^\d+\.\d+\.\d+(?:-[a-zA-Z0-9.-]+)?$/.test(version)) {
  throw new Error('invalid release version')
}

const header = Buffer.alloc(64)
const file = openSync(binary, 'r')
let bytesRead
try {
  bytesRead = readSync(file, header, 0, header.length, 0)
} finally {
  closeSync(file)
}
const elf64LittleEndian = '7f454c46020101'
const sharedObject = 3
const machine = process.arch === 'x64' ? 62 : 183
if (bytesRead !== header.length || header.subarray(0, 7).toString('hex') !== elf64LittleEndian ||
    header.readUInt16LE(16) !== sharedObject || header.readUInt16LE(18) !== machine) {
  throw new Error(`expected a Linux ${process.arch} ELF shared object`)
}

const wrapperDir = path.join(outputDir, 'wrapper')
const addonDir = path.join(outputDir, 'addon')
mkdirSync(wrapperDir, { recursive: true })
mkdirSync(addonDir, { recursive: true })

const manifest = (dir, data) => writeFileSync(
  path.join(dir, 'package.json'), JSON.stringify(data, null, 2) + '\n',
)
for (const file of ['index.js', 'index.d.ts', 'binding.js', 'host-errors.js', 'README.md']) {
  cpSync(path.join(source, file), path.join(wrapperDir, file))
}
for (const dir of [wrapperDir, addonDir]) {
  cpSync(path.join(root, 'LICENSE'), path.join(dir, 'LICENSE'))
}

wrapper.version = version
for (const name of Object.keys(wrapper.optionalDependencies)) {
  wrapper.optionalDependencies[name] = version
}
manifest(wrapperDir, wrapper)

const name = `${wrapper.name}-linux-${process.arch}-musl`
const main = `orch8_engine_native.linux-${process.arch}-musl.node`
manifest(addonDir, {
  name, version, main,
  license: wrapper.license,
  repository: wrapper.repository,
  description: `Orch8 Node native addon for Alpine Linux ${process.arch}`,
  os: ['linux'], cpu: [process.arch], libc: ['musl'],
  files: [main],
})
cpSync(binary, path.join(addonDir, main))
