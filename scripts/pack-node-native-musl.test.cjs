'use strict'

const assert = require('node:assert/strict')
const { spawnSync } = require('node:child_process')
const { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } = require('node:fs')
const { tmpdir } = require('node:os')
const { join } = require('node:path')
const test = require('node:test')

function elf(machine, type = 3) {
  const header = Buffer.alloc(64)
  Buffer.from('7f454c46020101', 'hex').copy(header)
  header.writeUInt16LE(type, 16)
  header.writeUInt16LE(machine, 18)
  return header
}

const machine = process.arch === 'x64' ? 62 : 183

function pack(t, contents) {
  const dir = mkdtempSync(join(tmpdir(), 'orch8-musl-unit-'))
  t.after(() => rmSync(dir, { recursive: true, force: true }))
  const binary = join(dir, 'input.node')
  const output = join(dir, 'packages')
  writeFileSync(binary, contents)
  const { status, stderr } = spawnSync(process.execPath,
    [join(__dirname, 'pack-node-native-musl.cjs'), binary, output, '1.2.3'], { encoding: 'utf8' })
  return { output, status, stderr }
}

test('packages a matching ELF shared object with an exact platform and version', (t) => {
  const contents = elf(machine)
  const result = pack(t, contents)
  assert.equal(result.status, 0, result.stderr)
  const addon = JSON.parse(readFileSync(join(result.output, 'addon/package.json'), 'utf8'))
  const wrapper = JSON.parse(readFileSync(join(result.output, 'wrapper/package.json'), 'utf8'))
  assert.equal(addon.name, `@orch8/engine-native-linux-${process.arch}-musl`)
  assert.deepEqual({ os: addon.os, cpu: addon.cpu, libc: addon.libc },
    { os: ['linux'], cpu: [process.arch], libc: ['musl'] })
  assert.equal(wrapper.version, '1.2.3')
  assert.equal(addon.version, wrapper.version)
  assert.equal(wrapper.optionalDependencies[addon.name], wrapper.version)
  assert.deepEqual(readFileSync(join(result.output, 'addon', addon.main)), contents)
})

for (const [name, contents] of [
  ['a truncated header', Buffer.from([0x7f, 0x45])],
  ['a Mach-O library', Buffer.alloc(64, Buffer.from('cffaedfe', 'hex'))],
  ['a binary for the other supported architecture', elf(machine === 62 ? 183 : 62)],
  ['an ELF executable', elf(machine, 2)],
]) {
  test(`rejects ${name} before creating package files`, (t) => {
    const result = pack(t, contents)
    assert.notEqual(result.status, 0)
    assert.match(result.stderr, /expected a Linux .+ ELF shared object/)
    assert.equal(existsSync(result.output), false)
  })
}
