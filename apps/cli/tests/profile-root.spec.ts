/** The profile root must stay readable while other processes of the profile start. */

import { mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, statSync, utimesSync, writeFileSync } from 'node:fs'
import { readFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { execa } from 'execa'
import yaml from 'js-yaml'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { PROFILE_ROOT_FILENAME, writeProfileRoot } from '../src/profile-boot.ts'

/** Runs in place of the next `renameSync` when set. */
const renameHook = vi.hoisted(() => ({ current: undefined as (() => void) | undefined }))

vi.mock('node:fs', async (importOriginal) => {
  const fs = await importOriginal<typeof import('node:fs')>()
  return {
    ...fs,
    renameSync(from: string, to: string): void {
      if (renameHook.current === undefined) fs.renameSync(from, to)
      else renameHook.current()
    },
  }
})

const childEntry = fileURLToPath(new URL('./fixtures/restore-profile-root.ts', import.meta.url))
const tsxLoader = import.meta.resolve('tsx/esm')

const dirs: string[] = []
afterEach(() => {
  renameHook.current = undefined
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true })
})

/** A fresh profile directory, removed after the test. */
function profileDir(): string {
  const dir = mkdtempSync(join(tmpdir(), 'dsh-profile-root-'))
  dirs.push(dir)
  return dir
}

describe('writeProfileRoot', () => {
  it('creates a missing root as an empty entry list and leaves no temp file', () => {
    const dir = profileDir()
    writeProfileRoot(dir)
    expect(yaml.load(readFileSync(join(dir, PROFILE_ROOT_FILENAME), 'utf8'))).toEqual([])
    expect(readdirSync(dir)).toEqual([PROFILE_ROOT_FILENAME])
  })

  it('does not touch a root that is already correct', () => {
    const dir = profileDir()
    writeProfileRoot(dir)
    const file = join(dir, PROFILE_ROOT_FILENAME)
    // Backdated, so a rewrite shows even within one timestamp tick.
    utimesSync(file, 1000, 1000)
    const before = statSync(file).ino
    writeProfileRoot(dir)
    expect(statSync(file).ino).toBe(before)
    expect(statSync(file).mtimeMs).toBe(1_000_000)
  })

  it('replaces a baked root by rename, not by writing through the old file', () => {
    const dir = profileDir()
    const file = join(dir, PROFILE_ROOT_FILENAME)
    writeFileSync(file, '- id: baked\n')
    const before = statSync(file).ino
    writeProfileRoot(dir)
    expect(statSync(file).ino).not.toBe(before)
    expect(yaml.load(readFileSync(file, 'utf8'))).toEqual([])
    expect(readdirSync(dir)).toEqual([PROFILE_ROOT_FILENAME])
  })

  it('accepts a refused rename when another writer already restored the root', () => {
    const dir = profileDir()
    const file = join(dir, PROFILE_ROOT_FILENAME)
    writeFileSync(file, '- id: baked\n')
    const good = profileDir()
    writeProfileRoot(good)
    renameHook.current = () => {
      writeFileSync(file, readFileSync(join(good, PROFILE_ROOT_FILENAME)))
      throw Object.assign(new Error('rename refused'), { code: 'EPERM' })
    }
    writeProfileRoot(dir)
    expect(yaml.load(readFileSync(file, 'utf8'))).toEqual([])
    expect(readdirSync(dir)).toEqual([PROFILE_ROOT_FILENAME])
  })

  it('reports a refused rename when the root is still wrong, and removes its temp file', () => {
    const dir = profileDir()
    const file = join(dir, PROFILE_ROOT_FILENAME)
    writeFileSync(file, '- id: baked\n')
    renameHook.current = () => {
      throw Object.assign(new Error('rename refused'), { code: 'EIO' })
    }
    expect(() => writeProfileRoot(dir)).toThrow('rename refused')
    expect(readdirSync(dir)).toEqual([PROFILE_ROOT_FILENAME])
  })

  it('reports a root it cannot read', () => {
    const dir = profileDir()
    mkdirSync(join(dir, PROFILE_ROOT_FILENAME))
    expect(() => writeProfileRoot(dir)).toThrow(/EISDIR/)
  })

  it('never exposes a truncated root to a concurrent reader', async () => {
    const dir = profileDir()
    writeProfileRoot(dir)
    const child = () => execa(process.execPath, ['--import', tsxLoader, childEntry, dir, '3000'])
    const children = [child(), child()]
    let running = true
    const finished = Promise.all(children).finally(() => { running = false })
    let reads = 0
    while (running) {
      // The same shape check Include.read applies at boot.
      expect(Array.isArray(yaml.load(await readFile(join(dir, PROFILE_ROOT_FILENAME), 'utf8')))).toBe(true)
      reads++
    }
    await finished
    expect(reads).toBeGreaterThan(0)
  }, 60_000)
})
