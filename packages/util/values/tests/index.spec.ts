import { describe, expect, it } from 'vitest'
import { isJsonValue, snapshotJsonValue } from '../src/index.ts'

/**
 * Render one constructor the way SpiderMonkey and JavaScriptCore do: the native
 * body on its own indented line. V8 renders the same function inline, which is
 * why an engine-specific comparison passes here and fails in Firefox.
 */
function renderForeignNativeSource(this: unknown): string {
  const name = (this as { name?: string }).name ?? ''
  return `function ${name}() {\n    [native code]\n}`
}

/** Run `check` while every native constructor renders in that foreign format. */
function withForeignNativeSource<T>(check: () => T): T {
  const original = Function.prototype.toString
  Function.prototype.toString = renderForeignNativeSource
  try {
    return check()
  } finally {
    Function.prototype.toString = original
  }
}

describe('lossless JSON detection', () => {
  it('accepts a plain record holding a plain array', () => {
    expect(isJsonValue({ a: [1, 'x', null, true] })).toBe(true)
    expect(snapshotJsonValue({ a: 1 })).toEqual({ a: 1 })
  })

  it('accepts them when the engine renders native source on its own line', () => {
    withForeignNativeSource(() => {
      expect(isJsonValue({ a: [1] })).toBe(true)
      expect(snapshotJsonValue([1, 2])).toEqual([1, 2])
      expect(snapshotJsonValue({ nested: { b: 'c' } })).toEqual({ nested: { b: 'c' } })
    })
  })

  it('refuses values whose prototype is not a realm intrinsic', () => {
    class Custom { readonly a = 1 }
    expect(isJsonValue(new Custom())).toBe(false)
    expect(isJsonValue(new Date())).toBe(false)
    expect(isJsonValue({ a: undefined })).toBe(false)
  })
})
