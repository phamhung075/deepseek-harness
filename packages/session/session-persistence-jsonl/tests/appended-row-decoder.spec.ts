import { describe, expect, it } from 'vitest'
import { SESSION_FORMAT_VERSION } from '@deepseek-ai/dsh-session'
import { AppendedRowDecoder, eventLine, toHeaderLine } from '../src/format.ts'
import { meta, oneTurnLog } from '../../session-persistence/tests/contract.ts'

const header = (id: string): Buffer => Buffer.from(`${JSON.stringify(toHeaderLine(meta(id)))}\n`)
const rows = (): Buffer[] => oneTurnLog().map(event => Buffer.from(`${eventLine(event)}\n`))

describe('AppendedRowDecoder', () => {
  it('decodes rows that continue a preserved prefix', () => {
    const decoder = new AppendedRowDecoder(header('append-decoder'))
    const decoded = rows().flatMap(row => decoder.decode(row))
    const expected = oneTurnLog()
    expect(decoded.map(event => event.type)).toEqual(expected.map(event => event.type))
    expect(decoded.map(event => event.seq)).toEqual(expected.map(event => event.seq))
  })

  it('reports no events for a header row when a reader restarts at the file head', () => {
    const decoder = new AppendedRowDecoder(header('append-decoder'))
    const headerRow = Buffer.from(JSON.stringify(toHeaderLine(meta('append-decoder'))))
    expect(decoder.decode(headerRow)).toEqual([])
  })

  it('refuses a header from another format generation', () => {
    const foreign = Buffer.from(`${JSON.stringify({
      ...toHeaderLine(meta('append-decoder')),
      version: SESSION_FORMAT_VERSION - 1,
    })}\n`)
    expect(() => new AppendedRowDecoder(foreign)).toThrow()
  })

  it('refuses an unparsable row instead of reporting it as empty', () => {
    const decoder = new AppendedRowDecoder(header('append-decoder'))
    expect(() => decoder.decode(Buffer.from('{not json'))).toThrow(/corrupt session log/)
  })
})
