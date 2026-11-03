/** Keep staling and restoring one profile root, so a test can read it during every restore. */

import { renameSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { PROFILE_ROOT_FILENAME, writeProfileRoot } from '../../src/profile-boot.ts'

const [dir, stopAfterMs] = process.argv.slice(2)
if (dir === undefined || stopAfterMs === undefined) throw new Error('fixture requires dir and stopAfterMs')

const file = join(dir, PROFILE_ROOT_FILENAME)
const staged = `${file}.${process.pid}.stale`
const deadline = Date.now() + Number(stopAfterMs)
while (Date.now() < deadline) {
  // Staled by rename, so the only writer that can expose a partial file is the one under test.
  writeFileSync(staged, '- id: baked\n')
  renameSync(staged, file)
  writeProfileRoot(dir)
}
