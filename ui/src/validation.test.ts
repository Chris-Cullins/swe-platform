import { describe, expect, it } from 'vitest'
import { validateCreateRun } from './App'
import type { CreateRun } from './contracts'

const valid = (overrides: Partial<CreateRun> = {}): CreateRun => ({ name: 'my-run', selector: { template: 'small' }, agent: 'agent', prompt: 'task', ...overrides })

describe('create run validation', () => {
  it('accepts project plus template and an environment by itself', () => {
    expect(validateCreateRun(valid({ selector: { project: 'repo', template: 'small' } }))).toBeUndefined()
    expect(validateCreateRun(valid({ selector: { environment: 'warm-1' } }))).toBeUndefined()
  })
  it('rejects missing and incompatible selectors', () => {
    expect(validateCreateRun(valid({ selector: {} }))).toMatch(/Choose/)
    expect(validateCreateRun(valid({ selector: { environment: 'env', project: 'repo' } }))).toMatch(/cannot be combined/)
  })
  it('enforces DNS names and selector references including maximums', () => {
    expect(validateCreateRun(valid({ name: 'Bad_Name' }))).toMatch(/DNS/)
    expect(validateCreateRun(valid({ name: `${'a'.repeat(63)}.${'b'.repeat(63)}.${'c'.repeat(63)}.${'d'.repeat(61)}` }))).toBeUndefined()
    expect(validateCreateRun(valid({ name: 'a'.repeat(254) }))).toMatch(/DNS/)
    expect(validateCreateRun(valid({ name: 'good.-bad' }))).toMatch(/DNS/)
    expect(validateCreateRun(valid({ name: 'bad-.good' }))).toMatch(/DNS/)
    expect(validateCreateRun(valid({ selector: { project: 'Bad' } }))).toMatch(/Selector/)
  })
  it('enforces agent and UTF-8 prompt limits', () => {
    expect(validateCreateRun(valid({ agent: '' }))).toMatch(/required/)
    expect(validateCreateRun(valid({ agent: 'a'.repeat(129) }))).toMatch(/128/)
    expect(validateCreateRun(valid({ prompt: '' }))).toMatch(/required/)
    expect(validateCreateRun(valid({ prompt: '🙂'.repeat(262145) }))).toMatch(/1 MiB/)
  })
})
