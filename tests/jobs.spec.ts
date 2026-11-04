import assert from 'node:assert/strict'
import { afterEach, beforeEach, describe, it } from 'node:test'
import { Context } from '@deepseek-ai/cordis'
import type { JobEvent, JobEventListener, JobView } from '@deepseek-ai/dsh-jobs'
import { JobId } from '@deepseek-ai/dsh-jobs/brand'
import { apply } from '../src/index.ts'
import { endRun, finishTurn, mockAgent, step, toolCall } from './helpers.ts'

describe('background job reservations', () => {
  let ctx: Context
  const root = mockAgent('root')
  let listeners: Set<JobEventListener>
  let job: JobView
  let entered: string[]
  const emit = (event: JobEvent) => {
    for (const listener of listeners) listener(event)
  }

  beforeEach(async () => {
    ctx = new Context()
    listeners = new Set()
    entered = []
    ctx.provide('jobs', {
      events: {
        subscribe(_filter, listener) {
          listeners.add(listener)
          return () => {
            listeners.delete(listener)
          }
        },
      },
    } as Context['jobs'])
    await ctx.plugin(apply, { maxConcurrentSubagents: 1, queueTimeoutMs: 20 })
    job = {
      id: JobId('subagent-1'),
      kind: 'subagent',
      label: 'child',
      owner: root.id,
      status: 'running',
      startedAt: 0,
      output: { total: 0, earliest: 0 },
    }
  })
  afterEach(() => ctx.fiber.dispose())

  for (const status of ['failed', 'killed', 'completed'] as const) {
    it(`releases a reservation when the job is ${status} without publishing a child`, async () => {
      await toolCall(ctx, root, 'job', entered, {
        start: false,
        onExecute: () => emit({ type: 'registered', job }),
      })
      const queued = toolCall(ctx, root, 'next', entered)
      assert.deepStrictEqual(entered, ['job'])
      emit({ type: 'settled', job: { ...job, status }, cause: 'producer', awaited: false })
      await queued
      assert.deepStrictEqual(entered, ['job', 'next'])
      endRun(ctx, 'next')
    })
  }

  it('keeps a transferred permit until the local child turn ends', async () => {
    const child = mockAgent('child', 1)
    await toolCall(ctx, root, 'job', entered, {
      deferStart: true,
      childId: child.id,
      onExecute: () => emit({ type: 'registered', job }),
    })
    await new Promise((resolve) => setTimeout(resolve, 0))
    await step(ctx, child, entered)
    emit({ type: 'settled', job: { ...job, status: 'completed' }, cause: 'producer', awaited: false })
    const queued = toolCall(ctx, root, 'next', entered)
    assert.deepStrictEqual(entered, ['job', 'child'])
    finishTurn(ctx, child)
    endRun(ctx, 'job')
    await queued
    endRun(ctx, 'next')
  })

  it('unsubscribes from jobs and rejects waiters on plugin disposal', async () => {
    assert.equal(listeners.size, 1)
    await toolCall(ctx, root, 'held', entered, { start: false })
    const queued = assert.rejects(toolCall(ctx, root, 'next', entered), /disposed/)
    await ctx.fiber.dispose()
    await queued
    assert.equal(listeners.size, 0)
    assert.deepStrictEqual(entered, ['held'])
  })
})
