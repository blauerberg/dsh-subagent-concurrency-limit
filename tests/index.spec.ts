import assert from 'node:assert/strict'
import { afterEach, beforeEach, describe, it } from 'node:test'
import { Context } from '@deepseek-ai/cordis'
import type { Agent } from '@deepseek-ai/dsh-agent'
import type { ToolDispatchExecution } from '@deepseek-ai/dsh-tools'
import { apply } from '../src/index.ts'
import { mockAgent, step, finishTurn, toolCall, startLocalRun, endRun } from './helpers.ts'

describe('subagent concurrency limit', () => {
  let ctx: Context
  let root: Agent
  let entered: string[]
  beforeEach(() => {
    ctx = new Context()
    root = mockAgent('root')
    entered = []
  })
  afterEach(() => ctx.fiber.dispose())

  it('releases an unpublished reservation when the tool returns an error result', async () => {
    apply(ctx, { maxConcurrentSubagents: 1, queueTimeoutMs: 20 })
    const result = { isError: true as const, error: { message: 'startup failed' }, content: [] }
    const exec = { name: 'subagent', agent: root, signal: new AbortController().signal } as ToolDispatchExecution

    assert.equal(await ctx.waterfall(ctx as never, 'tools/execute', exec, async () => result), result)
    await toolCall(ctx, root, 'after', entered)
    assert.deepStrictEqual(entered, ['after'])
    endRun(ctx, 'after')
  })

  it('rejects queue timeouts above the Node.js timer limit', () => {
    assert.throws(() => apply(ctx, { queueTimeoutMs: 2_147_483_648 }), /queueTimeoutMs/)
  })

  it('releases a reserved permit when the child arrives with an aborted signal', async () => {
    apply(ctx, { maxConcurrentSubagents: 1, queueTimeoutMs: 20 })
    const child = mockAgent('child', 1)
    await toolCall(ctx, root, 'initial', entered, { childId: child.id })
    await assert.rejects(step(ctx, child, entered, AbortSignal.abort(new Error('cancelled'))), /cancelled/)
    await toolCall(ctx, root, 'next', entered)
    assert.deepStrictEqual(entered, ['initial', 'next'])
    endRun(ctx, 'initial')
    endRun(ctx, 'next')
  })

  it('queues root direct calls before child creation in FIFO order', async () => {
    apply(ctx, { maxConcurrentSubagents: 2 })

    await Promise.all([toolCall(ctx, root, 'first', entered), toolCall(ctx, root, 'second', entered)])
    const third = toolCall(ctx, root, 'third', entered)
    const fourth = toolCall(ctx, root, 'fourth', entered)
    await Promise.resolve()
    assert.deepStrictEqual(entered, ['first', 'second'])

    endRun(ctx, 'first')
    await third
    assert.deepStrictEqual(entered, ['first', 'second', 'third'])

    endRun(ctx, 'second')
    await fourth
    assert.deepStrictEqual(entered, ['first', 'second', 'third', 'fourth'])
  })

  it('rejects a saturated nested foreground delegation and makes progress after release', async () => {
    apply(ctx, { maxConcurrentSubagents: 1 })
    const child = mockAgent('child', 1)

    await toolCall(ctx, root, 'root-run', entered, { childId: child.id })
    await step(ctx, child, entered)
    await assert.rejects(toolCall(ctx, child, 'grandchild', entered), /exhausted for nested delegation/)

    finishTurn(ctx, child)
    endRun(ctx, 'root-run')
    await toolCall(ctx, root, 'after', entered)
    assert.deepStrictEqual(entered, ['root-run', 'child', 'after'])
  })

  it('shares one pool across children and grandchildren without double counting', async () => {
    apply(ctx, { maxConcurrentSubagents: 2 })
    const childOne = mockAgent('child-one', 1)
    const childTwo = mockAgent('child-two', 1)
    const grandchild = mockAgent('grandchild', 2)

    await toolCall(ctx, root, 'child-one-run', entered, { childId: childOne.id })
    await toolCall(ctx, root, 'child-two-run', entered, { childId: childTwo.id })
    await step(ctx, childOne, entered)
    await step(ctx, childTwo, entered)
    await assert.rejects(toolCall(ctx, childOne, 'blocked-grandchild', entered), /exhausted for nested delegation/)

    finishTurn(ctx, childTwo)
    endRun(ctx, 'child-two-run')
    await toolCall(ctx, childOne, 'grandchild-run', entered, { childId: grandchild.id })
    await step(ctx, grandchild, entered)

    const waitingRoot = toolCall(ctx, root, 'waiting-root', entered)
    await Promise.resolve()
    assert.ok(!entered.includes('waiting-root'))

    finishTurn(ctx, grandchild)
    endRun(ctx, 'grandchild-run')
    finishTurn(ctx, childOne)
    endRun(ctx, 'child-one-run')
    await waitingRoot
    endRun(ctx, 'waiting-root')
  })

  it('limits resumed local non-root turns through the same pool', async () => {
    apply(ctx, { maxConcurrentSubagents: 1 })
    const child = mockAgent('child', 1)
    const other = mockAgent('other', 1)

    await toolCall(ctx, root, 'initial', entered, { childId: child.id })
    await step(ctx, child, entered)
    finishTurn(ctx, child)
    endRun(ctx, 'initial')

    startLocalRun(ctx, 'resumed', child.id)
    const resumed = step(ctx, child, entered)
    await Promise.resolve()
    const waiting = step(ctx, other, entered)
    await Promise.resolve()
    assert.deepStrictEqual(entered, ['initial', 'child', 'child'])

    finishTurn(ctx, child)
    await resumed
    await waiting
    assert.deepStrictEqual(entered, ['initial', 'child', 'child', 'other'])
    finishTurn(ctx, other)
    endRun(ctx, 'resumed')
  })

  it('limits remote runs at their observable lifecycle boundary', async () => {
    apply(ctx, { maxConcurrentSubagents: 1 })

    await toolCall(ctx, root, 'remote', entered, { local: false })
    const waiting = toolCall(ctx, root, 'next', entered)
    await Promise.resolve()
    assert.deepStrictEqual(entered, ['remote'])

    endRun(ctx, 'remote')
    await waiting
    assert.deepStrictEqual(entered, ['remote', 'next'])
    endRun(ctx, 'next')
  })

  it('does not count the root agent turn', async () => {
    apply(ctx, { maxConcurrentSubagents: 1 })
    const child = mockAgent('child', 1)

    await Promise.all([step(ctx, root, entered), step(ctx, child, entered)])
    assert.ok(entered.includes('root'))
    assert.ok(entered.includes('child'))
    finishTurn(ctx, child)
  })

  it('releases a permit when a direct call ends without starting a run', async () => {
    apply(ctx, { maxConcurrentSubagents: 1 })
    const error = new Error('delegation failed before start')

    await assert.rejects(toolCall(ctx, root, 'failed', entered, { start: false, error }), (thrown) => thrown === error)
    await toolCall(ctx, root, 'next', entered)
    assert.deepStrictEqual(entered, ['failed', 'next'])
    endRun(ctx, 'next')
  })

  it('removes an aborted root-call waiter without consuming a permit', async () => {
    apply(ctx, { maxConcurrentSubagents: 1 })
    const abort = new AbortController()

    await toolCall(ctx, root, 'active', entered)
    const rejected = toolCall(ctx, root, 'cancelled', entered, { signal: abort.signal })
    await Promise.resolve()
    const reason = new Error('cancelled while waiting')
    abort.abort(reason)
    await assert.rejects(rejected, (thrown) => thrown === reason)

    const next = toolCall(ctx, root, 'next', entered)
    endRun(ctx, 'active')
    await next
    assert.deepStrictEqual(entered, ['active', 'next'])
    endRun(ctx, 'next')
  })

  it('releases a permit when a waiter is aborted immediately after admission', async () => {
    apply(ctx, { maxConcurrentSubagents: 1 })
    const abort = new AbortController()

    await toolCall(ctx, root, 'active', entered)
    const admittedThenCancelled = toolCall(ctx, root, 'admitted-then-cancelled', entered, {
      signal: abort.signal,
    })
    endRun(ctx, 'active')
    abort.abort(new Error('cancelled after admission'))
    await assert.rejects(admittedThenCancelled, /cancelled after admission/)

    await toolCall(ctx, root, 'after', entered)
    endRun(ctx, 'after')
    assert.deepStrictEqual(entered, ['active', 'after'])
  })

  it('releases a local child permit on disposal', async () => {
    apply(ctx, { maxConcurrentSubagents: 1 })
    const child = mockAgent('child', 1)

    await toolCall(ctx, root, 'child', entered, { childId: child.id })
    await step(ctx, child, entered)
    const waiting = toolCall(ctx, root, 'next', entered)
    await Promise.resolve()
    ctx.emit(ctx as never, 'agent/disposed', { agent: child })
    await waiting
    endRun(ctx, 'next')
    assert.deepStrictEqual(entered, ['child', 'child', 'next'])
  })

  it('releases a permit when a local pre-step fails', async () => {
    apply(ctx, { maxConcurrentSubagents: 1 })
    const child = mockAgent('child', 1)
    const other = mockAgent('other', 1)
    const error = new Error('pre-step failed')

    await assert.rejects(
      step(ctx, child, entered, new AbortController().signal, () => Promise.reject(error)),
      (thrown) => thrown === error,
    )
    await step(ctx, other, entered)
    finishTurn(ctx, other)
    assert.deepStrictEqual(entered, ['other'])
  })

  it('re-admits a root-reserved local child after its first pre-step fails', async () => {
    apply(ctx, { maxConcurrentSubagents: 1 })
    const child = mockAgent('child', 1)
    const error = new Error('first child step failed')

    await toolCall(ctx, root, 'initial', entered, { childId: child.id })
    await assert.rejects(
      step(ctx, child, entered, new AbortController().signal, () => Promise.reject(error)),
      (thrown) => thrown === error,
    )
    await step(ctx, child, entered)

    const waiting = toolCall(ctx, root, 'waiting', entered)
    await Promise.resolve()
    assert.deepStrictEqual(entered, ['initial', 'child'])
    finishTurn(ctx, child)
    endRun(ctx, 'initial')
    await waiting
    endRun(ctx, 'waiting')
  })

  it('only gates configured delegation tool names', async () => {
    apply(ctx, { maxConcurrentSubagents: 1, subagentToolNames: ['delegate'] })

    await toolCall(ctx, root, 'active', entered, { name: 'delegate' })
    await toolCall(ctx, root, 'other', entered, { name: 'other' })
    assert.deepStrictEqual(entered, ['active', 'other'])
    endRun(ctx, 'active')
    endRun(ctx, 'other')
  })

  it('rejects a root-call queue wait after queueTimeoutMs', async () => {
    apply(ctx, { maxConcurrentSubagents: 1, queueTimeoutMs: 50 })

    await toolCall(ctx, root, 'active', entered)
    await assert.rejects(toolCall(ctx, root, 'queued', entered), /queue timeout after 50ms/)

    endRun(ctx, 'active')
    await toolCall(ctx, root, 'next', entered)
    assert.deepStrictEqual(entered, ['active', 'next'])
    endRun(ctx, 'next')
  })

  it('rejects a resumed child turn queue wait after queueTimeoutMs', async () => {
    apply(ctx, { maxConcurrentSubagents: 1, queueTimeoutMs: 50 })
    const child = mockAgent('child', 1)

    await toolCall(ctx, root, 'initial', entered, { childId: child.id })
    await step(ctx, child, entered)
    finishTurn(ctx, child)
    endRun(ctx, 'initial')
    await toolCall(ctx, root, 'holder', entered)
    await assert.rejects(step(ctx, child, entered), /queue timeout after 50ms/)

    endRun(ctx, 'holder')
    await step(ctx, child, entered)
    finishTurn(ctx, child)
    assert.deepStrictEqual(entered, ['initial', 'child', 'holder', 'child'])
  })

  it('keeps the reservation for a deferred job publish and admits the child first turn', async () => {
    apply(ctx, { maxConcurrentSubagents: 1 })
    const child = mockAgent('child', 1)

    await toolCall(ctx, root, 'job-run', entered, { deferStart: true, childId: child.id })
    const queued = toolCall(ctx, root, 'queued', entered)
    await new Promise((resolve) => setTimeout(resolve, 0))
    assert.deepStrictEqual(entered, ['job-run'])

    await step(ctx, child, entered)
    assert.deepStrictEqual(entered, ['job-run', 'child'])

    finishTurn(ctx, child)
    endRun(ctx, 'job-run')
    await queued
    assert.deepStrictEqual(entered, ['job-run', 'child', 'queued'])
    endRun(ctx, 'queued')
  })

  it('fails loudly for a reservation-less local run when the pool is full', async () => {
    apply(ctx, { maxConcurrentSubagents: 1 })
    const child = mockAgent('child', 1)

    await toolCall(ctx, root, 'active', entered)
    startLocalRun(ctx, 'orphan', child.id)
    await assert.rejects(step(ctx, child, entered), /exhausted for reservation-less local child turn/)

    endRun(ctx, 'active')
    await step(ctx, child, entered)
    finishTurn(ctx, child)
    endRun(ctx, 'orphan')
    assert.deepStrictEqual(entered, ['active', 'child'])
  })

  it('releases an unbound reservation when the delegating parent is disposed', async () => {
    apply(ctx, { maxConcurrentSubagents: 1 })

    await toolCall(ctx, root, 'held', entered, { start: false })
    const queued = toolCall(ctx, root, 'queued', entered)
    await Promise.resolve()
    assert.deepStrictEqual(entered, ['held'])

    ctx.emit(ctx as never, 'agent/disposed', { agent: root })
    await queued
    assert.deepStrictEqual(entered, ['held', 'queued'])
    endRun(ctx, 'queued')
  })

  for (const maxConcurrentSubagents of [0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1]) {
    it(`rejects invalid maxConcurrentSubagents ${maxConcurrentSubagents}`, () => {
      assert.throws(() => {
        apply(ctx, { maxConcurrentSubagents })
      }, /maxConcurrentSubagents must be a positive safe integer/)
    })
  }

  for (const queueTimeoutMs of [0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1]) {
    it(`rejects invalid queueTimeoutMs ${queueTimeoutMs}`, () => {
      assert.throws(() => {
        apply(ctx, { queueTimeoutMs })
      }, /queueTimeoutMs must be an integer from 1 to 2147483647/)
    })
  }

  for (const subagentToolNames of [[], [''], ['subagent', '']]) {
    it(`rejects invalid subagentToolNames ${JSON.stringify(subagentToolNames)}`, () => {
      assert.throws(() => {
        apply(ctx, { subagentToolNames })
      }, /subagentToolNames must contain at least one non-empty string/)
    })
  }
})
