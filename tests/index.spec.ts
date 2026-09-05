import assert from 'node:assert/strict'
import { describe, it } from 'node:test'
import { Context } from '@deepseek-ai/cordis'
import { agentEvents, type Agent, type PreStepDecision } from '@deepseek-ai/dsh-agent'
import type { ToolDispatchExecution, ToolExecutionResult } from '@deepseek-ai/dsh-tools'
import { apply } from '../src/index.ts'

function mockAgent(id: string, subagentDepth = 0): Agent {
  return {
    id,
    options: { subagentDepth },
    session: { header: {} },
    status: 'running',
  } as unknown as Agent
}

function status(ctx: Context, agent: Agent, value: 'idle' | 'running'): void {
  ctx.emit(ctx as never, 'agent/status', { agent, status: value })
}

function step(
  ctx: Context,
  agent: Agent,
  entered: string[],
  signal = new AbortController().signal,
  next: () => Promise<PreStepDecision> = () => {
    entered.push(String(agent.id))
    return Promise.resolve({ kind: 'enter', messages: [] })
  },
): Promise<PreStepDecision> {
  status(ctx, agent, 'running')
  return agentEvents(ctx, agent).waterfall('agent/pre-step', { messages: [], turn: 1, step: 1, signal }, next)
}

function finishTurn(ctx: Context, agent: Agent): void {
  status(ctx, agent, 'idle')
}

interface ToolCallOptions {
  name?: string
  signal?: AbortSignal
  start?: boolean
  error?: Error
  local?: boolean
  childId?: string
}

function toolCall(
  ctx: Context,
  agent: Agent,
  runId: string,
  entered: string[],
  options: ToolCallOptions = {},
): Promise<ToolExecutionResult> {
  const exec = {
    name: options.name ?? 'subagent',
    agent,
    signal: options.signal ?? new AbortController().signal,
  } as ToolDispatchExecution
  return ctx.waterfall(ctx as never, 'tools/execute', exec, () => {
    entered.push(runId)
    if (options.start !== false) {
      ctx.emit(ctx as never, 'subagent/start', {
        runId,
        provider: options.local === false ? 'remote' : 'spawn',
        id: options.childId ?? `child-${runId}`,
        local: options.local !== false,
      })
    }
    if (options.error !== undefined) return Promise.reject(options.error)
    return Promise.resolve({ isError: false, value: {}, content: [] })
  })
}

function startLocalRun(ctx: Context, runId: string, agentId: string): void {
  ctx.emit(ctx as never, 'subagent/start', {
    runId,
    provider: 'spawn',
    id: agentId,
    local: true,
  })
}

function endRun(ctx: Context, runId: string, local = true): void {
  ctx.emit(ctx as never, 'subagent/end', {
    runId,
    provider: local ? 'spawn' : 'remote',
    id: `child-${runId}`,
    local,
    stopReason: 'completed',
  })
}

describe('subagent concurrency limit', () => {
  it('queues root direct calls before child creation in FIFO order', async () => {
    const ctx = new Context()
    apply(ctx, { maxConcurrentSubagents: 2 })
    const root = mockAgent('root')
    const entered: string[] = []

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
    const ctx = new Context()
    apply(ctx, { maxConcurrentSubagents: 1 })
    const root = mockAgent('root')
    const child = mockAgent('child', 1)
    const entered: string[] = []

    await toolCall(ctx, root, 'root-run', entered, { childId: child.id })
    await step(ctx, child, entered)
    await assert.rejects(toolCall(ctx, child, 'grandchild', entered), /exhausted for nested delegation/)

    finishTurn(ctx, child)
    endRun(ctx, 'root-run')
    await toolCall(ctx, root, 'after', entered)
    assert.deepStrictEqual(entered, ['root-run', 'child', 'after'])
  })

  it('shares one pool across children and grandchildren without double counting', async () => {
    const ctx = new Context()
    apply(ctx, { maxConcurrentSubagents: 2 })
    const root = mockAgent('root')
    const childOne = mockAgent('child-one', 1)
    const childTwo = mockAgent('child-two', 1)
    const grandchild = mockAgent('grandchild', 2)
    const entered: string[] = []

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
    const ctx = new Context()
    apply(ctx, { maxConcurrentSubagents: 1 })
    const root = mockAgent('root')
    const child = mockAgent('child', 1)
    const other = mockAgent('other', 1)
    const entered: string[] = []

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
    const ctx = new Context()
    apply(ctx, { maxConcurrentSubagents: 1 })
    const root = mockAgent('root')
    const entered: string[] = []

    await toolCall(ctx, root, 'remote', entered, { local: false })
    const waiting = toolCall(ctx, root, 'next', entered)
    await Promise.resolve()
    assert.deepStrictEqual(entered, ['remote'])

    endRun(ctx, 'remote', false)
    await waiting
    assert.deepStrictEqual(entered, ['remote', 'next'])
    endRun(ctx, 'next')
  })

  it('does not count the root agent turn', async () => {
    const ctx = new Context()
    apply(ctx, { maxConcurrentSubagents: 1 })
    const root = mockAgent('root')
    const child = mockAgent('child', 1)
    const entered: string[] = []

    await Promise.all([step(ctx, root, entered), step(ctx, child, entered)])
    assert.ok(entered.includes('root'))
    assert.ok(entered.includes('child'))
    finishTurn(ctx, child)
  })

  it('releases a permit when a direct call ends without starting a run', async () => {
    const ctx = new Context()
    apply(ctx, { maxConcurrentSubagents: 1 })
    const root = mockAgent('root')
    const entered: string[] = []
    const error = new Error('delegation failed before start')

    await assert.rejects(toolCall(ctx, root, 'failed', entered, { start: false, error }), (thrown) => thrown === error)
    await toolCall(ctx, root, 'next', entered)
    assert.deepStrictEqual(entered, ['failed', 'next'])
    endRun(ctx, 'next')
  })

  it('removes an aborted root-call waiter without consuming a permit', async () => {
    const ctx = new Context()
    apply(ctx, { maxConcurrentSubagents: 1 })
    const root = mockAgent('root')
    const entered: string[] = []
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
    const ctx = new Context()
    apply(ctx, { maxConcurrentSubagents: 1 })
    const root = mockAgent('root')
    const entered: string[] = []
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
    const ctx = new Context()
    apply(ctx, { maxConcurrentSubagents: 1 })
    const root = mockAgent('root')
    const child = mockAgent('child', 1)
    const entered: string[] = []

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
    const ctx = new Context()
    apply(ctx, { maxConcurrentSubagents: 1 })
    const child = mockAgent('child', 1)
    const other = mockAgent('other', 1)
    const entered: string[] = []
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
    const ctx = new Context()
    apply(ctx, { maxConcurrentSubagents: 1 })
    const root = mockAgent('root')
    const child = mockAgent('child', 1)
    const entered: string[] = []
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
    const ctx = new Context()
    apply(ctx, { maxConcurrentSubagents: 1, subagentToolNames: ['delegate'] })
    const root = mockAgent('root')
    const entered: string[] = []

    await toolCall(ctx, root, 'active', entered, { name: 'delegate' })
    await toolCall(ctx, root, 'other', entered, { name: 'other' })
    assert.deepStrictEqual(entered, ['active', 'other'])
    endRun(ctx, 'active')
    endRun(ctx, 'other')
  })

  for (const maxConcurrentSubagents of [0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1]) {
    it(`rejects invalid maxConcurrentSubagents ${maxConcurrentSubagents}`, () => {
      assert.throws(() => {
        apply(new Context(), { maxConcurrentSubagents })
      }, /maxConcurrentSubagents must be a positive safe integer/)
    })
  }

  for (const subagentToolNames of [[], [''], ['subagent', '']]) {
    it(`rejects invalid subagentToolNames ${JSON.stringify(subagentToolNames)}`, () => {
      assert.throws(() => {
        apply(new Context(), { subagentToolNames })
      }, /subagentToolNames must contain at least one non-empty string/)
    })
  }
})
