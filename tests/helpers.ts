import assert from 'node:assert/strict'
import type { Context } from '@deepseek-ai/cordis'
import { agentEvents, type Agent, type PreStepDecision } from '@deepseek-ai/dsh-agent'
import type { ToolDispatchExecution, ToolExecutionResult } from '@deepseek-ai/dsh-tools'
import { SubagentRunId, type SubagentRunInfo } from '@deepseek-ai/dsh-subagent'

export function mockAgent(id: string, subagentDepth = 0): Agent {
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

export function step(
  ctx: Context,
  agent: Agent,
  entered: string[],
  signal: AbortSignal = new AbortController().signal,
  next: () => Promise<PreStepDecision> = () => {
    entered.push(String(agent.id))
    return Promise.resolve({ kind: 'enter', messages: [] })
  },
): Promise<PreStepDecision> {
  status(ctx, agent, 'running')
  return agentEvents(ctx, agent).waterfall('agent/pre-step', { messages: [], turn: 1, step: 1, signal }, next)
}

export function finishTurn(ctx: Context, agent: Agent): void {
  status(ctx, agent, 'idle')
}

interface ToolCallOptions {
  name?: string
  signal?: AbortSignal
  start?: boolean
  deferStart?: boolean
  error?: Error
  local?: boolean
  childId?: string
  onExecute?: () => void
}

export function toolCall(
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
  return ctx.waterfall(ctx as never, 'tools/execute', exec, (): Promise<ToolExecutionResult> => {
    entered.push(runId)
    options.onExecute?.()
    if (options.start !== false) {
      // `deferStart` schedules the publish after the tool returns, like a
      // background job, while keeping the tool call's async context alive.
      const emitStart = () => {
        startRun(ctx, {
          runId: SubagentRunId(runId),
          provider: options.local === false ? 'remote' : 'spawn',
          id: mockAgent(options.childId ?? `child-${runId}`).id,
          local: options.local !== false,
        })
      }
      if (options.deferStart) setTimeout(emitStart, 0)
      else emitStart()
    }
    if (options.error !== undefined) return Promise.reject(options.error)
    return Promise.resolve({ isError: false, value: {}, content: [] })
  })
}

const published = new WeakMap<Context, Map<string, SubagentRunInfo>>()

function startRun(ctx: Context, info: SubagentRunInfo): void {
  let runs = published.get(ctx)
  if (runs === undefined) {
    runs = new Map()
    published.set(ctx, runs)
  }
  runs.set(String(info.runId), info)
  ctx.emit(ctx as never, 'subagent/start', info)
}

export function startLocalRun(ctx: Context, runId: string, agentId: Agent['id']): void {
  startRun(ctx, { runId: SubagentRunId(runId), provider: 'spawn', id: agentId, local: true })
}

export function endRun(ctx: Context, runId: string): void {
  const run = published.get(ctx)?.get(runId)
  assert.ok(run, `run ${runId} was published`)
  published.get(ctx)?.delete(runId)
  ctx.emit(ctx as never, 'subagent/end', { ...run, stopReason: 'completed' })
}
