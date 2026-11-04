/** @module dsh-subagent-concurrency-limit */

import { AsyncLocalStorage } from 'node:async_hooks'
import type { Context } from '@deepseek-ai/cordis'
import { delegationDepthOf } from '@deepseek-ai/dsh-subagent'
import type {} from '@deepseek-ai/dsh-agent'
import type {} from '@deepseek-ai/dsh-tools'
import type {} from '@deepseek-ai/dsh-jobs'
import { ConcurrencyGate } from './concurrency-gate.ts'

export const name = 'subagent-concurrency-limit'
export const inject = ['tools']

const DEFAULT_MAX_CONCURRENT_SUBAGENTS = 2
const DEFAULT_QUEUE_TIMEOUT_MS = 300_000

export interface Config {
  /** Maximum local non-root turns and observable remote runs at once. */
  maxConcurrentSubagents?: number
  /** How long a pool wait may stay queued before failing loudly, in milliseconds. */
  queueTimeoutMs?: number
  /** Delegation tool names whose calls are limited. */
  subagentToolNames?: string[]
}

interface Reservation {
  state: 'open' | 'bound' | 'done'
  readonly owner: string
  jobId?: string
}

interface RunState {
  readonly agentId: string
  readonly local: boolean
  readonly reservation?: Reservation
}

function resolveConfig(config: Config): {
  limit: number
  queueTimeoutMs: number
  subagentToolNames: ReadonlySet<string>
} {
  const limit = config.maxConcurrentSubagents ?? DEFAULT_MAX_CONCURRENT_SUBAGENTS
  if (!Number.isSafeInteger(limit) || limit < 1) {
    throw new TypeError('maxConcurrentSubagents must be a positive safe integer')
  }
  const queueTimeoutMs = config.queueTimeoutMs ?? DEFAULT_QUEUE_TIMEOUT_MS
  if (!Number.isSafeInteger(queueTimeoutMs) || queueTimeoutMs < 1 || queueTimeoutMs > 2_147_483_647) {
    throw new TypeError('queueTimeoutMs must be an integer from 1 to 2147483647')
  }
  const names = config.subagentToolNames ?? ['subagent']
  if (
    !Array.isArray(names) ||
    names.length === 0 ||
    names.some((value) => typeof value !== 'string' || value.length === 0)
  ) {
    throw new TypeError('subagentToolNames must contain at least one non-empty string')
  }
  return { limit, queueTimeoutMs, subagentToolNames: new Set(names) }
}

export function apply(ctx: Context, config: Config = {}): void {
  const { limit, queueTimeoutMs, subagentToolNames } = resolveConfig(config)
  const gate = new ConcurrencyGate(limit)
  const startContext = new AsyncLocalStorage<Reservation>()
  const runs = new Map<string, RunState>()
  const turns = new Map<string, { key: object; run?: RunState }>()
  const pending = new Map<string, Set<Reservation>>()
  const jobs = new Map<string, Reservation>()

  function forgetReservation(reservation: Reservation): void {
    const owned = pending.get(reservation.owner)
    owned?.delete(reservation)
    if (owned?.size === 0) pending.delete(reservation.owner)
    if (reservation.jobId !== undefined) jobs.delete(reservation.jobId)
  }

  function releaseReservation(reservation: Reservation): void {
    if (reservation.state !== 'open') return
    reservation.state = 'done'
    forgetReservation(reservation)
    gate.release(reservation)
  }

  function releaseTurn(agentId: string): void {
    const turn = turns.get(agentId)
    if (turn === undefined) return
    turns.delete(agentId)
    gate.release(turn.key)
  }

  function finishRun(runId: string, run: RunState): void {
    runs.delete(runId)
    if (turns.get(run.agentId)?.run === run) releaseTurn(run.agentId)
    else if (run.reservation !== undefined) gate.release(run.reservation)
  }

  ctx.effect(
    () => () => {
      gate.close()
      pending.clear()
      jobs.clear()
      runs.clear()
      turns.clear()
      startContext.disable()
    },
    'subagent concurrency limit',
  )

  ctx.inject(['jobs'], (jobCtx) => {
    // JobRegistry.start publishes registration synchronously inside the delegating call.
    jobCtx.effect(() =>
      jobCtx.jobs.events.subscribe({ owners: 'scope' }, (event) => {
        if (event.type === 'registered' && event.job.kind === 'subagent') {
          const reservation = startContext.getStore()
          if (reservation?.state !== 'open' || String(event.job.owner) !== reservation.owner) return
          reservation.jobId = String(event.job.id)
          jobs.set(reservation.jobId, reservation)
        } else if (event.type === 'settled') {
          const reservation = jobs.get(String(event.job.id))
          if (reservation !== undefined) releaseReservation(reservation)
        }
      }),
    )
  })

  ctx.on('tools/execute', async (exec, next) => {
    if (!subagentToolNames.has(exec.name) || exec.agent === undefined) return next()

    const reservation: Reservation = { state: 'open', owner: String(exec.agent.id) }
    if (delegationDepthOf(exec.agent) === 0) {
      await gate.enter(reservation, exec.signal, queueTimeoutMs)
    } else if (!gate.tryEnter(reservation, exec.signal)) {
      throw new Error('subagent concurrency limit exhausted for nested delegation')
    }
    let owned = pending.get(reservation.owner)
    if (owned === undefined) {
      owned = new Set()
      pending.set(reservation.owner, owned)
    }
    owned.add(reservation)

    try {
      const result = await startContext.run(reservation, next)
      // Harness normalizes tool exceptions into error results before returning to this wrapper.
      if (result.isError) releaseReservation(reservation)
      return result
    } catch (error) {
      releaseReservation(reservation)
      throw error
    }
  })

  ctx.on('subagent/start', (info) => {
    const reservation = startContext.getStore()
    const owned = reservation?.state === 'open' ? reservation : undefined
    if (owned !== undefined) {
      owned.state = 'bound'
      forgetReservation(owned)
    }
    runs.set(String(info.runId), { agentId: String(info.id), local: info.local, reservation: owned })
  })

  ctx.on('subagent/end', (info) => {
    const runId = String(info.runId)
    const run = runs.get(runId)
    if (run !== undefined) finishRun(runId, run)
  })

  ctx.on('agent/pre-step', async (info, next) => {
    const agentId = String(info.agent.id)
    if (delegationDepthOf(info.agent) === 0 || turns.has(agentId)) return next()

    let run: RunState | undefined
    for (const candidate of runs.values()) {
      if (candidate.local && candidate.agentId === agentId) {
        run = candidate
        break
      }
    }
    const key = run?.reservation ?? {}
    try {
      if (run === undefined) {
        await gate.enter(key, info.signal, queueTimeoutMs)
      } else if (!gate.tryEnter(key, info.signal)) {
        // A published child cannot queue behind an ancestor waiting for its result.
        throw new Error(
          run.reservation === undefined
            ? 'subagent concurrency limit exhausted for reservation-less local child turn'
            : 'subagent concurrency limit reservation was lost',
        )
      }
      turns.set(agentId, { key, run })
      const decision = await next()
      if (decision.kind === 'reject') releaseTurn(agentId)
      return decision
    } catch (error) {
      if (turns.get(agentId)?.key === key) releaseTurn(agentId)
      else gate.release(key)
      throw error
    }
  })

  ctx.on('agent/status', (info) => {
    if (info.status === 'idle') releaseTurn(String(info.agent.id))
  })

  ctx.on('agent/disposed', (info) => {
    const agentId = String(info.agent.id)
    releaseTurn(agentId)
    for (const [runId, run] of runs) {
      if (run.local && run.agentId === agentId) finishRun(runId, run)
    }
    for (const reservation of pending.get(agentId) ?? []) releaseReservation(reservation)
  })
}
