/**
 * Process-local FIFO limit for DeepSeek Harness subagent turns.
 *
 * @module dsh-subagent-concurrency-limit
 */

import { AsyncLocalStorage } from 'node:async_hooks'
import type { Context } from '@deepseek-ai/cordis'
import { delegationDepthOf } from '@deepseek-ai/dsh-subagent'
import type {} from '@deepseek-ai/dsh-agent'
import type {} from '@deepseek-ai/dsh-tools'

export const name = 'subagent-concurrency-limit'
export const inject = ['tools']

const DEFAULT_MAX_CONCURRENT_SUBAGENTS = 2

/** Plugin configuration. */
export interface Config {
  /** Maximum local non-root turns and observable remote runs at once. */
  maxConcurrentSubagents?: number
  /** Delegation tool names whose calls are limited. */
  subagentToolNames?: string[]
}

interface Waiter {
  readonly key: object
  readonly signal: AbortSignal
  readonly resolve: () => void
  readonly reject: (reason?: unknown) => void
  readonly abort: () => void
}

class ConcurrencyGate {
  private readonly active = new Set<object>()
  private readonly waiters: Waiter[] = []
  private readonly limit: number
  private closed = false

  constructor(limit: number) {
    this.limit = limit
  }

  async enter(key: object, signal: AbortSignal): Promise<void> {
    if (this.active.has(key)) return
    signal.throwIfAborted()
    if (this.closed) throw new Error('subagent concurrency limit is disposed')

    if (this.active.size < this.limit && this.waiters.length === 0) {
      this.active.add(key)
      return
    }

    try {
      await new Promise<void>((resolve, reject) => {
        const waiter: Waiter = {
          key,
          signal,
          resolve,
          reject,
          abort: () => {
            const index = this.waiters.indexOf(waiter)
            if (index >= 0) this.waiters.splice(index, 1)
            reject(signal.reason)
          },
        }
        signal.addEventListener('abort', waiter.abort, { once: true })
        this.waiters.push(waiter)
      })
      signal.throwIfAborted()
    } catch (error) {
      // The listener is removed at admission, so abort can race this continuation.
      this.release(key)
      throw error
    }
  }

  /** Admit without queueing. Used by nested delegation to avoid deadlock. */
  tryEnter(key: object, signal: AbortSignal): boolean {
    if (this.active.has(key)) return true
    signal.throwIfAborted()
    if (this.closed) throw new Error('subagent concurrency limit is disposed')
    if (this.waiters.length > 0 || this.active.size >= this.limit) return false
    this.active.add(key)
    return true
  }

  release(key: object): void {
    if (!this.active.delete(key)) return
    this.admitNext()
  }

  close(): void {
    if (this.closed) return
    this.closed = true
    this.active.clear()
    const error = new Error('subagent concurrency limit is disposed')
    for (const waiter of this.waiters.splice(0)) {
      waiter.signal.removeEventListener('abort', waiter.abort)
      waiter.reject(error)
    }
  }

  private admitNext(): void {
    const waiter = this.waiters.shift()
    if (waiter === undefined) return
    waiter.signal.removeEventListener('abort', waiter.abort)
    this.active.add(waiter.key)
    waiter.resolve()
  }
}

/** A delegation call holding a permit until its child run claims it. */
interface Reservation {
  state: 'open' | 'bound' | 'done'
}

interface RunState {
  readonly agentId: string
  readonly local: boolean
  readonly reservation?: Reservation
  lease?: object
  ended: boolean
}

function resolveConfig(config: Config): {
  limit: number
  subagentToolNames: ReadonlySet<string>
} {
  const limit = config.maxConcurrentSubagents ?? DEFAULT_MAX_CONCURRENT_SUBAGENTS
  if (!Number.isSafeInteger(limit) || limit < 1) {
    throw new TypeError('maxConcurrentSubagents must be a positive safe integer')
  }
  const names = config.subagentToolNames ?? ['subagent']
  if (
    !Array.isArray(names) ||
    names.length === 0 ||
    names.some((value) => typeof value !== 'string' || value.length === 0)
  ) {
    throw new TypeError('subagentToolNames must contain at least one non-empty string')
  }
  return { limit, subagentToolNames: new Set(names) }
}

/**
 * Install the process-local delegation and child-turn gate.
 *
 * Root delegation calls wait in FIFO order. A nested configured delegation
 * call never waits when the pool is full, because its parent may be waiting
 * for that child. Local child turns use the same pool; root turns do not.
 */
export function apply(ctx: Context, config: Config = {}): void {
  const { limit, subagentToolNames } = resolveConfig(config)
  const gate = new ConcurrencyGate(limit)
  const startContext = new AsyncLocalStorage<Reservation>()
  const startsByRunId = new Map<string, RunState>()
  const activeTurnsByAgentId = new Map<string, object>()

  function releaseTurn(agentId: string): void {
    const lease = activeTurnsByAgentId.get(agentId)
    if (lease === undefined) return
    activeTurnsByAgentId.delete(agentId)
    for (const run of startsByRunId.values()) {
      if (run.lease === lease) run.lease = undefined
    }
    gate.release(lease)
  }

  ctx.effect(() => () => gate.close(), 'subagent concurrency limit')

  ctx.on('tools/execute', async (exec, next) => {
    if (!subagentToolNames.has(exec.name) || exec.agent === undefined) {
      return next()
    }

    const root = delegationDepthOf(exec.agent) === 0
    const reservation: Reservation = { state: 'open' }
    if (root) {
      await gate.enter(reservation, exec.signal)
    } else if (!gate.tryEnter(reservation, exec.signal)) {
      throw new Error('subagent concurrency limit exhausted for nested delegation')
    }

    try {
      return await startContext.run(reservation, next)
    } finally {
      if (reservation.state === 'open') gate.release(reservation)
      reservation.state = 'done'
    }
  })

  ctx.on('subagent/start', (info) => {
    const reservation = startContext.getStore()
    let ownedReservation: Reservation | undefined
    if (reservation?.state === 'open') {
      reservation.state = 'bound'
      ownedReservation = reservation
    }

    startsByRunId.set(String(info.runId), {
      agentId: String(info.id),
      local: info.local,
      reservation: ownedReservation,
      ended: false,
    })
  })

  ctx.on('subagent/end', (info) => {
    const runId = String(info.runId)
    const run = startsByRunId.get(runId)
    if (run === undefined) return
    run.ended = true
    startsByRunId.delete(runId)
    if (run.lease !== undefined) {
      const lease = run.lease
      run.lease = undefined
      if (activeTurnsByAgentId.get(run.agentId) === lease) {
        activeTurnsByAgentId.delete(run.agentId)
      }
      gate.release(lease)
    } else if (run.reservation !== undefined) {
      gate.release(run.reservation)
    }
  })

  ctx.on('agent/pre-step', async (info, next) => {
    const agentId = String(info.agent.id)
    if (delegationDepthOf(info.agent) === 0) return next()
    if (activeTurnsByAgentId.has(agentId)) return next()

    const run = [...startsByRunId.values()].find(
      (candidate) => candidate.local && candidate.agentId === agentId && candidate.lease === undefined,
    )
    const lease = run?.reservation ?? {}
    try {
      if (run?.reservation === undefined) await gate.enter(lease, info.signal)
      else if (!gate.tryEnter(lease, info.signal)) {
        // A locally published child's reservation must remain active until its first turn.
        throw new Error('subagent concurrency limit reservation was lost')
      }
      if (run?.ended) {
        gate.release(lease)
        return next()
      }
      if (run !== undefined) run.lease = lease
      activeTurnsByAgentId.set(agentId, lease)
      try {
        const decision = await next()
        if (decision?.kind === 'reject') releaseTurn(agentId)
        return decision
      } catch (error) {
        releaseTurn(agentId)
        throw error
      }
    } catch (error) {
      if (activeTurnsByAgentId.get(agentId) !== lease) gate.release(lease)
      throw error
    }
  })

  ctx.on('agent/status', (info) => {
    if (info.status === 'idle') releaseTurn(String(info.agent.id))
  })

  ctx.on('agent/disposed', (info) => {
    const agentId = String(info.agent.id)
    releaseTurn(agentId)
    for (const [runId, run] of startsByRunId) {
      if (!run.local || run.agentId !== agentId) continue
      run.ended = true
      if (run.lease !== undefined) {
        const lease = run.lease
        run.lease = undefined
        gate.release(lease)
      } else if (run.reservation !== undefined) {
        gate.release(run.reservation)
      }
      startsByRunId.delete(runId)
    }
  })
}
