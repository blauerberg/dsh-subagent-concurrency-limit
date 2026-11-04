interface Waiter {
  admit(): void
  cancel(reason: unknown): void
}

export class ConcurrencyGate {
  private readonly active = new Set<object>()
  private readonly waiters = new Set<Waiter>()
  private readonly limit: number
  private closed = false

  constructor(limit: number) {
    this.limit = limit
  }

  async enter(key: object, signal: AbortSignal, timeoutMs: number): Promise<void> {
    if (this.tryEnter(key, signal)) return
    try {
      await new Promise<void>((resolve, reject) => {
        const cleanup = () => {
          this.waiters.delete(waiter)
          clearTimeout(timeout)
          signal.removeEventListener('abort', abort)
        }
        const waiter: Waiter = {
          admit: () => {
            cleanup()
            this.active.add(key)
            resolve()
          },
          cancel: (reason) => {
            cleanup()
            reject(reason)
          },
        }
        const abort = () => waiter.cancel(signal.reason)
        const timeout = setTimeout(() => {
          waiter.cancel(new Error(`subagent concurrency limit queue timeout after ${timeoutMs}ms`))
        }, timeoutMs)
        this.waiters.add(waiter)
        signal.addEventListener('abort', abort, { once: true })
      })
      // Admission removes the listener before the awaiting caller resumes.
      this.checkOpen(signal)
    } catch (error) {
      this.release(key)
      throw error
    }
  }

  tryEnter(key: object, signal: AbortSignal): boolean {
    this.checkOpen(signal)
    if (this.active.has(key)) return true
    if (this.waiters.size > 0 || this.active.size >= this.limit) return false
    this.active.add(key)
    return true
  }

  release(key: object): void {
    if (this.active.delete(key)) this.waiters.values().next().value?.admit()
  }

  close(): void {
    this.closed = true
    this.active.clear()
    for (const waiter of this.waiters) {
      waiter.cancel(new Error('subagent concurrency limit is disposed'))
    }
  }

  private checkOpen(signal: AbortSignal): void {
    signal.throwIfAborted()
    if (this.closed) throw new Error('subagent concurrency limit is disposed')
  }
}
