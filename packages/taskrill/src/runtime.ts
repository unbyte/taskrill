import { Emitter } from './emitter'
import { Gate } from './gate'
import { TaskNodeImpl } from './node'
import { assertValidConcurrency, type JobQueue, Scheduler } from './scheduler'
import type {
  Handler,
  RuntimeEventMap,
  RuntimeOptions,
  TaskNode,
  TaskNodeOptions,
  Unsubscribe,
} from './types'

/** One scheduling environment shared by a dynamic graph of reusable task nodes. */
export class Runtime {
  /** Resolves after acceptance stops and every task terminates. Task failures do not reject it. */
  readonly closed: Promise<void>

  private readonly events = new Emitter<RuntimeEventMap>()
  private readonly scheduler: Scheduler
  private readonly concurrency: number
  private nextNodeId = 1

  constructor(options: RuntimeOptions) {
    const { concurrency, signal } = options
    this.concurrency = concurrency
    this.scheduler = new Scheduler(concurrency, signal, this.events)
    this.closed = this.scheduler.closed
  }

  /** Creates a reusable task node. Throws after the runtime stops accepting work. */
  node<I = void>(handler: Handler<I>, options?: TaskNodeOptions): TaskNode<I> {
    if (!this.scheduler.accepting)
      throw new Error('Cannot create a task node after the runtime stops accepting work')

    const queue = this.queueFor(options?.concurrency)
    const id = this.nextNodeId++
    return new TaskNodeImpl(this.scheduler, queue, id, options?.name ?? `node#${id}`, handler)
  }

  /** Stops accepting nodes and tasks while allowing accepted tasks to terminate normally. */
  close() {
    this.scheduler.close()
  }

  on<K extends keyof RuntimeEventMap>(
    event: K,
    listener: (value: RuntimeEventMap[K]) => void,
  ): Unsubscribe {
    return this.events.on(event, listener)
  }

  private queueFor(concurrency: number | undefined): JobQueue {
    if (concurrency === undefined) return this.scheduler

    assertValidConcurrency(concurrency, 'node concurrency')
    if (concurrency >= this.concurrency) return this.scheduler
    return new Gate(this.scheduler, concurrency)
  }
}
