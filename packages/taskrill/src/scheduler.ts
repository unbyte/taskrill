import type { Emitter } from './emitter'
import { FifoQueue } from './fifo'
import type { RuntimeEventMap, RuntimeTaskFailureEvent, TaskContext, Unsubscribe } from './types'

export interface Job {
  /** Resolves after the task's terminal lifecycle event has been emitted. */
  run(): Promise<void>
  /** Terminalizes a task that has not started. */
  cancel(): void
}

export interface JobQueue {
  enqueue(job: Job): void
}

export interface TaskDefinition {
  readonly job: Job
  readonly publish: () => void
}

export interface AbortableQueue {
  abort(): void
}

const NEVER_ABORT = new AbortController().signal

export function assertValidConcurrency(value: number, label = 'concurrency') {
  if (value !== Number.POSITIVE_INFINITY && !(Number.isInteger(value) && value >= 1)) {
    throw new RangeError(`${label} must be an integer >= 1 or Infinity, got ${value}`)
  }
}

/** Global FIFO dispatcher and owner of task acceptance, abort, and idle accounting. */
export class Scheduler implements JobQueue {
  readonly context: TaskContext

  private readonly queue = new FifoQueue<Job>()
  private readonly abortableQueues = new Set<AbortableQueue>()
  private nextTaskId = 1
  private pendingTasks = 0
  private runningJobs = 0
  private activityGeneration = 0
  private idleCandidateGeneration?: number
  private pumpScheduled = false
  private idleScheduled = false
  private lifecycleDepth = 0
  private abortRequested = false
  private aborting = false

  constructor(
    private readonly concurrency: number,
    private readonly signal: AbortSignal | undefined,
    private readonly events: Emitter<RuntimeEventMap>,
  ) {
    assertValidConcurrency(concurrency)
    this.context = { signal: signal ?? NEVER_ABORT }

    if (signal && !signal.aborted) {
      signal.addEventListener('abort', () => this.requestAbort(), { once: true })
    }
  }

  get aborted() {
    return this.signal?.aborted ?? false
  }

  accept(queue: JobQueue, createTask: (id: number) => TaskDefinition) {
    if (this.aborted) return

    const task = createTask(this.nextTaskId++)
    this.pendingTasks++
    this.activityGeneration++
    this.idleCandidateGeneration = undefined
    queue.enqueue(this.track(task.job))
    this.emitLifecycle(task.publish)
  }

  enqueue(job: Job) {
    if (this.aborted) {
      job.cancel()
      return
    }

    this.queue.push(job)
    this.schedulePump()
  }

  emitLifecycle(emit: () => void) {
    this.lifecycleDepth++
    try {
      emit()
    } finally {
      this.lifecycleDepth--
      this.flushAbort()
    }
  }

  reportFailure(failure: RuntimeTaskFailureEvent) {
    this.events.emit('task:failure', failure)
  }

  registerAbortable(queue: AbortableQueue): Unsubscribe {
    if (this.aborted) {
      queue.abort()
      return () => {}
    }

    this.abortableQueues.add(queue)
    return () => {
      this.abortableQueues.delete(queue)
    }
  }

  private track(job: Job): Job {
    let finished = false
    const finish = () => {
      if (finished) return
      finished = true
      this.pendingTasks--
      if (this.pendingTasks === 0) this.idleCandidateGeneration = this.activityGeneration
    }

    return {
      run: async () => {
        try {
          await job.run()
        } finally {
          finish()
        }
      },
      cancel: () => {
        try {
          job.cancel()
        } finally {
          finish()
        }
      },
    }
  }

  private requestAbort() {
    this.abortRequested = true
    this.flushAbort()
  }

  private flushAbort() {
    if (!this.abortRequested || this.aborting || this.lifecycleDepth !== 0) return

    this.aborting = true
    try {
      for (const queue of [...this.abortableQueues]) queue.abort()
      for (const job of this.queue.drain()) job.cancel()
    } finally {
      this.abortRequested = false
      this.aborting = false
    }
  }

  private schedulePump() {
    if (this.pumpScheduled || this.aborted) return
    this.pumpScheduled = true
    queueMicrotask(() => {
      this.pumpScheduled = false
      this.pump()
    })
  }

  private pump() {
    if (this.aborted) return

    while (this.runningJobs < this.concurrency) {
      const job = this.queue.shift()
      if (!job) break
      this.dispatch(job)
    }
  }

  private dispatch(job: Job) {
    this.runningJobs++
    void job.run().then(
      () => this.finishRunningJob(),
      (error) => {
        this.finishRunningJob()
        queueMicrotask(() => {
          throw error
        })
      },
    )
  }

  private finishRunningJob() {
    this.runningJobs--
    this.schedulePump()
    this.scheduleIdleCheckpoint()
  }

  private scheduleIdleCheckpoint() {
    const generation = this.idleCandidateGeneration
    if (generation === undefined || this.idleScheduled) return

    this.idleScheduled = true
    queueMicrotask(() => {
      this.idleScheduled = false
      if (this.aborted) return
      if (this.pendingTasks !== 0) return
      if (this.activityGeneration !== generation) return
      if (this.idleCandidateGeneration !== generation) return

      this.idleCandidateGeneration = undefined
      this.events.emit('idle', undefined)
    })
  }
}
