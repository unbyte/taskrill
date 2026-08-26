import { Emitter } from './emitter'
import type { Job, JobQueue, Scheduler, TaskDefinition } from './scheduler'
import type {
  Handler,
  SubmitArgs,
  TaskEvent,
  TaskFailureEvent,
  TaskNode,
  TaskNodeEventMap,
  Unsubscribe,
} from './types'

/** Reusable handler vertex. It emits facts but owns no aggregate completion state. */
export class TaskNodeImpl<I> implements TaskNode<I> {
  private readonly events = new Emitter<TaskNodeEventMap<I>>()

  constructor(
    private readonly scheduler: Scheduler,
    private readonly queue: JobQueue,
    readonly id: number,
    readonly name: string,
    private readonly handler: Handler<I>,
  ) {}

  submit(...args: SubmitArgs<I>) {
    const input = args[0] as I
    this.scheduler.accept(this.queue, (id) => this.createTask(id, input))
  }

  on<K extends keyof TaskNodeEventMap<I>>(
    event: K,
    listener: (event: TaskNodeEventMap<I>[K]) => void,
  ): Unsubscribe {
    return this.events.on(event, listener)
  }

  private createTask(id: number, input: I): TaskDefinition {
    const event: TaskEvent<I> = { id, node: this, input }
    return {
      job: this.createJob(event),
      publish: () => this.events.emit('task:submit', event),
    }
  }

  private createJob(event: TaskEvent<I>): Job {
    return {
      run: async () => {
        this.scheduler.emitLifecycle(() => this.events.emit('task:start', event))

        try {
          await this.handler(event.input, this.scheduler.context)
        } catch (error) {
          const failure: TaskFailureEvent<I> = { ...event, error }
          this.scheduler.emitLifecycle(() => {
            this.events.emit('task:failure', failure)
            this.scheduler.reportFailure(failure)
          })
          return
        }

        this.scheduler.emitLifecycle(() => this.events.emit('task:complete', event))
      },
      cancel: () => {
        this.scheduler.emitLifecycle(() => this.events.emit('task:cancel', event))
      },
    }
  }
}
