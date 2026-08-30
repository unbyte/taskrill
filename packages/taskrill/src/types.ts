export type MaybePromise<T> = T | Promise<T>

export type Unsubscribe = () => void

export interface RuntimeOptions {
  /** Maximum handlers running across the runtime. Must be a positive integer or `Infinity`. */
  concurrency: number
  /** Aborting closes the runtime, cancels queued tasks, and signals running handlers. */
  signal?: AbortSignal
}

export interface TaskNodeOptions {
  /** Overrides the generated `node#id` debugging name. */
  name?: string
  /** Optional concurrency cap for this node. Must be a positive integer or `Infinity`. */
  concurrency?: number
}

export interface TaskContext {
  /** The runtime signal, or a never-aborting signal when the runtime has none. */
  readonly signal: AbortSignal
}

/** A thrown or rejected value reports task failure without stopping the runtime. */
export type Handler<I> = (input: I, context: TaskContext) => MaybePromise<void>

export interface TaskNodeRef {
  /** Unique among nodes created by the runtime. */
  readonly id: number
  /** Human-readable diagnostic label. It is not required to be unique. */
  readonly name: string
}

export interface TaskEvent<I> {
  /** Runtime-wide task identity, shared by all lifecycle events for this submission. */
  readonly id: number
  /** Node that accepted and owns the task. */
  readonly node: TaskNode<I>
  /** Value passed to `submit`; object identity is preserved. */
  readonly input: I
}

export interface TaskFailureEvent<I> extends TaskEvent<I> {
  /** Value thrown by or rejected from the handler. */
  readonly error: unknown
}

export interface RuntimeTaskFailureEvent {
  /** Runtime-wide identity of the failed task. */
  readonly id: number
  /** Identity of the node whose handler failed. */
  readonly node: TaskNodeRef
  /** Submitted input, typed as `unknown` because a runtime can contain heterogeneous nodes. */
  readonly input: unknown
  /** Value thrown by or rejected from the handler. */
  readonly error: unknown
}

export interface TaskNodeEventMap<I> {
  /** Emitted synchronously after `submit` is accepted and before the task can start. */
  'task:submit': TaskEvent<I>
  /** Emitted immediately before the handler runs, after concurrency limits admit the task. */
  'task:start': TaskEvent<I>
  /** Emitted after the handler returns or resolves. */
  'task:complete': TaskEvent<I>
  /** Emitted after the handler throws or rejects, before the runtime failure event. */
  'task:failure': TaskFailureEvent<I>
  /** Emitted when abort cancels an accepted task before it starts. Running tasks are not cancelled. */
  'task:cancel': TaskEvent<I>
}

export interface RuntimeEventMap {
  /** Emitted for every failed task, after that node's failure event. */
  'task:failure': RuntimeTaskFailureEvent
  /** Emitted after accepted work drains to zero. It can recur and is suppressed after abort. */
  idle: undefined
}

export interface TaskNode<I = void> extends TaskNodeRef {
  /** Accepts a task and returns its ID, or `undefined` when the runtime no longer accepts work. */
  submit(...args: SubmitArgs<I>): number | undefined

  on<K extends keyof TaskNodeEventMap<I>>(
    event: K,
    listener: (event: TaskNodeEventMap<I>[K]) => void,
  ): Unsubscribe
}

// `void` lets a no-input node use `submit()` while preserving required typed inputs.
// biome-ignore lint/suspicious/noConfusingVoidType: intentional void-vs-typed input discrimination
export type SubmitArgs<I> = [I] extends [void] ? [] | [input: I] : [input: I]
