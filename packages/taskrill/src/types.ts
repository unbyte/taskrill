export type MaybePromise<T> = T | Promise<T>

export interface RuntimeOptions {
  /**
   * Maximum number of handlers executing simultaneously across the whole
   * Runtime. Must be an integer `>= 1` or `Infinity`. Required, no default.
   */
  concurrency: number
  /**
   * Cancellation is observed, never originated. When this signal aborts every
   * unit is force-sealed and every not-yet-started task terminalizes as
   * `cancelled`.
   */
  signal?: AbortSignal
  /**
   * Called exactly once per failed task. Fire-and-forget: a returned promise is
   * ignored. Not called for cancelled tasks.
   */
  onError?: (failure: TaskFailure) => void
}

export interface UnitOptions {
  /** Overrides the auto-generated `kind#id` display name. Debugging metadata only. */
  name?: string
}

export interface GroupOptions<I = void> extends UnitOptions {
  /**
   * Fires synchronously on each open `pending -> 0` transition, after the first
   * submission and never once sealed. Meant for the self-fed recursive fan-out
   * pattern, where the callback seals the group. Fire-and-forget.
   */
  onIdle?: (unit: Group<I>) => void
}

export interface TaskContext {
  /** The Runtime's signal, or a never-aborting placeholder when none is provided. */
  readonly signal: AbortSignal
}

export type Handler<I> = (input: I, context: TaskContext) => MaybePromise<void>

export interface TaskFailure {
  readonly error: unknown
  readonly unit: Unit
  readonly input: unknown
}

export interface Settlement {
  readonly submitted: number
  readonly succeeded: number
  readonly failed: number
  readonly cancelled: number

  /** `true` exactly when `failed === 0 && cancelled === 0`. Derived convenience. */
  readonly ok: boolean
}

export interface Unit {
  readonly id: number
  readonly name: string

  /** Resolves once the unit is sealed and every submitted task is terminal. Never rejects. */
  readonly done: Promise<Settlement>
}

export interface Single<I = void> extends Unit {
  submit(...args: SubmitArgs<I>): void
  skip(): void
}

export interface Group<I = void> extends Unit {
  submit(...args: SubmitArgs<I>): void
  seal(): void
}

// `void` (not `undefined`) is deliberate: it lets a `Single<void>`/`Group<void>`
// be submitted with no argument, while a typed input stays required.
// biome-ignore lint/suspicious/noConfusingVoidType: intentional void-vs-typed input discrimination
export type SubmitArgs<I> = [I] extends [void] ? [] | [input: I] : [input: I]
