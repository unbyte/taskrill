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
   * Caps how many of *this group's* handlers run simultaneously, independent of
   * every other unit. Must be an integer `>= 1` or `Infinity`. Clipped to the
   * Runtime's `concurrency`: a value at or above it imposes no extra limit.
   * Omit for no per-group limit — the group is then bounded only by the Runtime.
   */
  concurrency?: number
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

/**
 * A unit that accepts exactly one task submission. Conceptually a {@link Group}
 * with a maximum capacity of one and automatic sealing: its first `submit()`
 * seals it, and `skip()` seals it empty. Its `done` resolves once that one task
 * is terminal, or immediately when skipped.
 */
export interface Single<I = void> extends Unit {
  /**
   * Submits the single's one task and seals it in the same call. Throws
   * {@link SealedUnitError} synchronously if the single was already submitted or
   * skipped; a silent no-op after an abort.
   */
  submit(...args: SubmitArgs<I>): void
  /**
   * Seals the single empty without running it, so its `done` resolves with all
   * counts at `0`. Idempotent, but throws {@link SealedUnitError} after a
   * `submit()`; a silent no-op after an abort. Use it to give a conditional
   * terminal single a terminal state so a top-level `await done` cannot hang.
   */
  skip(): void
}

/**
 * A unit representing a dynamically growing set of tasks. Each `submit()` adds
 * one task that may start immediately; an explicit `seal()` finalizes the set,
 * after which the group settles once every submitted task is terminal.
 */
export interface Group<I = void> extends Unit {
  /**
   * Adds one task to the group and schedules it for asynchronous execution.
   * Non-blocking and returns nothing — the handler never runs inline. Throws
   * {@link SealedUnitError} synchronously if the group is already sealed; a
   * silent no-op after an abort.
   */
  submit(...args: SubmitArgs<I>): void
  /**
   * Declares that the group will accept no further submissions. Does not wait
   * for running tasks — settlement follows once every submitted task drains.
   * Idempotent; an empty sealed group settles immediately.
   */
  seal(): void
}

// `void` (not `undefined`) is deliberate: it lets a `Single<void>`/`Group<void>`
// be submitted with no argument, while a typed input stays required.
// biome-ignore lint/suspicious/noConfusingVoidType: intentional void-vs-typed input discrimination
export type SubmitArgs<I> = [I] extends [void] ? [] | [input: I] : [input: I]
