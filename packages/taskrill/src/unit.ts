import { SealedUnitError } from './errors'
import type { Job, Scheduler } from './scheduler'
import type { Group, Handler, Settlement, Single, SubmitArgs, Unit } from './types'

interface Counts {
  submitted: number
  pending: number
  succeeded: number
  failed: number
  cancelled: number
}

/**
 * Shared lifecycle for a single and a group. A unit moves open -> sealed ->
 * settled. `done` resolves exactly once, asynchronously, and never rejects.
 */
abstract class UnitImpl<I> implements Unit {
  readonly id: number
  readonly name: string
  readonly done: Promise<Settlement>

  protected sealed = false

  private settled = false
  private resolveDone!: (settlement: Settlement) => void
  private readonly counts: Counts = {
    submitted: 0,
    pending: 0,
    succeeded: 0,
    failed: 0,
    cancelled: 0,
  }

  constructor(
    protected readonly scheduler: Scheduler,
    id: number,
    kind: 'single' | 'group',
    name: string | undefined,
    private readonly handler: Handler<I>,
  ) {
    this.id = id
    this.name = name ?? `${kind}#${id}`
    this.done = new Promise((resolve) => {
      this.resolveDone = resolve
    })
  }

  /**
   * Registers one task with the Runtime. Returns whether it was registered:
   * `false` when dropped by an abort, so a single knows not to auto-seal.
   * Throws {@link SealedUnitError} when the caller has already sealed the unit.
   */
  protected register(input: I): boolean {
    // Abort is checked before sealing so an abort-induced force-seal drops the
    // submission rather than throwing.
    if (this.scheduler.aborted) return false
    if (this.sealed) throw new SealedUnitError(this)
    this.counts.submitted++
    this.counts.pending++
    this.scheduler.enqueue(this.createJob(input))
    return true
  }

  /** Seals the unit; settles immediately if no task is pending. Idempotent. */
  protected sealSelf() {
    if (this.sealed) return
    this.sealed = true
    this.trySettle()
  }

  /**
   * Force-sealing on abort. Marks the unit sealed *before* its tasks drain, so
   * a `pending -> 0` transition during teardown resolves as settlement and
   * never fires `onIdle`.
   *
   * Public because the Scheduler — a collaborator, not a subclass — drives this
   * during abort teardown, out of reach of `protected`. It is deliberately
   * absent from the `Single`/`Group` interfaces, so consumers never see it.
   */
  forceSeal() {
    this.sealed = true
    this.trySettle()
  }

  private createJob(input: I): Job {
    return {
      run: () =>
        // The executor invokes the handler synchronously, capturing a
        // synchronous throw as a rejection just like an async rejection.
        new Promise<void>((resolve) => resolve(this.handler(input, this.scheduler.context))).then(
          () => this.onSucceeded(),
          (error) => this.onFailed(error, input),
        ),
      cancel: () => this.onCancelled(),
    }
  }

  private onSucceeded() {
    this.counts.pending--
    this.counts.succeeded++
    this.afterTerminal()
  }

  private onFailed(error: unknown, input: I) {
    // Counts are updated before onError, and settlement is idempotent, so
    // bookkeeping stays consistent even when onError synchronously aborts the
    // Runtime and thereby settles this same unit reentrantly.
    this.counts.pending--
    this.counts.failed++
    this.scheduler.reportError({ error, unit: this, input })
    this.afterTerminal()
  }

  private onCancelled() {
    this.counts.pending--
    this.counts.cancelled++
    this.afterTerminal()
  }

  private afterTerminal() {
    if (this.counts.pending !== 0) return
    // A decrement reached zero: a genuine pending -> 0 transition.
    if (this.sealed) {
      this.trySettle()
    } else {
      this.onOpenIdle()
    }
  }

  /** Fires on an open `pending -> 0` transition. Only a group reacts. */
  protected onOpenIdle() {}

  private trySettle() {
    if (this.settled) return
    if (!this.sealed) return
    if (this.counts.pending !== 0) return
    this.settled = true
    const { submitted, succeeded, failed, cancelled } = this.counts
    this.resolveDone({
      submitted,
      succeeded,
      failed,
      cancelled,
      ok: failed === 0 && cancelled === 0,
    })
  }
}

/**
 * A single accepts exactly one submission, which also seals it. A single that
 * is never submitted is sealed empty by `skip()`.
 */
export class SingleImpl<I> extends UnitImpl<I> implements Single<I> {
  private skipped = false

  constructor(scheduler: Scheduler, id: number, name: string | undefined, handler: Handler<I>) {
    super(scheduler, id, 'single', name, handler)
  }

  submit(...args: SubmitArgs<I>) {
    if (this.register(args[0] as I)) this.sealSelf()
  }

  skip() {
    // skip() after abort is a silent no-op; skip() after skip() is idempotent;
    // skip() after submit() throws (the unit is already sealed by the submit).
    if (this.scheduler.aborted) return
    if (this.skipped) return
    if (this.sealed) throw new SealedUnitError(this)
    this.skipped = true
    this.sealSelf()
  }
}

/**
 * A group accepts many submissions and requires an explicit `seal()`. It fires
 * `onIdle` on each open `pending -> 0` transition.
 */
export class GroupImpl<I> extends UnitImpl<I> implements Group<I> {
  constructor(
    scheduler: Scheduler,
    id: number,
    name: string | undefined,
    handler: Handler<I>,
    private readonly onIdle?: (unit: Group<I>) => void,
  ) {
    super(scheduler, id, 'group', name, handler)
  }

  submit(...args: SubmitArgs<I>) {
    this.register(args[0] as I)
  }

  seal(): void {
    this.sealSelf()
  }

  protected override onOpenIdle() {
    if (!this.onIdle) return
    try {
      this.onIdle(this)
    } catch (error) {
      // Fire-and-forget: surface the bug loudly without disturbing bookkeeping.
      queueMicrotask(() => {
        throw error
      })
    }
  }
}
