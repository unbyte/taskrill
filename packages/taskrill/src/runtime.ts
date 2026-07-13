import { SchedulerImpl } from './scheduler'
import type { Group, GroupOptions, Handler, RuntimeOptions, Single, UnitOptions } from './types'
import { GroupImpl, SingleImpl } from './unit'

/**
 * The public entry point. A Runtime constructs the scheduler that owns all
 * execution, concurrency, and abort behavior, then hands out units bound to it.
 *
 * It has no "finished" state of its own — completion is a property of individual
 * units, observed through their `done`.
 */
export class Runtime {
  private readonly scheduler: SchedulerImpl
  private nextId = 1

  constructor(options: RuntimeOptions) {
    const { concurrency, signal, onError } = options
    this.scheduler = new SchedulerImpl(concurrency, signal, onError)
  }

  single<I = void>(handler: Handler<I>, options?: UnitOptions): Single<I> {
    const unit = new SingleImpl<I>(this.scheduler, this.nextId++, options?.name, handler)
    this.scheduler.track(unit)
    return unit
  }

  group<I = void>(handler: Handler<I>, options?: GroupOptions<I>): Group<I> {
    const unit = new GroupImpl<I>(this.scheduler, this.nextId++, options?.name, handler, options?.onIdle)
    this.scheduler.track(unit)
    return unit
  }
}
