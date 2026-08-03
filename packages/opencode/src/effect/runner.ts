import { Cause, Deferred, Effect, Exit, Fiber, Latch, Schema, Scope, Semaphore } from "effect"

export interface Runner<A, E = never> {
  readonly state: State<A, E>
  readonly busy: boolean
  readonly ensureRunning: (work: Effect.Effect<A, E>) => Effect.Effect<A, E>
  readonly submit: <B, E2, R>(
    admission: Effect.Effect<B, E2, R>,
    work: Effect.Effect<A, E>,
  ) => Effect.Effect<A, E | E2, R>
  readonly commit: <B, E2, R>(work: Effect.Effect<B, E2, R>) => Effect.Effect<B, E2 | Busy, R>
  readonly startShell: (work: Effect.Effect<A, E>, ready?: Latch.Latch) => Effect.Effect<A, E | Busy>
  readonly cancel: Effect.Effect<void>
}

export class Cancelled extends Schema.TaggedErrorClass<Cancelled>()("RunnerCancelled", {}) {}
export class Busy extends Schema.TaggedErrorClass<Busy>()("RunnerBusy", {}) {}

interface RunHandle<A, E> {
  id: number
  done: Deferred.Deferred<A, E | Cancelled>
  fiber: Fiber.Fiber<A, E>
}

interface ShellHandle<A, E> {
  id: number
  cancelled: Deferred.Deferred<void>
  ready?: Latch.Latch
  fiber: Fiber.Fiber<A, E>
}

interface PendingHandle<A, E> {
  id: number
  done: Deferred.Deferred<A, E | Cancelled>
  work: Effect.Effect<A, E>
}

type StableState<A, E> =
  | { readonly _tag: "Idle" }
  | { readonly _tag: "Running"; readonly run: RunHandle<A, E>; readonly next?: PendingHandle<A, E> }
  | { readonly _tag: "Shell"; readonly shell: ShellHandle<A, E> }
  | { readonly _tag: "ShellThenRun"; readonly shell: ShellHandle<A, E>; readonly run: PendingHandle<A, E> }

interface Reservation {
  readonly token: number
  readonly gate: Deferred.Deferred<void>
}

type RunOrigin<A, E> =
  | { readonly _tag: "Initial" }
  | { readonly _tag: "Run"; readonly run: RunHandle<A, E>; readonly exit: Exit.Exit<A, E> }
  | { readonly _tag: "Successor" }
  | { readonly _tag: "Shell" }

type StartingState<A, E> = Reservation &
  (
    | {
        readonly _tag: "Starting"
        readonly kind: "Run"
        readonly pending: PendingHandle<A, E>
        readonly origin: RunOrigin<A, E>
      }
    | {
        readonly _tag: "Starting"
        readonly kind: "Shell"
        readonly id: number
        readonly work: Effect.Effect<A, E>
        readonly cancelled: Deferred.Deferred<void>
        readonly ready?: Latch.Latch
      }
  )

type FinishingState<A, E> = {
  readonly _tag: "Finishing"
  readonly completion?: { readonly done: Deferred.Deferred<A, E | Cancelled>; readonly exit: Exit.Exit<A, E> }
  readonly next?: PendingHandle<A, E>
} & Reservation

export type State<A, E> =
  | StableState<A, E>
  | ({
      readonly _tag: "Admitting"
      readonly previous: StableState<A, E> | FinishingState<A, E>
      readonly cancelled: Deferred.Deferred<void>
    } & Reservation)
  | StartingState<A, E>
  | FinishingState<A, E>
  | ({
      readonly _tag: "Cancelling"
      readonly previous: Exclude<StableState<A, E>, { readonly _tag: "Idle" }>
    } & Reservation)

export const make = <A, E = never>(
  scope: Scope.Scope,
  opts?: {
    onIdle?: Effect.Effect<void>
    onBusy?: Effect.Effect<void>
    onInterrupt?: Effect.Effect<A, E>
    onRetire?: () => void
  },
): Runner<A, E> => {
  const lock = Semaphore.makeUnsafe(1)
  const idle = opts?.onIdle ?? Effect.void
  const onBusy = opts?.onBusy ?? Effect.void
  const onInterrupt = opts?.onInterrupt
  let current: State<A, E> = { _tag: "Idle" }
  let ids = 0

  type Restore = <B, E2, R>(effect: Effect.Effect<B, E2, R>) => Effect.Effect<B, E2, R>

  const next = () => {
    ids += 1
    return ids
  }

  // Caller-side permit acquisition is interruptible until a reservation owns
  // the lifecycle. Once acquired, the body and release are synchronous/masked,
  // so a state transfer cannot be interrupted after it becomes visible.
  const withLock = <B>(body: () => B, acquire?: Restore) =>
    Effect.uninterruptibleMask((restore) =>
      (acquire ?? restore)(lock.take(1)).pipe(
        Effect.flatMap(() => Effect.sync(body).pipe(Effect.ensuring(lock.release(1).pipe(Effect.asVoid)))),
      ),
    )

  const withLockEffect = <B, E2, R>(body: () => Effect.Effect<B, E2, R>, acquire?: Restore) =>
    Effect.uninterruptibleMask((restore) =>
      (acquire ?? restore)(lock.take(1)).pipe(
        Effect.flatMap(() => body().pipe(Effect.ensuring(lock.release(1).pipe(Effect.asVoid)))),
      ),
    )

  const reservation = (): Reservation => ({ token: next(), gate: Deferred.makeUnsafe<void>() })
  const open = (gate: Deferred.Deferred<void>) => Deferred.succeed(gate, undefined).pipe(Effect.asVoid)

  const complete = (done: Deferred.Deferred<A, E | Cancelled>, exit: Exit.Exit<A, E>) =>
    Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)
      ? Deferred.fail(done, new Cancelled()).pipe(Effect.asVoid)
      : Deferred.done(done, exit).pipe(Effect.asVoid)

  const awaitDone = (done: Deferred.Deferred<A, E | Cancelled>) =>
    Deferred.await(done).pipe(Effect.catchTag("RunnerCancelled", (error) => onInterrupt ?? Effect.die(error)))

  const commitIdle = (tag: "Starting" | "Finishing" | "Cancelling", token: number) =>
    withLock(() => {
      if (current._tag !== tag || current.token !== token) {
        return { committed: false as const, error: undefined as unknown | undefined }
      }
      current = { _tag: "Idle" }
      try {
        opts?.onRetire?.()
        return { committed: true as const, error: undefined as unknown | undefined }
      } catch (error) {
        return { committed: true as const, error }
      }
    })

  let finishRun: (id: number, done: Deferred.Deferred<A, E | Cancelled>, exit: Exit.Exit<A, E>) => Effect.Effect<void>
  let finishShell: (id: number) => Effect.Effect<void>

  const performRunStart = (starting: Extract<StartingState<A, E>, { readonly kind: "Run" }>) =>
    Effect.gen(function* () {
      const started = yield* Effect.gen(function* () {
        if (starting.origin._tag === "Initial") yield* onBusy
        const fiber = yield* starting.pending.work.pipe(
          Effect.onExit((exit) => finishRun(starting.pending.id, starting.pending.done, exit)),
          Effect.forkIn(scope),
        )
        return { id: starting.pending.id, done: starting.pending.done, fiber } satisfies RunHandle<A, E>
      }).pipe(Effect.exit)

      if (Exit.isFailure(started)) {
        const published = yield* idle.pipe(Effect.exit)
        const committed = yield* commitIdle("Starting", starting.token)
        if (starting.origin._tag === "Run") yield* complete(starting.origin.run.done, starting.origin.exit)
        yield* Deferred.failCause(starting.pending.done, started.cause).pipe(Effect.asVoid)
        yield* open(starting.gate)
        if (committed.committed && committed.error !== undefined) yield* Effect.die(committed.error)
        if (Exit.isFailure(published)) yield* Effect.failCause(published.cause)
        return yield* Effect.failCause(started.cause)
      }

      const committed = yield* withLock(() => {
        if (current._tag !== "Starting" || current.token !== starting.token || current.kind !== "Run") return false
        current = { _tag: "Running", run: started.value }
        return true
      })
      if (committed && starting.origin._tag === "Run") {
        yield* complete(starting.origin.run.done, starting.origin.exit)
      }
      yield* open(starting.gate)
    })

  const performShellStart = (starting: Extract<StartingState<A, E>, { readonly kind: "Shell" }>) =>
    Effect.gen(function* () {
      const started = yield* Effect.gen(function* () {
        yield* onBusy
        const fiber = yield* starting.work.pipe(Effect.ensuring(finishShell(starting.id)), Effect.forkChild)
        return {
          id: starting.id,
          cancelled: starting.cancelled,
          ready: starting.ready,
          fiber,
        } satisfies ShellHandle<A, E>
      }).pipe(Effect.exit)

      if (Exit.isFailure(started)) {
        const published = yield* idle.pipe(Effect.exit)
        const committed = yield* commitIdle("Starting", starting.token)
        yield* open(starting.gate)
        if (committed.committed && committed.error !== undefined) yield* Effect.die(committed.error)
        if (Exit.isFailure(published)) yield* Effect.failCause(published.cause)
        return yield* Effect.failCause(started.cause)
      }

      yield* withLock(() => {
        if (current._tag === "Starting" && current.token === starting.token && current.kind === "Shell") {
          current = { _tag: "Shell", shell: started.value }
        }
      })
      yield* open(starting.gate)
      return started.value
    })

  const finish = (finishing: FinishingState<A, E>) =>
    Effect.gen(function* () {
      type FinishAction =
        | { _tag: "Wait"; gate: Deferred.Deferred<void> }
        | { _tag: "Lost"; error: unknown | undefined }
        | { _tag: "Start"; starting: Extract<StartingState<A, E>, { readonly kind: "Run" }>; error: undefined }
        | { _tag: "Idle"; published: Exit.Exit<void, never>; error: unknown | undefined }
      let committed: Exclude<FinishAction, { _tag: "Wait" }>
      while (true) {
        const action = yield* withLockEffect<FinishAction, never, never>(() => {
          if (
            current._tag === "Admitting" &&
            current.previous._tag === "Finishing" &&
            current.previous.token === finishing.token
          ) {
            return Effect.succeed({ _tag: "Wait" as const, gate: current.gate })
          }
          if (current._tag !== "Finishing" || current.token !== finishing.token) {
            return Effect.succeed({ _tag: "Lost" as const, error: undefined as unknown | undefined })
          }
          if (current.next) {
            const starting = {
              _tag: "Starting",
              kind: "Run",
              token: next(),
              gate: Deferred.makeUnsafe<void>(),
              pending: current.next,
              origin: { _tag: "Successor" },
            } satisfies StartingState<A, E>
            current = starting
            return Effect.succeed({ _tag: "Start" as const, starting, error: undefined })
          }
          return idle.pipe(
            Effect.exit,
            Effect.map((published) => {
              current = { _tag: "Idle" }
              try {
                opts?.onRetire?.()
                return { _tag: "Idle" as const, published, error: undefined as unknown | undefined }
              } catch (error) {
                return { _tag: "Idle" as const, published, error }
              }
            }),
          )
        })
        if (action._tag === "Wait") {
          yield* Deferred.await(action.gate)
          continue
        }
        committed = action as Exclude<FinishAction, { _tag: "Wait" }>
        break
      }
      if (finishing.completion) yield* complete(finishing.completion.done, finishing.completion.exit)
      yield* open(finishing.gate)
      if (committed._tag === "Start") yield* performRunStart(committed.starting)
      if (committed._tag === "Idle" && committed.error !== undefined) yield* Effect.die(committed.error)
      if (committed._tag === "Idle" && Exit.isFailure(committed.published)) {
        yield* Effect.failCause(committed.published.cause)
      }
    })

  finishRun = (id, done, exit) =>
    Effect.gen(function* () {
      while (true) {
        const action = yield* withLock(() => {
          if (current._tag === "Admitting" && current.previous._tag === "Running" && current.previous.run.id === id) {
            return { _tag: "Wait" as const, gate: current.gate }
          }
          if (current._tag === "Starting" && current.kind === "Run" && current.pending.id === id) {
            return { _tag: "Wait" as const, gate: current.gate }
          }
          if (current._tag === "Cancelling" && current.previous._tag === "Running" && current.previous.run.id === id) {
            return { _tag: "CancelOwns" as const }
          }
          if (current._tag !== "Running" || current.run.id !== id) return { _tag: "Settle" as const }
          if (current.next) {
            const token = reservation()
            const starting = {
              _tag: "Starting",
              kind: "Run",
              ...token,
              pending: current.next,
              origin: { _tag: "Run", run: current.run, exit },
            } satisfies StartingState<A, E>
            current = starting
            return { _tag: "Start" as const, starting }
          }
          const token = reservation()
          const finishing = {
            _tag: "Finishing",
            ...token,
            completion: { done, exit },
          } satisfies FinishingState<A, E>
          current = finishing
          return { _tag: "Finish" as const, finishing }
        })

        if (action._tag === "Wait") {
          yield* Deferred.await(action.gate)
          continue
        }
        if (action._tag === "Settle") {
          yield* complete(done, exit)
          return
        }
        if (action._tag === "CancelOwns") return
        if (action._tag === "Start") {
          yield* performRunStart(action.starting)
          return
        }
        yield* finish(action.finishing)
        return
      }
    })

  finishShell = (id) =>
    Effect.gen(function* () {
      while (true) {
        const action = yield* withLock(() => {
          if (
            current._tag === "Admitting" &&
            (current.previous._tag === "Shell" || current.previous._tag === "ShellThenRun") &&
            current.previous.shell.id === id
          ) {
            return { _tag: "Wait" as const, gate: current.gate }
          }
          if (current._tag === "Starting" && current.kind === "Shell" && current.id === id) {
            return { _tag: "Wait" as const, gate: current.gate }
          }
          if (
            current._tag === "Cancelling" &&
            (current.previous._tag === "Shell" || current.previous._tag === "ShellThenRun") &&
            current.previous.shell.id === id
          ) {
            return { _tag: "Done" as const }
          }
          if (current._tag === "Shell" && current.shell.id === id) {
            const token = reservation()
            const finishing = { _tag: "Finishing", ...token } satisfies FinishingState<A, E>
            current = finishing
            return { _tag: "Finish" as const, finishing }
          }
          if (current._tag === "ShellThenRun" && current.shell.id === id) {
            const token = reservation()
            const starting = {
              _tag: "Starting",
              kind: "Run",
              ...token,
              pending: current.run,
              origin: { _tag: "Shell" },
            } satisfies StartingState<A, E>
            current = starting
            return { _tag: "Start" as const, starting }
          }
          return { _tag: "Done" as const }
        })

        if (action._tag === "Wait") {
          yield* Deferred.await(action.gate)
          continue
        }
        if (action._tag === "Start") yield* performRunStart(action.starting)
        if (action._tag === "Finish") yield* finish(action.finishing)
        return
      }
    })

  const stopShell = (shell: ShellHandle<A, E>) =>
    Effect.gen(function* () {
      if (shell.ready) yield* shell.ready.await.pipe(Effect.exit, Effect.asVoid)
      yield* Deferred.succeed(shell.cancelled, undefined).pipe(Effect.asVoid)
      yield* Fiber.interrupt(shell.fiber)
    })

  const reserveRun = (work: Effect.Effect<A, E>, acquire: Restore) =>
    withLock(() => {
      if (
        current._tag === "Admitting" ||
        current._tag === "Starting" ||
        current._tag === "Finishing" ||
        current._tag === "Cancelling"
      ) {
        return { _tag: "Wait" as const, gate: current.gate }
      }
      if (current._tag === "Running") return { _tag: "Done" as const, done: current.next?.done ?? current.run.done }
      if (current._tag === "ShellThenRun") return { _tag: "Done" as const, done: current.run.done }
      const pending = {
        id: next(),
        done: Deferred.makeUnsafe<A, E | Cancelled>(),
        work,
      } satisfies PendingHandle<A, E>
      if (current._tag === "Shell") {
        current = { _tag: "ShellThenRun", shell: current.shell, run: pending }
        return { _tag: "Done" as const, done: pending.done }
      }
      const token = reservation()
      const starting = {
        _tag: "Starting",
        kind: "Run",
        ...token,
        pending,
        origin: { _tag: "Initial" },
      } satisfies StartingState<A, E>
      current = starting
      return { _tag: "Start" as const, starting }
    }, acquire)

  const ensureRunning = (work: Effect.Effect<A, E>) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        while (true) {
          const action = yield* reserveRun(work, restore)
          if (action._tag === "Wait") {
            yield* restore(Deferred.await(action.gate))
            continue
          }
          if (action._tag === "Start") yield* performRunStart(action.starting)
          return yield* restore(awaitDone(action._tag === "Start" ? action.starting.pending.done : action.done))
        }
      }),
    )

  const reserveAdmission = (acquire: Restore) =>
    withLock(() => {
      if (current._tag === "Admitting" || current._tag === "Starting" || current._tag === "Cancelling") {
        return { _tag: "Wait" as const, gate: current.gate }
      }
      const token = reservation()
      const admitting = {
        _tag: "Admitting",
        ...token,
        previous: current,
        cancelled: Deferred.makeUnsafe<void>(),
      } satisfies Extract<State<A, E>, { readonly _tag: "Admitting" }>
      current = admitting
      return { _tag: "Reserved" as const, admitting }
    }, acquire)

  const rollbackAdmission = (admitting: Extract<State<A, E>, { readonly _tag: "Admitting" }>) =>
    Effect.gen(function* () {
      yield* withLock(() => {
        if (current._tag === "Admitting" && current.token === admitting.token) current = admitting.previous
      })
      yield* open(admitting.gate)
    })

  const commitAdmission = (
    admitting: Extract<State<A, E>, { readonly _tag: "Admitting" }>,
    work: Effect.Effect<A, E>,
  ) =>
    withLock(() => {
      if (current._tag !== "Admitting" || current.token !== admitting.token) {
        throw new Error("Runner admission reservation was lost")
      }
      const previous = admitting.previous
      if (previous._tag === "Finishing" && previous.next) {
        current = previous
        return { _tag: "Done" as const, done: previous.next.done }
      }
      if (previous._tag === "Running" && previous.next) {
        current = previous
        return { _tag: "Done" as const, done: previous.next.done }
      }
      if (previous._tag === "ShellThenRun") {
        current = previous
        return { _tag: "Done" as const, done: previous.run.done }
      }
      const pending = {
        id: next(),
        done: Deferred.makeUnsafe<A, E | Cancelled>(),
        work,
      } satisfies PendingHandle<A, E>
      if (previous._tag === "Finishing") {
        current = { ...previous, next: pending }
        return { _tag: "Done" as const, done: pending.done }
      }
      if (previous._tag === "Running") {
        current = { ...previous, next: pending }
        return { _tag: "Done" as const, done: pending.done }
      }
      if (previous._tag === "Shell") {
        current = { _tag: "ShellThenRun", shell: previous.shell, run: pending }
        return { _tag: "Done" as const, done: pending.done }
      }
      const starting = {
        _tag: "Starting",
        kind: "Run",
        token: next(),
        gate: Deferred.makeUnsafe<void>(),
        pending,
        origin: { _tag: "Initial" },
      } satisfies StartingState<A, E>
      current = starting
      return { _tag: "Start" as const, starting }
    })

  const submit = <B, E2, R>(
    admission: Effect.Effect<B, E2, R>,
    work: Effect.Effect<A, E>,
  ): Effect.Effect<A, E | E2, R> =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        let admitted: Extract<State<A, E>, { readonly _tag: "Admitting" }>
        while (true) {
          const action = yield* reserveAdmission(restore)
          if (action._tag === "Wait") {
            yield* restore(Deferred.await(action.gate))
            continue
          }
          admitted = action.admitting
          break
        }

        const result = yield* restore(admission)
          .pipe(Effect.raceFirst(Deferred.await(admitted.cancelled).pipe(Effect.andThen(Effect.fail(new Cancelled())))))
          .pipe(Effect.catchTag("RunnerCancelled", (error) => Effect.die(error)))
          .pipe(Effect.exit)
        if (Exit.isFailure(result)) {
          yield* rollbackAdmission(admitted)
          return yield* Effect.failCause(result.cause)
        }

        const action = yield* commitAdmission(admitted, work)
        yield* open(admitted.gate)
        if (action._tag === "Start") yield* performRunStart(action.starting)
        return yield* restore(awaitDone(action._tag === "Start" ? action.starting.pending.done : action.done))
      }),
    )

  const commit = <B, E2, R>(work: Effect.Effect<B, E2, R>): Effect.Effect<B, E2 | Busy, R> =>
    Effect.uninterruptibleMask((restore) =>
      withLockEffect<B, E2 | Busy, R>(
        () => (current._tag === "Idle" ? restore(work) : Effect.fail(new Busy())),
        restore,
      ),
    )

  const awaitShell = (shell: ShellHandle<A, E>) =>
    Effect.gen(function* () {
      const exit = yield* Fiber.await(shell.fiber)
      if (Exit.isSuccess(exit)) return exit.value
      if (
        Cause.hasInterruptsOnly(exit.cause) ||
        ((yield* Deferred.isDone(shell.cancelled)) && Cause.hasInterrupts(exit.cause) && !Cause.hasDies(exit.cause))
      ) {
        if (onInterrupt) return yield* onInterrupt
        return yield* Effect.die(new Cancelled())
      }
      return yield* Effect.failCause(exit.cause)
    })

  const startShell = (work: Effect.Effect<A, E>, ready?: Latch.Latch): Effect.Effect<A, E | Busy> =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        while (true) {
          const action = yield* withLock(() => {
            if (
              current._tag === "Admitting" ||
              current._tag === "Starting" ||
              current._tag === "Finishing" ||
              current._tag === "Cancelling"
            ) {
              return { _tag: "Wait" as const, gate: current.gate }
            }
            if (current._tag !== "Idle") return { _tag: "Busy" as const }
            const token = reservation()
            const starting = {
              _tag: "Starting",
              kind: "Shell",
              ...token,
              id: next(),
              work,
              cancelled: Deferred.makeUnsafe<void>(),
              ready,
            } satisfies StartingState<A, E>
            current = starting
            return { _tag: "Start" as const, starting }
          }, restore)
          if (action._tag === "Wait") {
            yield* restore(Deferred.await(action.gate))
            continue
          }
          if (action._tag === "Busy") return yield* new Busy()
          const shell = yield* performShellStart(action.starting)
          return yield* restore(awaitShell(shell))
        }
      }),
    )

  const cancel = Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      let cancelling: Extract<State<A, E>, { readonly _tag: "Cancelling" }> | undefined
      while (true) {
        const action = yield* withLock(() => {
          if (current._tag === "Admitting") {
            return { _tag: "CancelAdmission" as const, cancelled: current.cancelled, gate: current.gate }
          }
          if (current._tag === "Starting" || current._tag === "Finishing" || current._tag === "Cancelling") {
            return { _tag: "Wait" as const, gate: current.gate }
          }
          if (current._tag === "Idle") return { _tag: "Idle" as const }
          const token = reservation()
          const nextState = { _tag: "Cancelling", ...token, previous: current } satisfies Extract<
            State<A, E>,
            { readonly _tag: "Cancelling" }
          >
          current = nextState
          return { _tag: "Cancel" as const, cancelling: nextState }
        }, restore)
        if (action._tag === "CancelAdmission") {
          yield* Deferred.succeed(action.cancelled, undefined).pipe(Effect.asVoid)
          yield* restore(Deferred.await(action.gate))
          continue
        }
        if (action._tag === "Wait") {
          yield* restore(Deferred.await(action.gate))
          continue
        }
        if (action._tag === "Idle") return
        cancelling = action.cancelling
        break
      }

      const owned = cancelling!
      const cleanup = yield* Effect.gen(function* () {
        if (owned.previous._tag === "Running") {
          yield* Fiber.interrupt(owned.previous.run.fiber)
          return
        }
        yield* stopShell(owned.previous.shell)
      }).pipe(Effect.exit)
      const published = yield* idle.pipe(Effect.exit)
      const committed = yield* commitIdle("Cancelling", owned.token)
      if (owned.previous._tag === "Running") {
        yield* Deferred.fail(owned.previous.run.done, new Cancelled()).pipe(Effect.asVoid)
        if (owned.previous.next) {
          yield* Deferred.fail(owned.previous.next.done, new Cancelled()).pipe(Effect.asVoid)
        }
      }
      if (owned.previous._tag === "ShellThenRun") {
        yield* Deferred.fail(owned.previous.run.done, new Cancelled()).pipe(Effect.asVoid)
      }
      yield* open(owned.gate)
      if (committed.committed && committed.error !== undefined) yield* Effect.die(committed.error)
      if (Exit.isFailure(cleanup)) yield* Effect.failCause(cleanup.cause)
      if (Exit.isFailure(published)) yield* Effect.failCause(published.cause)
    }),
  )

  return {
    get state() {
      return current
    },
    get busy() {
      return current._tag !== "Idle"
    },
    ensureRunning,
    submit,
    commit,
    startShell,
    cancel,
  }
}

export * as Runner from "./runner"
