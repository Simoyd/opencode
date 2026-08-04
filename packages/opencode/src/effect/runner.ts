import { Cause, Deferred, Effect, Exit, Fiber, Schema, Scope, SynchronizedRef } from "effect"

export type StopMode = "Cancel" | "Close" | "Remove"

export interface StopLease {
  readonly owner: boolean
  readonly satisfied: boolean
  readonly stop?: StoppingHandle
}

export interface Runner<A, E = never> {
  readonly successorPending: Effect.Effect<boolean>
  readonly ensureRunning: (work: Effect.Effect<A, E>, onInterrupt?: Effect.Effect<A, E>) => Effect.Effect<A, E | Busy>
  readonly submit: <B, E2, R>(
    admission: Effect.Effect<B, E2, R>,
    work: Effect.Effect<A, E>,
    onInterrupt?: Effect.Effect<A, E>,
  ) => Effect.Effect<A, E | E2 | Busy, R>
  readonly submitManual: <B, E2, R>(
    admission: Effect.Effect<B, E2, R>,
    work: Effect.Effect<A, E>,
    onInterrupt?: Effect.Effect<A, E>,
  ) => Effect.Effect<A, E | E2 | Busy, R>
  readonly submitAdmitted: <B, E2, R>(
    admission: Effect.Effect<B, E2, R>,
    work: Effect.Effect<A, E>,
    onInterrupt?: Effect.Effect<A, E>,
  ) => Effect.Effect<B, E2 | Busy, R>
  readonly submitManualAdmitted: <B, E2, R>(
    admission: Effect.Effect<B, E2, R>,
    work: Effect.Effect<A, E>,
    onInterrupt?: Effect.Effect<A, E>,
  ) => Effect.Effect<B, E2 | Busy, R>
  readonly commit: <B, E2, R>(work: Effect.Effect<B, E2, R>) => Effect.Effect<B, E2 | Busy, R>
  readonly startShell: (work: Effect.Effect<A, E>, onInterrupt?: Effect.Effect<A, E>) => Effect.Effect<A, E | Busy>
  readonly quiesce: (mode: StopMode) => Effect.Effect<StopLease>
  readonly settleStop: (lease: StopLease) => Effect.Effect<void>
  readonly commitStop: (lease: StopLease, exit: Exit.Exit<void, unknown>) => Effect.Effect<void>
  readonly awaitStop: (lease: StopLease) => Effect.Effect<void>
}

export class Cancelled extends Schema.TaggedErrorClass<Cancelled>()("RunnerCancelled", {}) {}
export class Busy extends Schema.TaggedErrorClass<Busy>()("RunnerBusy", {}) {}

interface RunHandle<A, E> {
  readonly id: number
  readonly kind: "Run" | "Shell"
  readonly done: Deferred.Deferred<A, E | Cancelled>
  readonly fiber: Fiber.Fiber<A, E>
  readonly onInterrupt?: Effect.Effect<A, E>
  readonly cancel: Effect.Effect<void>
}

interface AdmissionHandle {
  readonly id: number
  readonly stop: Effect.Effect<void>
  readonly cancel: Effect.Effect<void>
}

interface Successor<A, E> {
  readonly id: number
  readonly done: Deferred.Deferred<A, E | Cancelled>
  readonly work: Effect.Effect<A, E>
  readonly onInterrupt?: Effect.Effect<A, E>
  readonly manual: boolean
  readonly accepted: boolean
  readonly admissions: ReadonlyArray<AdmissionHandle>
}

interface ExclusiveHandle {
  readonly id: number
  readonly stop: Effect.Effect<void>
  readonly cancel: Effect.Effect<void>
}

export interface StoppingHandle {
  readonly quiesced: Deferred.Deferred<void>
  readonly exit: Deferred.Deferred<void, unknown>
  readonly mode: StopMode
  readonly cancel: ReadonlyArray<Effect.Effect<void>>
}

export type State<A, E> =
  | { readonly _tag: "Idle" }
  | { readonly _tag: "Active"; readonly current?: RunHandle<A, E>; readonly successor?: Successor<A, E> }
  | { readonly _tag: "Exclusive"; readonly operation: ExclusiveHandle }
  | { readonly _tag: "Stopping"; readonly stop: StoppingHandle }
  | { readonly _tag: "Closed" }

type AdmissionResult<A, E> =
  | { readonly _tag: "Closed" }
  | { readonly _tag: "Busy" }
  | { readonly _tag: "Accepted"; readonly successor: Successor<A, E> }

type EnsureResult<A, E> =
  | { readonly _tag: "Closed" }
  | { readonly _tag: "Busy" }
  | { readonly _tag: "Current"; readonly run: RunHandle<A, E> }
  | { readonly _tag: "Successor"; readonly successor: Successor<A, E> }

type QuiesceResult =
  | { readonly _tag: "Satisfied" }
  | { readonly _tag: "Joined"; readonly stop: StoppingHandle }
  | { readonly _tag: "Owner"; readonly stop: StoppingHandle; readonly stops: readonly Effect.Effect<void>[] }

export const make = <A, E = never>(
  scope: Scope.Scope,
  opts?: {
    onIdle?: Effect.Effect<void>
  },
): Runner<A, E> => {
  const ref = SynchronizedRef.makeUnsafe<State<A, E>>({ _tag: "Idle" })
  const idle = opts?.onIdle ?? Effect.void
  let ids = 0

  const next = () => {
    ids += 1
    return ids
  }

  const complete = <B, E2>(done: Deferred.Deferred<B, E2 | Cancelled>, exit: Exit.Exit<B, E2>) =>
    Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)
      ? Deferred.fail(done, new Cancelled()).pipe(Effect.asVoid)
      : Deferred.done(done, exit).pipe(Effect.asVoid)

  const awaitDone = (
    done: Deferred.Deferred<A, E | Cancelled>,
    onInterrupt?: Effect.Effect<A, E>,
  ) => Deferred.await(done).pipe(Effect.catchTag("RunnerCancelled", (error) => onInterrupt ?? Effect.die(error)))

  const awaitCommit = <B, E2>(done: Deferred.Deferred<B, E2 | Cancelled>) =>
    Deferred.await(done).pipe(Effect.catchTag("RunnerCancelled", () => Effect.interrupt))

  let advance: Effect.Effect<void>

  const reconcile = (state: State<A, E>): Effect.Effect<readonly [Effect.Effect<void>, State<A, E>]> =>
    Effect.gen(function* () {
      if (state._tag !== "Active") return [Effect.void, state] as const
      if (state.current) return [Effect.void, state] as const

      const successor = state.successor
      if (successor?.admissions.length) return [Effect.void, state] as const
      if (successor?.accepted) {
        const gate = yield* Deferred.make<boolean>()
        const handleID = next()
        let handle: RunHandle<A, E>
        const fiber = yield* Deferred.await(gate).pipe(
          Effect.flatMap((accepted) =>
            accepted
              ? successor.work.pipe(Effect.onExit((exit) => finishCurrent(handle, exit)))
              : Effect.interrupt,
          ),
          Effect.forkIn(scope, { startImmediately: true }),
        )
        handle = {
          id: handleID,
          kind: "Run",
          done: successor.done,
          fiber,
          onInterrupt: successor.onInterrupt,
          cancel: Deferred.fail(successor.done, new Cancelled()).pipe(Effect.asVoid),
        }
        return [
          Deferred.succeed(gate, true).pipe(Effect.asVoid),
          { _tag: "Active", current: handle } as State<A, E>,
        ] as const
      }

      // The current handle remains the state owner until idle publication and
      // abandoned-successor settlement have both completed under this lock.
      yield* idle
      if (successor) yield* Deferred.fail(successor.done, new Cancelled()).pipe(Effect.asVoid)
      return [Effect.void, { _tag: "Idle" } as State<A, E>] as const
    })

  function finishCurrent(handle: RunHandle<A, E>, exit: Exit.Exit<A, E>): Effect.Effect<void> {
    return SynchronizedRef.modifyEffect(ref, (state) => {
      if (state._tag !== "Active" || state.current?.id !== handle.id) {
        return Effect.succeed([Effect.void, state] as const)
      }
      return reconcile({ ...state, current: undefined }).pipe(
        Effect.map(([bookkeeping, nextState]) => [complete(handle.done, exit).pipe(Effect.andThen(bookkeeping)), nextState] as const),
      )
    }).pipe(Effect.flatten)
  }

  advance = Effect.suspend(() => SynchronizedRef.modifyEffect(ref, reconcile).pipe(Effect.flatten))

  const finishAdmission = <B, E2>(
    successorID: number,
    admissionID: number,
    manual: boolean,
    work: Effect.Effect<A, E>,
    onInterrupt: Effect.Effect<A, E> | undefined,
    done: Deferred.Deferred<B, E2 | Cancelled>,
    exit: Exit.Exit<B, E2>,
  ): Effect.Effect<void> =>
    SynchronizedRef.modifyEffect(ref, (state) => {
        if (state._tag !== "Active" || state.successor?.id !== successorID) {
          return Effect.succeed([Effect.void, state] as const)
        }
        const successor = state.successor
        if (!successor.admissions.some((item) => item.id === admissionID)) {
          return Effect.succeed([Effect.void, state] as const)
        }
        const admissions = successor.admissions.filter((item) => item.id !== admissionID)
        const updated = Exit.isFailure(exit)
          ? { ...successor, admissions }
          : {
              ...successor,
              admissions,
              accepted: true,
              manual: successor.manual || manual,
              work: successor.manual && !manual ? successor.work : work,
              onInterrupt: successor.manual && !manual ? successor.onInterrupt : onInterrupt,
            }
        return reconcile({ ...state, successor: updated }).pipe(
          Effect.map(([bookkeeping, nextState]) => [bookkeeping.pipe(Effect.andThen(complete(done, exit))), nextState] as const),
        )
      }).pipe(Effect.flatten)

  function admit<B, E2, R>(
    admission: Effect.Effect<B, E2, R>,
    work: Effect.Effect<A, E>,
    manual: boolean,
    completion: "Admission",
    onInterrupt?: Effect.Effect<A, E>,
  ): Effect.Effect<B, E2 | Busy, R>
  function admit<B, E2, R>(
    admission: Effect.Effect<B, E2, R>,
    work: Effect.Effect<A, E>,
    manual: boolean,
    completion: "Run",
    onInterrupt?: Effect.Effect<A, E>,
  ): Effect.Effect<A, E | E2 | Busy, R>
  function admit<B, E2, R>(
    admission: Effect.Effect<B, E2, R>,
    work: Effect.Effect<A, E>,
    manual: boolean,
    completion: "Admission" | "Run",
    onInterrupt?: Effect.Effect<A, E>,
  ): Effect.Effect<A | B, E | E2 | Busy, R> {
    return Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const proposed = {
          id: next(),
          done: yield* Deferred.make<A, E | Cancelled>(),
          work,
          onInterrupt,
          manual: false,
          accepted: false,
          admissions: [],
        } satisfies Successor<A, E>
        const admissionID = next()
        const admissionDone = yield* Deferred.make<B, E2 | Cancelled>()
        const gate = yield* Deferred.make<boolean>()
        let successorID = proposed.id
        const fiber = yield* Deferred.await(gate).pipe(
          Effect.flatMap((accepted) =>
            accepted
              ? admission.pipe(
                  Effect.onExit((exit) =>
                    finishAdmission(successorID, admissionID, manual, work, onInterrupt, admissionDone, exit),
                  ),
                )
              : Effect.interrupt,
          ),
          Effect.forkIn(scope, { startImmediately: true }),
        )
        const operation = {
          id: admissionID,
          stop: Fiber.interrupt(fiber).pipe(Effect.asVoid),
          cancel: Deferred.fail(admissionDone, new Cancelled()).pipe(Effect.asVoid),
        }
        const result = yield* SynchronizedRef.modify(ref, (state): readonly [AdmissionResult<A, E>, State<A, E>] => {
          if (state._tag === "Closed") return [{ _tag: "Closed" as const }, state] as const
          if (state._tag === "Exclusive" || state._tag === "Stopping") {
            return [{ _tag: "Busy" as const }, state] as const
          }
          const successor = state._tag === "Idle" ? proposed : state.successor ?? proposed
          successorID = successor.id
          const nextSuccessor = { ...successor, admissions: [...successor.admissions, operation] }
          return [
            {
              _tag: "Accepted" as const,
              successor: nextSuccessor,
            },
            state._tag === "Idle"
              ? { _tag: "Active", successor: nextSuccessor }
              : { ...state, successor: nextSuccessor },
          ] as const
        })
        if (result._tag !== "Accepted") {
          yield* Deferred.succeed(gate, false).pipe(Effect.asVoid)
          yield* Fiber.interrupt(fiber).pipe(Effect.asVoid)
          if (result._tag === "Busy") return yield* new Busy()
          return yield* new Busy()
        }
        yield* Deferred.succeed(gate, true).pipe(Effect.asVoid)
        const admitted = yield* restore(
          Deferred.await(admissionDone).pipe(
            Effect.map((value) => ({ admitted: true as const, value })),
            Effect.catchTag("RunnerCancelled", () => Effect.succeed({ admitted: false as const })),
          ),
        )
        if (!admitted.admitted) {
          if (completion === "Admission") return yield* restore(Effect.interrupt)
          return yield* restore(onInterrupt ?? Effect.interrupt)
        }
        if (completion === "Admission") return admitted.value
        return yield* restore(awaitDone(result.successor.done, onInterrupt))
      }),
    )
  }

  const ensureRunning = (work: Effect.Effect<A, E>, onInterrupt?: Effect.Effect<A, E>) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const proposed = {
          id: next(),
          done: yield* Deferred.make<A, E | Cancelled>(),
          work,
          onInterrupt,
          manual: false,
          accepted: true,
          admissions: [],
        } satisfies Successor<A, E>
        const result = yield* SynchronizedRef.modifyEffect(ref, (state): Effect.Effect<readonly [EnsureResult<A, E>, State<A, E>]> =>
          Effect.gen(function* () {
            if (state._tag === "Closed") return [{ _tag: "Closed" as const }, state] as const
            if (state._tag === "Exclusive" || state._tag === "Stopping") {
              return [{ _tag: "Busy" as const }, state] as const
            }
            if (
              state._tag === "Active" &&
              state.current?.kind === "Run" &&
              !(yield* Deferred.isDone(state.current.done))
            ) {
              return [{ _tag: "Current" as const, run: state.current }, state] as const
            }
            const successor = state._tag === "Idle"
              ? proposed
              : state.successor
                ? state.successor.accepted
                  ? state.successor
                  : { ...state.successor, accepted: true, work, onInterrupt }
                : proposed
            return [
              { _tag: "Successor" as const, successor },
              state._tag === "Idle"
                ? { _tag: "Active", successor }
                : { ...state, successor },
            ] as const
          }),
        )
        if (result._tag === "Closed") return yield* new Busy()
        if (result._tag === "Busy") return yield* new Busy()
        if (result._tag === "Current") return yield* restore(awaitDone(result.run.done, result.run.onInterrupt))
        yield* advance
        return yield* restore(awaitDone(result.successor.done, result.successor.onInterrupt))
      }),
    )

  const finishExclusive = <B, E2>(
    id: number,
    done: Deferred.Deferred<B, E2 | Cancelled>,
    exit: Exit.Exit<B, E2>,
  ) =>
    SynchronizedRef.modifyEffect(ref, (state) => {
        if (state._tag !== "Exclusive" || state.operation.id !== id) {
          return Effect.succeed([Effect.void, state] as const)
        }
        return idle.pipe(
          Effect.as([complete(done, exit), { _tag: "Idle" } as State<A, E>] as const),
        )
      }).pipe(Effect.flatten)

  const commit = <B, E2, R>(work: Effect.Effect<B, E2, R>): Effect.Effect<B, E2 | Busy, R> =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const id = next()
        const done = yield* Deferred.make<B, E2 | Cancelled>()
        const gate = yield* Deferred.make<boolean>()
        const fiber = yield* Deferred.await(gate).pipe(
          Effect.flatMap((accepted) =>
            accepted ? work.pipe(Effect.onExit((exit) => finishExclusive(id, done, exit))) : Effect.interrupt,
          ),
          Effect.forkIn(scope, { startImmediately: true }),
        )
        const accepted = yield* SynchronizedRef.modify(ref, (state) => {
          if (state._tag === "Closed") return ["Closed" as const, state] as const
          if (state._tag !== "Idle") return ["Busy" as const, state] as const
          return [
            "Accepted" as const,
            {
              _tag: "Exclusive",
              operation: {
                id,
                stop: Fiber.interrupt(fiber).pipe(Effect.asVoid),
                cancel: Deferred.fail(done, new Cancelled()).pipe(Effect.asVoid),
              },
            },
          ] as const
        })
        if (accepted !== "Accepted") {
          yield* Deferred.succeed(gate, false).pipe(Effect.asVoid)
          yield* Fiber.interrupt(fiber).pipe(Effect.asVoid)
          if (accepted === "Busy") return yield* new Busy()
          return yield* new Busy()
        }
        yield* Deferred.succeed(gate, true).pipe(Effect.asVoid)
        return yield* restore(awaitCommit(done))
      }),
    )

  const startShell = (work: Effect.Effect<A, E>, onInterrupt?: Effect.Effect<A, E>): Effect.Effect<A, E | Busy> =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const gate = yield* Deferred.make<boolean>()
        const done = yield* Deferred.make<A, E | Cancelled>()
        const id = next()
        let handle: RunHandle<A, E>
        const fiber = yield* Deferred.await(gate).pipe(
          Effect.flatMap((accepted) =>
            accepted ? work.pipe(Effect.onExit((exit) => finishCurrent(handle, exit))) : Effect.interrupt,
          ),
          Effect.forkIn(scope, { startImmediately: true }),
        )
        handle = {
          id,
          kind: "Shell",
          done,
          fiber,
          onInterrupt,
          cancel: Deferred.fail(done, new Cancelled()).pipe(Effect.asVoid),
        }
        const accepted = yield* SynchronizedRef.modify(ref, (state) => {
          if (state._tag === "Closed") return ["Closed" as const, state] as const
          if (state._tag !== "Idle") return ["Busy" as const, state] as const
          return ["Accepted" as const, { _tag: "Active", current: handle }] as const
        })
        if (accepted !== "Accepted") {
          yield* Deferred.succeed(gate, false).pipe(Effect.asVoid)
          yield* Fiber.interrupt(fiber).pipe(Effect.asVoid)
          if (accepted === "Busy") return yield* new Busy()
          return yield* new Busy()
        }
        yield* Deferred.succeed(gate, true).pipe(Effect.asVoid)
        return yield* restore(awaitDone(done, onInterrupt))
      }),
    )

  const quiesce = (mode: StopMode): Effect.Effect<StopLease> =>
    Effect.uninterruptible(
      Effect.suspend(() =>
        Effect.gen(function* () {
          const quiesced = yield* Deferred.make<void>()
          const exit = yield* Deferred.make<void, unknown>()
          const result = yield* SynchronizedRef.modify(ref, (state): readonly [QuiesceResult, State<A, E>] => {
            if (state._tag === "Closed" && mode !== "Remove") {
              return [{ _tag: "Satisfied" as const }, state] as const
            }
            if (state._tag === "Stopping") {
              return [{ _tag: "Joined" as const, stop: state.stop }, state] as const
            }

            const stops: Effect.Effect<void>[] = []
            const cancel: Effect.Effect<void>[] = []
            if (state._tag === "Exclusive") {
              stops.push(state.operation.stop)
              cancel.push(state.operation.cancel)
            }
            if (state._tag === "Active") {
              if (state.current) {
                stops.push(Fiber.interrupt(state.current.fiber).pipe(Effect.asVoid))
                cancel.push(state.current.cancel)
              }
              if (state.successor) {
                cancel.push(Deferred.fail(state.successor.done, new Cancelled()).pipe(Effect.asVoid))
                stops.push(...state.successor.admissions.map((item) => item.stop))
                cancel.push(...state.successor.admissions.map((item) => item.cancel))
              }
            }
            const stop = { quiesced, exit, mode, cancel } satisfies StoppingHandle
            return [
              { _tag: "Owner" as const, stop, stops },
              { _tag: "Stopping", stop },
            ] as const
          })
          if (result._tag === "Satisfied") return { owner: false, satisfied: true }
          if (result._tag === "Joined") {
            yield* Deferred.await(result.stop.quiesced)
            return { owner: false, satisfied: false, stop: result.stop }
          }
          const stopped = yield* Effect.all(result.stops, { concurrency: "unbounded", discard: true }).pipe(Effect.exit)
          yield* Deferred.succeed(result.stop.quiesced, undefined).pipe(Effect.asVoid)
          if (Exit.isFailure(stopped)) {
            yield* Effect.all(result.stop.cancel, { concurrency: "unbounded", discard: true })
              .pipe(Effect.andThen(idle), Effect.exit)
            yield* SynchronizedRef.modifyEffect(ref, (state) => {
              if (state._tag !== "Stopping" || state.stop.exit !== result.stop.exit) {
                return Effect.succeed([undefined, state] as const)
              }
              const nextState: State<A, E> = state.stop.mode === "Cancel" ? { _tag: "Idle" } : { _tag: "Closed" }
              return Deferred.done(state.stop.exit, stopped).pipe(Effect.as([undefined, nextState] as const))
            })
            return yield* Effect.failCause(stopped.cause)
          }
          return { owner: true, satisfied: false, stop: result.stop }
        }),
      ),
    )

  const settleStop = (lease: StopLease) => {
    if (!lease.owner || !lease.stop) return Effect.void
    return Effect.all(lease.stop.cancel, { concurrency: "unbounded", discard: true }).pipe(Effect.andThen(idle))
  }

  const commitStop = (lease: StopLease, exit: Exit.Exit<void, unknown>) =>
    SynchronizedRef.modifyEffect(ref, (state) => {
      if (!lease.stop || state._tag !== "Stopping" || state.stop.exit !== lease.stop.exit) {
        return Effect.succeed([undefined, state] as const)
      }
      const nextState: State<A, E> = state.stop.mode === "Cancel" ? { _tag: "Idle" } : { _tag: "Closed" }
      return Deferred.done(state.stop.exit, exit).pipe(
        Effect.as([undefined, nextState] as const),
      )
    })

  const awaitStop = (lease: StopLease) => {
    if (lease.satisfied || !lease.stop) return Effect.void
    return Deferred.await(lease.stop.exit).pipe(Effect.orDie)
  }

  const successorPending = SynchronizedRef.modify(ref, (state) => [
    state._tag === "Active" && state.successor !== undefined,
    state,
  ] as const)

  return {
    successorPending,
    ensureRunning,
    submit: (admission, work, onInterrupt) => admit(admission, work, false, "Run", onInterrupt),
    submitManual: (admission, work, onInterrupt) => admit(admission, work, true, "Run", onInterrupt),
    submitAdmitted: (admission, work, onInterrupt) => admit(admission, work, false, "Admission", onInterrupt),
    submitManualAdmitted: (admission, work, onInterrupt) => admit(admission, work, true, "Admission", onInterrupt),
    commit,
    startShell,
    quiesce,
    settleStop,
    commitStop,
    awaitStop,
  }
}

export * as Runner from "./runner"
