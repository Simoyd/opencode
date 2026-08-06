import { describe, expect } from "bun:test"
import { Cause, Deferred, Effect, Exit, Fiber, Ref, Scope } from "effect"
import { Runner } from "@/effect/runner"
import { it } from "../lib/effect"

describe("Runner", () => {
  it.live(
    "ensureRunning shares one active run",
    Effect.gen(function* () {
      const scope = yield* Scope.Scope
      const runner = Runner.make<string>(scope)
      const release = yield* Deferred.make<void>()
      const calls = yield* Ref.make(0)
      const work = (result: string) =>
        Ref.update(calls, (value) => value + 1).pipe(Effect.andThen(Deferred.await(release)), Effect.as(result))
      const both = yield* Effect.all([runner.ensureRunning(work("first")), runner.ensureRunning(work("second"))], {
        concurrency: "unbounded",
      }).pipe(Effect.forkChild)
      while ((yield* Ref.get(calls)) === 0) yield* Effect.yieldNow
      yield* Deferred.succeed(release, undefined)

      const [first, second] = yield* Fiber.join(both)
      expect(first).toBe(second)
      expect(yield* Ref.get(calls)).toBe(1)
    }),
  )

  it.live(
    "ensureRunning propagates failure and permits a successor",
    Effect.gen(function* () {
      const scope = yield* Scope.Scope
      const runner = Runner.make<string, string>(scope)
      expect(Exit.isFailure(yield* runner.ensureRunning(Effect.fail("boom")).pipe(Effect.exit))).toBe(true)
      expect(yield* runner.ensureRunning(Effect.succeed("next"))).toBe("next")
    }),
  )

  it.live(
    "submit admits a successor behind the active run",
    Effect.gen(function* () {
      const scope = yield* Scope.Scope
      const runner = Runner.make<string>(scope)
      const started = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const admitted = yield* Deferred.make<void>()
      const current = yield* runner
        .ensureRunning(
          Deferred.succeed(started, undefined).pipe(Effect.andThen(Deferred.await(release)), Effect.as("current")),
        )
        .pipe(Effect.forkChild)
      yield* Deferred.await(started)

      const successor = yield* runner
        .submit(Deferred.succeed(admitted, undefined), Effect.succeed("successor"))
        .pipe(Effect.forkChild)
      yield* Deferred.await(admitted)
      expect(yield* runner.successorPending).toBe(true)

      yield* Deferred.succeed(release, undefined)
      expect(yield* Fiber.join(current)).toBe("current")
      expect(yield* Fiber.join(successor)).toBe("successor")
      expect(yield* runner.successorPending).toBe(false)
    }),
  )

  it.live(
    "submitAdmitted returns admission while the successor completes later",
    Effect.gen(function* () {
      const scope = yield* Scope.Scope
      const runner = Runner.make<string>(scope)
      const release = yield* Deferred.make<void>()
      const completed = yield* Deferred.make<void>()
      expect(
        yield* runner.submitAdmitted(
          Effect.succeed("accepted"),
          Deferred.await(release).pipe(Effect.as("done"), Effect.ensuring(Deferred.succeed(completed, undefined))),
        ),
      ).toBe("accepted")
      expect(yield* runner.successorPending).toBe(false)
      yield* Deferred.succeed(release, undefined)
      yield* Deferred.await(completed)
      expect(yield* runner.ensureRunning(Effect.succeed("next"))).toBe("next")
    }),
  )

  it.live(
    "submitManualAdmitted starts one successor after the active run settles",
    Effect.gen(function* () {
      const scope = yield* Scope.Scope
      const runner = Runner.make<string>(scope)
      const currentStarted = yield* Deferred.make<void>()
      const releaseCurrent = yield* Deferred.make<void>()
      const successorStarted = yield* Deferred.make<void>()
      const successorCalls = yield* Ref.make(0)
      const current = yield* runner
        .ensureRunning(
          Deferred.succeed(currentStarted, undefined).pipe(
            Effect.andThen(Deferred.await(releaseCurrent)),
            Effect.as("current"),
          ),
        )
        .pipe(Effect.forkChild)
      yield* Deferred.await(currentStarted)

      expect(
        yield* runner.submitManualAdmitted(
          Effect.succeed("accepted"),
          Ref.update(successorCalls, (value) => value + 1).pipe(
            Effect.andThen(Deferred.succeed(successorStarted, undefined)),
            Effect.as("successor"),
          ),
        ),
      ).toBe("accepted")
      expect(yield* runner.successorPending).toBe(true)
      expect(yield* Ref.get(successorCalls)).toBe(0)

      yield* Deferred.succeed(releaseCurrent, undefined)
      expect(yield* Fiber.join(current)).toBe("current")
      yield* Deferred.await(successorStarted)
      expect(yield* Ref.get(successorCalls)).toBe(1)
      expect(yield* runner.successorPending).toBe(false)
    }),
  )

  for (const boundary of ["pre-final-read", "post-final-read"] as const) {
    it.live(
      `${boundary} admissions share one coalesced successor result`,
      Effect.gen(function* () {
        const scope = yield* Scope.Scope
        const runner = Runner.make<string>(scope)
        const activeStarted = yield* Deferred.make<void>()
        const publishFinalRead = yield* Deferred.make<void>()
        const finalReadPublished = yield* Deferred.make<void>()
        const releaseActive = yield* Deferred.make<void>()
        const successorCalls = yield* Ref.make(0)
        const active = yield* runner
          .ensureRunning(
            Deferred.succeed(activeStarted, undefined).pipe(
              Effect.andThen(Deferred.await(publishFinalRead)),
              Effect.andThen(Deferred.succeed(finalReadPublished, undefined)),
              Effect.andThen(Deferred.await(releaseActive)),
              Effect.as("active"),
            ),
          )
          .pipe(Effect.forkChild)
        yield* Deferred.await(activeStarted)

        if (boundary === "post-final-read") {
          yield* Deferred.succeed(publishFinalRead, undefined)
          yield* Deferred.await(finalReadPublished)
        }

        const admitted = yield* Effect.all([Deferred.make<void>(), Deferred.make<void>(), Deferred.make<void>()])
        const submissions = yield* Effect.all(
          admitted.map((gate, index) =>
            runner
              .submit(
                Deferred.succeed(gate, undefined),
                Ref.update(successorCalls, (value) => value + 1).pipe(Effect.as(`successor-${index}`)),
              )
              .pipe(Effect.forkChild),
          ),
        )
        yield* Effect.all(admitted.map(Deferred.await), { discard: true })
        expect(yield* runner.successorPending).toBe(true)

        if (boundary === "pre-final-read") {
          expect(yield* Deferred.isDone(finalReadPublished)).toBe(false)
          yield* Deferred.succeed(publishFinalRead, undefined)
          yield* Deferred.await(finalReadPublished)
        }
        yield* Deferred.succeed(releaseActive, undefined)

        expect(yield* Fiber.join(active)).toBe("active")
        const results = yield* Effect.all(submissions.map(Fiber.join))
        expect(new Set(results).size).toBe(1)
        expect(yield* Ref.get(successorCalls)).toBe(1)
        expect(yield* runner.successorPending).toBe(false)
      }),
    )
  }

  it.live(
    "serializes idle publication before admitting new work",
    Effect.gen(function* () {
      const scope = yield* Scope.Scope
      const idleEntered = yield* Deferred.make<void>()
      const releaseIdle = yield* Deferred.make<void>()
      const idleCalls = yield* Ref.make(0)
      const runner = Runner.make<string>(scope, {
        onIdle: Ref.update(idleCalls, (value) => value + 1).pipe(
          Effect.andThen(Deferred.succeed(idleEntered, undefined)),
          Effect.andThen(Deferred.await(releaseIdle)),
        ),
      })
      const activeStarted = yield* Deferred.make<void>()
      const releaseActive = yield* Deferred.make<void>()
      const active = yield* runner
        .ensureRunning(
          Deferred.succeed(activeStarted, undefined).pipe(
            Effect.andThen(Deferred.await(releaseActive)),
            Effect.as("active"),
          ),
        )
        .pipe(Effect.forkChild)
      yield* Deferred.await(activeStarted)
      yield* Deferred.succeed(releaseActive, undefined)
      yield* Deferred.await(idleEntered)

      const nextStarted = yield* Deferred.make<void>()
      const next = yield* runner
        .ensureRunning(Deferred.succeed(nextStarted, undefined).pipe(Effect.as("next")))
        .pipe(Effect.forkChild)
      yield* Effect.yieldNow
      expect(yield* Deferred.isDone(nextStarted)).toBe(false)

      yield* Deferred.succeed(releaseIdle, undefined)
      expect(yield* Fiber.join(active)).toBe("active")
      yield* Deferred.await(nextStarted)
      expect(yield* Fiber.join(next)).toBe("next")
      expect(yield* Ref.get(idleCalls)).toBe(2)
    }),
  )

  it.live(
    "shares Busy publication failure without starting provider work",
    Effect.gen(function* () {
      const scope = yield* Scope.Scope
      const runner = Runner.make<string, string>(scope)
      const busyEntered = yield* Deferred.make<void>()
      const failBusy = yield* Deferred.make<void>()
      const providerCalls = yield* Ref.make(0)
      const work = Deferred.succeed(busyEntered, undefined).pipe(
        Effect.andThen(Deferred.await(failBusy)),
        Effect.andThen(Effect.fail("busy-publication-failed")),
        Effect.andThen(Ref.update(providerCalls, (value) => value + 1)),
        Effect.as("provider"),
      )
      const first = yield* runner.ensureRunning(work).pipe(Effect.exit, Effect.forkChild)
      yield* Deferred.await(busyEntered)
      const secondEntered = yield* Deferred.make<void>()
      const second = yield* Deferred.succeed(secondEntered, undefined).pipe(
        Effect.andThen(runner.ensureRunning(Effect.succeed("must-not-start"))),
        Effect.exit,
        Effect.forkChild,
      )
      yield* Deferred.await(secondEntered)
      yield* Effect.yieldNow
      yield* Deferred.succeed(failBusy, undefined)

      const [firstExit, secondExit] = yield* Effect.all([Fiber.join(first), Fiber.join(second)])
      expect(firstExit).toEqual(secondExit)
      expect(Exit.isFailure(firstExit)).toBe(true)
      expect(yield* Ref.get(providerCalls)).toBe(0)
      expect(yield* runner.ensureRunning(Effect.succeed("recovered"))).toBe("recovered")
    }),
  )

  it.live(
    "Cancel settles active and pending successor waiters",
    Effect.gen(function* () {
      const scope = yield* Scope.Scope
      const runner = Runner.make<string>(scope)
      const activeStarted = yield* Deferred.make<void>()
      const active = yield* runner
        .ensureRunning(
          Deferred.succeed(activeStarted, undefined).pipe(Effect.andThen(Effect.never), Effect.as("active")),
          Effect.succeed("active-cancelled"),
        )
        .pipe(Effect.forkChild)
      yield* Deferred.await(activeStarted)
      const admitted = yield* Deferred.make<void>()
      const successor = yield* runner
        .submit(
          Deferred.succeed(admitted, undefined),
          Effect.never.pipe(Effect.as("successor")),
          Effect.succeed("successor-cancelled"),
        )
        .pipe(Effect.forkChild)
      yield* Deferred.await(admitted)
      expect(yield* runner.successorPending).toBe(true)

      const lease = yield* runner.quiesce("Cancel")
      yield* runner.settleStop(lease)
      yield* runner.commitStop(lease, Exit.void)
      yield* runner.awaitStop(lease)

      expect(yield* Fiber.join(active)).toBe("active-cancelled")
      expect(yield* Fiber.join(successor)).toBe("successor-cancelled")
      expect(yield* runner.successorPending).toBe(false)
    }),
  )

  it.live(
    "commit is exclusive and translates overlap to Busy",
    Effect.gen(function* () {
      const scope = yield* Scope.Scope
      const runner = Runner.make<string>(scope)
      const started = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const commit = yield* runner
        .commit(
          Deferred.succeed(started, undefined).pipe(Effect.andThen(Deferred.await(release)), Effect.as("committed")),
        )
        .pipe(Effect.forkChild)
      yield* Deferred.await(started)

      const exit = yield* runner.submit(Effect.void, Effect.succeed("run")).pipe(Effect.exit)
      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBeInstanceOf(Runner.Busy)

      yield* Deferred.succeed(release, undefined)
      expect(yield* Fiber.join(commit)).toBe("committed")
    }),
  )

  it.live(
    "Cancel quiesce settles and commits before reuse",
    Effect.gen(function* () {
      const scope = yield* Scope.Scope
      const runner = Runner.make<string>(scope)
      const started = yield* Deferred.make<void>()
      const active = yield* runner
        .ensureRunning(
          Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never), Effect.as("never")),
          Effect.succeed("cancelled"),
        )
        .pipe(Effect.forkChild)
      yield* Deferred.await(started)

      const lease = yield* runner.quiesce("Cancel")
      expect(lease.owner).toBe(true)
      expect(lease.satisfied).toBe(false)
      yield* runner.settleStop(lease)
      yield* runner.commitStop(lease, Exit.void)
      yield* runner.awaitStop(lease)

      expect(yield* Fiber.join(active)).toBe("cancelled")
      expect(yield* runner.ensureRunning(Effect.succeed("reused"))).toBe("reused")
    }),
  )

  it.live(
    "concurrent quiesce joins the owner's stop",
    Effect.gen(function* () {
      const scope = yield* Scope.Scope
      const runner = Runner.make<string>(scope)
      const started = yield* Deferred.make<void>()
      const interrupted = yield* Deferred.make<void>()
      const cleanup = yield* Deferred.make<void>()
      const active = yield* runner
        .ensureRunning(
          Deferred.succeed(started, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.onInterrupt(() => Deferred.succeed(interrupted, undefined)),
            Effect.ensuring(Deferred.await(cleanup)),
            Effect.as("never"),
          ),
          Effect.succeed("closed"),
        )
        .pipe(Effect.forkChild)
      yield* Deferred.await(started)

      const ownerFiber = yield* runner.quiesce("Close").pipe(Effect.forkChild)
      yield* Deferred.await(interrupted)
      const joinedFiber = yield* runner.quiesce("Close").pipe(Effect.forkChild)
      yield* Deferred.succeed(cleanup, undefined)
      const owner = yield* Fiber.join(ownerFiber)
      const joined = yield* Fiber.join(joinedFiber)
      expect(owner.owner).toBe(true)
      expect(joined.owner).toBe(false)

      yield* runner.settleStop(owner)
      yield* runner.commitStop(owner, Exit.void)
      yield* runner.awaitStop(joined)
      expect(yield* Fiber.join(active)).toBe("closed")
      expect(Exit.isFailure(yield* runner.ensureRunning(Effect.succeed("late")).pipe(Effect.exit))).toBe(true)
    }),
  )

  it.live(
    "idle Close is satisfied and permanently rejects work",
    Effect.gen(function* () {
      const scope = yield* Scope.Scope
      const runner = Runner.make<string>(scope)
      const lease = yield* runner.quiesce("Close")
      expect(lease.owner).toBe(true)
      yield* runner.settleStop(lease)
      yield* runner.commitStop(lease, Exit.void)
      yield* runner.awaitStop(lease)
      expect(Exit.isFailure(yield* runner.startShell(Effect.succeed("late")).pipe(Effect.exit))).toBe(true)
    }),
  )
})
