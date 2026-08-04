import { describe, expect } from "bun:test"
import { Cause, Deferred, Effect, Exit, Fiber, Ref, Scope } from "effect"
import { Runner } from "@/effect/runner"
import { it } from "../lib/effect"

describe("Runner", () => {
  it.live("ensureRunning shares one active run", Effect.gen(function* () {
    const scope = yield* Scope.Scope
    const runner = Runner.make<string>(scope)
    const release = yield* Deferred.make<void>()
    const calls = yield* Ref.make(0)
    const work = (result: string) =>
      Ref.update(calls, (value) => value + 1).pipe(
        Effect.andThen(Deferred.await(release)),
        Effect.as(result),
      )
    const both = yield* Effect.all(
      [runner.ensureRunning(work("first")), runner.ensureRunning(work("second"))],
      { concurrency: "unbounded" },
    ).pipe(Effect.forkChild)
    while ((yield* Ref.get(calls)) === 0) yield* Effect.yieldNow
    yield* Deferred.succeed(release, undefined)

    const [first, second] = yield* Fiber.join(both)
    expect(first).toBe(second)
    expect(yield* Ref.get(calls)).toBe(1)
  }))

  it.live("ensureRunning propagates failure and permits a successor", Effect.gen(function* () {
    const scope = yield* Scope.Scope
    const runner = Runner.make<string, string>(scope)
    expect(Exit.isFailure(yield* runner.ensureRunning(Effect.fail("boom")).pipe(Effect.exit))).toBe(true)
    expect(yield* runner.ensureRunning(Effect.succeed("next"))).toBe("next")
  }))

  it.live("submit admits a successor behind the active run", Effect.gen(function* () {
    const scope = yield* Scope.Scope
    const runner = Runner.make<string>(scope)
    const started = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    const admitted = yield* Deferred.make<void>()
    const current = yield* runner.ensureRunning(
      Deferred.succeed(started, undefined).pipe(
        Effect.andThen(Deferred.await(release)),
        Effect.as("current"),
      ),
    ).pipe(Effect.forkChild)
    yield* Deferred.await(started)

    const successor = yield* runner.submit(
      Deferred.succeed(admitted, undefined),
      Effect.succeed("successor"),
    ).pipe(Effect.forkChild)
    yield* Deferred.await(admitted)
    expect(yield* runner.successorPending).toBe(true)

    yield* Deferred.succeed(release, undefined)
    expect(yield* Fiber.join(current)).toBe("current")
    expect(yield* Fiber.join(successor)).toBe("successor")
    expect(yield* runner.successorPending).toBe(false)
  }))

  it.live("submitAdmitted returns admission while the successor completes later", Effect.gen(function* () {
    const scope = yield* Scope.Scope
    const runner = Runner.make<string>(scope)
    const release = yield* Deferred.make<void>()
    const completed = yield* Deferred.make<void>()
    expect(yield* runner.submitAdmitted(
      Effect.succeed("accepted"),
      Deferred.await(release).pipe(Effect.as("done"), Effect.ensuring(Deferred.succeed(completed, undefined))),
    ))
      .toBe("accepted")
    expect(yield* runner.successorPending).toBe(false)
    yield* Deferred.succeed(release, undefined)
    yield* Deferred.await(completed)
    expect(yield* runner.ensureRunning(Effect.succeed("next"))).toBe("next")
  }))

  it.live("submitManualAdmitted starts one successor after the active run settles", Effect.gen(function* () {
    const scope = yield* Scope.Scope
    const runner = Runner.make<string>(scope)
    const currentStarted = yield* Deferred.make<void>()
    const releaseCurrent = yield* Deferred.make<void>()
    const successorStarted = yield* Deferred.make<void>()
    const successorCalls = yield* Ref.make(0)
    const current = yield* runner.ensureRunning(
      Deferred.succeed(currentStarted, undefined).pipe(
        Effect.andThen(Deferred.await(releaseCurrent)),
        Effect.as("current"),
      ),
    ).pipe(Effect.forkChild)
    yield* Deferred.await(currentStarted)

    expect(yield* runner.submitManualAdmitted(
      Effect.succeed("accepted"),
      Ref.update(successorCalls, (value) => value + 1).pipe(
        Effect.andThen(Deferred.succeed(successorStarted, undefined)),
        Effect.as("successor"),
      ),
    )).toBe("accepted")
    expect(yield* runner.successorPending).toBe(true)
    expect(yield* Ref.get(successorCalls)).toBe(0)

    yield* Deferred.succeed(releaseCurrent, undefined)
    expect(yield* Fiber.join(current)).toBe("current")
    yield* Deferred.await(successorStarted)
    expect(yield* Ref.get(successorCalls)).toBe(1)
    expect(yield* runner.successorPending).toBe(false)
  }))

  it.live("commit is exclusive and translates overlap to Busy", Effect.gen(function* () {
    const scope = yield* Scope.Scope
    const runner = Runner.make<string>(scope)
    const started = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    const commit = yield* runner.commit(
      Deferred.succeed(started, undefined).pipe(
        Effect.andThen(Deferred.await(release)),
        Effect.as("committed"),
      ),
    ).pipe(Effect.forkChild)
    yield* Deferred.await(started)

    const exit = yield* runner.submit(Effect.void, Effect.succeed("run")).pipe(Effect.exit)
    expect(Exit.isFailure(exit)).toBe(true)
    if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBeInstanceOf(Runner.Busy)

    yield* Deferred.succeed(release, undefined)
    expect(yield* Fiber.join(commit)).toBe("committed")
  }))

  it.live("Cancel quiesce settles and commits before reuse", Effect.gen(function* () {
    const scope = yield* Scope.Scope
    const runner = Runner.make<string>(scope)
    const started = yield* Deferred.make<void>()
    const active = yield* runner.ensureRunning(
      Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never), Effect.as("never")),
      Effect.succeed("cancelled"),
    ).pipe(Effect.forkChild)
    yield* Deferred.await(started)

    const lease = yield* runner.quiesce("Cancel")
    expect(lease.owner).toBe(true)
    expect(lease.satisfied).toBe(false)
    yield* runner.settleStop(lease)
    yield* runner.commitStop(lease, Exit.void)
    yield* runner.awaitStop(lease)

    expect(yield* Fiber.join(active)).toBe("cancelled")
    expect(yield* runner.ensureRunning(Effect.succeed("reused"))).toBe("reused")
  }))

  it.live("concurrent quiesce joins the owner's stop", Effect.gen(function* () {
    const scope = yield* Scope.Scope
    const runner = Runner.make<string>(scope)
    const started = yield* Deferred.make<void>()
    const interrupted = yield* Deferred.make<void>()
    const cleanup = yield* Deferred.make<void>()
    const active = yield* runner.ensureRunning(
      Deferred.succeed(started, undefined).pipe(
        Effect.andThen(Effect.never),
        Effect.onInterrupt(() => Deferred.succeed(interrupted, undefined)),
        Effect.ensuring(Deferred.await(cleanup)),
        Effect.as("never"),
      ),
      Effect.succeed("closed"),
    ).pipe(Effect.forkChild)
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
  }))

  it.live("idle Close is satisfied and permanently rejects work", Effect.gen(function* () {
    const scope = yield* Scope.Scope
    const runner = Runner.make<string>(scope)
    const lease = yield* runner.quiesce("Close")
    expect(lease.owner).toBe(true)
    yield* runner.settleStop(lease)
    yield* runner.commitStop(lease, Exit.void)
    yield* runner.awaitStop(lease)
    expect(Exit.isFailure(yield* runner.startShell(Effect.succeed("late")).pipe(Effect.exit))).toBe(true)
  }))
})
