import { describe, expect } from "bun:test"
import { Cause, Deferred, Effect, Exit, Fiber, Latch, Ref, Scope } from "effect"
import { Runner } from "@/effect/runner"
import { it } from "../lib/effect"

const waitForState = <A, E>(runner: Runner.Runner<A, E>, tag: Runner.State<A, E>["_tag"]) =>
  Effect.gen(function* () {
    while (runner.state._tag !== tag) yield* Effect.yieldNow
  }).pipe(Effect.timeout("1 second"))

describe("Runner", () => {
  // --- ensureRunning semantics ---

  it.live(
    "ensureRunning starts work and returns result",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)
      const result = yield* runner.ensureRunning(Effect.succeed("hello"))
      expect(result).toBe("hello")
      expect(runner.state._tag).toBe("Idle")
      expect(runner.busy).toBe(false)
    }),
  )

  it.live(
    "ensureRunning propagates work failures",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string, string>(s)
      const exit = yield* runner.ensureRunning(Effect.fail("boom")).pipe(Effect.exit)
      expect(Exit.isFailure(exit)).toBe(true)
      expect(runner.state._tag).toBe("Idle")
    }),
  )

  it.live(
    "concurrent callers share the same run",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)
      const calls = yield* Ref.make(0)
      const work = Effect.gen(function* () {
        yield* Ref.update(calls, (n) => n + 1)
        yield* Effect.sleep("10 millis")
        return "shared"
      })

      const [a, b] = yield* Effect.all([runner.ensureRunning(work), runner.ensureRunning(work)], {
        concurrency: "unbounded",
      })

      expect(a).toBe("shared")
      expect(b).toBe("shared")
      expect(yield* Ref.get(calls)).toBe(1)
    }),
  )

  it.live(
    "concurrent callers all receive same error",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string, string>(s)
      const work = Effect.gen(function* () {
        yield* Effect.sleep("10 millis")
        return yield* Effect.fail("boom")
      })

      const [a, b] = yield* Effect.all(
        [runner.ensureRunning(work).pipe(Effect.exit), runner.ensureRunning(work).pipe(Effect.exit)],
        { concurrency: "unbounded" },
      )

      expect(Exit.isFailure(a)).toBe(true)
      expect(Exit.isFailure(b)).toBe(true)
    }),
  )

  it.live(
    "ensureRunning can be called again after previous run completes",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)
      expect(yield* runner.ensureRunning(Effect.succeed("first"))).toBe("first")
      expect(yield* runner.ensureRunning(Effect.succeed("second"))).toBe("second")
    }),
  )

  it.live(
    "submit acquires ownership before admission and starts the run afterward",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const order = yield* Ref.make<string[]>([])
      const runner = Runner.make<string>(s, {
        onBusy: Ref.update(order, (items) => [...items, "busy"]),
        onIdle: Ref.update(order, (items) => [...items, "idle"]),
      })

      const result = yield* runner.submit(
        Ref.update(order, (items) => [...items, "admission"]),
        Ref.update(order, (items) => [...items, "run"]).pipe(Effect.as("done")),
      )

      expect(result).toBe("done")
      expect(yield* Ref.get(order)).toEqual(["admission", "busy", "run", "idle"])
    }),
  )

  it.live(
    "submit admits one coalesced successor and every caller awaits it",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)
      const started = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const admissions = yield* Ref.make<string[]>([])
      const replacementRuns = yield* Ref.make(0)
      const replacementStarted = yield* Deferred.make<void>()
      const replacementRelease = yield* Deferred.make<void>()
      const secondAdmissionEntered = yield* Deferred.make<void>()
      const secondAdmissionRelease = yield* Deferred.make<void>()
      const secondAdmissionApplied = yield* Deferred.make<void>()

      const first = yield* runner
        .ensureRunning(
          Effect.gen(function* () {
            yield* Deferred.succeed(started, undefined)
            yield* Deferred.await(release)
            return "shared"
          }),
        )
        .pipe(Effect.forkChild)
      yield* Deferred.await(started)

      const firstAttached = yield* runner
        .submit(
          Ref.update(admissions, (items) => [...items, "first-input"]),
          Effect.gen(function* () {
            yield* Ref.update(replacementRuns, (count) => count + 1)
            yield* Deferred.succeed(replacementStarted, undefined)
            yield* Deferred.await(replacementRelease)
            return "replacement"
          }),
        )
        .pipe(Effect.forkChild)
      const secondAttached = yield* runner
        .submit(
          Effect.gen(function* () {
            yield* Deferred.succeed(secondAdmissionEntered, undefined)
            yield* Deferred.await(secondAdmissionRelease)
            yield* Ref.update(admissions, (items) => [...items, "second-input"])
            yield* Deferred.succeed(secondAdmissionApplied, undefined)
          }),
          Ref.update(replacementRuns, (count) => count + 1).pipe(Effect.as("ignored-second-replacement")),
        )
        .pipe(Effect.forkChild)
      yield* Deferred.await(secondAdmissionEntered)
      expect(runner.state._tag).toBe("Admitting")
      yield* Deferred.succeed(secondAdmissionRelease, undefined)
      yield* Deferred.await(secondAdmissionApplied)
      expect(yield* Ref.get(admissions)).toEqual(["first-input", "second-input"])

      yield* Deferred.succeed(release, undefined)
      expect(yield* Fiber.join(first)).toBe("shared")
      yield* Deferred.await(replacementStarted)
      expect(yield* Ref.get(replacementRuns)).toBe(1)
      expect(runner.busy).toBe(true)
      yield* Deferred.succeed(replacementRelease, undefined)
      expect(yield* Fiber.join(firstAttached)).toBe("replacement")
      expect(yield* Fiber.join(secondAttached)).toBe("replacement")
      expect(runner.busy).toBe(false)
    }),
  )

  it.live(
    "admission reservation survives the active final read and commits one successor",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const statuses = yield* Ref.make<string[]>([])
      const activeStarted = yield* Deferred.make<void>()
      const releaseActive = yield* Deferred.make<void>()
      const activeReturned = yield* Deferred.make<void>()
      const admissionEntered = yield* Deferred.make<void>()
      const releaseAdmission = yield* Deferred.make<void>()
      const successorStarted = yield* Deferred.make<void>()
      const releaseSuccessor = yield* Deferred.make<void>()
      const successorRuns = yield* Ref.make(0)
      const runner = Runner.make<string>(s, {
        onBusy: Ref.update(statuses, (items) => [...items, "busy"]),
        onIdle: Ref.update(statuses, (items) => [...items, "idle"]),
      })

      const active = yield* runner
        .ensureRunning(
          Effect.gen(function* () {
            yield* Deferred.succeed(activeStarted, undefined)
            yield* Deferred.await(releaseActive)
            yield* Deferred.succeed(activeReturned, undefined)
            return "active"
          }),
        )
        .pipe(Effect.forkChild)
      yield* Deferred.await(activeStarted)

      const first = yield* runner
        .submit(
          Deferred.succeed(admissionEntered, undefined).pipe(Effect.andThen(Deferred.await(releaseAdmission))),
          Effect.gen(function* () {
            yield* Ref.update(successorRuns, (count) => count + 1)
            yield* Deferred.succeed(successorStarted, undefined)
            yield* Deferred.await(releaseSuccessor)
            return "successor"
          }),
        )
        .pipe(Effect.forkChild)
      yield* Deferred.await(admissionEntered)
      expect(runner.state._tag).toBe("Admitting")

      yield* Deferred.succeed(releaseActive, undefined)
      yield* Deferred.await(activeReturned)
      expect(yield* Ref.get(statuses)).toEqual(["busy"])

      yield* Deferred.succeed(releaseAdmission, undefined)
      yield* Deferred.await(successorStarted)

      expect(yield* Fiber.join(active)).toBe("active")
      expect(yield* Ref.get(successorRuns)).toBe(1)
      expect(yield* Ref.get(statuses)).toEqual(["busy"])
      yield* Deferred.succeed(releaseSuccessor, undefined)
      expect(yield* Fiber.join(first)).toBe("successor")
      expect(yield* Ref.get(statuses)).toEqual(["busy", "idle"])
      expect(runner.state._tag).toBe("Idle")
    }),
  )

  it.live(
    "admission after the active final boundary starts exactly one successor",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const idleEntered = yield* Deferred.make<void>()
      const releaseIdle = yield* Deferred.make<void>()
      const runs = yield* Ref.make<string[]>([])
      const runner = Runner.make<string>(s, {
        onIdle: Effect.gen(function* () {
          yield* Deferred.succeed(idleEntered, undefined)
          yield* Deferred.await(releaseIdle)
        }),
      })

      const active = yield* runner.ensureRunning(Effect.succeed("active")).pipe(Effect.forkChild)
      yield* Deferred.await(idleEntered)
      expect(runner.state._tag).toBe("Finishing")

      const successor = yield* runner
        .submit(Effect.void, Ref.update(runs, (items) => [...items, "successor"]).pipe(Effect.as("successor")))
        .pipe(Effect.forkChild)
      yield* Deferred.succeed(releaseIdle, undefined)

      expect(yield* Fiber.join(active)).toBe("active")
      expect(yield* Fiber.join(successor)).toBe("successor")
      expect(yield* Ref.get(runs)).toEqual(["successor"])
      expect(runner.state._tag).toBe("Idle")
    }),
  )

  it.live(
    "admission during final Idle publication waits and starts after committed Idle",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const idleEntered = yield* Deferred.make<void>()
      const releaseIdle = yield* Deferred.make<void>()
      const admissionAttempted = yield* Deferred.make<void>()
      const admissionApplied = yield* Deferred.make<void>()
      const successorStarted = yield* Deferred.make<void>()
      const statuses = yield* Ref.make<string[]>([])
      const runner = Runner.make<string>(s, {
        onIdle: Ref.update(statuses, (items) => [...items, "idle"]).pipe(
          Effect.andThen(Deferred.succeed(idleEntered, undefined)),
          Effect.andThen(Deferred.await(releaseIdle)),
        ),
        onBusy: Ref.update(statuses, (items) => [...items, "busy"]),
      })

      const active = yield* runner.ensureRunning(Effect.succeed("active")).pipe(Effect.forkChild)
      yield* Deferred.await(idleEntered)

      const successor = yield* Effect.gen(function* () {
        yield* Deferred.succeed(admissionAttempted, undefined)
        return yield* runner.submit(
          Deferred.succeed(admissionApplied, undefined),
          Deferred.succeed(successorStarted, undefined).pipe(Effect.as("successor")),
        )
      }).pipe(Effect.forkChild)
      yield* Deferred.await(admissionAttempted)
      expect(yield* Deferred.isDone(admissionApplied)).toBe(false)
      expect(yield* Ref.get(statuses)).toEqual(["busy", "idle"])

      yield* Deferred.succeed(releaseIdle, undefined)
      expect(yield* Fiber.join(active)).toBe("active")
      yield* Deferred.await(admissionApplied)
      yield* Deferred.await(successorStarted)
      expect(yield* Fiber.join(successor)).toBe("successor")
      expect(yield* Ref.get(statuses)).toEqual(["busy", "idle", "busy", "idle"])
    }),
  )

  it.live(
    "cancel settles active and submit-created pending successor without running it",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const activeStarted = yield* Deferred.make<void>()
      const admissionDone = yield* Deferred.make<void>()
      const successorRuns = yield* Ref.make(0)
      const runner = Runner.make<string>(s)
      const active = yield* runner
        .ensureRunning(
          Deferred.succeed(activeStarted, undefined).pipe(Effect.andThen(Effect.never), Effect.as("active")),
        )
        .pipe(Effect.forkChild)
      yield* Deferred.await(activeStarted)

      const pending = yield* runner
        .submit(
          Deferred.succeed(admissionDone, undefined),
          Ref.update(successorRuns, (count) => count + 1).pipe(Effect.as("successor")),
        )
        .pipe(Effect.forkChild)
      yield* Deferred.await(admissionDone)
      yield* Effect.gen(function* () {
        while (runner.state._tag !== "Running" || !runner.state.next) yield* Effect.yieldNow
      }).pipe(Effect.timeout("1 second"))

      yield* runner.cancel
      expect(Exit.isFailure(yield* Fiber.await(active))).toBe(true)
      expect(Exit.isFailure(yield* Fiber.await(pending))).toBe(true)
      expect(yield* Ref.get(successorRuns)).toBe(0)
      expect(runner.state._tag).toBe("Idle")
    }),
  )

  it.live(
    "cancel interrupts in-progress admission and restores idle ownership",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const admissionStarted = yield* Deferred.make<void>()
      const workRuns = yield* Ref.make(0)
      const runner = Runner.make<string>(s)
      const caller = yield* runner
        .submit(
          Deferred.succeed(admissionStarted, undefined).pipe(Effect.andThen(Effect.never)),
          Ref.update(workRuns, (count) => count + 1).pipe(Effect.as("work")),
        )
        .pipe(Effect.forkChild)
      yield* Deferred.await(admissionStarted)

      yield* runner.cancel

      expect(Exit.isFailure(yield* Fiber.await(caller))).toBe(true)
      expect(yield* Ref.get(workRuns)).toBe(0)
      expect(runner.state._tag).toBe("Idle")
    }),
  )

  it.live(
    "Busy publishes before run work and Busy failure starts no work",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const order = yield* Ref.make<string[]>([])
      const runner = Runner.make<string>(s, {
        onBusy: Ref.update(order, (items) => [...items, "busy"]),
      })

      expect(yield* runner.ensureRunning(Ref.update(order, (items) => [...items, "run"]).pipe(Effect.as("done")))).toBe(
        "done",
      )
      expect(yield* Ref.get(order)).toEqual(["busy", "run"])

      const failedOrder = yield* Ref.make<string[]>([])
      const admitted = yield* Ref.make(0)
      const failed = Runner.make<string>(s, {
        onBusy: Effect.die("busy-failed"),
      })
      const exit = yield* failed
        .submit(
          Ref.update(admitted, (count) => count + 1),
          Ref.update(failedOrder, (items) => [...items, "run"]).pipe(Effect.as("done")),
        )
        .pipe(Effect.exit)
      expect(Exit.isFailure(exit)).toBe(true)
      expect(yield* Ref.get(admitted)).toBe(1)
      expect(yield* Ref.get(failedOrder)).toEqual([])
      expect(failed.state._tag).toBe("Idle")
    }),
  )

  it.live(
    "commit is idle-only and does not start a run lifecycle",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const busyCount = yield* Ref.make(0)
      const runner = Runner.make<string>(s, {
        onBusy: Ref.update(busyCount, (count) => count + 1),
      })

      expect(yield* runner.commit(Effect.succeed("committed"))).toBe("committed")
      expect(yield* Ref.get(busyCount)).toBe(0)
      expect(runner.state._tag).toBe("Idle")

      const running = yield* runner.ensureRunning(Effect.never.pipe(Effect.as("running"))).pipe(Effect.forkChild)
      yield* waitForState(runner, "Running")
      const blocked = yield* runner.commit(Effect.succeed("rejected")).pipe(Effect.exit)
      expect(Exit.isFailure(blocked)).toBe(true)
      if (Exit.isFailure(blocked)) expect(Cause.squash(blocked.cause)).toBeInstanceOf(Runner.Busy)
      yield* runner.cancel
      yield* Fiber.await(running)
    }),
  )

  it.live(
    "cancel waits for an idle-only commit instead of interrupting it",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const commitStarted = yield* Deferred.make<void>()
      const releaseCommit = yield* Deferred.make<void>()
      const runner = Runner.make<string>(s)

      const committed = yield* runner
        .commit(
          Deferred.succeed(commitStarted, undefined).pipe(
            Effect.andThen(Deferred.await(releaseCommit)),
            Effect.as("committed"),
          ),
        )
        .pipe(Effect.forkChild)
      yield* Deferred.await(commitStarted)
      const cancelAttempted = yield* Deferred.make<void>()
      const cancelReturned = yield* Deferred.make<void>()
      const cancelled = yield* Deferred.succeed(cancelAttempted, undefined).pipe(
        Effect.andThen(runner.cancel),
        Effect.tap(() => Deferred.succeed(cancelReturned, undefined)),
        Effect.forkChild,
      )
      yield* Deferred.await(cancelAttempted)
      expect(yield* Deferred.isDone(cancelReturned)).toBe(false)

      yield* Deferred.succeed(releaseCommit, undefined)
      expect(yield* Fiber.join(committed)).toBe("committed")
      expect(Exit.isSuccess(yield* Fiber.await(cancelled))).toBe(true)
      expect(runner.state._tag).toBe("Idle")
    }),
  )

  // --- cancel semantics ---

  it.live(
    "cancel interrupts running work",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)
      const started = yield* Deferred.make<void>()
      const fiber = yield* runner
        .ensureRunning(
          Effect.gen(function* () {
            yield* Deferred.succeed(started, void 0)
            return yield* Effect.never.pipe(Effect.as("never"))
          }),
        )
        .pipe(Effect.forkChild)
      yield* Deferred.await(started)
      expect(runner.busy).toBe(true)
      expect(runner.state._tag).toBe("Running")

      yield* runner.cancel
      expect(runner.busy).toBe(false)

      const exit = yield* Fiber.await(fiber)
      expect(Exit.isFailure(exit)).toBe(true)
    }),
  )

  it.live(
    "cancel on idle is a no-op",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)
      yield* runner.cancel
      expect(runner.busy).toBe(false)
    }),
  )

  it.live(
    "cancel with onInterrupt resolves callers gracefully",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s, { onInterrupt: Effect.succeed("fallback") })
      const fiber = yield* runner.ensureRunning(Effect.never.pipe(Effect.as("never"))).pipe(Effect.forkChild)
      yield* waitForState(runner, "Running")

      yield* runner.cancel

      const exit = yield* Fiber.await(fiber)
      expect(Exit.isSuccess(exit)).toBe(true)
      if (Exit.isSuccess(exit)) expect(exit.value).toBe("fallback")
    }),
  )

  it.live(
    "cancel with queued callers resolves all",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s, { onInterrupt: Effect.succeed("fallback") })

      const a = yield* runner.ensureRunning(Effect.never.pipe(Effect.as("x"))).pipe(Effect.forkChild)
      yield* waitForState(runner, "Running")
      const b = yield* runner.ensureRunning(Effect.succeed("y")).pipe(Effect.forkChild)
      yield* Effect.yieldNow

      yield* runner.cancel

      const [exitA, exitB] = yield* Effect.all([Fiber.await(a), Fiber.await(b)])
      expect(Exit.isSuccess(exitA)).toBe(true)
      expect(Exit.isSuccess(exitB)).toBe(true)
      if (Exit.isSuccess(exitA)) expect(exitA.value).toBe("fallback")
      if (Exit.isSuccess(exitB)) expect(exitB.value).toBe("fallback")
    }),
  )

  it.live(
    "work can be started after cancel",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)
      const fiber = yield* runner.ensureRunning(Effect.never.pipe(Effect.as("x"))).pipe(Effect.forkChild)
      yield* waitForState(runner, "Running")
      yield* runner.cancel
      yield* Fiber.await(fiber)

      const result = yield* runner.ensureRunning(Effect.succeed("after-cancel"))
      expect(result).toBe("after-cancel")
    }),
  )

  it.live(
    "cancel does not deadlock when replacement work starts before interrupted run exits",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const hit = yield* Deferred.make<void>()
      const hold = yield* Deferred.make<void>()
      const done = yield* Deferred.make<void>()

      yield* Effect.gen(function* () {
        const runner = Runner.make<string>(s)
        const first = Effect.never.pipe(
          Effect.onInterrupt(() => Deferred.succeed(hit, undefined)),
          Effect.ensuring(Deferred.await(hold)),
          Effect.as("first"),
        )

        const a = yield* runner.ensureRunning(first).pipe(Effect.exit, Effect.forkChild)
        yield* waitForState(runner, "Running")

        const stop = yield* runner.cancel.pipe(Effect.forkChild)
        yield* Deferred.await(hit).pipe(Effect.timeout("250 millis"))

        const b = yield* runner.ensureRunning(Deferred.await(done).pipe(Effect.as("second"))).pipe(Effect.forkChild)
        yield* Effect.yieldNow
        expect(runner.busy).toBe(true)

        yield* Deferred.succeed(hold, undefined)
        const stopExit = yield* Fiber.await(stop).pipe(Effect.timeout("250 millis"))
        expect(Exit.isSuccess(stopExit)).toBe(true)

        expect(runner.busy).toBe(true)
        yield* Deferred.succeed(done, undefined)
        expect(yield* Fiber.join(b).pipe(Effect.timeout("250 millis"))).toBe("second")
        expect(runner.busy).toBe(false)

        const exit = yield* Fiber.join(a)
        expect(Exit.isFailure(exit)).toBe(true)
      }).pipe(
        Effect.ensuring(
          Effect.all([Deferred.succeed(hold, undefined), Deferred.succeed(done, undefined)], { discard: true }).pipe(
            Effect.ignore,
          ),
        ),
      )
    }),
  )

  // --- shell semantics ---

  it.live(
    "shell runs exclusively",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)
      const result = yield* runner.startShell(Effect.succeed("shell-done"))
      expect(result).toBe("shell-done")
      expect(runner.busy).toBe(false)
    }),
  )

  it.live(
    "shell rejects when run is active",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)
      const started = yield* Deferred.make<void>()
      const fiber = yield* runner
        .ensureRunning(
          Effect.gen(function* () {
            yield* Deferred.succeed(started, undefined)
            return yield* Effect.never.pipe(Effect.as("x"))
          }),
        )
        .pipe(Effect.forkChild)
      yield* Deferred.await(started).pipe(Effect.timeout("250 millis"))
      yield* Effect.gen(function* () {
        while (runner.state._tag !== "Running") yield* Effect.yieldNow
      }).pipe(Effect.timeout("250 millis"))

      const exit = yield* runner.startShell(Effect.succeed("nope")).pipe(Effect.exit)
      expect(Exit.isFailure(exit)).toBe(true)

      yield* runner.cancel
      yield* Fiber.await(fiber).pipe(Effect.timeout("250 millis"))
    }),
  )

  it.live(
    "shell rejects when another shell is running",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)
      const gate = yield* Deferred.make<void>()

      const sh = yield* runner.startShell(Deferred.await(gate).pipe(Effect.as("first"))).pipe(Effect.forkChild)
      yield* waitForState(runner, "Shell")

      const exit = yield* runner.startShell(Effect.succeed("second")).pipe(Effect.exit)
      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBeInstanceOf(Runner.Busy)

      yield* Deferred.succeed(gate, undefined)
      yield* Fiber.await(sh)
    }),
  )

  it.live(
    "cancel interrupts shell",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)
      const gate = yield* Deferred.make<void>()

      const sh = yield* runner.startShell(Deferred.await(gate).pipe(Effect.as("ignored"))).pipe(Effect.forkChild)
      yield* waitForState(runner, "Shell")

      const stop = yield* runner.cancel.pipe(Effect.forkChild)
      const stopExit = yield* Fiber.await(stop).pipe(Effect.timeout("250 millis"))
      expect(Exit.isSuccess(stopExit)).toBe(true)
      expect(runner.busy).toBe(false)

      const shellExit = yield* Fiber.await(sh)
      expect(Exit.isFailure(shellExit)).toBe(true)

      yield* Deferred.succeed(gate, undefined).pipe(Effect.ignore)
    }),
  )

  it.live(
    "cancel does not mask shell defects",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s, { onInterrupt: Effect.succeed("interrupted") })
      const ready = yield* Latch.make()

      const sh = yield* runner
        .startShell(
          Effect.gen(function* () {
            yield* ready.open
            return yield* Effect.never.pipe(Effect.as("ignored"))
          }).pipe(Effect.ensuring(Effect.die("boom"))),
          ready,
        )
        .pipe(Effect.forkChild)
      yield* ready.await.pipe(Effect.timeout("250 millis"))

      yield* runner.cancel
      expect(Exit.isFailure(yield* Fiber.await(sh))).toBe(true)
    }),
  )

  // --- shell→run handoff ---

  it.live(
    "ensureRunning queues behind shell then runs after",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)
      const gate = yield* Deferred.make<void>()

      const sh = yield* runner.startShell(Deferred.await(gate).pipe(Effect.as("shell-result"))).pipe(Effect.forkChild)
      yield* waitForState(runner, "Shell")
      expect(runner.state._tag).toBe("Shell")

      const run = yield* runner.ensureRunning(Effect.succeed("run-result")).pipe(Effect.forkChild)
      yield* waitForState(runner, "ShellThenRun")
      expect(runner.state._tag).toBe("ShellThenRun")

      yield* Deferred.succeed(gate, undefined)
      yield* Fiber.await(sh)

      const exit = yield* Fiber.await(run)
      expect(Exit.isSuccess(exit)).toBe(true)
      if (Exit.isSuccess(exit)) expect(exit.value).toBe("run-result")
      expect(runner.state._tag).toBe("Idle")
    }),
  )

  it.live(
    "multiple ensureRunning callers share the queued run behind shell",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)
      const calls = yield* Ref.make(0)
      const gate = yield* Deferred.make<void>()

      const sh = yield* runner.startShell(Deferred.await(gate).pipe(Effect.as("shell"))).pipe(Effect.forkChild)
      yield* waitForState(runner, "Shell")

      const work = Effect.gen(function* () {
        yield* Ref.update(calls, (n) => n + 1)
        return "run"
      })
      const a = yield* runner.ensureRunning(work).pipe(Effect.forkChild)
      const b = yield* runner.ensureRunning(work).pipe(Effect.forkChild)
      yield* waitForState(runner, "ShellThenRun")

      yield* Deferred.succeed(gate, undefined)
      yield* Fiber.await(sh)

      const [exitA, exitB] = yield* Effect.all([Fiber.await(a), Fiber.await(b)])
      expect(Exit.isSuccess(exitA)).toBe(true)
      expect(Exit.isSuccess(exitB)).toBe(true)
      expect(yield* Ref.get(calls)).toBe(1)
    }),
  )

  it.live(
    "cancel during shell_then_run cancels both",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)

      const sh = yield* runner.startShell(Effect.never.pipe(Effect.as("aborted"))).pipe(Effect.forkChild)
      yield* waitForState(runner, "Shell")

      const run = yield* runner.ensureRunning(Effect.succeed("y")).pipe(Effect.forkChild)
      yield* waitForState(runner, "ShellThenRun")
      expect(runner.state._tag).toBe("ShellThenRun")

      yield* runner.cancel
      expect(runner.busy).toBe(false)

      yield* Fiber.await(sh)
      const exit = yield* Fiber.await(run)
      expect(Exit.isFailure(exit)).toBe(true)
    }),
  )

  // --- lifecycle callbacks ---

  it.live(
    "onIdle fires when returning to idle from running",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const count = yield* Ref.make(0)
      const runner = Runner.make<string>(s, {
        onIdle: Ref.update(count, (n) => n + 1),
      })
      yield* runner.ensureRunning(Effect.succeed("ok"))
      expect(yield* Ref.get(count)).toBe(1)
    }),
  )

  it.live(
    "onIdle fires on cancel",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const count = yield* Ref.make(0)
      const runner = Runner.make<string>(s, {
        onIdle: Ref.update(count, (n) => n + 1),
      })
      const fiber = yield* runner.ensureRunning(Effect.never.pipe(Effect.as("x"))).pipe(Effect.forkChild)
      yield* waitForState(runner, "Running")
      yield* runner.cancel
      yield* Fiber.await(fiber)
      expect(yield* Ref.get(count)).toBeGreaterThanOrEqual(1)
    }),
  )

  it.live(
    "onBusy fires when shell starts",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const count = yield* Ref.make(0)
      const runner = Runner.make<string>(s, {
        onBusy: Ref.update(count, (n) => n + 1),
      })
      yield* runner.startShell(Effect.succeed("done"))
      expect(yield* Ref.get(count)).toBe(1)
    }),
  )

  // --- busy flag ---

  it.live(
    "busy is true during run",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)
      const gate = yield* Deferred.make<void>()

      const fiber = yield* runner.ensureRunning(Deferred.await(gate).pipe(Effect.as("ok"))).pipe(Effect.forkChild)
      yield* waitForState(runner, "Running")
      expect(runner.busy).toBe(true)

      yield* Deferred.succeed(gate, undefined)
      yield* Fiber.await(fiber)
      expect(runner.busy).toBe(false)
    }),
  )

  it.live(
    "busy is true during shell",
    Effect.gen(function* () {
      const s = yield* Scope.Scope
      const runner = Runner.make<string>(s)
      const gate = yield* Deferred.make<void>()

      const fiber = yield* runner.startShell(Deferred.await(gate).pipe(Effect.as("ok"))).pipe(Effect.forkChild)
      yield* waitForState(runner, "Shell")
      expect(runner.busy).toBe(true)

      yield* Deferred.succeed(gate, undefined)
      yield* Fiber.await(fiber)
      expect(runner.busy).toBe(false)
    }),
  )
})
