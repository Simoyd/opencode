import { describe, expect } from "bun:test"
import { BackgroundJob } from "@opencode-ai/core/background-job"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Deferred, Effect, Exit, Scope } from "effect"
import { it } from "./lib/effect"

const jobsLayer = LayerNode.compile(BackgroundJob.node)

describe("BackgroundJob", () => {
  it.live("tracks process-local work through explicit observation", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const latch = yield* Deferred.make<void>()
      const job = yield* jobs.start({
        type: "test",
        metadata: { durable: false },
        run: Deferred.await(latch).pipe(Effect.as("done")),
      })

      expect(job).toMatchObject({ type: "test", status: "running", metadata: { durable: false } })
      expect(yield* jobs.wait({ id: job.id, timeout: 0 })).toMatchObject({
        timedOut: true,
        info: { status: "running" },
      })

      yield* Deferred.succeed(latch, undefined)
      expect(yield* jobs.wait({ id: job.id })).toMatchObject({
        timedOut: false,
        info: { status: "completed", output: "done" },
      })
    }).pipe(Effect.provide(jobsLayer)),
  )

  it.live("publishes jobs before starting immediately settling work", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service

      yield* Effect.forEach(Array.from({ length: 100 }), (_, index) => {
        const id = `job_immediate_start_${index}`
        return Effect.gen(function* () {
          const job = yield* jobs.start({
            id,
            type: "test",
            run: jobs
              .get(id)
              .pipe(
                Effect.flatMap((info) =>
                  info?.status === "running"
                    ? Effect.succeed(`done-${index}`)
                    : Effect.fail("job started before publish"),
                ),
              ),
          })

          expect(yield* jobs.wait({ id: job.id })).toMatchObject({
            timedOut: false,
            info: { status: "completed", output: `done-${index}` },
          })
        })
      })
    }).pipe(Effect.provide(jobsLayer)),
  )

  it.live("increments pending work before starting immediately settling extensions", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service

      yield* Effect.forEach(Array.from({ length: 100 }), (_, index) =>
        Effect.gen(function* () {
          const first = yield* Deferred.make<void>()
          const job = yield* jobs.start({
            type: "test",
            run: Deferred.await(first).pipe(Effect.as(`first-${index}`)),
          })

          expect(yield* jobs.extend({ id: job.id, run: Effect.succeed(`second-${index}`) })).toBe(true)
          expect((yield* jobs.get(job.id))?.status).toBe("running")

          yield* Deferred.succeed(first, undefined)
          expect(yield* jobs.wait({ id: job.id })).toMatchObject({
            timedOut: false,
            info: { status: "completed", output: `second-${index}` },
          })
        }),
      )
    }).pipe(Effect.provide(jobsLayer)),
  )

  it.live("publishes terminal completion after delivery registration but before delivery finishes", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const finish = yield* Deferred.make<void>()
      const registered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const delivered = yield* Deferred.make<void>()
      const job = yield* jobs.start({
        type: "test",
        metadata: { background: true },
        run: Deferred.await(finish).pipe(Effect.as("done")),
        terminalDelivery: () =>
          Deferred.succeed(registered, undefined).pipe(
            Effect.as({
              run: Deferred.await(release).pipe(Effect.ensuring(Deferred.succeed(delivered, undefined))),
            }),
          ),
      })

      yield* Deferred.succeed(finish, undefined)
      yield* Deferred.await(registered)
      const completed = yield* jobs.wait({ id: job.id })

      expect(completed.info).toMatchObject({ status: "completed", output: "done" })
      expect(yield* Deferred.isDone(delivered)).toBeFalse()
      yield* Deferred.succeed(release, undefined)
      yield* Deferred.await(delivered)
    }).pipe(Effect.provide(jobsLayer)),
  )

  it.live("reports terminal registration failure without exposing successful completion", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const job = yield* jobs.start({
        type: "test",
        metadata: { background: true },
        run: Effect.succeed("done"),
        terminalDelivery: () => Effect.fail(new Error("registration rejected")),
      })

      expect(yield* jobs.wait({ id: job.id })).toMatchObject({
        timedOut: false,
        info: { status: "error", error: "registration rejected" },
      })
    }).pipe(Effect.provide(jobsLayer)),
  )

  it.live("keeps completion nonblocking and records a later terminal delivery failure", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const release = yield* Deferred.make<void>()
      const delivered = yield* Deferred.make<void>()
      const job = yield* jobs.start({
        type: "test",
        metadata: { background: true },
        run: Effect.succeed("done"),
        terminalDelivery: () =>
          Effect.succeed({
            run: Deferred.await(release).pipe(
              Effect.andThen(Effect.fail(new Error("delivery failed"))),
              Effect.ensuring(Deferred.succeed(delivered, undefined)),
            ),
          }),
      })

      expect(yield* jobs.wait({ id: job.id })).toMatchObject({ info: { status: "completed" } })
      yield* Deferred.succeed(release, undefined)
      yield* Deferred.await(delivered)
      yield* Effect.yieldNow
      expect(yield* jobs.get(job.id)).toMatchObject({ status: "error", error: "delivery failed" })
    }).pipe(Effect.provide(jobsLayer)),
  )

  it.live("interrupts live work without promising settlement after the owning process-local scope closes", () =>
    Effect.gen(function* () {
      const scope = yield* Scope.make()
      const interrupted = yield* Deferred.make<void>()
      const jobs = yield* BackgroundJob.make.pipe(Scope.provide(scope))
      const job = yield* jobs.start({
        type: "test",
        run: Effect.never.pipe(Effect.ensuring(Deferred.succeed(interrupted, undefined))),
      })

      yield* Scope.close(scope, Exit.void)

      yield* Deferred.await(interrupted).pipe(Effect.timeout("1 second"))
      // The abandoned in-memory registry is not a durable observation channel.
      expect((yield* jobs.get(job.id))?.status).toBe("running")
    }),
  )
})
