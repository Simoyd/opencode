import { InstanceState } from "@/effect/instance-state"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Runner } from "@/effect/runner"
import { BackgroundJob } from "@/background/job"
import { Effect, Latch, Layer, Scope, Context } from "effect"
import { Session } from "./session"
import { SessionID } from "./schema"
import { SessionStatus } from "./status"

export interface Interface {
  readonly assertNotBusy: (sessionID: SessionID) => Effect.Effect<void, Session.BusyError>
  readonly cancel: (sessionID: SessionID) => Effect.Effect<void>
  readonly commit: <B, E, R>(
    sessionID: SessionID,
    onInterrupt: Effect.Effect<SessionV1.WithParts>,
    work: Effect.Effect<B, E, R>,
  ) => Effect.Effect<B, E | Session.BusyError, R>
  readonly submit: <B, E, R>(
    sessionID: SessionID,
    onInterrupt: Effect.Effect<SessionV1.WithParts>,
    admission: Effect.Effect<B, E, R>,
    work: Effect.Effect<SessionV1.WithParts>,
  ) => Effect.Effect<SessionV1.WithParts, E, R>
  readonly ensureRunning: (
    sessionID: SessionID,
    onInterrupt: Effect.Effect<SessionV1.WithParts>,
    work: Effect.Effect<SessionV1.WithParts>,
  ) => Effect.Effect<SessionV1.WithParts>
  readonly startShell: (
    sessionID: SessionID,
    onInterrupt: Effect.Effect<SessionV1.WithParts>,
    work: Effect.Effect<SessionV1.WithParts>,
    ready?: Latch.Latch,
  ) => Effect.Effect<SessionV1.WithParts, Session.BusyError>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SessionRunState") {}

interface Entry {
  runner: Runner.Runner<SessionV1.WithParts>
  uses: number
}

interface Lease {
  data: { runners: Map<SessionID, Entry> }
  entry: Entry | undefined
}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const background = yield* BackgroundJob.Service
    const status = yield* SessionStatus.Service

    const state = yield* InstanceState.make(
      Effect.fn("SessionRunState.state")(function* () {
        const scope = yield* Scope.Scope
        const runners = new Map<SessionID, Entry>()
        yield* Effect.addFinalizer(
          Effect.fnUntraced(function* () {
            const entries = yield* Effect.sync(() => {
              const current = [...runners.values()]
              runners.clear()
              return current
            })
            yield* Effect.forEach(entries, (entry) => entry.runner.cancel, {
              concurrency: "unbounded",
              discard: true,
            })
          }),
        )
        return { runners, scope }
      }),
    )

    const retire = (data: { runners: Map<SessionID, Entry> }, sessionID: SessionID, entry: Entry) => {
      if (data.runners.get(sessionID) !== entry || entry.uses !== 0 || entry.runner.busy) return
      data.runners.delete(sessionID)
    }

    const acquire = Effect.fn("SessionRunState.acquire")(function* (
      sessionID: SessionID,
      onInterrupt: Effect.Effect<SessionV1.WithParts>,
      create: boolean,
    ) {
      const data = yield* InstanceState.get(state)
      const entry = yield* Effect.sync(() => {
        const existing = data.runners.get(sessionID)
        if (existing) {
          existing.uses += 1
          return existing
        }
        if (!create) return undefined
        let created: Entry
        const runner = Runner.make<SessionV1.WithParts>(data.scope, {
          onIdle: status.set(sessionID, { type: "idle" }),
          onBusy: status.set(sessionID, { type: "busy" }),
          onInterrupt,
          onRetire: () => retire(data, sessionID, created),
        })
        created = { runner, uses: 1 }
        data.runners.set(sessionID, created)
        return created
      })
      return { data, entry } satisfies Lease
    })

    const release = (sessionID: SessionID, lease: Lease) =>
      Effect.sync(() => {
        if (!lease.entry) return
        if (lease.entry.uses <= 0) throw new Error(`Runner lease underflow: ${sessionID}`)
        lease.entry.uses -= 1
        retire(lease.data, sessionID, lease.entry)
      })

    const use = <A, E, R>(
      sessionID: SessionID,
      onInterrupt: Effect.Effect<SessionV1.WithParts>,
      create: boolean,
      operation: (entry: Entry | undefined) => Effect.Effect<A, E, R>,
    ) =>
      Effect.acquireUseRelease(
        acquire(sessionID, onInterrupt, create),
        (lease) => operation(lease.entry),
        (lease) => release(sessionID, lease),
      )

    const assertNotBusy = Effect.fn("SessionRunState.assertNotBusy")(function* (sessionID: SessionID) {
      const data = yield* InstanceState.get(state)
      const existing = data.runners.get(sessionID)
      if (existing?.runner.busy) yield* busyError(sessionID)
    })

    const cancel = Effect.fn("SessionRunState.cancel")(function* (sessionID: SessionID) {
      yield* cancelBackgroundJobs(background, sessionID)
      yield* use(sessionID, Effect.die(new Error("Cancel cannot create a Runner")), false, (entry) =>
        entry ? entry.runner.cancel : status.set(sessionID, { type: "idle" }),
      )
    })

    const ensureRunning = Effect.fn("SessionRunState.ensureRunning")(function* (
      sessionID: SessionID,
      onInterrupt: Effect.Effect<SessionV1.WithParts>,
      work: Effect.Effect<SessionV1.WithParts>,
    ) {
      return yield* use(sessionID, onInterrupt, true, (entry) => entry!.runner.ensureRunning(work))
    })

    const commit = Effect.fn("SessionRunState.commit")(function* <B, E, R>(
      sessionID: SessionID,
      onInterrupt: Effect.Effect<SessionV1.WithParts>,
      work: Effect.Effect<B, E, R>,
    ) {
      return yield* use(sessionID, onInterrupt, true, (entry) =>
        entry!.runner.commit(work).pipe(Effect.catchTag("RunnerBusy", () => Effect.fail(busyError(sessionID)))),
      )
    })

    const submit = Effect.fn("SessionRunState.submit")(function* <B, E, R>(
      sessionID: SessionID,
      onInterrupt: Effect.Effect<SessionV1.WithParts>,
      admission: Effect.Effect<B, E, R>,
      work: Effect.Effect<SessionV1.WithParts>,
    ) {
      return yield* use(sessionID, onInterrupt, true, (entry) => entry!.runner.submit(admission, work))
    })

    const startShell = Effect.fn("SessionRunState.startShell")(function* (
      sessionID: SessionID,
      onInterrupt: Effect.Effect<SessionV1.WithParts>,
      work: Effect.Effect<SessionV1.WithParts>,
      ready?: Latch.Latch,
    ) {
      return yield* use(sessionID, onInterrupt, true, (entry) =>
        entry!.runner
          .startShell(work, ready)
          .pipe(Effect.catchTag("RunnerBusy", () => Effect.fail(busyError(sessionID)))),
      )
    })

    return Service.of({ assertNotBusy, cancel, commit, submit, ensureRunning, startShell })
  }),
)

export const defaultLayer = layer.pipe(
  Layer.provide(BackgroundJob.defaultLayer),
  Layer.provide(SessionStatus.defaultLayer),
)

const cancelBackgroundJobs = Effect.fn("SessionRunState.cancelBackgroundJobs")(function* (
  background: BackgroundJob.Interface,
  sessionID: SessionID,
) {
  const jobs = yield* background.list()
  const pending = new Set<string>([sessionID])
  const cancelled = new Set<string>()
  const matches = (job: BackgroundJob.Info) => {
    if (job.status !== "running") return false
    if (cancelled.has(job.id)) return false
    if (pending.has(job.id)) return true
    if (typeof job.metadata?.sessionId === "string" && pending.has(job.metadata.sessionId)) return true
    return typeof job.metadata?.parentSessionId === "string" && pending.has(job.metadata.parentSessionId)
  }
  let batch = jobs.filter(matches)
  while (batch.length > 0) {
    yield* Effect.forEach(
      batch,
      (job) =>
        background.cancel(job.id).pipe(
          Effect.tap(() =>
            Effect.sync(() => {
              cancelled.add(job.id)
              pending.add(job.id)
              if (typeof job.metadata?.sessionId === "string") pending.add(job.metadata.sessionId)
            }),
          ),
        ),
      { concurrency: "unbounded", discard: true },
    )
    batch = jobs.filter(matches)
  }
})

function busyError(sessionID: SessionID) {
  return new Session.BusyError({ sessionID })
}

export * as SessionRunState from "./run-state"
