import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { InstanceState } from "@/effect/instance-state"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { WorkspaceV2 } from "@opencode-ai/core/workspace"
import { Runner } from "@/effect/runner"
import { BackgroundJob } from "@/background/job"
import { Context, Deferred, Effect, Exit, Layer, Scope, Semaphore } from "effect"
import { Session } from "./session"
import { SessionID } from "./schema"
import { SessionStatus } from "./status"
import { SessionStagedContext } from "./staged-context"
import { NotFoundError } from "@/storage/storage"

type Admission = {
  readonly id: number
  readonly sessionID?: SessionID
  readonly ancestors: Set<SessionID>
  readonly workspaceIDs: ReadonlySet<WorkspaceV2.ID>
  readonly done: Deferred.Deferred<void>
}

type AdmissionMode = "existing" | "new"
type RunnerBusyError = { readonly _tag: "RunnerBusy" }

type State = {
  readonly runners: Map<SessionID, Runner.Runner<SessionV1.WithParts>>
  readonly scope: Scope.Scope
  readonly gate: Semaphore.Semaphore
  readonly closeGate: Semaphore.Semaphore
  readonly admissions: Map<number, Admission>
  readonly closingRoots: Set<SessionID>
  readonly closedRoots: Set<SessionID>
  readonly removalRoots: Set<SessionID>
  readonly removalClosures: Map<SessionID, readonly SessionID[]>
  readonly removalWorkspaces: Map<SessionID, WorkspaceV2.ID | undefined>
  readonly removalCompletions: Map<SessionID, Deferred.Deferred<void, unknown>>
  readonly closingWorkspaces: Set<WorkspaceV2.ID>
  readonly closedWorkspaces: Set<WorkspaceV2.ID>
  closed: boolean
  nextAdmission: number
}

export interface Interface {
  readonly successorPending: (sessionID: SessionID) => Effect.Effect<boolean>
  readonly admit: <A, E, R>(sessionID: SessionID, work: Effect.Effect<A, E, R>) => Effect.Effect<A, E | Session.NotFound | Session.BusyError, R>
  readonly create: (input?: Session.CreateInput) => Effect.Effect<Session.Info, Session.NotFound | Session.BusyError>
  readonly fork: (input: { sessionID: SessionID; messageID?: import("./schema").MessageID }) => Effect.Effect<Session.Info, Session.NotFound | Session.BusyError>
  readonly cancel: (sessionID: SessionID) => Effect.Effect<void, Session.BusyError>
  readonly remove: (sessionID: SessionID) => Effect.Effect<void, Session.NotFound | Session.BusyError>
  readonly removeWorkspace: <A, E, R>(
    workspaceID: WorkspaceV2.ID,
    stop: Effect.Effect<void, E, R>,
    remove: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E, R>
  readonly commit: <B, E, R>(sessionID: SessionID, work: Effect.Effect<B, E, R>) => Effect.Effect<B, Exclude<E, RunnerBusyError> | Session.NotFound | Session.BusyError, R>
  readonly submit: <B, E, R>(
    sessionID: SessionID,
    onInterrupt: Effect.Effect<SessionV1.WithParts>,
    admission: Effect.Effect<B, E, R>,
    work: Effect.Effect<SessionV1.WithParts>,
  ) => Effect.Effect<SessionV1.WithParts, Exclude<E, RunnerBusyError> | Session.NotFound | Session.BusyError, R>
  readonly submitManual: Interface["submit"]
  readonly submitPrepared: <B, E, R>(
    parentSessionID: SessionID,
    sessionID: SessionID,
    onInterrupt: Effect.Effect<SessionV1.WithParts>,
    admission: Effect.Effect<B, E, R>,
    work: Effect.Effect<SessionV1.WithParts>,
  ) => Effect.Effect<SessionV1.WithParts, Exclude<E, RunnerBusyError> | Session.NotFound | Session.BusyError, R>
  readonly submitAdmitted: <B, E, R>(
    sessionID: SessionID,
    onInterrupt: Effect.Effect<SessionV1.WithParts>,
    admission: Effect.Effect<B, E, R>,
    work: Effect.Effect<SessionV1.WithParts>,
  ) => Effect.Effect<B, Exclude<E, RunnerBusyError> | Session.NotFound | Session.BusyError, R>
  readonly submitManualAdmitted: Interface["submitAdmitted"]
  readonly ensureRunning: (
    sessionID: SessionID,
    onInterrupt: Effect.Effect<SessionV1.WithParts>,
    work: Effect.Effect<SessionV1.WithParts>,
  ) => Effect.Effect<SessionV1.WithParts, Session.NotFound | Session.BusyError>
  readonly startShell: (
    sessionID: SessionID,
    onInterrupt: Effect.Effect<SessionV1.WithParts>,
    work: Effect.Effect<SessionV1.WithParts>,
  ) => Effect.Effect<SessionV1.WithParts, Session.NotFound | Session.BusyError>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SessionLifecycle") {}

const AdmissionOwner = Context.Reference<number | undefined>("@opencode/SessionLifecycle/AdmissionOwner", {
  defaultValue: () => undefined,
})

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const background = yield* BackgroundJob.Service
    const status = yield* SessionStatus.Service
    const sessions = yield* Session.Service
    const stagedContext = yield* SessionStagedContext.Service

    const state = yield* InstanceState.make(
      Effect.fn("SessionLifecycle.state")(function* () {
        const data: State = {
          runners: new Map(),
          scope: yield* Scope.Scope,
          gate: Semaphore.makeUnsafe(1),
          closeGate: Semaphore.makeUnsafe(1),
          admissions: new Map(),
          closingRoots: new Set(),
          closedRoots: new Set(),
          removalRoots: new Set(),
          removalClosures: new Map(),
          removalWorkspaces: new Map(),
          removalCompletions: new Map(),
          closingWorkspaces: new Set(),
          closedWorkspaces: new Set(),
          closed: false,
          nextAdmission: 0,
        }
        yield* Effect.addFinalizer(() => closeInstance(data))
        return data
      }),
    )

    const ancestry = Effect.fn("SessionLifecycle.ancestry")(function* (sessionID: SessionID) {
      const ancestors = new Set<SessionID>()
      let current: SessionID | undefined = sessionID
      let workspaceID: WorkspaceV2.ID | undefined
      while (current) {
        if (ancestors.has(current)) return yield* Effect.die(new Error(`Session ancestry cycle detected at ${current}`))
        ancestors.add(current)
        const info: Session.Info = yield* sessions.get(current)
        workspaceID ??= info.workspaceID
        current = info.parentID
      }
      return { ancestors, workspaceID }
    })

    const beginAdmission = Effect.fn("SessionLifecycle.beginAdmission")(function* (
      data: State,
      sessionID?: SessionID,
      workspaceID?: WorkspaceV2.ID,
      mode: AdmissionMode = "existing",
      closeRoot = false,
    ) {
      return yield* data.gate.withPermits(1)(
        Effect.gen(function* () {
          if (data.closed) return yield* busyError(sessionID)
          const owner = !sessionID || mode === "new"
            ? {
                ancestors: new Set<SessionID>(),
                workspaceIDs: new Set(
                  [workspaceID].filter((id): id is WorkspaceV2.ID => id !== undefined),
                ),
              }
            : yield* ancestry(sessionID).pipe(
                Effect.map((current) => ({
                  ancestors: current.ancestors,
                  workspaceIDs: new Set(
                    [current.workspaceID, workspaceID].filter(
                      (id): id is WorkspaceV2.ID => id !== undefined,
                    ),
                  ),
                })),
              )
          const blockedRoot = Array.from(owner.ancestors).find(
            (id) => data.closingRoots.has(id) || data.closedRoots.has(id),
          )
          if (blockedRoot) return yield* busyError(blockedRoot)
          const blockedWorkspace = Array.from(owner.workspaceIDs).find(
            (id) => data.closingWorkspaces.has(id) || data.closedWorkspaces.has(id),
          )
          if (blockedWorkspace) return yield* busyError(sessionID)
          const admission: Admission = {
            id: ++data.nextAdmission,
            sessionID,
            ancestors: owner.ancestors,
            workspaceIDs: owner.workspaceIDs,
            done: Deferred.makeUnsafe<void>(),
          }
          data.admissions.set(admission.id, admission)
          if (closeRoot && sessionID) data.closingRoots.add(sessionID)
          return admission
        }),
      )
    })

    const endAdmission = (data: State, admission: Admission, reopenRoot?: SessionID) =>
      data.gate.withPermits(1)(
        Effect.gen(function* () {
          if (reopenRoot) data.closingRoots.delete(reopenRoot)
          if (data.admissions.get(admission.id) !== admission) return
          data.admissions.delete(admission.id)
          yield* Deferred.succeed(admission.done, undefined).pipe(Effect.asVoid)
        }),
      )

    const admit = <A, E, R>(sessionID: SessionID, work: Effect.Effect<A, E, R>) =>
      Effect.acquireUseRelease(
        InstanceState.get(state).pipe(Effect.flatMap((data) => beginAdmission(data, sessionID))),
        (admission) => work.pipe(Effect.provideService(AdmissionOwner, admission.id)),
        (admission) => InstanceState.get(state).pipe(Effect.flatMap((data) => endAdmission(data, admission))),
      )

    const admitNewSession = <A, E, R>(
      sessionID: SessionID,
      workspaceID: WorkspaceV2.ID | undefined,
      work: Effect.Effect<A, E, R>,
    ) =>
      Effect.acquireUseRelease(
        InstanceState.get(state).pipe(
          Effect.flatMap((data) => beginAdmission(data, sessionID, workspaceID, "new")),
        ),
        (admission) => work.pipe(Effect.provideService(AdmissionOwner, admission.id)),
        (admission) => InstanceState.get(state).pipe(Effect.flatMap((data) => endAdmission(data, admission))),
      )

    const runner = Effect.fn("SessionLifecycle.runner")(function* (sessionID: SessionID) {
      const data = yield* InstanceState.get(state)
      const existing = data.runners.get(sessionID)
      if (existing) return existing
      const next = Runner.make<SessionV1.WithParts>(data.scope, { onIdle: status.set(sessionID, { type: "idle" }) })
      data.runners.set(sessionID, next)
      return next
    })

    const successorPending = Effect.fn("SessionLifecycle.successorPending")(function* (sessionID: SessionID) {
      const data = yield* InstanceState.get(state)
      return data.runners.get(sessionID) ? yield* data.runners.get(sessionID)!.successorPending : false
    })

    const settleTree = Effect.fn("SessionLifecycle.settleTree")(function* (
      data: State,
      sessionID: SessionID,
      mode: Runner.StopMode,
      options?: {
        readonly admissionAlreadyClosed?: boolean
        readonly removalWorkspaceID?: WorkspaceV2.ID
        readonly ownerAdmissionID?: number
        readonly retainClosingRoot?: boolean
      },
    ) {
      const leases = new Map<SessionID, [Runner.Runner<SessionV1.WithParts>, Runner.StopLease]>()
      const settledResources = new Set<SessionID>()
      const finalize = (exit: Exit.Exit<unknown, unknown>) =>
        Effect.forEach(
          Array.from(leases.entries()),
          ([id, [current, lease]]) => lease.owner
            ? Effect.gen(function* () {
                if (!settledResources.has(id)) yield* current.settleStop(lease).pipe(Effect.exit)
                yield* current.commitStop(lease, voidExit(exit))
              })
            : Effect.void,
          { concurrency: "unbounded", discard: true },
        ).pipe(
          Effect.ensuring(
            data.gate.withPermits(1)(
              Effect.sync(() => {
                if (Exit.isFailure(exit) && mode === "Remove") return
                if (!options?.retainClosingRoot) data.closingRoots.delete(sessionID)
                if (mode === "Remove") {
                  data.removalRoots.delete(sessionID)
                  data.removalClosures.delete(sessionID)
                  data.removalWorkspaces.delete(sessionID)
                }
                if (Exit.isSuccess(exit) && mode !== "Cancel") data.closedRoots.add(sessionID)
              }),
            ),
          ),
        )

      return yield* Effect.gen(function* () {
        const retainedPostorder = data.removalClosures.get(sessionID)
        const sessionInfo = retainedPostorder
          ? undefined
          : yield* sessions.get(sessionID)
        yield* data.gate.withPermits(1)(
          Effect.gen(function* () {
            if (data.removalRoots.has(sessionID) && mode !== "Remove") return yield* busyError(sessionID)
            if (!options?.admissionAlreadyClosed) data.closingRoots.add(sessionID)
            if (mode === "Remove") {
              data.removalRoots.add(sessionID)
              data.removalWorkspaces.set(sessionID, options?.removalWorkspaceID ?? sessionInfo?.workspaceID)
            }
          }),
        )

        const rootRunner = data.runners.get(sessionID)
        if (rootRunner) {
          const lease = yield* rootRunner.quiesce(mode)
          if (!lease.satisfied) leases.set(sessionID, [rootRunner, lease])
        }
        yield* cancelBackgroundClosure(background, new Set([sessionID]))
        yield* settleLeaseResources([sessionID], leases, settledResources).pipe(Effect.orDie)
        const admitted = yield* data.gate.withPermits(1)(
          Effect.sync(() =>
            Array.from(data.admissions.values())
              .filter((item) => item.id !== options?.ownerAdmissionID && item.ancestors.has(sessionID))
              .map((item) => item.done),
          ),
        )
        yield* Effect.forEach(admitted, Deferred.await, { concurrency: "unbounded", discard: true })

        if (!leases.has(sessionID)) {
          const current = data.runners.get(sessionID)
          if (current) {
            const lease = yield* current.quiesce(mode)
            if (!lease.satisfied) leases.set(sessionID, [current, lease])
          }
        }

        const postorder = retainedPostorder ? [...retainedPostorder] : []
        if (!retainedPostorder) {
          const visit: (id: SessionID) => Effect.Effect<void> = Effect.fnUntraced(function* (id: SessionID) {
            const children = (yield* sessions.children(id)).toSorted((left, right) => left.id.localeCompare(right.id))
            for (const child of children) yield* visit(child.id)
            postorder.push(id)
          })
          yield* visit(sessionID)
          if (mode === "Remove") data.removalClosures.set(sessionID, [...postorder])
        }
        yield* cancelBackgroundClosure(background, new Set(postorder))

        for (const id of postorder) {
          if (id === sessionID) continue
          const current = data.runners.get(id)
          if (!current) continue
          const lease = yield* current.quiesce(mode)
          if (!lease.satisfied) leases.set(id, [current, lease])
        }
        yield* settleLeaseResources(postorder, leases, settledResources).pipe(Effect.orDie)

        if (mode === "Remove") {
          yield* Effect.forEach(
            postorder,
            (id) =>
              sessions.removeLeaf(id).pipe(
                Effect.catchIf(NotFoundError.isInstance, () => Effect.void),
                Effect.andThen(stagedContext.retire({ sessionID: id })),
              ),
            { concurrency: 1, discard: true },
          )
        }
      }).pipe(Effect.onExit((exit) => finalize(exit).pipe(Effect.orDie)))
    })

    const serializedTreeSettlement = (sessionID: SessionID, mode: Runner.StopMode) =>
      Effect.gen(function* () {
        const data = yield* InstanceState.get(state)
        const ownerAdmissionID = yield* AdmissionOwner
        yield* data.closeGate.withPermits(1)(settleTree(data, sessionID, mode, { ownerAdmissionID }))
      })

    const cancel = Effect.fn("SessionLifecycle.cancel")(function* (sessionID: SessionID) {
      const data = yield* InstanceState.get(state)
      const covered = yield* data.gate.withPermits(1)(
        ancestry(sessionID).pipe(
          Effect.map((owner) => Array.from(owner.ancestors).some((id) => data.closingRoots.has(id))),
          Effect.catchIf(NotFoundError.isInstance, () => status.set(sessionID, { type: "idle" }).pipe(Effect.as(true))),
        ),
      )
      if (covered) return
      yield* serializedTreeSettlement(sessionID, "Cancel").pipe(
        Effect.catchIf(NotFoundError.isInstance, () => status.set(sessionID, { type: "idle" })),
      )
    })

    const remove = Effect.fn("SessionLifecycle.remove")(function* (sessionID: SessionID) {
      const data = yield* InstanceState.get(state)
      const completion = yield* Deferred.make<void, unknown>()
      const removal = yield* data.gate.withPermits(1)(
        Effect.sync(() => {
          const existing = data.removalCompletions.get(sessionID)
          if (existing) return { owner: false as const, completion: existing }
          data.removalCompletions.set(sessionID, completion)
          return { owner: true as const, completion }
        }),
      )
      if (!removal.owner) return yield* Deferred.await(removal.completion).pipe(Effect.orDie)
      const exit = yield* serializedTreeSettlement(sessionID, "Remove").pipe(Effect.exit)
      yield* data.gate.withPermits(1)(
        Effect.sync(() => {
          if (data.removalCompletions.get(sessionID) === completion) data.removalCompletions.delete(sessionID)
        }),
      )
      yield* Deferred.done(completion, exit).pipe(Effect.asVoid)
      return yield* exit
    })

    const create: Interface["create"] = (input) =>
      Effect.gen(function* () {
        const prepared = yield* sessions.prepare(input)
        if (prepared.parentID) return yield* admit(prepared.parentID, sessions.createPrepared(prepared))
        return yield* admitNewSession(prepared.id, prepared.workspaceID, sessions.createPrepared(prepared))
      })

    const ownAdmissionSession = (data: State, admission: Admission, sessionID: SessionID) =>
      data.gate.withPermits(1)(
        Effect.gen(function* () {
          if (data.closed || data.closingRoots.has(sessionID) || data.closedRoots.has(sessionID)) {
            return yield* busyError(sessionID)
          }
          if (data.admissions.get(admission.id) !== admission) return yield* busyError(sessionID)
          admission.ancestors.add(sessionID)
        }),
      )

    const fork: Interface["fork"] = (input) =>
      Effect.acquireUseRelease(
        InstanceState.get(state).pipe(Effect.flatMap((data) => beginAdmission(data, input.sessionID))),
        (admission) =>
          Effect.gen(function* () {
            const prepared = yield* sessions.prepareFork(input)
            const data = yield* InstanceState.get(state)
            yield* ownAdmissionSession(data, admission, prepared.info.id)
            return yield* sessions.forkPrepared(prepared)
          }).pipe(Effect.provideService(AdmissionOwner, admission.id)),
        (admission) => InstanceState.get(state).pipe(Effect.flatMap((data) => endAdmission(data, admission))),
      )

    const removeWorkspace: Interface["removeWorkspace"] = (workspaceID, stop, removeEffect) =>
      Effect.gen(function* () {
        const data = yield* InstanceState.get(state)
        const ownerAdmissionID = yield* AdmissionOwner
        return yield* data.closeGate.withPermits(1)(
          Effect.gen(function* () {
              yield* data.gate.withPermits(1)(Effect.sync(() => data.closingWorkspaces.add(workspaceID)))
              yield* stop
              const admitted = yield* data.gate.withPermits(1)(
                Effect.sync(() =>
                  Array.from(data.admissions.values())
                    .filter((item) => item.id !== ownerAdmissionID && item.workspaceIDs.has(workspaceID))
                    .map((item) => item.done),
                ),
              )
              yield* Effect.forEach(admitted, Deferred.await, { concurrency: "unbounded", discard: true })
              const roots = (yield* sessions.list({ workspaceID, roots: true })).toSorted((left, right) =>
                left.id.localeCompare(right.id),
              )
              const rootIDs = new Set(roots.map((root) => root.id))
              for (const [rootID, ownerWorkspaceID] of data.removalWorkspaces) {
                if (ownerWorkspaceID === workspaceID) rootIDs.add(rootID)
              }
              for (const rootID of Array.from(rootIDs).toSorted()) {
                yield* settleTree(data, rootID, "Remove", {
                  admissionAlreadyClosed: true,
                  removalWorkspaceID: workspaceID,
                }).pipe(Effect.orDie)
              }
              const result = yield* removeEffect
              yield* data.gate.withPermits(1)(
                Effect.sync(() => {
                  data.closingWorkspaces.delete(workspaceID)
                  data.closedWorkspaces.add(workspaceID)
                }),
              )
              return result
            }).pipe(
              Effect.onExit((exit) =>
                Exit.isSuccess(exit)
                  ? Effect.void
                  : data.gate.withPermits(1)(Effect.sync(() => data.closingWorkspaces.delete(workspaceID))),
              ),
            ),
        )
      })

    const ensureRunning: Interface["ensureRunning"] = (sessionID, onInterrupt, work) =>
      admit(
        sessionID,
        runner(sessionID).pipe(
          Effect.flatMap((current) => current.ensureRunning(work, onInterrupt)),
          mapRunnerBusy(sessionID),
        ),
      )

    const commit: Interface["commit"] = (sessionID, work) =>
      admit(
        sessionID,
        runner(sessionID).pipe(
          Effect.flatMap((current) => current.commit(work)),
          mapRunnerBusy(sessionID),
        ),
      )

    const submit: Interface["submit"] = (sessionID, onInterrupt, admission, work) =>
      admit(
        sessionID,
        runner(sessionID).pipe(
          Effect.flatMap((current) => current.submit(admission, work, onInterrupt)),
          mapRunnerBusy(sessionID),
        ),
      )

    const submitManual: Interface["submitManual"] = (sessionID, onInterrupt, admission, work) =>
      admit(
        sessionID,
        runner(sessionID).pipe(
          Effect.flatMap((current) => current.submitManual(admission, work, onInterrupt)),
          mapRunnerBusy(sessionID),
        ),
      )

    const submitPrepared: Interface["submitPrepared"] = (
      parentSessionID,
      sessionID,
      onInterrupt,
      admission,
      work,
    ) =>
      Effect.acquireUseRelease(
        InstanceState.get(state).pipe(Effect.flatMap((data) => beginAdmission(data, parentSessionID))),
        (parentAdmission) =>
          Effect.gen(function* () {
            const data = yield* InstanceState.get(state)
            yield* ownAdmissionSession(data, parentAdmission, sessionID)
            return yield* runner(sessionID).pipe(
              Effect.flatMap((current) => current.submit(admission, work, onInterrupt)),
              mapRunnerBusy(sessionID),
            )
          }).pipe(Effect.provideService(AdmissionOwner, parentAdmission.id)),
        (parentAdmission) =>
          InstanceState.get(state).pipe(Effect.flatMap((data) => endAdmission(data, parentAdmission))),
      )

    const submitAdmitted: Interface["submitAdmitted"] = (sessionID, onInterrupt, admission, work) =>
      admit(
        sessionID,
        runner(sessionID).pipe(
          Effect.flatMap((current) => current.submitAdmitted(admission, work, onInterrupt)),
          mapRunnerBusy(sessionID),
        ),
      )

    const submitManualAdmitted: Interface["submitManualAdmitted"] = (sessionID, onInterrupt, admission, work) =>
      admit(
        sessionID,
        runner(sessionID).pipe(
          Effect.flatMap((current) => current.submitManualAdmitted(admission, work, onInterrupt)),
          mapRunnerBusy(sessionID),
        ),
      )

    const startShell: Interface["startShell"] = (sessionID, onInterrupt, work) =>
      admit(
        sessionID,
        runner(sessionID).pipe(
          Effect.flatMap((current) => current.startShell(work, onInterrupt)),
          mapRunnerBusy(sessionID),
        ),
      )

    function closeInstance(data: State) {
      const leases = new Map<SessionID, [Runner.Runner<SessionV1.WithParts>, Runner.StopLease]>()
      const settledResources = new Set<SessionID>()
      const finalize = (exit: Exit.Exit<unknown, unknown>) =>
        Effect.forEach(
          Array.from(leases.entries()),
          ([id, [current, lease]]) => lease.owner
            ? Effect.gen(function* () {
                if (!settledResources.has(id)) yield* current.settleStop(lease).pipe(Effect.exit)
                yield* current.commitStop(lease, voidExit(exit))
              })
            : Effect.void,
          { concurrency: "unbounded", discard: true },
        )
      const close = Effect.gen(function* () {
          for (const [id, current] of Array.from(data.runners.entries())) {
            const lease = yield* current.quiesce("Close")
            if (!lease.satisfied) leases.set(id, [current, lease])
          }
          yield* background.close()
           yield* settleLeaseResources(Array.from(leases.keys()), leases, settledResources).pipe(Effect.orDie)
          const admitted = yield* data.gate.withPermits(1)(
            Effect.sync(() => Array.from(data.admissions.values()).map((item) => item.done)),
          )
          yield* Effect.forEach(admitted, Deferred.await, { concurrency: "unbounded", discard: true })
          const late: SessionID[] = []
          for (const [id, current] of Array.from(data.runners.entries())) {
            if (leases.has(id)) continue
            const lease = yield* current.quiesce("Close")
            if (lease.satisfied) continue
            leases.set(id, [current, lease])
            late.push(id)
          }
          yield* settleLeaseResources(late, leases, settledResources).pipe(Effect.orDie)
          data.runners.clear()
        }).pipe(Effect.onExit((exit) => finalize(exit).pipe(Effect.orDie)))
      return Effect.uninterruptible(
        Effect.gen(function* () {
          yield* data.gate.withPermits(1)(Effect.sync(() => { data.closed = true }))
          yield* data.closeGate.withPermits(1)(close)
        }),
      )
    }

    return Service.of({
      successorPending,
      admit,
      create,
      fork,
      cancel,
      remove,
      removeWorkspace,
      commit,
      submit,
      submitManual,
      submitPrepared,
      submitAdmitted,
      submitManualAdmitted,
      ensureRunning,
      startShell,
    })
  }),
)

const settleLeaseResources = Effect.fnUntraced(function* (
  order: readonly SessionID[],
  leases: ReadonlyMap<SessionID, [Runner.Runner<SessionV1.WithParts>, Runner.StopLease]>,
  settled: Set<SessionID>,
) {
  const exits = yield* Effect.forEach(
    order,
    (id) => {
      const item = leases.get(id)
      if (!item) return Effect.succeed(Exit.void)
      return (item[1].owner
        ? item[0].settleStop(item[1]).pipe(Effect.tap(() => Effect.sync(() => settled.add(id))))
        : item[0].awaitStop(item[1])).pipe(Effect.exit)
    },
    { concurrency: 1 },
  )
  for (const exit of exits) {
    if (Exit.isFailure(exit)) return yield* Effect.failCause(exit.cause)
  }
})

function voidExit(exit: Exit.Exit<unknown, unknown>): Exit.Exit<void, unknown> {
  return Exit.isSuccess(exit) ? Exit.void : Exit.failCause(exit.cause)
}

const cancelBackgroundClosure = Effect.fn("SessionLifecycle.cancelBackgroundClosure")(function* (
  background: BackgroundJob.Interface,
  closure: ReadonlySet<SessionID>,
) {
  const jobs = yield* background.list()
  const reached = new Set<string>(closure)
  const selected = new Set<string>()
  for (const job of jobs) {
    const child = typeof job.metadata?.sessionId === "string" ? job.metadata.sessionId : undefined
    const parent = typeof job.metadata?.parentSessionId === "string" ? job.metadata.parentSessionId : undefined
    if (!reached.has(job.id) && (!child || !reached.has(child)) && (!parent || !reached.has(parent))) continue
    selected.add(job.id)
    reached.add(job.id)
    if (child) reached.add(child)
    if (parent) reached.add(parent)
  }
  yield* Effect.forEach(Array.from(selected).toSorted(), (id) => background.cancel(id), {
    concurrency: 1,
    discard: true,
  })
})

function busyError(sessionID?: SessionID) {
  return new Session.BusyError({ sessionID: sessionID ?? SessionID.make("ses_lifecycle") })
}

function mapRunnerBusy(sessionID: SessionID) {
  return <A, E, R>(self: Effect.Effect<A, E | Runner.Busy, R>) =>
    self.pipe(Effect.catchTag("RunnerBusy", () => Effect.fail(busyError(sessionID))))
}

export const node = LayerNode.make({
  service: Service,
  layer,
  deps: [BackgroundJob.node, Session.node, SessionStatus.node, SessionStagedContext.node],
})

export * as SessionLifecycle from "./lifecycle"
