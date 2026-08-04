import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { makeGlobalNode, Node } from "@opencode-ai/core/effect/app-node"
import { GlobalBus } from "@/bus/global"
import { serviceUse } from "@opencode-ai/core/effect/service-use"
import { WorkspaceContext } from "@/control-plane/workspace-context"
import { InstanceRef } from "@/effect/instance-ref"
import { disposeInstance as runDisposers } from "@/effect/instance-registry"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { Cause, Context, Deferred, Effect, Exit, Layer, Scope, Semaphore } from "effect"
import { type InstanceContext } from "./instance-context"
import { InstanceBootstrap } from "./bootstrap-service"
import * as Project from "./project"

export interface LoadInput {
  directory: string
  worktree?: string
  project?: Project.Info
}

export interface Interface {
  readonly load: (input: LoadInput) => Effect.Effect<InstanceContext>
  readonly reload: (input: LoadInput) => Effect.Effect<InstanceContext>
  readonly dispose: (ctx: InstanceContext) => Effect.Effect<void>
  readonly disposeDirectory: (directory: string) => Effect.Effect<void>
  readonly disposeAll: () => Effect.Effect<void>
  readonly provide: <A, E, R>(input: LoadInput, effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/InstanceStore") {}

export const use = serviceUse(Service)

interface Entry {
  readonly deferred: Deferred.Deferred<InstanceContext>
  context?: InstanceContext
  disposal?: Deferred.Deferred<void, unknown>
}

type Lifecycle =
  | { readonly _tag: "Open" }
  | { readonly _tag: "Disposing"; readonly completion: Deferred.Deferred<void, unknown> }
  | { readonly _tag: "Closed" }
  | { readonly _tag: "Failed"; readonly completion: Deferred.Deferred<void, unknown> }

const layer: Layer.Layer<Service, never, Project.Service | InstanceBootstrap.Service> = Layer.effect(
  Service,
  Effect.gen(function* () {
    const project = yield* Project.Service
    const bootstrap = yield* InstanceBootstrap.Service
    const scope = yield* Scope.Scope
    const cache = new Map<string, Entry>()
    const gate = Semaphore.makeUnsafe(1)
    let lifecycle: Lifecycle = { _tag: "Open" }
    const locked = gate.withPermits(1)

    const boot = (input: LoadInput & { directory: string }) =>
      Effect.gen(function* () {
        const ctx: InstanceContext =
          input.project && input.worktree
            ? {
                directory: input.directory,
                worktree: input.worktree,
                project: input.project,
              }
            : yield* project.fromDirectory(input.directory).pipe(
                Effect.map((result) => ({
                  directory: input.directory,
                  worktree: result.sandbox,
                  project: result.project,
                })),
              )
        yield* bootstrap.run.pipe(Effect.provideService(InstanceRef, ctx))
        return ctx
      }).pipe(Effect.withSpan("InstanceStore.boot"))

    const completeLoad = (directory: string, input: LoadInput, entry: Entry) =>
      Effect.gen(function* () {
        const exit = yield* Effect.exit(boot({ ...input, directory }))
        yield* locked(Effect.sync(() => {
          if (cache.get(directory) !== entry) return
          if (Exit.isFailure(exit)) cache.delete(directory)
          else entry.context = exit.value
        }))
        yield* Deferred.done(entry.deferred, exit).pipe(Effect.asVoid)
      })

    const emitDisposed = (input: { directory: string; project?: string }) =>
      Effect.sync(() =>
        GlobalBus.emit("event", {
          directory: input.directory,
          project: input.project,
          workspace: WorkspaceContext.workspaceID,
          payload: {
            type: "server.instance.disposed",
            properties: {
              directory: input.directory,
            },
          },
        }),
      )

    const disposeContext = Effect.fn("InstanceStore.disposeContext")(function* (ctx: InstanceContext) {
      yield* Effect.logInfo("disposing instance", { directory: ctx.directory })
      yield* Effect.promise(() => runDisposers(ctx.directory))
    })

    const claimDisposal = (entry: Entry) => {
      const existing = entry.disposal
      if (existing) return { owner: false as const, completion: existing }
      const disposal = Deferred.makeUnsafe<void, unknown>()
      entry.disposal = disposal
      return { owner: true as const, completion: disposal }
    }

    const awaitDisposal = (completion: Deferred.Deferred<void, unknown>) =>
      Deferred.await(completion).pipe(Effect.orDie)

    const failStore = (cause: Cause.Cause<unknown>) =>
      locked(
        Effect.gen(function* () {
          if (lifecycle._tag === "Failed") return lifecycle.completion
          const completion = Deferred.makeUnsafe<void, unknown>()
          yield* Deferred.done(completion, Exit.failCause(cause)).pipe(Effect.asVoid)
          lifecycle = { _tag: "Failed", completion }
          return completion
        }),
      )

    const disposeEntry = Effect.fnUntraced(function* (
      directory: string,
      entry: Entry,
      claimed = claimDisposal(entry),
    ) {
      return yield* Effect.uninterruptible(
        Effect.gen(function* () {
          if (!claimed.owner) return yield* awaitDisposal(claimed.completion)
          const bootExit = yield* Deferred.await(entry.deferred).pipe(Effect.exit)
          if (Exit.isFailure(bootExit)) {
            yield* locked(Effect.sync(() => {
              if (cache.get(directory) === entry) cache.delete(directory)
            }))
            yield* Deferred.succeed(claimed.completion, undefined).pipe(Effect.asVoid)
            return
          }
          const exit = yield* Effect.gen(function* () {
            const ctx = bootExit.value
            yield* disposeContext(ctx)
            yield* emitDisposed({ directory: ctx.directory, project: ctx.project.id })
            yield* locked(Effect.sync(() => {
              if (cache.get(directory) !== entry) {
                throw new Error("Instance disposal lost its owned cache entry")
              }
              cache.delete(directory)
            }))
          }).pipe(Effect.exit)
          if (Exit.isFailure(exit)) {
            yield* failStore(exit.cause)
          }
          yield* Deferred.done(claimed.completion, exit).pipe(Effect.asVoid)
          if (Exit.isFailure(exit)) return yield* Effect.failCause(exit.cause)
        }),
      )
    })

    const load = (input: LoadInput): Effect.Effect<InstanceContext> => {
      const directory = FSUtil.resolve(input.directory)
      return Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          while (true) {
            const admission = yield* locked(Effect.sync(() => {
              if (lifecycle._tag === "Failed") {
                return { _tag: "Failure" as const, completion: lifecycle.completion }
              }
              if (lifecycle._tag === "Closed") return { _tag: "Closed" as const }
              if (lifecycle._tag === "Disposing") {
                return { _tag: "Lifecycle" as const, completion: lifecycle.completion }
              }
              const existing = cache.get(directory)
              if (existing?.disposal) return { _tag: "Disposal" as const, completion: existing.disposal }
              if (existing) return { _tag: "Existing" as const, entry: existing }
              const entry: Entry = { deferred: Deferred.makeUnsafe<InstanceContext>() }
              cache.set(directory, entry)
              return { _tag: "Boot" as const, entry }
            }))
            if (admission._tag === "Failure") {
              yield* restore(awaitDisposal(admission.completion))
              return yield* Effect.interrupt
            }
            if (admission._tag === "Closed") return yield* Effect.interrupt
            if (admission._tag === "Lifecycle" || admission._tag === "Disposal") {
              yield* restore(awaitDisposal(admission.completion))
              continue
            }
            if (admission._tag === "Existing") {
              const ctx = yield* restore(Deferred.await(admission.entry.deferred))
              const disposal = yield* locked(Effect.sync(() => admission.entry.disposal))
              if (!disposal) return ctx
              yield* restore(awaitDisposal(disposal))
              continue
            }

            yield* Effect.gen(function* () {
              yield* Effect.logInfo("creating instance", { directory: directory })
              yield* completeLoad(directory, input, admission.entry)
            }).pipe(Effect.forkIn(scope, { startImmediately: true }))
            const ctx = yield* restore(Deferred.await(admission.entry.deferred))
            const disposal = yield* locked(Effect.sync(() => admission.entry.disposal))
            if (!disposal) return ctx
            yield* restore(awaitDisposal(disposal))
          }
        }),
      ).pipe(Effect.withSpan("InstanceStore.load"))
    }

    const reload = (input: LoadInput): Effect.Effect<InstanceContext> => {
      const directory = FSUtil.resolve(input.directory)
      return Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          while (true) {
            const admission = yield* locked(Effect.sync(() => {
              if (lifecycle._tag === "Failed") {
                return { _tag: "Failure" as const, completion: lifecycle.completion }
              }
              if (lifecycle._tag === "Closed") return { _tag: "Closed" as const }
              if (lifecycle._tag === "Disposing") {
                return { _tag: "Lifecycle" as const, completion: lifecycle.completion }
              }
              const previous = cache.get(directory)
              return previous
                ? { _tag: "Entry" as const, entry: previous, claim: claimDisposal(previous) }
                : { _tag: "Empty" as const }
            }))
            if (admission._tag === "Failure") {
              yield* restore(awaitDisposal(admission.completion))
              return yield* Effect.interrupt
            }
            if (admission._tag === "Closed") return yield* Effect.interrupt
            if (admission._tag === "Lifecycle") {
              yield* restore(awaitDisposal(admission.completion))
              continue
            }
            if (admission._tag === "Entry") yield* restore(disposeEntry(directory, admission.entry, admission.claim))
            break
          }
          yield* Effect.logInfo("reloading instance", { directory: directory })
          return yield* restore(load({ ...input, directory }))
        }),
      ).pipe(Effect.withSpan("InstanceStore.reload"))
    }

    const dispose = Effect.fn("InstanceStore.dispose")(function* (ctx: InstanceContext) {
      const decision = yield* locked(Effect.sync(() => {
        if (lifecycle._tag === "Failed") {
          return { _tag: "Failure" as const, completion: lifecycle.completion }
        }
        if (lifecycle._tag === "Closed") return { _tag: "Stale" as const }
        const entry = cache.get(ctx.directory)
        if (!entry) {
          return lifecycle._tag === "Disposing"
            ? { _tag: "Lifecycle" as const, completion: lifecycle.completion }
            : { _tag: "Stale" as const }
        }
        if (entry.context !== ctx) return { _tag: "Stale" as const }
        return { _tag: "Entry" as const, entry, claim: claimDisposal(entry) }
      }))
      if (decision._tag === "Failure") return yield* awaitDisposal(decision.completion)
      if (decision._tag === "Lifecycle") return yield* awaitDisposal(decision.completion)
      if (decision._tag === "Stale") return
      yield* disposeEntry(ctx.directory, decision.entry, decision.claim)
    })

    const disposeDirectory = Effect.fn("InstanceStore.disposeDirectory")(function* (input: string) {
      const directory = FSUtil.resolve(input)
      const decision = yield* locked(Effect.sync(() => {
        if (lifecycle._tag === "Failed") {
          return { _tag: "Failure" as const, completion: lifecycle.completion }
        }
        if (lifecycle._tag === "Closed") return { _tag: "Empty" as const }
        const entry = cache.get(directory)
        if (entry) return { _tag: "Entry" as const, entry, claim: claimDisposal(entry) }
        return lifecycle._tag === "Disposing"
          ? { _tag: "Lifecycle" as const, completion: lifecycle.completion }
          : { _tag: "Empty" as const }
      }))
      if (decision._tag === "Failure") return yield* awaitDisposal(decision.completion)
      if (decision._tag === "Lifecycle") return yield* awaitDisposal(decision.completion)
      if (decision._tag === "Entry") yield* disposeEntry(directory, decision.entry, decision.claim)
    })

    const disposeAllWith = Effect.fn("InstanceStore.disposeAll")(function* (close: boolean) {
      return yield* Effect.uninterruptible(Effect.gen(function* () {
      const admission = yield* locked(Effect.sync(() => {
        if (lifecycle._tag === "Failed") {
          return { _tag: "Failed" as const, completion: lifecycle.completion }
        }
        if (lifecycle._tag === "Disposing") {
          return { _tag: "Joined" as const, completion: lifecycle.completion }
        }
        if (lifecycle._tag === "Closed") return { _tag: "Closed" as const }
        const completion = Deferred.makeUnsafe<void, unknown>()
        lifecycle = { _tag: "Disposing", completion }
        const entries = [...cache.entries()].toSorted(([left], [right]) => left.localeCompare(right))
        const claims = entries.map(([, entry]) => claimDisposal(entry))
        return { _tag: "Owner" as const, completion, entries, claims }
      }))
      if (admission._tag === "Failed") return yield* awaitDisposal(admission.completion)
      if (admission._tag === "Joined") return yield* awaitDisposal(admission.completion)
      if (admission._tag === "Closed") return

      yield* Effect.logInfo("disposing all instances")
      const exits = yield* Effect.forEach(
        admission.entries,
        (item, index) =>
          Effect.gen(function* () {
            yield* disposeEntry(item[0], item[1], admission.claims[index])
          }).pipe(Effect.exit),
        { concurrency: 1 },
      )
      const failure = exits.find(Exit.isFailure)
      const exit = failure ? Exit.failCause(failure.cause) : Exit.void
      yield* locked(Effect.gen(function* () {
        yield* Deferred.done(admission.completion, exit).pipe(Effect.asVoid)
        if (Exit.isSuccess(exit) && lifecycle._tag === "Disposing" && lifecycle.completion === admission.completion) {
          lifecycle = close ? { _tag: "Closed" } : { _tag: "Open" }
        }
      }))
      return yield* awaitDisposal(admission.completion)
      }))
    })

    const disposeAll = () => disposeAllWith(false)

    const provide = <A, E, R>(input: LoadInput, effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
      load(input).pipe(Effect.flatMap((ctx) => effect.pipe(Effect.provideService(InstanceRef, ctx))))

    yield* Effect.addFinalizer(() => disposeAllWith(true))

    return Service.of({
      load,
      reload,
      dispose,
      disposeDirectory,
      disposeAll,
      provide,
    })
  }),
)

export const bootstrapNode = LayerNode.unbound(InstanceBootstrap.Service, Node.tags.values.global)

export const node = makeGlobalNode({
  service: Service,
  layer: layer,
  deps: [Project.node, bootstrapNode],
})

export * as InstanceStore from "./instance-store"
