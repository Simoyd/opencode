export * as BackgroundJob from "./background-job"

import { Cause, Clock, Context, Deferred, Effect, Exit, Layer, Scope, SynchronizedRef } from "effect"
import { Identifier } from "./id/id"
import { makeGlobalNode } from "./effect/app-node"

export type Status = "running" | "completed" | "error" | "cancelled"

export type Info = {
  id: string
  type: string
  title?: string
  status: Status
  started_at: number
  completed_at?: number
  output?: string
  error?: string
  metadata?: Record<string, unknown>
}

type Active = {
  info: Info
  completion: Deferred.Deferred<Info>
  scope: Scope.Closeable
  token: object
  pending: number
  next: number
  output?: { sequence: number; text: string }
  tail: Deferred.Deferred<void>
  promoted: Deferred.Deferred<Info>
  onPromote?: Effect.Effect<void>
  terminalDelivery?: (info: Info) => Effect.Effect<RegisteredTerminalDelivery, unknown>
}

export type RegisteredTerminalDelivery = {
  readonly run: Effect.Effect<void, unknown>
}

type Registry = {
  jobs: Map<string, Active>
  closed: boolean
  closing?: Deferred.Deferred<void>
}

type State = {
  registry: SynchronizedRef.SynchronizedRef<Registry>
  scope: Scope.Scope
}

type FinishResult = {
  info?: Info
  scope?: Scope.Closeable
  completion?: Deferred.Deferred<Info>
}

type PrepareResult = {
  info?: Info
  delivery?: (info: Info) => Effect.Effect<RegisteredTerminalDelivery, unknown>
  scope?: Scope.Closeable
  completion?: Deferred.Deferred<Info>
}

type PromoteResult = {
  info?: Info
  promoted?: Deferred.Deferred<Info>
  onPromote?: Effect.Effect<void>
}

type StartResult = { info: Info } | { info: Info; scope: Scope.Closeable; token: object }

type ExtendResult =
  | { extended: false }
  | {
      extended: true
      previous: Deferred.Deferred<void>
      scope: Scope.Closeable
      tail: Deferred.Deferred<void>
      token: object
      sequence: number
    }

type CloseResult =
  | { readonly owner: false; readonly done?: Deferred.Deferred<void>; readonly ids: readonly [] }
  | { readonly owner: true; readonly done: Deferred.Deferred<void>; readonly ids: string[] }

export type StartInput = {
  id?: string
  type: string
  title?: string
  metadata?: Record<string, unknown>
  onPromote?: Effect.Effect<void>
  terminalDelivery?: (info: Info) => Effect.Effect<RegisteredTerminalDelivery, unknown>
  run: Effect.Effect<string, unknown>
}

export type ExtendInput = {
  id: string
  run: Effect.Effect<string, unknown>
}

export type WaitInput = {
  id: string
  timeout?: number
}

export type WaitResult = {
  info?: Info
  timedOut: boolean
}

export interface Interface {
  readonly list: () => Effect.Effect<Info[]>
  readonly get: (id: string) => Effect.Effect<Info | undefined>
  readonly start: (input: StartInput) => Effect.Effect<Info>
  readonly extend: (input: ExtendInput) => Effect.Effect<boolean>
  readonly wait: (input: WaitInput) => Effect.Effect<WaitResult>
  readonly waitForPromotion: (id: string) => Effect.Effect<Info>
  readonly promote: (id: string) => Effect.Effect<Info | undefined>
  readonly cancel: (id: string) => Effect.Effect<Info | undefined>
  readonly close: () => Effect.Effect<void>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/BackgroundJob") {}

function snapshot(job: Active): Info {
  return {
    ...job.info,
    ...(job.info.metadata ? { metadata: { ...job.info.metadata } } : {}),
  }
}

function errorText(error: unknown) {
  if (error instanceof Error) return error.message
  return String(error)
}

/**
 * Makes one scoped, process-local registry. Entries are intentionally not
 * durable: process restart or owner-scope closure loses status and interrupts
 * live work. Persisted observation, restart recovery, and remote workers need a
 * separate durable ownership slice rather than pretending this registry has
 * those semantics.
 */
export const make = Effect.gen(function* () {
  const state: State = {
    registry: yield* SynchronizedRef.make({ jobs: new Map(), closed: false }),
    scope: yield* Scope.Scope,
  }

  const settle = Effect.fn("BackgroundJob.settle")(function* (
    id: string,
    token: object,
    sequence: number,
    exit: Exit.Exit<string, unknown>,
  ) {
    const completed_at = yield* Clock.currentTimeMillis
    const prepared = yield* SynchronizedRef.modify(
      state.registry,
      (registry): readonly [PrepareResult, Registry] => {
        const job = registry.jobs.get(id)
        if (!job || job.token !== token || job.info.status !== "running") {
          return [{}, registry] as const
        }
        const pending = job.pending - 1
        const output =
          Exit.isSuccess(exit) && (!job.output || sequence > job.output.sequence)
            ? { sequence, text: exit.value }
            : job.output
        if (Exit.isSuccess(exit) && pending > 0) {
          return [
            {},
            { ...registry, jobs: new Map(registry.jobs).set(id, { ...job, pending, output }) },
          ] as const
        }
        const status: Exclude<Status, "running"> = Exit.isSuccess(exit)
          ? "completed"
          : Cause.hasInterruptsOnly(exit.cause)
            ? "cancelled"
            : "error"
        const info = {
          ...job.info,
          status,
          completed_at,
          ...(output ? { output: output.text } : {}),
          ...(Exit.isFailure(exit) ? { error: errorText(Cause.squash(exit.cause)) } : {}),
        }
        const next = { ...job, info, pending: 0, output, onPromote: undefined, terminalDelivery: undefined }
        return [
          {
            info,
            delivery: job.info.metadata?.background === true ? job.terminalDelivery : undefined,
            scope: job.scope,
            completion: job.completion,
          },
          { ...registry, jobs: new Map(registry.jobs).set(id, next) },
        ] as const
      },
    )
    if (!prepared.info) return
    const preparedInfo = prepared.info
    const registrationExit = prepared.delivery
      ? yield* prepared.delivery(preparedInfo).pipe(Effect.exit)
      : Exit.succeed(undefined)
    const registeredInfo = Exit.isFailure(registrationExit)
      ? {
          ...preparedInfo,
          status: "error" as const,
          error: errorText(Cause.squash(registrationExit.cause)),
        }
      : preparedInfo
    if (prepared.completion) {
      yield* Deferred.succeed(prepared.completion, registeredInfo).pipe(Effect.asVoid)
    }

    const deliveryExit = Exit.isSuccess(registrationExit) && registrationExit.value
      ? yield* registrationExit.value.run.pipe(Effect.exit)
      : registrationExit
    const result = yield* SynchronizedRef.modify(
      state.registry,
      (registry): readonly [PrepareResult, Registry] => {
        const job = registry.jobs.get(id)
        const info = Exit.isFailure(deliveryExit)
          ? {
              ...preparedInfo,
              status: "error" as const,
              error: errorText(Cause.squash(deliveryExit.cause)),
            }
          : registeredInfo
        if (!job || job.token !== token) {
          return [{ info, scope: prepared.scope, completion: prepared.completion }, registry]
        }
        const next = { ...job, info }
        return [
          { info: snapshot(next), scope: job.scope, completion: job.completion },
          { ...registry, jobs: new Map(registry.jobs).set(id, next) },
        ]
      },
    )
    if (result.info && result.scope && result.completion) {
      const info = result.info
      const completion = result.completion
      yield* Scope.close(result.scope, Exit.void).pipe(
        Effect.onExit((exit) => Deferred.done(completion, Exit.map(exit, () => info)).pipe(Effect.ignore)),
        Effect.forkIn(state.scope, { startImmediately: true }),
      )
    }
    return result.info
  })

  const fork = Effect.fn("BackgroundJob.fork")(function* (
    scope: Scope.Scope,
    id: string,
    token: object,
    sequence: number,
    run: Effect.Effect<string, unknown>,
  ) {
    return yield* run.pipe(
      Effect.matchCauseEffect({
        onSuccess: (output) => settle(id, token, sequence, Exit.succeed(output)),
        onFailure: (cause) => settle(id, token, sequence, Exit.failCause(cause)),
      }),
      Effect.asVoid,
      Effect.forkIn(scope, { startImmediately: true }),
    )
  })

  const list: Interface["list"] = Effect.fn("BackgroundJob.list")(function* () {
    return Array.from((yield* SynchronizedRef.get(state.registry)).jobs.values())
      .map(snapshot)
      .toSorted((a, b) => a.started_at - b.started_at)
  })

  const get: Interface["get"] = Effect.fn("BackgroundJob.get")(function* (id) {
    const job = (yield* SynchronizedRef.get(state.registry)).jobs.get(id)
    if (!job) return
    return snapshot(job)
  })

  const start: Interface["start"] = Effect.fn("BackgroundJob.start")(function* (input) {
    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const id = input.id ?? Identifier.ascending("job")
        const started_at = yield* Clock.currentTimeMillis
        const completion = yield* Deferred.make<Info>()
        const promoted = yield* Deferred.make<Info>()
        const tail = yield* Deferred.make<void>()
        const result = yield* SynchronizedRef.modifyEffect(
          state.registry,
          Effect.fnUntraced(function* (registry) {
            const existing = registry.jobs.get(id)
            if (existing?.info.status === "running") {
              return [{ info: snapshot(existing) }, registry] as readonly [StartResult, Registry]
            }
            if (registry.closed) return yield* Effect.interrupt
            const scope = yield* Scope.fork(state.scope, "parallel")
            const token = {}
            const job = {
              info: {
                id,
                type: input.type,
                title: input.title,
                status: "running" as const,
                started_at,
                metadata: input.metadata,
              },
              completion,
              scope,
              token,
              pending: 1,
              next: 1,
              tail,
              promoted,
              onPromote: input.onPromote,
              terminalDelivery: input.terminalDelivery,
            }
            return [{ info: snapshot(job), scope, token }, { ...registry, jobs: new Map(registry.jobs).set(id, job) }] as readonly [
              StartResult,
              Registry,
            ]
          }),
        )
        if ("scope" in result)
          yield* fork(
            result.scope,
            id,
            result.token,
            0,
            restore(input.run).pipe(Effect.ensuring(Deferred.succeed(tail, undefined))),
          )
        return result.info
      }),
    )
  })

  const extend: Interface["extend"] = Effect.fn("BackgroundJob.extend")(function* (input) {
    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const tail = yield* Deferred.make<void>()
        const result = yield* SynchronizedRef.modify(
          state.registry,
          (registry): readonly [ExtendResult, Registry] => {
            const job = registry.jobs.get(input.id)
            if (registry.closed || !job || job.info.status !== "running") {
              return [{ extended: false }, registry]
            }
            return [
              { extended: true, previous: job.tail, scope: job.scope, tail, token: job.token, sequence: job.next },
              {
                ...registry,
                jobs: new Map(registry.jobs).set(input.id, {
                  ...job,
                  pending: job.pending + 1,
                  next: job.next + 1,
                  tail,
                }),
              },
            ]
          },
        )
        if (!result.extended) return false
        yield* fork(
          result.scope,
          input.id,
          result.token,
          result.sequence,
          Deferred.await(result.previous).pipe(
            Effect.andThen(restore(input.run)),
            Effect.ensuring(Deferred.succeed(result.tail, undefined)),
          ),
        )
        return true
      }),
    )
  })

  const wait: Interface["wait"] = Effect.fn("BackgroundJob.wait")(function* (input) {
    const job = (yield* SynchronizedRef.get(state.registry)).jobs.get(input.id)
    if (!job) return { timedOut: false }
    if (input.timeout === undefined) return { info: yield* Deferred.await(job.completion), timedOut: false }
    if (input.timeout <= 0) return { info: snapshot(job), timedOut: true }
    const info = yield* Deferred.await(job.completion).pipe(Effect.timeoutOption(input.timeout))
    if (info._tag === "Some") return { info: info.value, timedOut: false }
    return { info: snapshot(job), timedOut: true }
  })

  const waitForPromotion: Interface["waitForPromotion"] = Effect.fn("BackgroundJob.waitForPromotion")(function* (id) {
    const job = (yield* SynchronizedRef.get(state.registry)).jobs.get(id)
    if (!job || job.info.status !== "running") return yield* Effect.never
    if (job.info.metadata?.background === true) return snapshot(job)
    return yield* Deferred.await(job.promoted)
  })

  const promote: Interface["promote"] = Effect.fn("BackgroundJob.promote")(function* (id) {
    const result = yield* SynchronizedRef.modifyEffect(
      state.registry,
      Effect.fnUntraced(function* (registry) {
        const job = registry.jobs.get(id)
        if (!job || job.info.status !== "running") return [{}, registry] as readonly [PromoteResult, Registry]
        if (job.info.metadata?.background === true)
          return [{ info: snapshot(job) }, registry] as readonly [PromoteResult, Registry]
        const next = {
          ...job,
          onPromote: undefined,
          info: {
            ...job.info,
            metadata: { ...job.info.metadata, background: true },
          },
        }
        return [
          { info: snapshot(next), onPromote: job.onPromote, promoted: job.promoted },
          { ...registry, jobs: new Map(registry.jobs).set(id, next) },
        ] as readonly [PromoteResult, Registry]
      }),
    )
    if (result.info && result.promoted) yield* Deferred.succeed(result.promoted, result.info).pipe(Effect.ignore)
    if (result.onPromote) yield* result.onPromote.pipe(Effect.ignore)
    return result.info
  })

  const cancel: Interface["cancel"] = Effect.fn("BackgroundJob.cancel")(function* (id) {
    const completed_at = yield* Clock.currentTimeMillis
    const result = yield* SynchronizedRef.modify(state.registry, (registry): readonly [FinishResult, Registry] => {
      const job = registry.jobs.get(id)
      if (!job) return [{}, registry]
      if (job.info.status !== "running") return [{ info: snapshot(job), completion: job.completion }, registry]
      const next = {
        ...job,
        onPromote: undefined,
        pending: 0,
        info: {
          ...job.info,
          status: "cancelled" as const,
          completed_at,
        },
      }
      return [
        { info: snapshot(next), scope: job.scope, completion: job.completion },
        { ...registry, jobs: new Map(registry.jobs).set(id, next) },
      ]
    })
    if (result.scope && result.info && result.completion) {
      const exit = yield* Scope.close(result.scope, Exit.void).pipe(Effect.exit)
      yield* Deferred.done(result.completion, Exit.map(exit, () => result.info!)).pipe(Effect.ignore)
    }
    return result.completion ? yield* Deferred.await(result.completion) : result.info
  })

  const close: Interface["close"] = Effect.fn("BackgroundJob.close")(function* () {
    const closing = yield* Deferred.make<void>()
    const result = yield* SynchronizedRef.modify(state.registry, (registry): readonly [CloseResult, Registry] => {
      if (registry.closing) return [{ owner: false as const, done: registry.closing, ids: [] }, registry] as const
      if (registry.closed) return [{ owner: false as const, ids: [] }, registry] as const
      const ids = Array.from(registry.jobs.values())
        .map((job) => job.info.id)
        .toSorted()
      return [
        { owner: true as const, done: closing, ids },
        { ...registry, closed: true, closing },
      ] as const
    })
    if (!result.owner) {
      if (result.done) yield* Deferred.await(result.done)
      return
    }
    const exits = yield* Effect.forEach(
      result.ids,
      (id) => cancel(id).pipe(Effect.asVoid, Effect.exit),
      { concurrency: 1 },
    )
    yield* SynchronizedRef.update(state.registry, (registry) => ({ ...registry, jobs: new Map() }))
    const failure = exits.find(Exit.isFailure)
    yield* Deferred.done(result.done, failure ?? Exit.void).pipe(Effect.ignore)
    yield* Deferred.await(result.done)
  })

  return Service.of({ list, get, start, extend, wait, waitForPromotion, promote, cancel, close })
})

const layer = Layer.effect(Service, make)

export const node = makeGlobalNode({ service: Service, layer, deps: [] })
