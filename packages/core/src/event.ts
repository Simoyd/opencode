export * as EventV2 from "./event"

import { Cause, Context, Deferred, Effect, Fiber, Layer, Option, PubSub, Queue, Schema, Scope, Semaphore, Stream } from "effect"
import { Event } from "@opencode-ai/schema/event"
import type { Data, Definition, Payload } from "@opencode-ai/schema/event"
import { and, asc, eq, gt, inArray } from "drizzle-orm"
import { Database } from "./database/database"
import { EventSequenceTable, EventTable } from "./event/sql"
import { Location } from "./location"
import { makeGlobalNode } from "./effect/app-node"
import { isDeepStrictEqual } from "node:util"
import { Durable } from "@opencode-ai/schema/durable-event-manifest"

export const ID = Event.ID
export type ID = import("@opencode-ai/schema/event").ID
export type { Data, Definition, Payload } from "@opencode-ai/schema/event"

export type Subscriber<D extends Definition = Definition> = (event: Payload<D>) => Effect.Effect<void>
export type Route = (event: Payload) => void
export type Unsubscribe = Effect.Effect<void>

export const latestSequence = Effect.fn("EventV2.latestSequence")(function* (
  db: Database.Interface["db"],
  aggregateID: string,
) {
  const row = yield* db
    .select({ seq: EventSequenceTable.seq })
    .from(EventSequenceTable)
    .where(eq(EventSequenceTable.aggregate_id, aggregateID))
    .get()
    .pipe(Effect.orDie)
  return row?.seq ?? -1
})

export type SerializedEvent = {
  readonly id: ID
  readonly type: string
  readonly seq: number
  readonly aggregateID: string
  readonly data: Record<string, unknown>
}

type ReplayEvent = {
  readonly definition: Definition
  readonly payload: Payload
}

type ReplayPlan = {
  readonly aggregateID: string
  readonly events: readonly ReplayEvent[]
}

export class InvalidDurableEventError extends Schema.TaggedErrorClass<InvalidDurableEventError>()(
  "EventV2.InvalidDurableEvent",
  {
    type: Schema.String,
    message: Schema.String,
  },
) {}

const decodeSerializedEvent = (event: SerializedEvent): Payload => {
  const definition = Durable.get(event.type)
  if (!definition?.durable) {
    throw new InvalidDurableEventError({ type: event.type, message: `Unknown durable event type ${event.type}` })
  }
  const decoded = Schema.decodeUnknownOption(definition.data)(event.data)
  if (Option.isNone(decoded)) {
    throw new InvalidDurableEventError({
      type: event.type,
      message: `Invalid durable event payload for ${event.type}`,
    })
  }
  const data = decoded.value
  if ((data as Record<string, unknown>)[definition.durable.aggregate] !== event.aggregateID) {
    throw new InvalidDurableEventError({
      type: event.type,
      message: `Aggregate mismatch: expected ${event.aggregateID}`,
    })
  }
  return {
    id: event.id,
    type: definition.type,
    durable: { aggregateID: event.aggregateID, seq: event.seq, version: definition.durable.version },
    data,
  }
}

export const readAggregate = Effect.fn("EventV2.readAggregate")(function* <A>(
  db: Database.Interface["db"],
  input: {
    readonly aggregateID: string
    readonly after?: number
    readonly limit: number
    readonly manifest: {
      readonly definitions: ReadonlyMap<string, Definition>
      readonly schema: Schema.Decoder<A, never>
    }
  },
) {
  const after = input.after ?? -1
  const rows = yield* db
    .select()
    .from(EventTable)
    .where(
      and(
        eq(EventTable.aggregate_id, input.aggregateID),
        gt(EventTable.seq, after),
        inArray(EventTable.type, Array.from(input.manifest.definitions.keys())),
      ),
    )
    .orderBy(asc(EventTable.seq))
    .limit(input.limit + 1)
    .all()
    .pipe(Effect.orDie)
  const page = rows.slice(0, input.limit)
  const decode = Schema.decodeUnknownSync(input.manifest.schema)
  const events = page.map((event) =>
    decode({
      id: event.id,
      type: input.manifest.definitions.get(event.type)?.type ?? event.type,
      durable: {
        aggregateID: event.aggregate_id,
        seq: event.seq,
        version: input.manifest.definitions.get(event.type)?.durable?.version,
      },
      data: event.data,
    }),
  )
  return {
    events,
    hasMore: rows.length > input.limit,
  }
})

export class SubscriberOverflowError extends Schema.TaggedErrorClass<SubscriberOverflowError>()(
  "EventV2.SubscriberOverflow",
  { capacity: Schema.Int },
) {}

export const define = Event.define
export const versionedType = Event.versionedType

export interface PublishOptions {
  readonly id?: ID
  readonly metadata?: Record<string, unknown>
  readonly location?: Location.Ref
  /** Local operational projection committed atomically with a new durable event. Not replayed or serialized. */
  readonly commit?: (seq: number) => Effect.Effect<void>
  /** Admit a live-only notification to this service's callback lifecycle without waiting for observer completion. */
  readonly awaitObservers?: boolean
}

export interface TransactionPublisher {
  readonly publish: <D extends Definition>(
    definition: D,
    data: Data<D>,
    options?: PublishOptions,
  ) => Effect.Effect<Payload<D>>
}

export interface Interface {
  readonly publish: <D extends Definition>(
    definition: D,
    data: Data<D>,
    options?: PublishOptions,
  ) => Effect.Effect<Payload<D>>
  readonly publishTransaction: <A>(use: (publisher: TransactionPublisher) => Effect.Effect<A>) => Effect.Effect<A>
  readonly subscribe: <D extends Definition>(definition: D) => Stream.Stream<Payload<D>>
  readonly all: () => Stream.Stream<Payload>
  readonly durable: (input: { readonly aggregateID: string; readonly after?: number }) => Stream.Stream<Payload>
  /** Admit events to controlled route queues in commit order before arbitrary observer traversal. */
  readonly route: (route: Route) => Effect.Effect<Unsubscribe>
  /** @deprecated Use `all()` and consume the returned stream. */
  readonly listen: (listener: Subscriber) => Effect.Effect<Unsubscribe>
  readonly afterNotify: (listener: Subscriber) => Effect.Effect<Unsubscribe>
  readonly project: <D extends Definition>(definition: D, projector: Subscriber<D>) => Effect.Effect<void>
  readonly replay: (
    event: SerializedEvent,
    options?: { readonly publish?: boolean; readonly ownerID?: string; readonly strictOwner?: boolean },
  ) => Effect.Effect<void>
  readonly replayAll: (
    replay: readonly SerializedEvent[],
    options?: { readonly publish?: boolean; readonly ownerID?: string; readonly strictOwner?: boolean },
  ) => Effect.Effect<string | undefined>
  readonly remove: (aggregateID: string) => Effect.Effect<void>
  readonly claim: (aggregateID: string, ownerID: string) => Effect.Effect<void>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/Event") {}

export const allBounded = (events: Interface, capacity: number) =>
  Effect.gen(function* () {
    const queue = yield* Queue.dropping<Payload, SubscriberOverflowError>(capacity)
    const unsubscribe = yield* events.listen((event) =>
      Queue.offer(queue, event).pipe(
        Effect.flatMap((accepted) =>
          accepted ? Effect.void : Queue.fail(queue, new SubscriberOverflowError({ capacity })).pipe(Effect.asVoid),
        ),
      ),
    )
    yield* Effect.addFinalizer(() => unsubscribe.pipe(Effect.andThen(Queue.shutdown(queue)), Effect.asVoid))
    return Stream.fromQueue(queue)
  })

export interface LayerOptions {
  readonly beforeAggregateRead?: (aggregateID: string) => Effect.Effect<void>
}

export const layerWith = (options?: LayerOptions) =>
  Layer.effect(
    Service,
    Effect.gen(function* () {
      const pubsub = {
        all: yield* PubSub.unbounded<Payload>(),
        durable: new Map<string, Set<PubSub.PubSub<void>>>(),
        typed: new Map<string, PubSub.PubSub<Payload>>(),
      }
      const projectors = new Map<string, Subscriber[]>()
      // TODO: Bind durable projectors to exact type+version before supporting incompatible historical payloads.
      const routes = new Array<Route>()
      const listeners = new Array<Subscriber>()
      const afterListeners = new Array<Subscriber>()
      const { db } = yield* Database.Service
      const postCommitScope = yield* Scope.make()
      const postCommitGate = Semaphore.makeUnsafe(1)
      const activePostCommit = new Set<number>()
      const postCommitDrained = Deferred.makeUnsafe<void>()
      let postCommitClosed = false
      let nextPostCommit = 0

      const requirePostCommitAdmission = Effect.gen(function* () {
        if (postCommitClosed) return yield* Effect.interrupt
      })

      const inspectReplay = Effect.fnUntraced(function* (
        replay: ReplayPlan,
        options?: { readonly ownerID?: string; readonly strictOwner?: boolean },
      ) {
        const row = yield* db
          .select({ seq: EventSequenceTable.seq, ownerID: EventSequenceTable.owner_id })
          .from(EventSequenceTable)
          .where(eq(EventSequenceTable.aggregate_id, replay.aggregateID))
          .get()
          .pipe(Effect.orDie)
        if (options?.strictOwner && row?.ownerID && row.ownerID !== options.ownerID) {
          throw new InvalidDurableEventError({
            type: replay.events[0]?.payload.type ?? "unknown",
            message: `Replay owner mismatch for aggregate ${replay.aggregateID}: expected ${row.ownerID}, got ${options.ownerID ?? "none"}`,
          })
        }

        const currentSequence = row?.seq ?? -1
        const eventIDs = replay.events.map((item) => item.payload.id)
        const existingByID = eventIDs.length === 0
          ? []
          : yield* db
              .select()
              .from(EventTable)
              .where(inArray(EventTable.id, eventIDs))
              .all()
              .pipe(Effect.orDie)
        const existingIDs = new Map(existingByID.map((item) => [item.id, item]))
        for (const item of replay.events) {
          const durable = item.payload.durable!
          const encoded = Schema.encodeUnknownSync(item.definition.data)(item.payload.data) as Record<string, unknown>
          if (durable.aggregateID !== replay.aggregateID) {
            throw new InvalidDurableEventError({
              type: item.payload.type,
              message: `Aggregate mismatch: expected ${replay.aggregateID}, got ${durable.aggregateID}`,
            })
          }
          if (durable.seq <= currentSequence) {
            const stored = existingIDs.get(item.payload.id)
            if (
              stored?.aggregate_id !== replay.aggregateID ||
              stored.seq !== durable.seq ||
              stored.type !== versionedType(item.definition.type, item.definition.durable!.version) ||
              !isDeepStrictEqual(stored.data, encoded)
            ) {
              throw new InvalidDurableEventError({
                type: item.payload.type,
                message: `Replay diverged at aggregate ${replay.aggregateID} sequence ${durable.seq}`,
              })
            }
            continue
          }
          if (existingIDs.has(item.payload.id)) {
            const stored = existingIDs.get(item.payload.id)!
            throw new InvalidDurableEventError({
              type: item.payload.type,
              message: `Event ${item.payload.id} already exists at aggregate ${stored.aggregate_id} sequence ${stored.seq}`,
            })
          }
        }

        const acceptedPrefixLength = replay.events.findIndex((item) => item.payload.durable!.seq > currentSequence)
        const firstNew = acceptedPrefixLength < 0 ? replay.events.length : acceptedPrefixLength
        if (firstNew < replay.events.length && replay.events[firstNew]!.payload.durable!.seq !== currentSequence + 1) {
          throw new InvalidDurableEventError({
            type: replay.events[firstNew]!.payload.type,
            message: `Sequence mismatch for aggregate ${replay.aggregateID}: expected ${currentSequence + 1}, got ${replay.events[firstNew]!.payload.durable!.seq}`,
          })
        }
        return { row, currentSequence, acceptedPrefixLength: firstNew }
      })

      const getOrCreate = (definition: Definition) =>
        Effect.gen(function* () {
          const existing = pubsub.typed.get(definition.type)
          if (existing) return existing
          const created = yield* PubSub.unbounded<Payload>()
          pubsub.typed.set(definition.type, created)
          return created
        })

      yield* Effect.addFinalizer((exit) =>
        Effect.gen(function* () {
          yield* postCommitGate.withPermits(1)(
            Effect.gen(function* () {
              postCommitClosed = true
              if (activePostCommit.size === 0) {
                yield* Deferred.succeed(postCommitDrained, undefined).pipe(Effect.asVoid)
              }
            }),
          )
           yield* Deferred.await(postCommitDrained).pipe(
             Effect.andThen(
               Effect.gen(function* () {
                  listeners.length = 0
                  afterListeners.length = 0
                  routes.length = 0
                  projectors.clear()
                 yield* PubSub.shutdown(pubsub.all)
                 yield* Effect.forEach(
                   pubsub.durable.values(),
                   (pubsubs) => Effect.forEach(pubsubs, PubSub.shutdown, { discard: true }),
                   { discard: true },
                 )
                 yield* Effect.forEach(pubsub.typed.values(), PubSub.shutdown, { discard: true })
                 pubsub.durable.clear()
                 pubsub.typed.clear()
                 yield* Scope.close(postCommitScope, exit)
               }),
             ),
             Effect.forkDetach,
           )
        }),
      )

      function commitDurableEvent(
        definition: Definition,
        event: Payload,
        input?: {
          readonly seq: number
          readonly aggregateID: string
          readonly ownerID?: string
          readonly strictOwner?: boolean
        },
        commit?: (seq: number) => Effect.Effect<void>,
      ) {
        return Effect.gen(function* () {
          const durable = definition.durable
          if (!durable) return
          const aggregateID = (event.data as Record<string, unknown>)[durable.aggregate]
          if (typeof aggregateID !== "string") {
            return yield* Effect.die(
              new InvalidDurableEventError({
                type: event.type,
                message: `Expected string aggregate field ${durable.aggregate}`,
              }),
            )
          }
          if (input && input.aggregateID !== aggregateID) {
            return yield* Effect.die(
              new InvalidDurableEventError({
                type: event.type,
                message: `Aggregate mismatch: expected ${input.aggregateID}, got ${aggregateID}`,
              }),
            )
          }
          const row = yield* db
            .select({ seq: EventSequenceTable.seq, ownerID: EventSequenceTable.owner_id })
            .from(EventSequenceTable)
            .where(eq(EventSequenceTable.aggregate_id, aggregateID))
            .get()
            .pipe(Effect.orDie)
          const latest = row?.seq ?? -1
          const encoded = Schema.encodeUnknownSync(definition.data)(event.data) as Record<string, unknown>
          if (input?.strictOwner && row?.ownerID && row.ownerID !== input.ownerID) {
            return yield* Effect.die(
              new InvalidDurableEventError({
                type: event.type,
                message: `Replay owner mismatch for aggregate ${aggregateID}: expected ${row.ownerID}, got ${input.ownerID ?? "none"}`,
              }),
            )
          }
          if (input && input.seq <= latest) {
            const stored = yield* db
              .select()
              .from(EventTable)
              .where(and(eq(EventTable.aggregate_id, aggregateID), eq(EventTable.seq, input.seq)))
              .get()
              .pipe(Effect.orDie)
            if (
              stored?.id === event.id &&
              stored.type === versionedType(definition.type, durable.version) &&
              isDeepStrictEqual(stored.data, encoded)
            ) {
              if (input.ownerID && row?.ownerID == null) {
                yield* db
                  .update(EventSequenceTable)
                  .set({ owner_id: input.ownerID })
                  .where(eq(EventSequenceTable.aggregate_id, aggregateID))
                  .run()
                  .pipe(Effect.orDie)
              }
              return
            }
            return yield* Effect.die(
              new InvalidDurableEventError({
                type: event.type,
                message: `Replay diverged at aggregate ${aggregateID} sequence ${input.seq}`,
              }),
            )
          }
          if (input && row?.ownerID && row.ownerID !== input.ownerID) return
          const seq = input?.seq ?? latest + 1
          if (input && seq !== latest + 1) {
            return yield* Effect.die(
              new InvalidDurableEventError({
                type: event.type,
                message: `Sequence mismatch for aggregate ${aggregateID}: expected ${latest + 1}, got ${seq}`,
              }),
            )
          }
          const stored = yield* db
            .select({ aggregateID: EventTable.aggregate_id, seq: EventTable.seq })
            .from(EventTable)
            .where(eq(EventTable.id, event.id))
            .get()
            .pipe(Effect.orDie)
          if (stored) {
            return yield* Effect.die(
              new InvalidDurableEventError({
                type: event.type,
                message: `Event ${event.id} already exists at aggregate ${stored.aggregateID} sequence ${stored.seq}`,
              }),
            )
          }
          const committed = { ...event, durable: { aggregateID, seq, version: durable.version } } as Payload
          for (const projector of projectors.get(event.type) ?? []) yield* projector(committed)
          if (commit) yield* commit(seq)
          yield* db
            .insert(EventSequenceTable)
            .values([{ aggregate_id: aggregateID, seq, owner_id: input?.ownerID }])
            .onConflictDoUpdate({
              target: EventSequenceTable.aggregate_id,
              set: {
                seq,
                ...(input?.ownerID && row?.ownerID == null ? { owner_id: input.ownerID } : {}),
              },
            })
            .run()
            .pipe(Effect.orDie)
          yield* db
            .insert(EventTable)
            .values([
              {
                id: event.id,
                aggregate_id: aggregateID,
                seq,
                type: versionedType(definition.type, durable.version),
                data: encoded,
              },
            ])
            .run()
            .pipe(Effect.orDie)
          return { aggregateID, seq }
        })
      }

      const postCommit = (
        aggregateIDs: ReadonlySet<string>,
        events: readonly Payload[],
        isolateListeners: boolean,
      ) =>
        Effect.gen(function* () {
          yield* Effect.forEach(
            aggregateIDs,
            (aggregateID) =>
              Effect.forEach(pubsub.durable.get(aggregateID) ?? [], (wake) => PubSub.publish(wake, undefined), {
                discard: true,
              }),
            { discard: true },
          )
          yield* Effect.forEach(events, (event) => notify(event, isolateListeners), { concurrency: 1, discard: true })
        })

      const admitRoutes = (events: readonly Payload[]) =>
        Effect.sync(() => {
          for (const event of events) {
            for (const route of routes) route(event)
          }
        })

      const admitPostCommit = (
        aggregateIDs: ReadonlySet<string>,
        events: readonly Payload[],
        isolateListeners: boolean,
      ) =>
        Effect.gen(function* () {
          const id = ++nextPostCommit
          activePostCommit.add(id)
          return yield* postCommit(aggregateIDs, events, isolateListeners).pipe(
             Effect.interruptible,
            Effect.ensuring(
              postCommitGate.withPermits(1)(
                Effect.gen(function* () {
                  activePostCommit.delete(id)
                  if (postCommitClosed && activePostCommit.size === 0) {
                    yield* Deferred.succeed(postCommitDrained, undefined).pipe(Effect.asVoid)
                  }
                }),
              ),
            ),
            Effect.forkIn(postCommitScope, { startImmediately: true }),
          )
        })

      function publishEvent<D extends Definition>(definition: D, event: Payload<D>, options?: PublishOptions) {
        return Effect.gen(function* () {
          if (!definition?.durable && options?.commit)
            return yield* Effect.die(
              new InvalidDurableEventError({
                type: event.type,
                message: "Local commit hooks require a durable event",
              }),
            )
          const durableDefinition = definition?.durable
          if (durableDefinition) {
            const published = yield* Effect.uninterruptibleMask((restore) =>
              Effect.gen(function* () {
                const admitted = yield* postCommitGate.withPermits(1)(
                  Effect.gen(function* () {
                    yield* requirePostCommitAdmission
                    const committed = yield* db
                      .transaction(
                        () => restore(commitDurableEvent(definition, event as Payload, undefined, options?.commit)),
                        { behavior: "immediate" },
                      )
                      .pipe(Effect.orDie)
                    if (!committed) return undefined
                    const durableEvent = {
                      ...event,
                      durable: {
                        aggregateID: committed.aggregateID,
                        seq: committed.seq,
                        version: durableDefinition.version,
                      },
                    } as Payload<D>
                    yield* admitRoutes([durableEvent as Payload])
                    const continuation = yield* admitPostCommit(
                      new Set([committed.aggregateID]),
                      [durableEvent as Payload],
                      true,
                    )
                    return { event: durableEvent, continuation }
                  }),
                )
                if (!admitted) return undefined
                yield* restore(Fiber.join(admitted.continuation))
                return admitted.event
              }),
            )
            if (published) return published
          }
          const notification = yield* postCommitGate.withPermits(1)(
            Effect.gen(function* () {
              yield* requirePostCommitAdmission
              yield* admitRoutes([event as Payload])
              return yield* admitPostCommit(new Set(), [event as Payload], options?.awaitObservers === false)
            }),
          )
          if (options?.awaitObservers !== false) yield* Fiber.join(notification)
          return event
        })
      }

      const observe = (event: Payload, observer: (event: Payload) => Effect.Effect<void>) =>
        Effect.suspend(() => observer(event)).pipe(
          Effect.catchCauseIf(
            (cause) => !Cause.hasInterrupts(cause),
            (cause) => Effect.logError("Event listener failed", { eventID: event.id, eventType: event.type, cause }),
          ),
        )

      function notify(event: Payload, isolateListeners: boolean) {
        return Effect.gen(function* () {
          yield* Effect.forEach(
            listeners,
            (listener) => (isolateListeners ? observe(event, listener) : listener(event)),
            { discard: true },
          )
          const typed = pubsub.typed.get(event.type)
          if (typed) yield* PubSub.publish(typed, event)
          yield* PubSub.publish(pubsub.all, event)
          yield* Effect.forEach(
            afterListeners,
            (listener) => (isolateListeners ? observe(event, listener) : listener(event)),
            { discard: true },
          )
        })
      }

      function publish<D extends Definition>(definition: D, data: Data<D>, options?: PublishOptions) {
        return Effect.gen(function* () {
          const serviceLocation = Option.getOrUndefined(yield* Effect.serviceOption(Location.Service))
          const location =
            options?.location ??
            (serviceLocation
              ? { directory: serviceLocation.directory, workspaceID: serviceLocation.workspaceID }
              : undefined)
          return yield* publishEvent(
            definition,
            {
              id: options?.id ?? ID.create(),
              ...(options?.metadata ? { metadata: options.metadata } : {}),
              type: definition.type,
              ...(location ? { location } : {}),
              data,
            } as Payload<D>,
            options,
          )
        })
      }

      function publishTransaction<A>(use: (publisher: TransactionPublisher) => Effect.Effect<A>) {
        return Effect.gen(function* () {
          const serviceLocation = Option.getOrUndefined(yield* Effect.serviceOption(Location.Service))
          const published: Payload[] = []
          return yield* Effect.uninterruptibleMask((restore) =>
            Effect.gen(function* () {
              const admitted = yield* postCommitGate.withPermits(1)(
                Effect.gen(function* () {
                  yield* requirePostCommitAdmission
                  const result = yield* db
                    .transaction(
                      () => restore(
                        use({
                          publish: <D extends Definition>(definition: D, data: Data<D>, options?: PublishOptions) =>
                            Effect.gen(function* () {
                              if (!definition.durable) {
                                return yield* Effect.die(
                                  new InvalidDurableEventError({
                                    type: definition.type,
                                    message: "Atomic event publication requires durable events",
                                  }),
                                )
                              }
                              const location = options?.location ?? serviceLocation
                              const event = {
                                id: options?.id ?? ID.create(),
                                ...(options?.metadata ? { metadata: options.metadata } : {}),
                                type: definition.type,
                                ...(location ? { location } : {}),
                                data,
                              } as Payload<D>
                              const committed = yield* commitDurableEvent(
                                definition,
                                event,
                                undefined,
                                options?.commit,
                              )
                              if (!committed) {
                                return yield* Effect.die(
                                  new InvalidDurableEventError({
                                    type: definition.type,
                                    message: `Atomic event publication did not commit ${definition.type}`,
                                  }),
                                )
                              }
                              const next = {
                                ...event,
                                durable: {
                                  aggregateID: committed.aggregateID,
                                  seq: committed.seq,
                                  version: definition.durable.version,
                                },
                              } as Payload<D>
                              published.push(next)
                              return next
                            }),
                        }),
                      ),
                      { behavior: "immediate" },
                    )
                    .pipe(Effect.orDie)
                  if (published.length === 0) return { result, continuation: undefined }
                  yield* admitRoutes(published)
                  const continuation = yield* admitPostCommit(
                    new Set(published.map((event) => event.durable!.aggregateID)),
                    published,
                    true,
                  )
                  return { result, continuation }
                }),
              )
              if (admitted.continuation) yield* restore(Fiber.join(admitted.continuation))
              return admitted.result
            }),
          )
        })
      }

      function replay(
        event: SerializedEvent,
        options?: { readonly publish?: boolean; readonly ownerID?: string; readonly strictOwner?: boolean },
      ) {
        return Effect.gen(function* () {
          yield* replayAll([event], options)
        })
      }

      const prepareReplay = (events: readonly SerializedEvent[]) =>
        Effect.gen(function* () {
          const source = events[0]?.aggregateID
          if (!source) {
            throw new InvalidDurableEventError({ type: "unknown", message: "Replay requires at least one event" })
          }
          if (events.some((event) => event.aggregateID !== source)) {
            throw new InvalidDurableEventError({
              type: events[0]?.type ?? "unknown",
              message: "Replay events must belong to the same aggregate",
            })
          }
          const start = events[0]?.seq ?? 0
          const ids = new Set<ID>()
          const prepared = events.map((event, index) => {
            const seq = start + index
            if (event.seq !== seq) {
              throw new InvalidDurableEventError({
                type: event.type,
                message: `Replay sequence mismatch at index ${index}: expected ${seq}, got ${event.seq}`,
              })
            }
            if (!ids.add(event.id)) {
              throw new InvalidDurableEventError({
                type: event.type,
                message: `Replay contains duplicate event ID ${event.id}`,
              })
            }
            const definition = Durable.get(event.type)
            const payload = decodeSerializedEvent(event)
            return { definition: definition!, payload }
          })
          const inspection = yield* inspectReplay({ aggregateID: source, events: prepared })
          return {
            aggregateID: source,
            events: prepared,
          } satisfies ReplayPlan
        })

      function replayAll(
        input: readonly SerializedEvent[],
        options?: { readonly publish?: boolean; readonly ownerID?: string; readonly strictOwner?: boolean },
      ) {
        return Effect.gen(function* () {
          const replay = yield* prepareReplay(input)
          return yield* replayPrepared(replay, options)
        })
      }

      function replayPrepared(
        replay: ReplayPlan,
        options?: { readonly publish?: boolean; readonly ownerID?: string; readonly strictOwner?: boolean },
      ) {
        return Effect.uninterruptibleMask((restore) =>
          Effect.gen(function* () {
            const admitted = yield* postCommitGate.withPermits(1)(
              Effect.gen(function* () {
                yield* requirePostCommitAdmission
                const committed = yield* db
                  .transaction(
                    () => restore(
                      Effect.gen(function* () {
                        const inspection = yield* inspectReplay(replay, options)
                        const row = inspection.row
                        const currentSequence = inspection.currentSequence
                        const firstNew = inspection.acceptedPrefixLength
                        if (options?.ownerID && row && row.ownerID == null) {
                          yield* db
                            .update(EventSequenceTable)
                            .set({ owner_id: options.ownerID })
                            .where(eq(EventSequenceTable.aggregate_id, replay.aggregateID))
                            .run()
                            .pipe(Effect.orDie)
                        }

                        const accepted: Payload[] = []
                        for (const item of replay.events) {
                          const durable = item.payload.durable!
                          if (durable.seq <= currentSequence) continue
                          const result = yield* commitDurableEvent(
                            item.definition,
                            item.payload,
                            {
                              seq: durable.seq,
                              aggregateID: replay.aggregateID,
                              ownerID: options?.ownerID,
                              strictOwner: options?.strictOwner,
                            },
                            undefined,
                          )
                          if (!result) continue
                          accepted.push({
                            ...item.payload,
                            durable: {
                              aggregateID: result.aggregateID,
                              seq: result.seq,
                              version: item.definition.durable!.version,
                            },
                          })
                        }
                        return accepted
                      }),
                    ),
                    { behavior: "immediate" },
                  )
                  .pipe(Effect.orDie)
                if (committed.length === 0) return undefined
                const published = options?.publish ? committed : []
                yield* admitRoutes(published)
                return yield* admitPostCommit(
                  new Set([replay.aggregateID]),
                  published,
                  true,
                )
              }),
            )
            if (admitted) yield* restore(Fiber.join(admitted))
            return replay.aggregateID
          }),
        )
      }

      function remove(aggregateID: string) {
        return db
          .transaction(() =>
            Effect.gen(function* () {
              yield* db.delete(EventSequenceTable).where(eq(EventSequenceTable.aggregate_id, aggregateID)).run()
              yield* db.delete(EventTable).where(eq(EventTable.aggregate_id, aggregateID)).run()
            }),
          )
          .pipe(Effect.orDie)
      }

      function claim(aggregateID: string, ownerID: string) {
        return db
          .update(EventSequenceTable)
          .set({ owner_id: ownerID })
          .where(eq(EventSequenceTable.aggregate_id, aggregateID))
          .run()
          .pipe(Effect.orDie)
      }

      const subscribe = <D extends Definition>(definition: D): Stream.Stream<Payload<D>> =>
        Stream.unwrap(
          postCommitGate.withPermits(1)(
            Effect.gen(function* () {
              yield* requirePostCommitAdmission
              return Stream.fromPubSub(yield* getOrCreate(definition))
            }),
          ),
        ).pipe(
          Stream.map((event) => event as Payload<D>),
        )

      const streamAll = (): Stream.Stream<Payload> =>
        Stream.unwrap(
          postCommitGate.withPermits(1)(
            Effect.gen(function* () {
              yield* requirePostCommitAdmission
              return Stream.fromPubSub(pubsub.all)
            }),
          ),
        )

      const readAfter = (aggregateID: string, after: number) =>
        (options?.beforeAggregateRead?.(aggregateID) ?? Effect.void).pipe(
          Effect.andThen(
            db
              .select()
              .from(EventTable)
              .where(and(eq(EventTable.aggregate_id, aggregateID), gt(EventTable.seq, after)))
              .orderBy(asc(EventTable.seq))
              .all(),
          ),
          Effect.orDie,
          Effect.map((rows) =>
            rows.map((event) =>
              decodeSerializedEvent({
                id: event.id,
                aggregateID: event.aggregate_id,
                seq: event.seq,
                type: event.type,
                data: event.data,
              }),
            ),
          ),
        )

      const subscribeDurable = (aggregateID: string) =>
        postCommitGate.withPermits(1)(
          Effect.gen(function* () {
            yield* requirePostCommitAdmission
            const wake = yield* PubSub.sliding<void>(1)
            const subscription = yield* PubSub.subscribe(wake)
            yield* Effect.acquireRelease(
              Effect.sync(() => {
                const wakes = pubsub.durable.get(aggregateID) ?? new Set()
                wakes.add(wake)
                pubsub.durable.set(aggregateID, wakes)
              }),
              () =>
                Effect.sync(() => {
                  const wakes = pubsub.durable.get(aggregateID)
                  wakes?.delete(wake)
                  if (wakes?.size === 0) pubsub.durable.delete(aggregateID)
                }).pipe(Effect.andThen(PubSub.shutdown(wake))),
            )
            return subscription
          }),
        )

      const durable = (input: { readonly aggregateID: string; readonly after?: number }): Stream.Stream<Payload> =>
        Stream.unwrap(
          Effect.gen(function* () {
            const wakes = yield* subscribeDurable(input.aggregateID)
            let sequence = input.after ?? -1
            const read = Effect.suspend(() => readAfter(input.aggregateID, sequence)).pipe(
              Effect.tap((events) =>
                Effect.sync(() => {
                  sequence = events.at(-1)?.durable?.seq ?? sequence
                }),
              ),
            )
            const historical = yield* read
            const live = Stream.fromSubscription(wakes).pipe(
              Stream.mapEffect(() => read),
              Stream.flattenIterable,
            )
            return Stream.concat(Stream.fromIterable(historical), live)
          }),
        )

      const registerListener = (target: Subscriber[], listener: Subscriber): Effect.Effect<Unsubscribe> =>
        postCommitGate.withPermits(1)(
          Effect.gen(function* () {
            yield* requirePostCommitAdmission
            return yield* Effect.sync(() => {
              target.push(listener)
              return Effect.sync(() => {
                const index = target.indexOf(listener)
                if (index >= 0) target.splice(index, 1)
              })
            })
          }),
        )

      const registerRoute = (route: Route): Effect.Effect<Unsubscribe> =>
        postCommitGate.withPermits(1)(
          Effect.gen(function* () {
            yield* requirePostCommitAdmission
            return yield* Effect.sync(() => {
              routes.push(route)
              return Effect.sync(() => {
                const index = routes.indexOf(route)
                if (index >= 0) routes.splice(index, 1)
              })
            })
          }),
        )

      const listen = (listener: Subscriber): Effect.Effect<Unsubscribe> => registerListener(listeners, listener)
      const afterNotify = (listener: Subscriber): Effect.Effect<Unsubscribe> =>
        registerListener(afterListeners, listener)

      const project = <D extends Definition>(definition: D, projector: Subscriber<D>): Effect.Effect<void> =>
        postCommitGate.withPermits(1)(
          Effect.gen(function* () {
            yield* requirePostCommitAdmission
            yield* Effect.sync(() => {
              const list = projectors.get(definition.type) ?? []
              list.push((event) => projector(event as Payload<D>))
              projectors.set(definition.type, list)
            })
          }),
        )

      return Service.of({
        publish,
        publishTransaction,
        subscribe,
        all: streamAll,
        durable,
        route: registerRoute,
        listen,
        afterNotify,
        project,
        replay,
        replayAll,
        remove,
        claim,
      })
    }),
  )

const layer = layerWith()
export const node = makeGlobalNode({ service: Service, layer: layer, deps: [Database.node] })
