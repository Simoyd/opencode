import { describe, expect } from "bun:test"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { Deferred, Effect, Exit, Layer } from "effect"
import { Session as SessionNs } from "@/session/session"
import * as Log from "@opencode-ai/core/util/log"
import { MessageV2 } from "../../src/session/message-v2"
import { MessageID, PartID, SessionID } from "../../src/session/schema"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { provideInstance, testInstanceStoreLayer, tmpdirScoped } from "../fixture/fixture"
import { testEffect } from "../lib/effect"
import { Storage } from "@/storage/storage"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { BackgroundJob } from "@/background/job"
import { EventV2Bridge } from "@/event-v2-bridge"
import { GlobalBus } from "@/bus/global"
import { CompactionCatalog } from "@/session/compaction-catalog"
import { CompactionRegionTable, MessageTable, PartTable, SessionTable } from "@opencode-ai/core/session/sql"
import { persistImportedSession, type ExportData } from "@/cli/cmd/import"
import { InstanceRef } from "@/effect/instance-ref"

void Log.init({ print: false })

const it = testEffect(
  Layer.mergeAll(
    SessionNs.layer.pipe(
      Layer.provide(Storage.defaultLayer),
      Layer.provide(Database.defaultLayer),
      Layer.provideMerge(EventV2Bridge.defaultLayer),
      Layer.provide(SessionProjector.defaultLayer),
      Layer.provide(RuntimeFlags.layer({ experimentalWorkspaces: false })),
      Layer.provide(BackgroundJob.defaultLayer),
    ),
    Database.defaultLayer,
    CrossSpawnSpawner.defaultLayer,
    testInstanceStoreLayer,
  ),
)

const awaitDeferred = <T>(deferred: Deferred.Deferred<T>, message: string) =>
  Effect.race(
    Deferred.await(deferred),
    Effect.sleep("2 seconds").pipe(Effect.flatMap(() => Effect.fail(new Error(message)))),
  )

const remove = (id: SessionID) => SessionNs.use.remove(id)

describe("session.created event", () => {
  it.instance("should emit session.created event when session is created", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const events = yield* EventV2Bridge.Service
      const received = yield* Deferred.make<SessionNs.Info>()

      const unsub = yield* events.listen((event) => {
        if (event.type === SessionNs.Event.Created.type)
          Deferred.doneUnsafe(
            received,
            Effect.succeed((event.data as typeof SessionNs.Event.Created.data.Type).info as SessionNs.Info),
          )
        return Effect.void
      })
      yield* Effect.addFinalizer(() => unsub)

      const info = yield* session.create({})
      const receivedInfo = yield* awaitDeferred(received, "timed out waiting for session.created")

      expect(receivedInfo.id).toBe(info.id)
      expect(receivedInfo.projectID).toBe(info.projectID)
      expect(receivedInfo.directory).toBe(info.directory)
      expect(receivedInfo.path).toBe(info.path)
      expect(receivedInfo.title).toBe(info.title)

      yield* session.remove(info.id)
    }),
  )

  it.instance("session.created event should be emitted before session.updated", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const source = yield* EventV2Bridge.Service
      const events: string[] = []
      const received = yield* Deferred.make<string[]>()
      const push = (event: string) => {
        events.push(event)
        if (events.includes("created") && events.includes("updated")) {
          Deferred.doneUnsafe(received, Effect.succeed(events))
        }
      }

      const unsubscribe = yield* source.listen((event) => {
        if (event.type === SessionNs.Event.Created.type) push("created")
        if (event.type === SessionNs.Event.Updated.type) push("updated")
        return Effect.void
      })
      yield* Effect.addFinalizer(() => unsubscribe)

      const info = yield* session.create({})
      yield* session.setTitle({ sessionID: info.id, title: "updated" })
      const receivedEvents = yield* awaitDeferred(received, "timed out waiting for session created/updated events")

      expect(receivedEvents).toContain("created")
      expect(receivedEvents).toContain("updated")
      expect(receivedEvents.indexOf("created")).toBeLessThan(receivedEvents.indexOf("updated"))

      yield* session.remove(info.id)
    }),
  )

  it.instance("emits legacy global sync payload", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const received = yield* Deferred.make<{ syncEvent: EventV2.SerializedEvent }>()
      const listener = (event: { payload: { type?: string; syncEvent?: EventV2.SerializedEvent } }) => {
        if (event.payload.type === "sync" && event.payload.syncEvent)
          Deferred.doneUnsafe(received, Effect.succeed({ syncEvent: event.payload.syncEvent }))
      }
      GlobalBus.on("event", listener)
      yield* Effect.addFinalizer(() => Effect.sync(() => GlobalBus.off("event", listener)))

      const info = yield* session.create({})
      const event = yield* awaitDeferred(received, "timed out waiting for legacy global sync event")

      expect(event.syncEvent).toMatchObject({
        type: EventV2.versionedType(SessionNs.Event.Created.type, 1),
        seq: 0,
        aggregateID: info.id,
        data: { sessionID: info.id },
      })

      yield* session.remove(info.id)
    }),
  )
})

describe("compaction catalog invalidation", () => {
  it.instance("publishes only after the completed region metadata commits", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const events = yield* EventV2Bridge.Service
      const info = yield* session.create({})
      const received = yield* Deferred.make<SessionID>()
      const unsubscribe = yield* events.listen((event) => {
        if (event.type === CompactionCatalog.Event.Changed.type) {
          Deferred.doneUnsafe(received, Effect.succeed((event.data as { sessionID: SessionID }).sessionID))
        }
        return Effect.void
      })
      yield* Effect.addFinalizer(() => unsubscribe)

      const prompt = MessageID.ascending()
      yield* session.updateMessage({
        id: prompt,
        sessionID: info.id,
        role: "user",
        time: { created: Date.now() },
        agent: "test",
        model: { providerID: "test", modelID: "test" },
        tools: {},
      } as unknown as SessionV1.Info)
      yield* session.updatePart({
        id: PartID.ascending(),
        sessionID: info.id,
        messageID: prompt,
        type: "text",
        text: "prompt",
      })
      const marker = MessageID.ascending()
      yield* session.updateMessage({
        id: marker,
        sessionID: info.id,
        role: "user",
        time: { created: Date.now() + 1 },
        agent: "test",
        model: { providerID: "test", modelID: "test" },
        tools: {},
      } as unknown as SessionV1.Info)
      yield* session.updatePart({
        id: PartID.ascending(),
        sessionID: info.id,
        messageID: marker,
        type: "compaction",
        auto: false,
      } as SessionV1.Part)
      const summary = MessageID.ascending()
      yield* session.updateMessage({
        id: summary,
        sessionID: info.id,
        role: "assistant",
        parentID: marker,
        summary: true,
        finish: "end_turn",
        time: { created: Date.now() + 2 },
        modelID: "test",
        providerID: "test",
        agent: "test",
        mode: "",
        path: { cwd: "/", root: "/" },
        cost: 0,
        tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
      } as unknown as SessionV1.Info)
      const order: string[] = []
      const unsubscribeOrder = yield* events.listen((event) => {
        if (
          event.type === MessageV2.Event.PartUpdated.type &&
          (event.data as typeof MessageV2.Event.PartUpdated.data.Type).part.messageID === summary
        )
          order.push("mutation")
        if (event.type === CompactionCatalog.Event.Changed.type) order.push("catalog")
        return Effect.void
      })
      yield* Effect.addFinalizer(() => unsubscribeOrder)
      yield* session.updatePart({
        id: PartID.ascending(),
        sessionID: info.id,
        messageID: summary,
        type: "text",
        text: "summary",
      })

      expect(yield* awaitDeferred(received, "timed out waiting for catalog invalidation")).toBe(info.id)
      expect(order).toEqual(["mutation", "catalog"])
      yield* session.remove(info.id)
    }),
  )

  it.instance("routes replay-derived catalog invalidation with the active instance location", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const events = yield* EventV2Bridge.Service
      const ctx = yield* InstanceRef
      if (!ctx) return yield* Effect.die("InstanceRef not provided")
      const info = yield* session.create({})
      const changed = yield* Deferred.make<EventV2.Payload>()
      const unsubscribe = yield* events.listen((event) =>
        event.type === CompactionCatalog.Event.Changed.type
          ? Deferred.succeed(changed, event).pipe(Effect.asVoid)
          : Effect.void,
      )
      yield* Effect.addFinalizer(() => unsubscribe)

      const prompt = MessageID.ascending()
      const marker = MessageID.ascending()
      const summary = MessageID.ascending()
      const user = (id: MessageID, created: number) => ({
        id,
        sessionID: info.id,
        role: "user" as const,
        time: { created },
        agent: "test",
        model: { providerID: "test", modelID: "test" },
        tools: {},
        mode: "",
      })
      const serialized = [
        {
          type: SessionV1.Event.MessageUpdated,
          data: { sessionID: info.id, info: user(prompt, 1) },
        },
        {
          type: SessionV1.Event.PartUpdated,
          data: {
            sessionID: info.id,
            part: { id: PartID.ascending(), sessionID: info.id, messageID: prompt, type: "text", text: "prompt" },
            time: 1,
          },
        },
        {
          type: SessionV1.Event.MessageUpdated,
          data: { sessionID: info.id, info: user(marker, 2) },
        },
        {
          type: SessionV1.Event.PartUpdated,
          data: {
            sessionID: info.id,
            part: { id: PartID.ascending(), sessionID: info.id, messageID: marker, type: "compaction", auto: false },
            time: 2,
          },
        },
        {
          type: SessionV1.Event.MessageUpdated,
          data: {
            sessionID: info.id,
            info: {
              id: summary,
              sessionID: info.id,
              role: "assistant" as const,
              parentID: marker,
              summary: true,
              finish: "end_turn",
              time: { created: 3 },
              modelID: "test",
              providerID: "test",
              agent: "test",
              mode: "",
              path: { cwd: ctx.directory, root: ctx.directory },
              cost: 0,
              tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
            },
          },
        },
        {
          type: SessionV1.Event.PartUpdated,
          data: {
            sessionID: info.id,
            part: { id: PartID.ascending(), sessionID: info.id, messageID: summary, type: "text", text: "summary" },
            time: 3,
          },
        },
      ].map((event, index) => ({
        id: EventV2.ID.create(),
        type: EventV2.versionedType(event.type.type, event.type.sync!.version),
        seq: index + 1,
        aggregateID: info.id,
        data: event.data as Record<string, unknown>,
      }))

      yield* events.replayAll(serialized, { publish: true })

      expect(String((yield* Deferred.await(changed)).location?.directory)).toBe(ctx.directory)
      yield* session.remove(info.id)
    }),
  )

  it.instance("rejects replay whose outer and nested transcript owners disagree", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const events = yield* EventV2Bridge.Service
      const outer = yield* session.create({})
      const nested = yield* session.create({})
      const messageID = MessageID.ascending()
      const invalid = {
        id: EventV2.ID.create(),
        type: EventV2.versionedType(SessionV1.Event.MessageUpdated.type, SessionV1.Event.MessageUpdated.sync!.version),
        seq: 1,
        aggregateID: outer.id,
        data: {
          sessionID: outer.id,
          info: {
            id: messageID,
            sessionID: nested.id,
            role: "user" as const,
            time: { created: 1 },
            agent: "test",
            model: { providerID: "test", modelID: "test" },
            tools: {},
          },
        },
      }

      const exit = yield* events.replayAll([invalid], { publish: true }).pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      expect((yield* session.messages({ sessionID: nested.id })).some((item) => item.info.id === messageID)).toBe(false)

      const valid = {
        ...invalid,
        id: EventV2.ID.create(),
        data: { ...invalid.data, info: { ...invalid.data.info, sessionID: outer.id } },
      }
      yield* events.replayAll([valid], { publish: true })
      const invalidPart = {
        id: EventV2.ID.create(),
        type: EventV2.versionedType(SessionV1.Event.PartUpdated.type, SessionV1.Event.PartUpdated.sync!.version),
        seq: 2,
        aggregateID: outer.id,
        data: {
          sessionID: outer.id,
          part: {
            id: PartID.ascending(),
            sessionID: nested.id,
            messageID,
            type: "text" as const,
            text: "wrong owner",
          },
          time: 1,
        },
      }
      const partExit = yield* events.replayAll([invalidPart], { publish: true }).pipe(Effect.exit)
      expect(Exit.isFailure(partExit)).toBe(true)
      expect(
        (yield* session.messages({ sessionID: outer.id })).find((item) => item.info.id === messageID)?.parts,
      ).toEqual([])
      yield* session.remove(outer.id)
      yield* session.remove(nested.id)
    }),
  )
})

describe("step-finish token propagation via event", () => {
  it.instance(
    "non-zero tokens propagate through PartUpdated event",
    () =>
      Effect.gen(function* () {
        const session = yield* SessionNs.Service
        const events = yield* EventV2Bridge.Service
        const info = yield* session.create({})

        const messageID = MessageID.ascending()
        yield* session.updateMessage({
          id: messageID,
          sessionID: info.id,
          role: "user",
          time: { created: Date.now() },
          agent: "user",
          model: { providerID: "test", modelID: "test" },
          tools: {},
          mode: "",
        } as unknown as SessionV1.Info)

        // Event subscribers receive readonly Schema.Type payloads; `SessionV1.Part`
        // is the mutable domain type. Cast bridges the two — safe because the
        // test only reads the value afterwards.
        const received = yield* Deferred.make<SessionV1.Part>()
        const unsub = yield* events.listen((event) => {
          if (event.type === MessageV2.Event.PartUpdated.type)
            Deferred.doneUnsafe(
              received,
              Effect.succeed((event.data as typeof MessageV2.Event.PartUpdated.data.Type).part as SessionV1.Part),
            )
          return Effect.void
        })
        yield* Effect.addFinalizer(() => unsub)

        const tokens = {
          total: 1500,
          input: 500,
          output: 800,
          reasoning: 200,
          cache: { read: 100, write: 50 },
        }

        const partInput = {
          id: PartID.ascending(),
          messageID,
          sessionID: info.id,
          type: "step-finish" as const,
          reason: "stop",
          cost: 0.005,
          tokens,
        }

        yield* session.updatePart(partInput)
        const receivedPart = yield* awaitDeferred(received, "timed out waiting for message.part.updated")

        expect(receivedPart.type).toBe("step-finish")
        const finish = receivedPart as SessionV1.StepFinishPart
        expect(finish.tokens.input).toBe(500)
        expect(finish.tokens.output).toBe(800)
        expect(finish.tokens.reasoning).toBe(200)
        expect(finish.tokens.total).toBe(1500)
        expect(finish.tokens.cache.read).toBe(100)
        expect(finish.tokens.cache.write).toBe(50)
        expect(finish.cost).toBe(0.005)
        expect(receivedPart).not.toBe(partInput)

        yield* session.remove(info.id)
      }),
    { timeout: 30000 },
  )
})

describe("Session", () => {
  it.live("remove works without an instance", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const dir = yield* tmpdirScoped({ git: true })
      const info = yield* provideInstance(dir)(session.create({ title: "remove-without-instance" }))

      const removeExit = yield* remove(info.id).pipe(Effect.exit)
      expect(Exit.isSuccess(removeExit)).toBe(true)

      const getExit = yield* session.get(info.id).pipe(Effect.exit)
      expect(Exit.isFailure(getExit)).toBe(true)
    }),
  )

  it.instance("persists metadata and copies it on fork by default", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const meta = { source: "sdk", trace: { id: "abc" } }
      const created = yield* Effect.acquireRelease(session.create({ title: "with-meta", metadata: meta }), (info) =>
        session.remove(info.id).pipe(Effect.ignore),
      )
      const saved = yield* session.get(created.id)
      const fork = yield* Effect.acquireRelease(session.fork({ sessionID: created.id }), (info) =>
        session.remove(info.id).pipe(Effect.ignore),
      )

      expect(saved.metadata).toEqual(meta)
      expect(fork.metadata).toEqual(meta)
      expect(fork.metadata).not.toBe(meta)
    }),
  )

  it.instance("omits metadata when not provided", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const created = yield* Effect.acquireRelease(session.create({ title: "empty-meta" }), (info) =>
        session.remove(info.id).pipe(Effect.ignore),
      )
      const saved = yield* session.get(created.id)

      expect(created.metadata).toBeUndefined()
      expect(saved.metadata).toBeUndefined()
    }),
  )
})

describe("session import persistence", () => {
  it.instance("persists compact catalog truth atomically for existing and new sessions", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const { db } = yield* Database.Service
      const ctx = yield* InstanceRef
      if (!ctx) return yield* Effect.die("InstanceRef not provided")

      const existing = yield* session.create({ title: "existing import" })
      const fresh = { ...existing, id: SessionID.descending(), title: "new import" }
      const rollback = { ...existing, id: SessionID.descending(), title: "rollback import" }

      const makeData = (info: SessionNs.Info, prefix: string): ExportData => {
        const start = MessageID.ascending()
        const marker = MessageID.ascending()
        const summary = MessageID.ascending()
        const user = (id: MessageID, created: number) => ({
          id,
          sessionID: info.id,
          role: "user",
          time: { created },
          agent: "test",
          model: { providerID: "test", modelID: "test" },
          tools: {},
          mode: "",
        })
        return {
          info: Object.fromEntries(Object.entries(info).filter(([, value]) => value !== undefined)) as never,
          messages: [
            {
              info: user(start, 1) as never,
              parts: [{ id: PartID.ascending(), sessionID: info.id, messageID: start, type: "text", text: `${prefix} body` } as never],
            },
            {
              info: user(marker, 2) as never,
              parts: [
                {
                  id: PartID.ascending(),
                  sessionID: info.id,
                  messageID: marker,
                  type: "compaction",
                  auto: true,
                  tail_start_id: start,
                } as never,
              ],
            },
            {
              info: {
                id: summary,
                sessionID: info.id,
                role: "assistant",
                time: { created: 3 },
                parentID: marker,
                modelID: "test",
                providerID: "test",
                mode: "",
                agent: "test",
                path: { cwd: ctx.directory, root: ctx.directory },
                cost: 0,
                tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
                summary: true,
                finish: "end_turn",
              } as never,
              parts: [
                { id: PartID.ascending(), sessionID: info.id, messageID: summary, type: "text", text: `${prefix} summary` } as never,
              ],
            },
          ],
        }
      }

      const makeMultiRegionData = (info: SessionNs.Info, prefix: string): ExportData => {
        const data = makeData(info, `${prefix} first`)
        const start = MessageID.ascending()
        const marker = MessageID.ascending()
        const summary = MessageID.ascending()
        data.messages.push(
          {
            info: {
              id: start,
              sessionID: info.id,
              role: "user",
              time: { created: 4 },
              agent: "test",
              model: { providerID: "test", modelID: "test" },
              tools: {},
              mode: "",
            } as never,
            parts: [{ id: PartID.ascending(), sessionID: info.id, messageID: start, type: "text", text: `${prefix} second body` } as never],
          },
          {
            info: {
              id: marker,
              sessionID: info.id,
              role: "user",
              time: { created: 5 },
              agent: "test",
              model: { providerID: "test", modelID: "test" },
              tools: {},
              mode: "",
            } as never,
            parts: [{ id: PartID.ascending(), sessionID: info.id, messageID: marker, type: "compaction", auto: true, tail_start_id: start } as never],
          },
          {
            info: {
              id: summary,
              sessionID: info.id,
              role: "assistant",
              time: { created: 6 },
              parentID: marker,
              modelID: "test",
              providerID: "test",
              mode: "",
              agent: "test",
              path: { cwd: ctx.directory, root: ctx.directory },
              cost: 0,
              tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
              summary: true,
              finish: "end_turn",
            } as never,
            parts: [{ id: PartID.ascending(), sessionID: info.id, messageID: summary, type: "text", text: `${prefix} second summary` } as never],
          },
        )
        return data
      }

      yield* persistImportedSession(makeData(existing, "existing"), ctx)
      yield* persistImportedSession(makeMultiRegionData(fresh, "new"), ctx)

      const rows = yield* db.select().from(CompactionRegionTable).all().pipe(Effect.orDie)
      const importedRows = rows.filter((row) => row.session_id === existing.id || row.session_id === fresh.id)
      expect(importedRows).toHaveLength(3)
      expect(importedRows.map((row) => row.summary_preview).sort()).toEqual([
        "existing summary",
        "new first summary",
        "new second summary",
      ])
      expect(importedRows.every((row) => row.physical_message_count >= 1 && row.semantic_message_count >= 1)).toBe(true)

      const rollbackData = makeData(rollback, "rollback")
      rollbackData.messages[1]!.parts.push({
        id: PartID.ascending(),
        sessionID: rollback.id,
        messageID: rollbackData.messages[1]!.info.id,
        type: "invalid-import-part",
      } as never)
      const rollbackExit = yield* persistImportedSession(rollbackData, ctx).pipe(Effect.exit)
      expect(Exit.isFailure(rollbackExit)).toBe(true)

      const sessions = yield* db.select().from(SessionTable).all().pipe(Effect.orDie)
      const messages = yield* db.select().from(MessageTable).all().pipe(Effect.orDie)
      const parts = yield* db.select().from(PartTable).all().pipe(Effect.orDie)
      const regions = yield* db.select().from(CompactionRegionTable).all().pipe(Effect.orDie)
      expect(sessions.some((row) => row.id === rollback.id)).toBe(false)
      expect(messages.some((row) => row.session_id === rollback.id)).toBe(false)
      expect(parts.some((row) => row.session_id === rollback.id)).toBe(false)
      expect(regions.some((row) => row.session_id === rollback.id)).toBe(false)
    }),
  )
})
