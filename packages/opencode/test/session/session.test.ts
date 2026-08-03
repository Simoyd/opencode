import { describe, expect } from "bun:test"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"
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
import { persistImportedSession, transformShareData, type ExportData, type ShareData } from "@/cli/cmd/import"
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
          model: { providerID: ProviderV2.ID.make("test"), modelID: ModelV2.ID.make("test") },
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

  it.instance("fork remaps every continuity variant, nested identity, and safe cutoff", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const original = yield* session.create({ title: "continuity fork" })
      const createUser = Effect.fnUntraced(function* (text: string, created: number) {
        const info = yield* session.updateMessage({
          id: MessageID.ascending(),
          sessionID: original.id,
          role: "user",
          time: { created },
          agent: "build",
          model: { providerID: ProviderV2.ID.make("test"), modelID: ModelV2.ID.make("test") },
        } satisfies SessionV1.User)
        yield* session.updatePart({
          id: PartID.ascending(),
          sessionID: original.id,
          messageID: info.id,
          type: "text",
          text,
        })
        return info
      })
      const createAssistant = Effect.fnUntraced(function* (parentID: MessageID, created: number, summary = false) {
        return yield* session.updateMessage({
          id: MessageID.ascending(),
          sessionID: original.id,
          role: "assistant",
          parentID,
          time: { created, completed: created },
          modelID: ModelV2.ID.make("test"),
          providerID: ProviderV2.ID.make("test"),
          mode: "build",
          agent: "build",
          path: { cwd: "/", root: "/" },
          cost: 0,
          tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
          finish: "stop",
          summary: summary || undefined,
        } satisfies SessionV1.Assistant)
      })

      const a = yield* createUser("A", 1)
      const marker = yield* createUser("marker", 2)
      const markerPart = yield* session.updatePart({
        id: PartID.ascending(),
        sessionID: original.id,
        messageID: marker.id,
        type: "compaction",
        auto: false,
        tail_start_id: a.id,
      } satisfies SessionV1.CompactionPart)
      const summary = yield* createAssistant(marker.id, 3, true)
      yield* session.updatePart({
        id: PartID.ascending(),
        sessionID: original.id,
        messageID: summary.id,
        type: "text",
        text: "summary",
      })
      const replay = yield* createUser("replay", 4)
      yield* session.updatePart({
        id: PartID.ascending(),
        sessionID: original.id,
        messageID: replay.id,
        type: "text",
        text: "A",
        serverProvenance: { type: "compaction-replay", ownerMessageID: marker.id, sourceMessageID: a.id },
      } satisfies SessionV1.TextPart)
      const compactContinuation = yield* createUser("continue", 5)
      yield* session.updatePart({
        id: PartID.ascending(),
        sessionID: original.id,
        messageID: compactContinuation.id,
        type: "text",
        text: "continue",
        synthetic: true,
        serverProvenance: { type: "compaction-continuation", ownerMessageID: marker.id },
      } satisfies SessionV1.TextPart)
      const taskOwner = yield* createUser("task", 6)
      const task = yield* session.updatePart({
        id: PartID.ascending(),
        sessionID: original.id,
        messageID: taskOwner.id,
        type: "subtask",
        prompt: "same",
        description: "same",
        agent: "build",
        command: "review",
      } satisfies SessionV1.SubtaskPart)
      const taskOutput = yield* createAssistant(taskOwner.id, 7)
      const attachment = {
        id: PartID.ascending(),
        sessionID: original.id,
        messageID: taskOutput.id,
        type: "file" as const,
        mime: "text/plain",
        url: "data:text/plain;base64,ZA==",
      }
      yield* session.updatePart({
        id: PartID.ascending(),
        sessionID: original.id,
        messageID: taskOutput.id,
        type: "tool",
        callID: "call",
        tool: "task",
        serverProvenance: { type: "subtask-output", ownerMessageID: taskOwner.id, taskPartID: task.id },
        state: {
          status: "completed",
          input: {},
          output: "done",
          title: "task",
          metadata: {},
          time: { start: 1, end: 2 },
          attachments: [attachment],
        },
      } satisfies SessionV1.ToolPart)
      const taskContinuation = yield* createUser("task continue", 8)
      yield* session.updatePart({
        id: PartID.ascending(),
        sessionID: original.id,
        messageID: taskContinuation.id,
        type: "text",
        text: "continue task",
        synthetic: true,
        serverProvenance: {
          type: "subtask-continuation",
          ownerMessageID: taskOwner.id,
          taskPartID: task.id,
          sourceMessageID: taskOutput.id,
        },
      } satisfies SessionV1.TextPart)

      const source = yield* session.messages({ sessionID: original.id })
      const fork = yield* session.fork({ sessionID: original.id })
      const copied = yield* session.messages({ sessionID: fork.id })
      expect(copied).toHaveLength(source.length)
      const messageMap = new Map<MessageID, MessageID>(
        source.map((message, index) => [message.info.id, copied[index]!.info.id]),
      )
      const partMap = new Map<PartID, PartID>(
        source.flatMap((message, messageIndex) =>
          message.parts.map((part, partIndex) => [part.id, copied[messageIndex]!.parts[partIndex]!.id] as const),
        ),
      )
      const copiedA = messageMap.get(a.id)!
      const copiedMarkerID = messageMap.get(marker.id)!
      const copiedTaskOwner = messageMap.get(taskOwner.id)!
      const copiedTaskOutput = messageMap.get(taskOutput.id)!
      const copiedTask = partMap.get(task.id)!
      const copiedMarker = copied.find((message) => message.info.id === copiedMarkerID)!
      const copiedMarkerPart = copiedMarker.parts.find((part) => part.type === "compaction")
      expect(copiedMarkerPart?.type === "compaction" ? copiedMarkerPart.tail_start_id : undefined).toBe(copiedA)
      const provenances = copied.flatMap((message) =>
        message.parts.flatMap((part) =>
          (part.type === "text" || part.type === "tool") && part.serverProvenance ? [part.serverProvenance] : [],
        ),
      )
      expect(provenances).toEqual([
        {
          type: "compaction-replay",
          ownerMessageID: copiedMarkerID,
          sourceMessageID: copiedA,
        },
        { type: "compaction-continuation", ownerMessageID: copiedMarkerID },
        {
          type: "subtask-output",
          ownerMessageID: copiedTaskOwner,
          taskPartID: copiedTask,
        },
        {
          type: "subtask-continuation",
          ownerMessageID: copiedTaskOwner,
          taskPartID: copiedTask,
          sourceMessageID: copiedTaskOutput,
        },
      ])
      const copiedAttachment = copied
        .flatMap((message) => message.parts)
        .flatMap((part) =>
          part.type === "tool" && part.state.status === "completed" ? (part.state.attachments ?? []) : [],
        )
      expect(copiedAttachment).toHaveLength(1)
      expect(copiedAttachment[0]?.id).not.toBe(attachment.id)
      expect(copiedAttachment[0]?.sessionID).toBe(fork.id)
      expect(copiedAttachment[0]?.messageID).toBe(copiedTaskOutput)
      expect(partMap.get(markerPart.id)).toBe(copiedMarkerPart?.id)

      const compactionCutoff = yield* session.fork({ sessionID: original.id, messageID: replay.id })
      const compactionPrefix = yield* session.messages({ sessionID: compactionCutoff.id })
      expect(compactionPrefix).toHaveLength(source.findIndex((message) => message.info.id === replay.id))
      expect(
        compactionPrefix.some((message) =>
          message.parts.some(
            (part) => (part.type === "text" || part.type === "tool") && part.serverProvenance !== undefined,
          ),
        ),
      ).toBe(false)

      const subtaskCutoff = yield* session.fork({ sessionID: original.id, messageID: taskOutput.id })
      const subtaskPrefix = yield* session.messages({ sessionID: subtaskCutoff.id })
      expect(subtaskPrefix).toHaveLength(source.findIndex((message) => message.info.id === taskOutput.id))
      expect(
        subtaskPrefix.some((message) =>
          message.parts.some(
            (part) =>
              (part.type === "text" || part.type === "tool") && part.serverProvenance?.type === "subtask-output",
          ),
        ),
      ).toBe(false)
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
              parts: [
                {
                  id: PartID.ascending(),
                  sessionID: info.id,
                  messageID: start,
                  type: "text",
                  text: `${prefix} body`,
                } as never,
              ],
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
                time: { created: 3, completed: 3 },
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
                {
                  id: PartID.ascending(),
                  sessionID: info.id,
                  messageID: summary,
                  type: "text",
                  text: `${prefix} summary`,
                } as never,
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
            parts: [
              {
                id: PartID.ascending(),
                sessionID: info.id,
                messageID: start,
                type: "text",
                text: `${prefix} second body`,
              } as never,
            ],
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
              time: { created: 6, completed: 6 },
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
              {
                id: PartID.ascending(),
                sessionID: info.id,
                messageID: summary,
                type: "text",
                text: `${prefix} second summary`,
              } as never,
            ],
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

  it.instance("imports every continuity provenance variant and rolls malformed graphs back", () =>
    Effect.gen(function* () {
      const sessions = yield* SessionNs.Service
      const { db } = yield* Database.Service
      const ctx = yield* InstanceRef
      if (!ctx) return yield* Effect.die("InstanceRef not provided")
      const template = yield* sessions.create({ title: "import template" })

      const fixture = (id: SessionID): ExportData => {
        const a = MessageID.ascending()
        const marker = MessageID.ascending()
        const summary = MessageID.ascending()
        const replay = MessageID.ascending()
        const continuation = MessageID.ascending()
        const taskOwner = MessageID.ascending()
        const taskOutput = MessageID.ascending()
        const taskContinuation = MessageID.ascending()
        const task = PartID.ascending()
        const text = (messageID: MessageID, value: string, provenance?: SessionV1.ContinuityProvenance) => ({
          id: PartID.ascending(),
          sessionID: id,
          messageID,
          type: "text" as const,
          text: value,
          ...(provenance ? { serverProvenance: provenance } : {}),
        })
        const user = (messageID: MessageID, created: number) => ({
          id: messageID,
          sessionID: id,
          role: "user" as const,
          time: { created },
          agent: "build",
          model: { providerID: "test", modelID: "test" },
        })
        const assistant = (messageID: MessageID, parentID: MessageID, created: number) => ({
          id: messageID,
          sessionID: id,
          role: "assistant" as const,
          parentID,
          time: { created, completed: created },
          modelID: "test",
          providerID: "test",
          mode: "build",
          agent: "build",
          path: { cwd: ctx.directory, root: ctx.directory },
          cost: 0,
          tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
          finish: "stop",
        })
        const info = { ...template, id, title: `import ${id}` }
        return {
          info: Object.fromEntries(Object.entries(info).filter(([, value]) => value !== undefined)) as never,
          messages: [
            { info: user(a, 1) as never, parts: [text(a, "A") as never] },
            {
              info: user(marker, 2) as never,
              parts: [
                {
                  id: PartID.ascending(),
                  sessionID: id,
                  messageID: marker,
                  type: "compaction",
                  auto: false,
                } as never,
              ],
            },
            {
              info: { ...assistant(summary, marker, 3), summary: true } as never,
              parts: [text(summary, "summary A") as never],
            },
            {
              info: user(replay, 4) as never,
              parts: [
                text(replay, "A", {
                  type: "compaction-replay",
                  ownerMessageID: marker,
                  sourceMessageID: a,
                }) as never,
              ],
            },
            {
              info: user(continuation, 5) as never,
              parts: [
                {
                  ...text(continuation, "continue", {
                    type: "compaction-continuation",
                    ownerMessageID: marker,
                  }),
                  synthetic: true,
                } as never,
              ],
            },
            {
              info: user(taskOwner, 6) as never,
              parts: [
                {
                  id: task,
                  sessionID: id,
                  messageID: taskOwner,
                  type: "subtask",
                  prompt: "same payload",
                  description: "same payload",
                  agent: "build",
                  command: "review",
                } as never,
              ],
            },
            {
              info: assistant(taskOutput, taskOwner, 7) as never,
              parts: [
                {
                  id: PartID.ascending(),
                  sessionID: id,
                  messageID: taskOutput,
                  type: "tool",
                  callID: "call-1",
                  tool: "task",
                  serverProvenance: { type: "subtask-output", ownerMessageID: taskOwner, taskPartID: task },
                  state: {
                    status: "completed",
                    input: {},
                    output: "done",
                    title: "task",
                    metadata: {},
                    time: { start: 1, end: 2 },
                    attachments: [
                      {
                        id: PartID.ascending(),
                        sessionID: id,
                        messageID: taskOutput,
                        type: "file",
                        mime: "text/plain",
                        url: "data:text/plain;base64,ZA==",
                      },
                    ],
                  },
                } as never,
              ],
            },
            {
              info: user(taskContinuation, 8) as never,
              parts: [
                {
                  ...text(taskContinuation, "continue task", {
                    type: "subtask-continuation",
                    ownerMessageID: taskOwner,
                    taskPartID: task,
                    sourceMessageID: taskOutput,
                  }),
                  synthetic: true,
                } as never,
              ],
            },
          ],
        }
      }

      const validID = SessionID.descending()
      const valid = fixture(validID)
      yield* persistImportedSession(valid, ctx)
      const imported = yield* sessions.messages({ sessionID: validID })
      expect(
        imported.flatMap((message) =>
          message.parts.flatMap((part) =>
            (part.type === "text" || part.type === "tool") && part.serverProvenance ? [part.serverProvenance.type] : [],
          ),
        ),
      ).toEqual(["compaction-replay", "compaction-continuation", "subtask-output", "subtask-continuation"])
      const jsonRoundTrip = JSON.parse(JSON.stringify(valid)) as ExportData
      const shared = transformShareData([
        { type: "session", data: jsonRoundTrip.info },
        ...jsonRoundTrip.messages.map((message) => ({ type: "message" as const, data: message.info })),
        ...jsonRoundTrip.messages.flatMap((message) =>
          message.parts.map((part) => ({ type: "part" as const, data: part })),
        ),
      ] as ShareData[])
      expect(shared).toEqual(jsonRoundTrip)
      if (!shared) return yield* Effect.die("ShareNext transform dropped continuity data")
      yield* persistImportedSession(shared, ctx)
      expect(yield* sessions.messages({ sessionID: validID })).toEqual(imported)

      const emptyID = SessionID.descending()
      const empty = fixture(emptyID)
      yield* persistImportedSession({ ...empty, messages: [] }, ctx)
      yield* persistImportedSession(empty, ctx)
      expect(yield* sessions.messages({ sessionID: emptyID })).toHaveLength(empty.messages.length)

      const legacyID = SessionID.descending()
      const legacy = fixture(legacyID)
      for (const message of legacy.messages) {
        for (const part of message.parts) delete (part as any).serverProvenance
      }
      yield* persistImportedSession(legacy, ctx)
      expect(
        (yield* sessions.messages({ sessionID: legacyID })).some((message) =>
          message.parts.some((part) => (part.type === "text" || part.type === "tool") && part.serverProvenance),
        ),
      ).toBe(false)

      const invalidCases: Array<[string, (data: ExportData) => void]> = [
        ["malformed variant", (data) => ((data.messages[3]!.parts[0] as any).serverProvenance.type = "unknown")],
        [
          "dangling owner",
          (data) => ((data.messages[4]!.parts[0] as any).serverProvenance.ownerMessageID = MessageID.ascending()),
        ],
        [
          "wrong target kind",
          (data) =>
            ((data.messages[6]!.parts[0] as any).serverProvenance = {
              type: "compaction-continuation",
              ownerMessageID: data.messages[1]!.info.id,
            }),
        ],
        [
          "wrong physical direction",
          (data) => ((data.messages[3]!.parts[0] as any).serverProvenance.sourceMessageID = data.messages[7]!.info.id),
        ],
        [
          "dangling replay source",
          (data) => ((data.messages[3]!.parts[0] as any).serverProvenance.sourceMessageID = MessageID.ascending()),
        ],
        [
          "dangling task target",
          (data) => ((data.messages[6]!.parts[0] as any).serverProvenance.taskPartID = PartID.ascending()),
        ],
        [
          "owner after carrier",
          (data) => ((data.messages[4]!.parts[0] as any).serverProvenance.ownerMessageID = data.messages[7]!.info.id),
        ],
        [
          "subtask continuation source after carrier",
          (data) => ((data.messages[7]!.parts[0] as any).serverProvenance.sourceMessageID = data.messages[7]!.info.id),
        ],
        [
          "cross-session nested attachment",
          (data) => ((data.messages[6]!.parts[0] as any).state.attachments[0].sessionID = SessionID.descending()),
        ],
        [
          "cross-message nested attachment",
          (data) => ((data.messages[6]!.parts[0] as any).state.attachments[0].messageID = data.messages[5]!.info.id),
        ],
        [
          "duplicate semantic task output",
          (data) => {
            const duplicate = structuredClone(data.messages[6]!) as any
            duplicate.info.id = MessageID.ascending()
            duplicate.info.time = { created: 7.5, completed: 7.5 }
            duplicate.parts[0].id = PartID.ascending()
            duplicate.parts[0].messageID = duplicate.info.id
            duplicate.parts[0].state.attachments[0].id = PartID.ascending()
            duplicate.parts[0].state.attachments[0].messageID = duplicate.info.id
            data.messages.splice(7, 0, duplicate)
          },
        ],
        [
          "conflicting claims on generated message",
          (data) => {
            const conflicting = structuredClone(data.messages[4]!.parts[0]!) as any
            conflicting.id = PartID.ascending()
            conflicting.serverProvenance = {
              type: "compaction-replay",
              ownerMessageID: data.messages[1]!.info.id,
              sourceMessageID: data.messages[0]!.info.id,
            }
            data.messages[4]!.parts.push(conflicting)
          },
        ],
        [
          "nested attachment collision",
          (data) => ((data.messages[6]!.parts[0] as any).state.attachments[0].id = data.messages[0]!.parts[0]!.id),
        ],
        [
          "duplicate nested attachment IDs",
          (data) => {
            const attachment = structuredClone((data.messages[6]!.parts[0] as any).state.attachments[0])
            ;(data.messages[6]!.parts[0] as any).state.attachments.push(attachment)
          },
        ],
      ]

      for (const [name, mutate] of invalidCases) {
        const id = SessionID.descending()
        const data = fixture(id)
        mutate(data)
        const exit = yield* persistImportedSession(data, ctx).pipe(Effect.exit)
        expect(Exit.isFailure(exit), name).toBe(true)
        expect(
          (yield* db.select().from(SessionTable).all().pipe(Effect.orDie)).some((row) => row.id === id),
          `${name} wrote a partial session`,
        ).toBe(false)
        expect(
          (yield* db.select().from(MessageTable).all().pipe(Effect.orDie)).some((row) => row.session_id === id),
          `${name} wrote partial messages`,
        ).toBe(false)
        expect(
          (yield* db.select().from(PartTable).all().pipe(Effect.orDie)).some((row) => row.session_id === id),
          `${name} wrote partial parts`,
        ).toBe(false)
        expect(
          (yield* db.select().from(CompactionRegionTable).all().pipe(Effect.orDie)).some(
            (row) => row.session_id === id,
          ),
          `${name} wrote partial regions`,
        ).toBe(false)
      }
    }),
  )
})
