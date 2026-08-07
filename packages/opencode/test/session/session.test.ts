import { describe, expect } from "bun:test"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { EventV2 } from "@opencode-ai/core/event"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { Deferred, Effect, Exit, Fiber, Layer, Option } from "effect"
import { Session as SessionNs } from "@/session/session"
import { MessageV2 } from "../../src/session/message-v2"
import { MessageID, PartID, type SessionID } from "../../src/session/schema"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { provideInstance, tmpdirScoped } from "../fixture/fixture"
import { testEffect } from "../lib/effect"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { EventV2Bridge } from "@/event-v2-bridge"
import { GlobalBus } from "@/bus/global"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { InstanceStore } from "@/project/instance-store"
import { InstanceBootstrap } from "@/project/bootstrap"
import { SessionLifecycle } from "@/session/lifecycle"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"
import { WorkspaceV2 } from "@opencode-ai/core/workspace"
import { NotFoundError } from "@/storage/storage"
import { collectExportData } from "@/cli/cmd/export"
import { persistImportedSession, type ExportData } from "@/cli/cmd/import"
import { InstanceState } from "@/effect/instance-state"
import { Database } from "@opencode-ai/core/database/database"
import { SessionStatus } from "@/session/status"

const it = testEffect(
  AppNodeBuilder.build(
    LayerNode.group([
      SessionNs.node,
      SessionLifecycle.node,
      EventV2Bridge.node,
      SessionProjector.node,
      CrossSpawnSpawner.node,
      InstanceStore.node,
      Database.node,
      SessionStatus.node,
    ]),
    [
      [RuntimeFlags.node, RuntimeFlags.layer({ experimentalWorkspaces: false })],
      [
        InstanceBootstrap.node,
        Layer.succeed(InstanceBootstrap.Service, InstanceBootstrap.Service.of({ run: Effect.void })),
      ],
    ],
  ),
)

const awaitDeferred = <T>(deferred: Deferred.Deferred<T>, message: string) =>
  Effect.race(
    Deferred.await(deferred),
    Effect.sleep("2 seconds").pipe(Effect.flatMap(() => Effect.fail(new Error(message)))),
  )

const remove = (id: SessionID) => SessionLifecycle.Service.use((lifecycle) => lifecycle.remove(id))

function lifecycleResult(sessionID: SessionID): SessionV1.WithParts {
  return {
    info: {
      id: MessageID.ascending(),
      role: "user",
      sessionID,
      agent: "build",
      model: { providerID: ProviderV2.ID.make("test"), modelID: ModelV2.ID.make("test-model") },
      time: { created: Date.now() },
    },
    parts: [],
  }
}

const seedContinuityGraph = Effect.fn("TestSession.seedContinuityGraph")(function* () {
  const sessions = yield* SessionNs.Service
  const info = yield* sessions.create({ title: "continuity interchange" })
  let created = Date.now()
  const addUser = Effect.fnUntraced(function* (text: string) {
    const message = yield* sessions.updateMessage({
      id: MessageID.ascending(),
      sessionID: info.id,
      role: "user",
      agent: "build",
      model: { providerID: ProviderV2.ID.make("test"), modelID: ModelV2.ID.make("test-model") },
      time: { created: created++ },
    })
    const part = yield* sessions.updatePart({
      id: PartID.ascending(),
      sessionID: info.id,
      messageID: message.id,
      type: "text",
      text,
    })
    return { message, part }
  })
  const addAssistant = Effect.fnUntraced(function* (parentID: MessageID, summary = false) {
    const message = yield* sessions.updateMessage({
      id: MessageID.ascending(),
      sessionID: info.id,
      role: "assistant",
      parentID,
      mode: summary ? "compaction" : "build",
      agent: summary ? "compaction" : "build",
      providerID: ProviderV2.ID.make("test"),
      modelID: ModelV2.ID.make("test-model"),
      path: { cwd: "/tmp", root: "/tmp" },
      cost: 0,
      tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
      time: { created, completed: created++ },
      finish: "stop",
      summary: summary || undefined,
    })
    return message
  })

  const source = yield* addUser("source")
  const marker = yield* addUser("compaction owner")
  const markerPart = yield* sessions.updatePart({
    id: PartID.ascending(),
    sessionID: info.id,
    messageID: marker.message.id,
    type: "compaction",
    auto: false,
  })
  const summary = yield* addAssistant(marker.message.id, true)
  yield* sessions.updatePart({
    id: PartID.ascending(),
    sessionID: info.id,
    messageID: summary.id,
    type: "text",
    text: "summary",
  })
  const replay = yield* addUser("replay")
  const replayPart = yield* sessions.updatePart({
    ...replay.part,
    serverProvenance: {
      type: "compaction-replay",
      ownerMessageID: marker.message.id,
      sourceMessageID: source.message.id,
    },
  })
  const compactionContinuation = yield* addUser("compaction continuation")
  const compactionContinuationPart = yield* sessions.updatePart({
    ...compactionContinuation.part,
    synthetic: true,
    serverProvenance: { type: "compaction-continuation", ownerMessageID: marker.message.id },
  })
  const taskOwner = yield* addUser("subtask owner")
  const taskPart = yield* sessions.updatePart({
    id: PartID.ascending(),
    sessionID: info.id,
    messageID: taskOwner.message.id,
    type: "subtask",
    prompt: "do work",
    description: "work",
    agent: "build",
    command: "continue-work",
  })
  const taskOutput = yield* addAssistant(taskOwner.message.id)
  const taskOutputPart = yield* sessions.updatePart({
    id: PartID.ascending(),
    sessionID: info.id,
    messageID: taskOutput.id,
    type: "tool",
    callID: "task-call",
    tool: "task",
    serverProvenance: {
      type: "subtask-output",
      ownerMessageID: taskOwner.message.id,
      taskPartID: taskPart.id,
    },
    state: {
      status: "completed",
      input: {},
      output: "done",
      title: "task",
      metadata: {},
      time: { start: created - 1, end: created },
    },
  })
  const taskContinuation = yield* addUser("subtask continuation")
  const taskContinuationPart = yield* sessions.updatePart({
    ...taskContinuation.part,
    synthetic: true,
    serverProvenance: {
      type: "subtask-continuation",
      ownerMessageID: taskOwner.message.id,
      taskPartID: taskPart.id,
      sourceMessageID: taskOutput.id,
    },
  })

  return {
    info,
    source,
    marker,
    markerPart,
    replayPart,
    compactionContinuationPart,
    taskOwner,
    taskPart,
    taskOutput,
    taskOutputPart,
    taskContinuationPart,
  }
})

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

      yield* remove(info.id)
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

      yield* remove(info.id)
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

      yield* remove(info.id)
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

        yield* remove(info.id)
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

      const removeExit = yield* provideInstance(dir)(remove(info.id)).pipe(Effect.exit)
      expect(Exit.isSuccess(removeExit)).toBe(true)

      const getExit = yield* session.get(info.id).pipe(Effect.exit)
      expect(Exit.isFailure(getExit)).toBe(true)
    }),
  )

  it.instance("persists metadata and copies it on fork by default", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const lifecycle = yield* SessionLifecycle.Service
      const meta = { source: "sdk", trace: { id: "abc" } }
      const created = yield* Effect.acquireRelease(session.create({ title: "with-meta", metadata: meta }), (info) =>
        remove(info.id).pipe(Effect.ignore),
      )
      const saved = yield* session.get(created.id)
      const fork = yield* Effect.acquireRelease(lifecycle.fork({ sessionID: created.id }), (info) =>
        remove(info.id).pipe(Effect.ignore),
      )

      expect(saved.metadata).toEqual(meta)
      expect(fork.metadata).toEqual(meta)
      expect(fork.metadata).not.toBe(meta)
    }),
  )

  it.instance("forks the chronological prefix across mixed message ID ordering", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const lifecycle = yield* SessionLifecycle.Service
      const created = yield* Effect.acquireRelease(session.create({}), (info) => remove(info.id).pipe(Effect.ignore))
      const ids = ["msg_z9-before", "msg_z1-before-wrap", "msg_a0-after-wrap", "msg_a1-after"]
      for (const [index, id] of ids.entries()) {
        yield* session.updateMessage({
          id: MessageID.make(id),
          sessionID: created.id,
          role: "user",
          time: { created: index + 1 },
          agent: "user",
          model: { providerID: "test", modelID: "test" },
        } as SessionV1.User)
      }

      const beforeWrap = yield* Effect.acquireRelease(
        lifecycle.fork({ sessionID: created.id, messageID: MessageID.make(ids[1]!) }),
        (info) => remove(info.id).pipe(Effect.ignore),
      )
      const afterWrap = yield* Effect.acquireRelease(
        lifecycle.fork({ sessionID: created.id, messageID: MessageID.make(ids[2]!) }),
        (info) => remove(info.id).pipe(Effect.ignore),
      )

      expect((yield* session.messages({ sessionID: beforeWrap.id })).map((msg) => msg.info.time.created)).toEqual([1])
      expect((yield* session.messages({ sessionID: afterWrap.id })).map((msg) => msg.info.time.created)).toEqual([1, 2])
    }),
  )

  it.instance("omits metadata when not provided", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const created = yield* Effect.acquireRelease(session.create({ title: "empty-meta" }), (info) =>
        remove(info.id).pipe(Effect.ignore),
      )
      const saved = yield* session.get(created.id)

      expect(created.metadata).toBeUndefined()
      expect(saved.metadata).toBeUndefined()
    }),
  )

  it.instance("fork remaps every typed continuity provenance identity exactly", () =>
    Effect.gen(function* () {
      const sessions = yield* SessionNs.Service
      const lifecycle = yield* SessionLifecycle.Service
      const graph = yield* seedContinuityGraph()
      const source = yield* sessions.messages({ sessionID: graph.info.id })
      const fork = yield* lifecycle.fork({ sessionID: graph.info.id })
      const copied = yield* sessions.messages({ sessionID: fork.id })
      expect(copied).toHaveLength(source.length)

      const messageMap = new Map(source.map((message, index) => [message.info.id, copied[index]!.info.id]))
      const partMap = new Map(
        source.flatMap((message, messageIndex) =>
          message.parts.map((part, partIndex) => [part.id, copied[messageIndex]!.parts[partIndex]!.id] as const),
        ),
      )
      const mappedMessage = (id: MessageID) => {
        const mapped = messageMap.get(id)
        if (!mapped) throw new Error(`missing forked message mapping for ${id}`)
        return mapped
      }
      const mappedPart = (id: PartID) => {
        const mapped = partMap.get(id)
        if (!mapped) throw new Error(`missing forked part mapping for ${id}`)
        return mapped
      }
      for (const [index, message] of source.entries()) {
        const clone = copied[index]!
        expect(clone.info.id).not.toBe(message.info.id)
        expect(clone.info.sessionID).toBe(fork.id)
        for (const [partIndex, part] of message.parts.entries()) {
          const clonedPart = clone.parts[partIndex]!
          expect(clonedPart.id).not.toBe(part.id)
          if ((part.type !== "text" && part.type !== "tool") || !part.serverProvenance) continue
          const provenance = part.serverProvenance
          const expected = (() => {
            switch (provenance.type) {
              case "compaction-replay":
                return {
                  ...provenance,
                  ownerMessageID: mappedMessage(provenance.ownerMessageID),
                  sourceMessageID: mappedMessage(provenance.sourceMessageID),
                }
              case "compaction-continuation":
                return { ...provenance, ownerMessageID: mappedMessage(provenance.ownerMessageID) }
              case "subtask-output":
                return {
                  ...provenance,
                  ownerMessageID: mappedMessage(provenance.ownerMessageID),
                  taskPartID: mappedPart(provenance.taskPartID),
                }
              case "subtask-continuation":
                return {
                  ...provenance,
                  ownerMessageID: mappedMessage(provenance.ownerMessageID),
                  taskPartID: mappedPart(provenance.taskPartID),
                  sourceMessageID: mappedMessage(provenance.sourceMessageID),
                }
            }
          })()
          if (clonedPart.type !== "text" && clonedPart.type !== "tool") {
            throw new Error(`continuity clone ${clonedPart.id} has an invalid target kind`)
          }
          expect(clonedPart.serverProvenance).toEqual(expected)
        }
      }
    }),
  )

  it.instance(
    "CLI export and import preserve valid typed provenance and reject invalid graphs before persistence",
    () =>
      Effect.gen(function* () {
        const sessions = yield* SessionNs.Service
        const ctx = yield* InstanceState.context
        const graph = yield* seedContinuityGraph()
        const exported = JSON.parse(JSON.stringify(yield* collectExportData(graph.info.id))) as ExportData
        const exportedClaims = exported.messages.flatMap((message) =>
          message.parts.flatMap((part) =>
            (part.type === "text" || part.type === "tool") && part.serverProvenance ? [part.serverProvenance] : [],
          ),
        )
        expect(JSON.stringify(exportedClaims)).toBe(
          JSON.stringify([
            {
              type: "compaction-replay",
              ownerMessageID: graph.marker.message.id,
              sourceMessageID: graph.source.message.id,
            },
            { type: "compaction-continuation", ownerMessageID: graph.marker.message.id },
            {
              type: "subtask-output",
              ownerMessageID: graph.taskOwner.message.id,
              taskPartID: graph.taskPart.id,
            },
            {
              type: "subtask-continuation",
              ownerMessageID: graph.taskOwner.message.id,
              taskPartID: graph.taskPart.id,
              sourceMessageID: graph.taskOutput.id,
            },
          ]),
        )

        yield* persistImportedSession(exported, ctx)
        let persisted = yield* sessions.messages({ sessionID: graph.info.id })
        expect(
          JSON.stringify(
            persisted.flatMap((message) =>
              message.parts.flatMap((part) =>
                (part.type === "text" || part.type === "tool") && part.serverProvenance ? [part.serverProvenance] : [],
              ),
            ),
          ),
        ).toBe(JSON.stringify(exportedClaims))

        yield* remove(graph.info.id)
        for (const synthetic of [false, undefined] as const) {
          const variant = structuredClone(exported)
          const continuation = variant.messages
            .flatMap((message) => message.parts)
            .find((part) => part.id === graph.compactionContinuationPart.id)
          if (!continuation || continuation.type !== "text") throw new Error("missing compaction continuation")
          if (synthetic === undefined) delete continuation.synthetic
          else continuation.synthetic = synthetic
          yield* persistImportedSession(JSON.parse(JSON.stringify(variant)) as ExportData, ctx)
          persisted = yield* sessions.messages({ sessionID: graph.info.id })
          const persistedContinuation = persisted
            .flatMap((message) => message.parts)
            .find((part) => part.id === graph.compactionContinuationPart.id)
          expect(persistedContinuation?.type === "text" ? persistedContinuation.synthetic : true).toBe(synthetic)
          const projected = MessageV2.modelTurn(persisted).messages.map((message) => message.info.id)
          expect(projected.indexOf(graph.compactionContinuationPart.messageID)).toBeLessThan(
            projected.indexOf(graph.taskOwner.message.id),
          )
          if (synthetic === false) yield* remove(graph.info.id)
        }

        const claim = (data: typeof exported, id: PartID) => {
          const part = data.messages.flatMap((message) => message.parts).find((candidate) => candidate.id === id)
          if (!part || (part.type !== "text" && part.type !== "tool")) throw new Error(`missing claim ${id}`)
          return part
        }
        const otherSession = yield* sessions.create({ title: "cross-session owner" })
        yield* sessions.updateMessage({
          id: MessageID.make("msg_other_session_owner"),
          sessionID: otherSession.id,
          role: "user",
          agent: "build",
          model: { providerID: ProviderV2.ID.make("test"), modelID: ModelV2.ID.make("test-model") },
          time: { created: Date.now() },
        })
        const cases = [
          {
            name: "malformed discriminator",
            mutate(data: typeof exported) {
              claim(data, graph.replayPart.id).serverProvenance = { type: "unknown" } as never
            },
          },
          {
            name: "dangling owner",
            mutate(data: typeof exported) {
              claim(data, graph.compactionContinuationPart.id).serverProvenance = {
                type: "compaction-continuation",
                ownerMessageID: MessageID.make("msg_dangling_owner"),
              }
            },
          },
          {
            name: "cross-session owner",
            mutate(data: typeof exported) {
              claim(data, graph.compactionContinuationPart.id).serverProvenance = {
                type: "compaction-continuation",
                ownerMessageID: MessageID.make("msg_other_session_owner"),
              }
            },
          },
          {
            name: "wrong target kind",
            mutate(data: typeof exported) {
              claim(data, graph.replayPart.id).serverProvenance = {
                type: "subtask-output",
                ownerMessageID: graph.taskOwner.message.id,
                taskPartID: graph.taskPart.id,
              }
            },
          },
          {
            name: "invalid source direction",
            mutate(data: typeof exported) {
              claim(data, graph.replayPart.id).serverProvenance = {
                type: "compaction-replay",
                ownerMessageID: graph.marker.message.id,
                sourceMessageID: claim(data, graph.replayPart.id).messageID,
              }
            },
          },
        ]
        for (const item of cases) {
          const invalid = structuredClone(exported)
          item.mutate(invalid)
          const exit = yield* persistImportedSession(invalid, ctx).pipe(Effect.exit)
          expect(Exit.isFailure(exit), item.name).toBe(true)
          expect((yield* sessions.messages({ sessionID: graph.info.id })).length, item.name).toBe(persisted.length)
        }

        const legacy = yield* sessions.create({ title: "legacy absence" })
        const legacyMessage = yield* sessions.updateMessage({
          id: MessageID.ascending(),
          sessionID: legacy.id,
          role: "user",
          agent: "build",
          model: { providerID: ProviderV2.ID.make("test"), modelID: ModelV2.ID.make("test-model") },
          time: { created: Date.now() },
        })
        yield* sessions.updatePart({
          id: PartID.ascending(),
          sessionID: legacy.id,
          messageID: legacyMessage.id,
          type: "text",
          text: "legacy without provenance",
        })
        const legacyExport = JSON.parse(JSON.stringify(yield* collectExportData(legacy.id))) as typeof exported
        expect(JSON.stringify(legacyExport)).not.toContain("serverProvenance")
        yield* persistImportedSession(legacyExport, ctx)
        const legacyPart = (yield* sessions.messages({ sessionID: legacy.id }))[0]?.parts[0]
        expect(
          legacyPart && (legacyPart.type === "text" || legacyPart.type === "tool")
            ? legacyPart.serverProvenance
            : undefined,
        ).toBeUndefined()
      }),
  )
})

describe("SessionLifecycle", () => {
  it.instance("does not publish stale Idle over successor work accepted through the lifecycle map", () =>
    Effect.gen(function* () {
      const sessions = yield* SessionNs.Service
      const lifecycle = yield* SessionLifecycle.Service
      const status = yield* SessionStatus.Service
      const events = yield* EventV2Bridge.Service
      const info = yield* sessions.create({ title: "idle successor ownership" })
      const result = lifecycleResult(info.id)
      const activeStarted = yield* Deferred.make<void>()
      const releaseActive = yield* Deferred.make<void>()
      const idleEntered = yield* Deferred.make<void>()
      const releaseIdle = yield* Deferred.make<void>()
      const successorStarted = yield* Deferred.make<void>()
      const releaseSuccessor = yield* Deferred.make<void>()
      yield* events.listen((event) => {
        if (event.type !== SessionStatus.Event.Idle.type) return Effect.void
        const data = event.data as typeof SessionStatus.Event.Idle.data.Type
        if (data.sessionID !== info.id) return Effect.void
        return Deferred.isDone(idleEntered).pipe(
          Effect.flatMap((seen) =>
            seen
              ? Effect.void
              : Deferred.succeed(idleEntered, undefined).pipe(Effect.andThen(Deferred.await(releaseIdle))),
          ),
        )
      })
      const active = yield* lifecycle
        .ensureRunning(
          info.id,
          Effect.succeed(result),
          status
            .set(info.id, { type: "busy" })
            .pipe(
              Effect.andThen(Deferred.succeed(activeStarted, undefined)),
              Effect.andThen(Deferred.await(releaseActive)),
              Effect.as(result),
            ),
        )
        .pipe(Effect.forkChild)
      yield* Deferred.await(activeStarted)
      yield* Deferred.succeed(releaseActive, undefined)
      yield* Deferred.await(idleEntered)

      const successor = yield* lifecycle
        .submit(
          info.id,
          Effect.succeed(result),
          Effect.void,
          status
            .set(info.id, { type: "busy" })
            .pipe(
              Effect.andThen(Deferred.succeed(successorStarted, undefined)),
              Effect.andThen(Deferred.await(releaseSuccessor)),
              Effect.as(result),
            ),
        )
        .pipe(Effect.forkChild)
      yield* Effect.yieldNow
      expect(yield* Deferred.isDone(successorStarted)).toBe(false)
      yield* Deferred.succeed(releaseIdle, undefined)
      yield* Fiber.join(active)
      yield* Deferred.await(successorStarted)
      expect(yield* status.get(info.id)).toEqual({ type: "busy" })
      yield* Deferred.succeed(releaseSuccessor, undefined)
      expect((yield* Fiber.join(successor)).info.sessionID).toBe(info.id)
      expect(yield* status.get(info.id)).toEqual({ type: "idle" })
    }),
  )

  it.instance("rejects prompt runner admissions during removal and joins concurrent removals", () =>
    Effect.gen(function* () {
      const sessions = yield* SessionNs.Service
      const lifecycle = yield* SessionLifecycle.Service
      const info = yield* sessions.create({ title: "lifecycle-race" })
      const output = lifecycleResult(info.id)
      const running = yield* Deferred.make<void>()
      const interrupted = yield* Deferred.make<void>()
      const blocked = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const active = yield* lifecycle
        .ensureRunning(
          info.id,
          Effect.succeed(output),
          Deferred.succeed(running, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.ensuring(Deferred.succeed(interrupted, undefined)),
          ),
        )
        .pipe(Effect.forkChild)
      yield* Deferred.await(running)
      const admitted = yield* lifecycle
        .admit(info.id, Deferred.succeed(blocked, undefined).pipe(Effect.andThen(Deferred.await(release))))
        .pipe(Effect.forkChild)
      yield* Deferred.await(blocked)

      const firstRemoval = yield* lifecycle.remove(info.id).pipe(Effect.forkChild)
      yield* Deferred.await(interrupted)
      const secondRemoval = yield* lifecycle.remove(info.id).pipe(Effect.forkChild)
      const rejected = [
        lifecycle.submit(info.id, Effect.succeed(output), Effect.void, Effect.succeed(output)),
        lifecycle.submitManual(info.id, Effect.succeed(output), Effect.void, Effect.succeed(output)),
        lifecycle.commit(info.id, Effect.void),
        lifecycle.ensureRunning(info.id, Effect.succeed(output), Effect.succeed(output)),
        lifecycle.startShell(info.id, Effect.succeed(output), Effect.succeed(output)),
      ]
      for (const action of rejected) {
        const exit = yield* action.pipe(Effect.exit)
        const error = Exit.isFailure(exit) ? Option.getOrUndefined(Exit.findErrorOption(exit)) : undefined
        expect(SessionNs.BusyError.isInstance(error)).toBeTrue()
        if (SessionNs.BusyError.isInstance(error)) expect(error.sessionID).toBe(info.id)
      }

      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(admitted)
      yield* Fiber.join(firstRemoval)
      yield* Fiber.join(secondRemoval)
      yield* Fiber.join(active)

      const missing = yield* lifecycle.commit(info.id, Effect.void).pipe(Effect.exit)
      expect(
        Option.getOrUndefined(Exit.isFailure(missing) ? Exit.findErrorOption(missing) : Option.none()),
      ).toBeInstanceOf(NotFoundError)
    }),
  )

  it.instance("removes from inside its own lifecycle admission without self-wait", () =>
    Effect.gen(function* () {
      const sessions = yield* SessionNs.Service
      const lifecycle = yield* SessionLifecycle.Service
      const info = yield* sessions.create({ title: "self-removal" })

      yield* lifecycle.admit(info.id, lifecycle.remove(info.id))

      const missing = yield* sessions.get(info.id).pipe(Effect.exit)
      expect(
        Option.getOrUndefined(Exit.isFailure(missing) ? Exit.findErrorOption(missing) : Option.none()),
      ).toBeInstanceOf(NotFoundError)
    }),
  )

  it.instance("deletes a session tree in postorder", () =>
    Effect.gen(function* () {
      const sessions = yield* SessionNs.Service
      const lifecycle = yield* SessionLifecycle.Service
      const events = yield* EventV2Bridge.Service
      const root = yield* sessions.create({ title: "root" })
      const child = yield* sessions.create({ title: "child", parentID: root.id })
      const grandchild = yield* sessions.create({ title: "grandchild", parentID: child.id })
      const deleted: SessionID[] = []
      yield* events.listen((event) =>
        event.type === SessionNs.Event.Deleted.type
          ? Effect.sync(() => deleted.push((event.data as typeof SessionNs.Event.Deleted.data.Type).info.id))
          : Effect.void,
      )

      yield* lifecycle.remove(root.id)

      expect(deleted).toEqual([grandchild.id, child.id, root.id])
    }),
  )

  it.instance("closes workspace admission, drains work, and reopens admission after close failure", () =>
    Effect.gen(function* () {
      const lifecycle = yield* SessionLifecycle.Service
      const workspaceID = WorkspaceV2.ID.ascending()
      const info = yield* lifecycle.create({ title: "workspace-root", workspaceID })
      const started = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const stopping = yield* Deferred.make<void>()
      const admitted = yield* lifecycle
        .admit(info.id, Deferred.succeed(started, undefined).pipe(Effect.andThen(Deferred.await(release))))
        .pipe(Effect.forkChild)
      yield* Deferred.await(started)
      const closing = yield* lifecycle
        .removeWorkspace(workspaceID, Deferred.succeed(stopping, undefined), Effect.die("workspace removal failed"))
        .pipe(Effect.forkChild)
      yield* Deferred.await(stopping)

      const late = yield* lifecycle.create({ title: "late", workspaceID }).pipe(Effect.exit)
      const lateError = Exit.isFailure(late) ? Option.getOrUndefined(Exit.findErrorOption(late)) : undefined
      expect(SessionNs.BusyError.isInstance(lateError)).toBeTrue()

      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(admitted)
      const closeExit = yield* Fiber.await(closing)
      expect(String(closeExit)).toContain("workspace removal failed")

      const reopened = yield* lifecycle.create({ title: "reopened", workspaceID })
      expect(reopened.workspaceID).toBe(workspaceID)
      yield* lifecycle.remove(reopened.id)
    }),
  )
})
