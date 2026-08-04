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

const it = testEffect(
  AppNodeBuilder.build(
    LayerNode.group([
      SessionNs.node,
      SessionLifecycle.node,
      EventV2Bridge.node,
      SessionProjector.node,
      CrossSpawnSpawner.node,
      InstanceStore.node,
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
})

describe("SessionLifecycle", () => {
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
        .admit(
          info.id,
          Deferred.succeed(blocked, undefined).pipe(Effect.andThen(Deferred.await(release))),
        )
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
      expect(Option.getOrUndefined(Exit.isFailure(missing) ? Exit.findErrorOption(missing) : Option.none())).toBeInstanceOf(
        NotFoundError,
      )
    }),
  )

  it.instance("removes from inside its own lifecycle admission without self-wait", () =>
    Effect.gen(function* () {
      const sessions = yield* SessionNs.Service
      const lifecycle = yield* SessionLifecycle.Service
      const info = yield* sessions.create({ title: "self-removal" })

      yield* lifecycle.admit(info.id, lifecycle.remove(info.id))

      const missing = yield* sessions.get(info.id).pipe(Effect.exit)
      expect(Option.getOrUndefined(Exit.isFailure(missing) ? Exit.findErrorOption(missing) : Option.none())).toBeInstanceOf(
        NotFoundError,
      )
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
        .admit(
          info.id,
          Deferred.succeed(started, undefined).pipe(Effect.andThen(Deferred.await(release))),
        )
        .pipe(Effect.forkChild)
      yield* Deferred.await(started)
      const closing = yield* lifecycle
        .removeWorkspace(
          workspaceID,
          Deferred.succeed(stopping, undefined),
          Effect.die("workspace removal failed"),
        )
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
