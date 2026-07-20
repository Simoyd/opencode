import { NodeHttpServer } from "@effect/platform-node"
import { describe, expect } from "bun:test"
import { Context, Deferred, Effect, Fiber, Layer, Option, Stream } from "effect"
import { HttpBody, HttpClient, HttpClientRequest, HttpRouter } from "effect/unstable/http"
import { HttpApiBuilder } from "effect/unstable/httpapi"
import { Auth } from "../../src/auth"
import { Config } from "../../src/config/config"
import { Installation } from "../../src/installation"
import { GlobalBus } from "../../src/bus/global"
import { MoveSession } from "@opencode-ai/core/control-plane/move-session"
import { ServerAuth } from "../../src/server/auth"
import { RootHttpApi } from "../../src/server/routes/instance/httpapi/api"
import { GlobalPaths } from "../../src/server/routes/instance/httpapi/groups/global"
import { controlHandlers } from "../../src/server/routes/instance/httpapi/handlers/control"
import { controlPlaneHandlers } from "../../src/server/routes/instance/httpapi/handlers/control-plane"
import { globalEventStream, globalHandlers } from "../../src/server/routes/instance/httpapi/handlers/global"
import { authorizationLayer } from "../../src/server/routes/instance/httpapi/middleware/authorization"
import { schemaErrorLayer } from "../../src/server/routes/instance/httpapi/middleware/schema-error"
import { testEffect } from "../lib/effect"

const apiLayer = HttpRouter.serve(
  HttpApiBuilder.layer(RootHttpApi).pipe(
    Layer.provide([controlHandlers, controlPlaneHandlers, globalHandlers]),
    Layer.provide([authorizationLayer, schemaErrorLayer]),
    // Raw HttpApi routes expose an opaque handler context at the request boundary.
    // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion
    HttpRouter.provideRequest(Layer.succeedContext(Context.empty() as Context.Context<unknown>)),
  ),
  { disableListenLog: true, disableLogger: true },
).pipe(
  Layer.provideMerge(NodeHttpServer.layerTest),
  Layer.provide(Layer.mock(Auth.Service)({})),
  Layer.provide(Layer.mock(Config.Service)({})),
  Layer.provide(Layer.mock(MoveSession.Service)({})),
  Layer.provide(
    Layer.mock(Installation.Service)({
      method: () => Effect.succeed("npm"),
      latest: () => Effect.succeed("9.9.9"),
      upgrade: () => Effect.void,
    }),
  ),
  Layer.provide(ServerAuth.Config.layer({ password: Option.none(), username: "opencode" })),
)
const it = testEffect(apiLayer)

describe("global HttpApi", () => {
  it.effect("continues global event delivery after an observer throws", () =>
    Effect.acquireUseRelease(
      Effect.sync(() => {
        const received = new Array<string>()
        const broken = () => {
          throw new Error("observer failed")
        }
        const healthy = (event: { payload: { type: string } }) => received.push(event.payload.type)
        GlobalBus.on("event", broken)
        GlobalBus.on("event", healthy)
        return { broken, healthy, received }
      }),
      ({ received }) =>
        Effect.sync(() => {
          expect(
            GlobalBus.emit("event", {
              directory: "/workspace",
              payload: { type: "test.observer-isolation", properties: {} },
            }),
          ).toBe(true)
          expect(received).toEqual(["test.observer-isolation"])
        }),
      ({ broken, healthy }) =>
        Effect.sync(() => {
          GlobalBus.off("event", broken)
          GlobalBus.off("event", healthy)
        }),
    ),
  )

  it.live("registers before connected and preserves acquisition events across reconnect", () =>
    Effect.gen(function* () {
      const baselineListeners = GlobalBus.listenerCount("event")

      const connect = (eventType: string) =>
        Effect.scoped(
          Effect.gen(function* () {
            const barrierReached = yield* Deferred.make<void>()
            const releaseConnected = yield* Deferred.make<void>()
            const streamFiber = yield* globalEventStream(
              Deferred.succeed(barrierReached, undefined).pipe(Effect.andThen(Deferred.await(releaseConnected))),
            ).pipe(Effect.forkScoped)

            yield* Deferred.await(barrierReached)
            expect(GlobalBus.listenerCount("event")).toBe(baselineListeners + 1)

            GlobalBus.emit("event", {
              directory: "/workspace",
              payload: { type: eventType, properties: { sequence: 1 } },
            })
            yield* Deferred.succeed(releaseConnected, undefined)

            const stream = yield* Fiber.join(streamFiber)
            const events = yield* stream.pipe(Stream.take(2), Stream.runCollect)
            expect(Array.from(events).map((event) => event.payload.type)).toEqual(["server.connected", eventType])
          }),
        )

      yield* connect("test.acquisition.first")
      expect(GlobalBus.listenerCount("event")).toBe(baselineListeners)
      yield* connect("test.acquisition.reconnect")
      expect(GlobalBus.listenerCount("event")).toBe(baselineListeners)
    }),
  )

  it.live("upgrades to latest when the request body is omitted", () =>
    Effect.gen(function* () {
      const response = yield* HttpClient.post(GlobalPaths.upgrade)

      expect(response.status).toBe(200)
      expect(yield* response.json).toEqual({ success: true, version: "9.9.9" })
    }),
  )

  it.live("rejects malformed upgrade payloads", () =>
    Effect.gen(function* () {
      const response = yield* HttpClientRequest.post(GlobalPaths.upgrade).pipe(
        HttpClientRequest.setBody(HttpBody.text("{", "application/json")),
        HttpClient.execute,
      )

      expect(response.status).toBe(400)
      expect(yield* response.json).toEqual({ success: false, error: "Invalid request body" })
    }),
  )
})
