import { Config } from "@/config/config"
import { GlobalBus, type GlobalEvent as GlobalBusEvent } from "@/bus/global"
import { EffectBridge } from "@/effect/bridge"
import { EventV2 } from "@opencode-ai/core/event"
import { StreamDiagnostics } from "@/diagnostic/stream"
import { Installation } from "@/installation"
import { disposeAllInstancesAndEmitGlobalDisposed } from "@/server/global-lifecycle"
import { InstallationVersion } from "@opencode-ai/core/installation/version"
import * as Log from "@opencode-ai/core/util/log"
import { Effect, Queue, Schema } from "effect"
import * as Stream from "effect/Stream"
import { HttpServerRequest, HttpServerResponse } from "effect/unstable/http"
import { HttpApiBuilder } from "effect/unstable/httpapi"
import * as Sse from "effect/unstable/encoding/Sse"
import { RootHttpApi } from "../api"
import { GlobalUpgradeInput } from "../groups/global"
import * as SelectedEventProjection from "@/server/shared/selected-event-projection"

const log = Log.create({ service: "server" })

function eventData(data: unknown): Sse.Event {
  StreamDiagnostics.record({
    stage: "route.global",
    action: "write",
    eventType: StreamDiagnostics.eventType(data),
    length: JSON.stringify(data).length,
    routeMode: "global",
    shape: StreamDiagnostics.shape(data),
    correlation: StreamDiagnostics.correlationForPayload(data),
  })
  return {
    _tag: "Event",
    event: "message",
    id: undefined,
    data: JSON.stringify(data),
  }
}

function parseBody(body: string) {
  try {
    return JSON.parse(body || "{}") as unknown
  } catch {
    return undefined
  }
}

export function globalEventStream(beforeConnected: Effect.Effect<void> = Effect.void, selectedProjection = false) {
  return Effect.gen(function* () {
    // Register eagerly so an event published after request admission cannot be
    // lost while the response body starts or emits server.connected.
    const queue = yield* Queue.unbounded<GlobalBusEvent>()
    const handler = (event: GlobalBusEvent) => {
      return Queue.offerUnsafe(queue, event)
    }
    GlobalBus.on("event", handler)
    yield* Effect.addFinalizer(() => Effect.sync(() => GlobalBus.off("event", handler)))
    const events = Stream.fromQueue(queue).pipe(
      Stream.filter((event) =>
        selectedProjection
          ? SelectedEventProjection.includesInstanceEvent({
              type: event.payload.type ?? "",
              properties: event.payload.properties,
            })
          : event.payload.type !== SelectedEventProjection.CatalogChangedType,
      ),
    )
    const heartbeat = Stream.tick("10 seconds").pipe(
      Stream.drop(1),
      Stream.map(() => ({ payload: { id: EventV2.ID.create(), type: "server.heartbeat", properties: {} } })),
      Stream.tap((event) =>
        Effect.sync(() =>
          StreamDiagnostics.record({
            stage: "route.global",
            action: "queue",
            eventType: "server.heartbeat",
            length: JSON.stringify(event).length,
            routeMode: "global",
            shape: "global-envelope",
          }),
        ),
      ),
    )
    yield* beforeConnected

    log.info("global event connected")
    StreamDiagnostics.record({
      stage: "route.global",
      action: "connect",
      routeMode: "global",
      readiness: "connected",
    })
    const connected = { payload: { id: EventV2.ID.create(), type: "server.connected", properties: {} } }
    StreamDiagnostics.record({
      stage: "route.global",
      action: "queue",
      eventType: "server.connected",
      length: JSON.stringify(connected).length,
      routeMode: "global",
      shape: "global-envelope",
    })

    return Stream.make(connected).pipe(
      Stream.concat(
        events.pipe(
          Stream.tap((event) =>
            Effect.sync(() => {
              StreamDiagnostics.record({
                stage: "route.global",
                action: "queue",
                eventType: StreamDiagnostics.eventType(event),
                length: JSON.stringify(event).length,
                routeMode: "global",
                shape: StreamDiagnostics.shape(event),
                correlation: StreamDiagnostics.correlationForPayload(event),
              })
            }),
          ),
          Stream.merge(heartbeat, { haltStrategy: "left" }),
        ),
      ),
    )
  })
}

function eventResponse() {
  return Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest
    const selectedProjection = SelectedEventProjection.selected(request)
    const events = yield* globalEventStream(Effect.void, selectedProjection)
    return HttpServerResponse.stream(
      events.pipe(
        Stream.map(eventData),
        Stream.pipeThroughChannel(Sse.encode()),
        Stream.encodeText,
        Stream.ensuring(
          Effect.sync(() => {
            log.info("global event disconnected")
            StreamDiagnostics.record({ stage: "route.global", action: "disconnect", routeMode: "global" })
          }),
        ),
      ),
      {
        contentType: "text/event-stream",
        headers: {
          "Cache-Control": "no-cache, no-transform",
          "X-Accel-Buffering": "no",
          "X-Content-Type-Options": "nosniff",
          ...(selectedProjection
            ? { [SelectedEventProjection.AcknowledgementHeader]: SelectedEventProjection.Selector }
            : {}),
        },
      },
    )
  })
}

export const globalHandlers = HttpApiBuilder.group(RootHttpApi, "global", (handlers) =>
  Effect.gen(function* () {
    const config = yield* Config.Service
    const installation = yield* Installation.Service
    const bridge = yield* EffectBridge.make()

    const health = Effect.fn("GlobalHttpApi.health")(function* () {
      return { healthy: true as const, version: InstallationVersion }
    })

    const event = Effect.fn("GlobalHttpApi.event")(function* () {
      return yield* eventResponse()
    })

    const configGet = Effect.fn("GlobalHttpApi.configGet")(function* () {
      return yield* config.getGlobal()
    })

    const configUpdate = Effect.fn("GlobalHttpApi.configUpdate")(function* (ctx) {
      const result = yield* config.updateGlobal(ctx.payload)
      if (result.changed) bridge.fork(disposeAllInstancesAndEmitGlobalDisposed({ swallowErrors: true }))
      return result.info
    })

    const dispose = Effect.fn("GlobalHttpApi.dispose")(function* () {
      yield* disposeAllInstancesAndEmitGlobalDisposed()
      return true
    })

    const upgrade = Effect.fn("GlobalHttpApi.upgrade")(function* (ctx: { payload: typeof GlobalUpgradeInput.Type }) {
      const method = yield* installation.method()
      if (method === "unknown") {
        return {
          status: 400,
          body: { success: false as const, error: "Unknown installation method" },
        }
      }
      const target = ctx.payload.target || (yield* installation.latest(method))
      const result = yield* installation.upgrade(method, target).pipe(
        Effect.as({ status: 200, body: { success: true as const, version: target } }),
        Effect.catch((err) =>
          Effect.succeed({
            status: 500,
            body: {
              success: false as const,
              error: err instanceof Error ? err.message : String(err),
            },
          }),
        ),
      )
      if (!result.body.success) return result
      GlobalBus.emit("event", {
        directory: "global",
        payload: {
          type: Installation.Event.Updated.type,
          properties: { version: target },
        },
      })
      return result
    })

    const upgradeRaw = Effect.fn("GlobalHttpApi.upgradeRaw")(function* (ctx: {
      request: HttpServerRequest.HttpServerRequest
    }) {
      const body = yield* Effect.orDie(ctx.request.text)
      const json = parseBody(body)
      if (json === undefined) {
        return HttpServerResponse.jsonUnsafe({ success: false, error: "Invalid request body" }, { status: 400 })
      }
      const payload = yield* Schema.decodeUnknownEffect(GlobalUpgradeInput)(json).pipe(
        Effect.map((payload) => ({ valid: true as const, payload })),
        Effect.catch(() => Effect.succeed({ valid: false as const })),
      )
      if (!payload.valid) {
        return HttpServerResponse.jsonUnsafe({ success: false, error: "Invalid request body" }, { status: 400 })
      }
      const result = yield* upgrade({ payload: payload.payload })
      return HttpServerResponse.jsonUnsafe(result.body, { status: result.status })
    })

    return handlers
      .handle("health", health)
      .handleRaw("event", event)
      .handle("configGet", configGet)
      .handle("configUpdate", configUpdate)
      .handle("dispose", dispose)
      .handleRaw("upgrade", upgradeRaw)
  }),
)
