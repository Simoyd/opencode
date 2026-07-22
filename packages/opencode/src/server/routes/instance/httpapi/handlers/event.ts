import { EventV2Bridge } from "@/event-v2-bridge"
import { InstanceState } from "@/effect/instance-state"
import { GlobalBus } from "@/bus/global"
import { StreamDiagnostics } from "@/diagnostic/stream"
import { EventV2 } from "@opencode-ai/core/event"
import * as Log from "@opencode-ai/core/util/log"
import { Effect, Option, Queue } from "effect"
import * as Stream from "effect/Stream"
import { HttpServerRequest, HttpServerResponse } from "effect/unstable/http"
import { HttpApiBuilder } from "effect/unstable/httpapi"
import * as Sse from "effect/unstable/encoding/Sse"
import { EventApi } from "../groups/event"
import * as SelectedEventProjection from "@/server/shared/selected-event-projection"

const log = Log.create({ service: "server" })

type InstanceEvent = { id: string; type: string; properties: unknown }

function eventData(data: unknown, routeMode: string, correlation?: string): Sse.Event {
  StreamDiagnostics.record({
    stage: "route.event",
    action: "write",
    eventType: StreamDiagnostics.eventType(data),
    length: JSON.stringify(data).length,
    routeMode,
    shape: StreamDiagnostics.shape(data),
    correlation,
  })
  return {
    _tag: "Event",
    event: "message",
    id: undefined,
    data: JSON.stringify(data),
  }
}

function eventID() {
  return EventV2.ID.create()
}

function correlationForSessionEvent(sessionID: string | undefined, event?: unknown) {
  if (sessionID) return StreamDiagnostics.correlationForSession(sessionID)
  if (!event || typeof event !== "object") return undefined
  return StreamDiagnostics.correlationForSession(
    (event as { properties?: { sessionID?: string } }).properties?.sessionID,
  )
}

function sessionMatches(sessionID: string | undefined, event: InstanceEvent) {
  if (!sessionID) return true
  const properties = event.properties as { sessionID?: string }
  return !properties.sessionID || properties.sessionID === sessionID
}

function typeMatches(typeFilter: Set<string>, event: InstanceEvent) {
  return typeFilter.size === 0 || typeFilter.has(event.type)
}

function eventResponse(events: EventV2.Interface) {
  return Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest
    const url = Option.getOrElse(HttpServerRequest.toURL(request), () => new URL(request.url, "http://localhost"))
    const sessionID = url.searchParams.get("sessionID") ?? undefined
    const selectedProjection = SelectedEventProjection.selected(request)
    const routeMode = sessionID ? "instance-event-filtered" : "instance-event-unfiltered"
    const typeFilter = new Set(
      (url.searchParams.get("type") ?? "")
        .split(",")
        .map((item) => item.trim())
        .filter(Boolean),
    )
    const correlationForEvent = (event?: unknown) => correlationForSessionEvent(sessionID, event)

    const instance = yield* InstanceState.context
    const workspaceID = yield* InstanceState.workspaceID
    // Listener registration is eager, so events published after this point cannot
    // be lost while the HTTP body fiber is starting or emitting server.connected.
    const queue = yield* Queue.unbounded<EventV2.Payload>()
    const unsubscribe = yield* events.listen((event) => Effect.sync(() => Queue.offerUnsafe(queue, event)))
    yield* Effect.addFinalizer(() => unsubscribe)
    const stream = Stream.fromQueue(queue).pipe(
      Stream.filter(
        (event) =>
          event.location?.directory === instance.directory &&
          (event.location.workspaceID === undefined || event.location.workspaceID === workspaceID),
      ),
      Stream.map((event) => ({ id: event.id, type: event.type, properties: event.data })),
    )
    const disposed = Stream.callback<InstanceEvent>((queue) => {
      const listener = (event: {
        directory?: string
        payload: { id?: string; type?: string; properties?: unknown }
      }) => {
        if (event.directory !== instance.directory || event.payload.type !== "server.instance.disposed") return
        Queue.offerUnsafe(queue, {
          id: event.payload.id ?? eventID(),
          type: "server.instance.disposed",
          properties: event.payload.properties ?? {},
        })
      }
      return Effect.acquireRelease(
        Effect.sync(() => GlobalBus.on("event", listener)),
        () => Effect.sync(() => GlobalBus.off("event", listener)),
      )
    })
    const output = stream.pipe(
      Stream.merge(disposed, { haltStrategy: "left" }),
      Stream.filter(
        (event) =>
          (selectedProjection || event.type !== SelectedEventProjection.CatalogChangedType) &&
          typeMatches(typeFilter, event) &&
          sessionMatches(sessionID, event),
      ),
      Stream.tap((event) =>
        Effect.sync(() =>
          StreamDiagnostics.record({
            stage: "route.event",
            action: "queue",
            eventType: StreamDiagnostics.eventType(event),
            length: JSON.stringify(event).length,
            routeMode,
            shape: StreamDiagnostics.shape(event),
            correlation: correlationForEvent(event),
            match: true,
          }),
        ),
      ),
      Stream.takeUntil((event) => event.type === "server.instance.disposed"),
    )
    const heartbeat = Stream.tick("10 seconds").pipe(
      Stream.drop(1),
      Stream.map(() => ({ id: eventID(), type: "server.heartbeat", properties: {} })),
      Stream.tap((event) =>
        Effect.sync(() =>
          StreamDiagnostics.record({
            stage: "route.event",
            action: "queue",
            eventType: "server.heartbeat",
            length: JSON.stringify(event).length,
            routeMode,
            shape: "bus-payload",
            correlation: correlationForEvent(event),
          }),
        ),
      ),
    )

    log.info("event connected")
    StreamDiagnostics.record({
      stage: "route.event",
      action: "connect",
      routeMode,
      readiness: "connected",
      count: typeFilter.size,
      correlation: correlationForEvent(),
    })
    const connected = { id: eventID(), type: "server.connected", properties: {} }
    StreamDiagnostics.record({
      stage: "route.event",
      action: "queue",
      eventType: "server.connected",
      length: JSON.stringify(connected).length,
      routeMode,
      shape: "bus-payload",
      correlation: correlationForEvent(connected),
    })
    return HttpServerResponse.stream(
      Stream.make(connected).pipe(
        Stream.concat(output.pipe(Stream.merge(heartbeat, { haltStrategy: "left" }))),
        Stream.map((event) => eventData(event, routeMode, correlationForEvent(event))),
        Stream.pipeThroughChannel(Sse.encode()),
        Stream.encodeText,
        Stream.ensuring(
          Effect.sync(() => {
            log.info("event disconnected")
            StreamDiagnostics.record({
              stage: "route.event",
              action: "disconnect",
              routeMode,
              correlation: correlationForEvent(),
            })
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

export const eventHandlers = HttpApiBuilder.group(EventApi, "event", (handlers) =>
  Effect.gen(function* () {
    const events = yield* EventV2Bridge.Service
    return handlers.handleRaw(
      "subscribe",
      Effect.fn("EventHttpApi.subscribe")(function* () {
        return yield* eventResponse(events)
      }),
    )
  }),
)
