import { EventV2Bridge } from "@/event-v2-bridge"
import { InstanceState } from "@/effect/instance-state"
import { GlobalBus } from "@/bus/global"
import { EventV2 } from "@opencode-ai/core/event"
import { Effect, Option, Queue } from "effect"
import * as Stream from "effect/Stream"
import { HttpServerRequest, HttpServerResponse } from "effect/unstable/http"
import { HttpApiBuilder } from "effect/unstable/httpapi"
import * as Sse from "effect/unstable/encoding/Sse"
import { EventApi } from "../groups/event"
import { SelectedEventProjection } from "@/server/shared/selected-event-projection"

type InstanceEvent = { id: string; type: string; properties: unknown }

function eventData(data: unknown): Sse.Event {
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

function sessionMatches(sessionID: string | undefined, event: InstanceEvent, selectedProjection: boolean) {
  if (!sessionID) return true
  if (
    event.type === "server.connected" ||
    event.type === "server.heartbeat" ||
    event.type === "server.instance.disposed" ||
    event.type === "global.disposed"
  )
    return true
  if (!event.properties || typeof event.properties !== "object" || Array.isArray(event.properties)) return false
  const properties = event.properties as { sessionID?: unknown; sessionId?: unknown }
  if (selectedProjection) {
    if (typeof properties.sessionID !== "string" || properties.sessionId !== undefined) {
      throw new Error(`Selected ${event.type} event omitted canonical sessionID ownership`)
    }
    return properties.sessionID === sessionID
  }
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
    const typeFilter = new Set(
      (url.searchParams.get("type") ?? "")
        .split(",")
        .map((item) => item.trim())
        .filter(Boolean),
    )
    const instance = yield* InstanceState.context
    const workspaceID = yield* InstanceState.workspaceID
    // Listener registration is eager, so events published after this point cannot
    // be lost while the HTTP body fiber is starting or emitting server.connected.
    const queue = yield* Queue.unbounded<EventV2.Payload>()
    const selectedQueue = yield* Queue.unbounded<InstanceEvent>()
    const belongsToInstance = (event: EventV2.Payload) =>
      event.location?.directory === instance.directory &&
      (event.location.workspaceID === undefined || event.location.workspaceID === workspaceID)
    const unsubscribe = selectedProjection
      ? yield* events.route((event) => {
          if (!belongsToInstance(event)) return
          Queue.offerUnsafe(selectedQueue, { id: event.id, type: event.type, properties: event.data })
        })
      : yield* events.listen((event) => Effect.sync(() => Queue.offerUnsafe(queue, event)))
    yield* Effect.addFinalizer(() => unsubscribe)
    const stream = Stream.fromQueue(queue).pipe(
      Stream.filter((event) => belongsToInstance(event)),
      Stream.map((event) => ({ id: event.id, type: event.type, properties: event.data })),
    )
    const disposedQueue = yield* Queue.unbounded<InstanceEvent>()
    const disposedListener = (event: {
      directory?: string
      payload: { id?: string; type?: string; properties?: unknown }
    }) => {
      const type = event.payload.type
      if (type !== "global.disposed" && (type !== "server.instance.disposed" || event.directory !== instance.directory))
        return
      Queue.offerUnsafe(selectedProjection ? selectedQueue : disposedQueue, {
        id: event.payload.id ?? eventID(),
        type,
        properties: event.payload.properties ?? {},
      })
    }
    GlobalBus.on("event", disposedListener)
    yield* Effect.addFinalizer(() => Effect.sync(() => GlobalBus.off("event", disposedListener)))
    const disposed = Stream.fromQueue(disposedQueue)
    const ordered = selectedProjection
      ? Stream.fromQueue(selectedQueue)
      : stream.pipe(Stream.merge(disposed, { haltStrategy: "left" }))
    const output = ordered.pipe(
      Stream.filter(
        (event) =>
          (selectedProjection || event.type !== SelectedEventProjection.CatalogChangedType) &&
          (!selectedProjection || SelectedEventProjection.includesInstanceEvent(event)) &&
          typeMatches(typeFilter, event) &&
          sessionMatches(sessionID, event, selectedProjection),
      ),
      Stream.takeUntil((event) => event.type === "server.instance.disposed" || event.type === "global.disposed"),
    )
    const heartbeat = Stream.tick("10 seconds").pipe(
      Stream.drop(1),
      Stream.map(() => ({ id: eventID(), type: "server.heartbeat", properties: {} })),
    )

    yield* Effect.logInfo("event connected")
    const connected = { id: eventID(), type: "server.connected", properties: {} }
    return HttpServerResponse.stream(
      Stream.make(connected).pipe(
        Stream.concat(output.pipe(Stream.merge(heartbeat, { haltStrategy: "left" }))),
        Stream.map(eventData),
        Stream.pipeThroughChannel(Sse.encode()),
        Stream.encodeText,
        Stream.ensuring(Effect.logInfo("event disconnected")),
      ),
      {
        contentType: "text/event-stream",
        headers: {
          "Cache-Control": "no-cache, no-transform",
          "X-Accel-Buffering": "no",
          "X-Content-Type-Options": "nosniff",
          ...SelectedEventProjection.acknowledgement(selectedProjection),
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
