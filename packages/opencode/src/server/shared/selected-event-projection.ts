import { Option, Schema } from "effect"
import { HttpServerRequest } from "effect/unstable/http"
import { HttpApiError } from "effect/unstable/httpapi"

export const SelectorQuery = "oca_event_projection"
export const Selector = "transcript-history-v1"
export const AcknowledgementHeader = "X-OpenCode-Avalonia-Event-Projection"
export const CatalogChangedType = "compaction.catalog.changed"

export const EventTypes = [
  "server.connected",
  "server.heartbeat",
  "server.instance.disposed",
  "global.disposed",
  "session.status",
  "permission.asked",
  "permission.replied",
  "question.asked",
  "question.replied",
  "question.rejected",
  "message.updated",
  "message.removed",
  "message.part.updated",
  "message.part.delta",
  "message.part.removed",
  "session.updated",
  "todo.updated",
  "session.created",
  "session.deleted",
  "session.compacted",
  CatalogChangedType,
  "session.error",
] as const
export const EventType = Schema.Literals(EventTypes)
const eventTypes = new Set<string>(EventTypes)

export function includesEventType(type: string) {
  return eventTypes.has(type)
}

export function includesInstanceEvent(event: { type: string; properties: unknown }) {
  if (!includesEventType(event.type)) return false
  if (
    event.type === "server.connected" ||
    event.type === "server.heartbeat" ||
    event.type === "server.instance.disposed" ||
    event.type === "global.disposed"
  )
    return true
  if (!event.properties || typeof event.properties !== "object" || Array.isArray(event.properties)) return false
  const properties = event.properties as { sessionID?: unknown; sessionId?: unknown }
  return typeof properties.sessionID === "string" && properties.sessionId === undefined
}

export function acknowledgement(selectedProjection: boolean) {
  return selectedProjection ? { [AcknowledgementHeader]: Selector } : {}
}

export function selected(request: HttpServerRequest.HttpServerRequest) {
  const url = Option.getOrElse(HttpServerRequest.toURL(request), () => new URL(request.url, "http://localhost"))
  const value = url.searchParams.get(SelectorQuery)
  if (value === null) return false
  if (value !== Selector) throw new HttpApiError.BadRequest({})
  return true
}

export * as SelectedEventProjection from "./selected-event-projection"
