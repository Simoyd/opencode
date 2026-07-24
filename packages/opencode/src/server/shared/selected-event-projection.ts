import { Option } from "effect"
import { HttpServerRequest } from "effect/unstable/http"
import { HttpApiError } from "effect/unstable/httpapi"

export const SelectorQuery = "oca_event_projection"
export const Selector = "transcript-history-v1"
export const AcknowledgementHeader = "X-OpenCode-Avalonia-Event-Projection"
export const CatalogChangedType = "compaction.catalog.changed"

export function includesInstanceEvent(event: { type: string; properties: unknown }) {
  if (
    event.type === "server.connected" ||
    event.type === "server.heartbeat" ||
    event.type === "server.instance.disposed" ||
    event.type === "global.disposed"
  )
    return true
  if (!event.properties || typeof event.properties !== "object" || Array.isArray(event.properties)) return false
  return "sessionID" in event.properties || "sessionId" in event.properties
}

export function selected(request: HttpServerRequest.HttpServerRequest) {
  const url = Option.getOrElse(HttpServerRequest.toURL(request), () => new URL(request.url, "http://localhost"))
  const value = url.searchParams.get(SelectorQuery)
  if (value === null) return false
  if (value !== Selector) throw new HttpApiError.BadRequest({})
  return true
}
