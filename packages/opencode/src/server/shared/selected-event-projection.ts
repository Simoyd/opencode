import { Option } from "effect"
import { HttpServerRequest } from "effect/unstable/http"
import { HttpApiError } from "effect/unstable/httpapi"

export const SelectorQuery = "oca_event_projection"
export const Selector = "transcript-history-v1"
export const AcknowledgementHeader = "X-OpenCode-Avalonia-Event-Projection"
export const CatalogChangedType = "compaction.catalog.changed"

export function selected(request: HttpServerRequest.HttpServerRequest) {
  const url = Option.getOrElse(HttpServerRequest.toURL(request), () => new URL(request.url, "http://localhost"))
  const value = url.searchParams.get(SelectorQuery)
  if (value === null) return false
  if (value !== Selector) throw new HttpApiError.BadRequest({})
  return true
}
