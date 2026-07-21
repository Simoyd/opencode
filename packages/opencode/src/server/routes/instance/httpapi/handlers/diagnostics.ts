import { CompactionDiagnostics } from "@/diagnostic/compaction"
import { StreamDiagnostics } from "@/diagnostic/stream"
import { Effect } from "effect"
import { HttpServerRequest, HttpServerResponse } from "effect/unstable/http"
import { HttpApiBuilder } from "effect/unstable/httpapi"
import { DiagnosticsApi } from "../groups/diagnostics"

export const diagnosticsHandlers = HttpApiBuilder.group(DiagnosticsApi, "diagnostics", (handlers) =>
  Effect.gen(function* () {
    return handlers
      .handleRaw(
        "streamDiagnostics",
        Effect.fn("DiagnosticsHttpApi.streamDiagnostics")(function* () {
          if (!StreamDiagnostics.enabled()) return HttpServerResponse.empty({ status: 404 })
          return HttpServerResponse.jsonUnsafe(StreamDiagnostics.snapshot())
        }),
      )
      .handleRaw(
        "compactionIncident",
        Effect.fn("DiagnosticsHttpApi.compactionIncident")(function* () {
          const request = yield* HttpServerRequest.HttpServerRequest
          const action = new URL(request.url, "http://localhost").searchParams.get("action") ?? ""
          const snapshot = CompactionDiagnostics.snapshot(action)
          if (!snapshot) return HttpServerResponse.empty({ status: 404 })
          return HttpServerResponse.jsonUnsafe(snapshot)
        }),
      )
  }),
)
