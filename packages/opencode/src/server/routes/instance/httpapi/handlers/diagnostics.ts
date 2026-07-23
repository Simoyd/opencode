import { StreamDiagnostics } from "@/diagnostic/stream"
import { Effect } from "effect"
import { HttpServerResponse } from "effect/unstable/http"
import { HttpApiBuilder } from "effect/unstable/httpapi"
import { DiagnosticsApi } from "../groups/diagnostics"

export const diagnosticsHandlers = HttpApiBuilder.group(DiagnosticsApi, "diagnostics", (handlers) =>
  Effect.gen(function* () {
    return handlers.handleRaw(
      "streamDiagnostics",
      Effect.fn("DiagnosticsHttpApi.streamDiagnostics")(function* () {
        if (!StreamDiagnostics.enabled()) return HttpServerResponse.empty({ status: 404 })
        return HttpServerResponse.jsonUnsafe(StreamDiagnostics.snapshot())
      }),
    )
  }),
)
