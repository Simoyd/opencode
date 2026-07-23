import { Schema } from "effect"
import { HttpApi, HttpApiEndpoint, HttpApiGroup, OpenApi } from "effect/unstable/httpapi"
import { described } from "./metadata"

export const DiagnosticsPaths = {
  streamDiagnostics: "/debug/stream-diagnostics",
} as const

export const StreamDiagnosticsSnapshot = Schema.Struct({
  schema: Schema.String,
  enabled: Schema.Boolean,
  limit: Schema.Number,
  summaryLimit: Schema.Number,
  dropped: Schema.Number,
  droppedSummaries: Schema.Number,
  overflow: Schema.Boolean,
  summaryOverflow: Schema.Boolean,
  stageSummaries: Schema.Array(Schema.Record(Schema.String, Schema.Any)),
  events: Schema.Array(Schema.Record(Schema.String, Schema.Any)),
}).annotate({ identifier: "StreamDiagnosticsSnapshot" })

export const DiagnosticsApi = HttpApi.make("diagnostics").add(
  HttpApiGroup.make("diagnostics")
    .add(
      HttpApiEndpoint.get("streamDiagnostics", DiagnosticsPaths.streamDiagnostics, {
        success: described(StreamDiagnosticsSnapshot, "Stream diagnostics snapshot"),
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "debug.streamDiagnostics",
          summary: "Get stream diagnostics",
          description: "Return sanitized aggregate stream diagnostics when enabled for the custom sidecar.",
        }),
      ),
    )
    .annotateMerge(OpenApi.annotations({ title: "diagnostics", description: "Debug diagnostics routes." })),
)
