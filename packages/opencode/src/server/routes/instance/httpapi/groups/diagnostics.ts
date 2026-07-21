import { Schema } from "effect"
import { HttpApi, HttpApiEndpoint, HttpApiGroup, OpenApi } from "effect/unstable/httpapi"
import { described } from "./metadata"

export const DiagnosticsPaths = {
  streamDiagnostics: "/debug/stream-diagnostics",
  compactionIncident: "/debug/compaction-incident",
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

export const CompactionIncidentQuery = Schema.Struct({
  action: Schema.String,
})

export const CompactionIncidentSnapshot = Schema.Struct({
  schema: Schema.String,
  actionToken: Schema.String,
  available: Schema.Boolean,
  persistenceFailed: Schema.Boolean,
  firstSequence: Schema.Number,
  lastSequence: Schema.Number,
  persistedThroughSequence: Schema.Number,
  records: Schema.Array(Schema.Record(Schema.String, Schema.Any)),
}).annotate({ identifier: "CompactionIncidentSnapshot" })

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
    .add(
      HttpApiEndpoint.get("compactionIncident", DiagnosticsPaths.compactionIncident, {
        query: CompactionIncidentQuery,
        success: described(CompactionIncidentSnapshot, "Compaction incident fragment"),
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "debug.compactionIncident",
          summary: "Get a compaction incident fragment",
          description: "Return one sanitized persisted sidecar fragment for a host-issued Compact action token.",
        }),
      ),
    )
    .annotateMerge(OpenApi.annotations({ title: "diagnostics", description: "Debug diagnostics routes." })),
)
