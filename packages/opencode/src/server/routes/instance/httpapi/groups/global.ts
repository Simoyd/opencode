import { ConfigV1 } from "@opencode-ai/core/v1/config/config"
import { EventV2 } from "@opencode-ai/core/event"
import { EventManifest } from "@/event-manifest"
import { Event as ServerEvent, InstanceDisposed } from "@/server/event"
import "@opencode-ai/core/account"
import "@/server/event"
import { Schema } from "effect"
import { HttpApi, HttpApiEndpoint, HttpApiError, HttpApiGroup, HttpApiSchema, OpenApi } from "effect/unstable/httpapi"
import { described } from "./metadata"
import { SelectedEventProjection } from "@/server/shared/selected-event-projection"

const GlobalHealth = Schema.Struct({
  healthy: Schema.Literal(true),
  version: Schema.String,
})

const SyncEventSchemas = EventManifest.Latest.values()
  .flatMap((definition) => {
    if (!definition.durable) return []
    return [
      Schema.Struct({
        type: Schema.Literal("sync"),
        id: EventV2.ID,
        syncEvent: Schema.Struct({
          type: Schema.Literal(EventV2.versionedType(definition.type, definition.durable.version)),
          id: EventV2.ID,
          seq: Schema.Finite,
          aggregateID: Schema.String,
          data: definition.data,
        }),
      }).annotate({ identifier: `SyncEvent.${definition.type}` }),
    ]
  })
  .toArray()

const GlobalControlPayload = Schema.Union([
  Schema.Struct({
    id: EventV2.ID,
    type: Schema.Literal(ServerEvent.Connected.type),
    properties: ServerEvent.Connected.data,
  }),
  Schema.Struct({
    id: EventV2.ID,
    type: Schema.Literal(ServerEvent.Heartbeat.type),
    properties: ServerEvent.Heartbeat.data,
  }),
])

const GlobalDirectoryPayload = Schema.Union([
  ...EventManifest.Latest.values()
    .filter(
      (definition) =>
        SelectedEventProjection.includesEventType(definition.type) &&
        definition.type !== ServerEvent.Connected.type &&
        definition.type !== ServerEvent.Heartbeat.type,
    )
    .map((definition) =>
      Schema.Struct({ id: EventV2.ID, type: Schema.Literal(definition.type), properties: definition.data }),
    )
    .toArray(),
  ...EventManifest.Latest.values()
    .filter(
      (definition) =>
        !SelectedEventProjection.includesEventType(definition.type) &&
        definition.type !== ServerEvent.Connected.type &&
        definition.type !== ServerEvent.Heartbeat.type,
    )
    .map((definition) =>
      Schema.Struct({ id: EventV2.ID, type: Schema.Literal(definition.type), properties: definition.data }),
    )
    .toArray(),
  InstanceDisposed,
  ...SyncEventSchemas,
])

const GlobalEventSchema = Schema.Union([
  Schema.Struct({ payload: GlobalControlPayload }),
  Schema.Struct({
    directory: Schema.String,
    project: Schema.optional(Schema.String),
    workspace: Schema.optional(Schema.String),
    payload: GlobalDirectoryPayload,
  }),
]).annotate({ identifier: "GlobalEvent" })

export const GlobalUpgradeInput = Schema.Struct({
  target: Schema.optional(Schema.String),
})

const GlobalUpgradeResult = Schema.Union([
  Schema.Struct({
    success: Schema.Literal(true),
    version: Schema.String,
  }),
  Schema.Struct({
    success: Schema.Literal(false),
    error: Schema.String,
  }),
])

export const GlobalPaths = {
  health: "/global/health",
  event: "/global/event",
  config: "/global/config",
  dispose: "/global/dispose",
  upgrade: "/global/upgrade",
} as const

export const GlobalApi = HttpApi.make("global").add(
  HttpApiGroup.make("global")
    .add(
      HttpApiEndpoint.get("health", GlobalPaths.health, {
        success: described(GlobalHealth, "Health information"),
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "global.health",
          summary: "Get health",
          description: "Get health information about the OpenCode server.",
        }),
      ),
      HttpApiEndpoint.get("event", GlobalPaths.event, {
        query: Schema.Struct({
          [SelectedEventProjection.SelectorQuery]: Schema.optional(Schema.Literal(SelectedEventProjection.Selector)),
        }),
        success: HttpApiSchema.StreamSse({ data: GlobalEventSchema }),
        error: HttpApiError.BadRequest,
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "global.event",
          summary: "Get global events",
          description: "Subscribe to global events from the OpenCode system using server-sent events.",
          transform: (operation) => ({
            ...operation,
            responses: {
              ...operation.responses,
              200: {
                ...operation.responses[200],
                content: {
                  ...operation.responses[200]?.content,
                  "text/event-stream": {
                    ...operation.responses[200]?.content?.["text/event-stream"],
                    schema: { $ref: "#/components/schemas/GlobalEvent" },
                  },
                },
                headers: {
                  [SelectedEventProjection.AcknowledgementHeader]: {
                    required: false,
                    description: "Present only when the matching event projection selector was requested.",
                    schema: { type: "string", enum: [SelectedEventProjection.Selector] },
                  },
                },
              },
            },
          }),
        }),
      ),
      HttpApiEndpoint.get("configGet", GlobalPaths.config, {
        success: described(ConfigV1.Info, "Get global config info"),
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "global.config.get",
          summary: "Get global configuration",
          description: "Retrieve the current global OpenCode configuration settings and preferences.",
        }),
      ),
      HttpApiEndpoint.patch("configUpdate", GlobalPaths.config, {
        payload: ConfigV1.Info,
        success: described(ConfigV1.Info, "Successfully updated global config"),
        error: HttpApiError.BadRequest,
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "global.config.update",
          summary: "Update global configuration",
          description: "Update global OpenCode configuration settings and preferences.",
        }),
      ),
      HttpApiEndpoint.post("dispose", GlobalPaths.dispose, {
        success: described(Schema.Boolean, "Global disposed"),
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "global.dispose",
          summary: "Dispose instance",
          description: "Clean up and dispose all OpenCode instances, releasing all resources.",
        }),
      ),
      HttpApiEndpoint.post("upgrade", GlobalPaths.upgrade, {
        payload: [HttpApiSchema.NoContent, GlobalUpgradeInput],
        success: described(GlobalUpgradeResult, "Upgrade result"),
        error: HttpApiError.BadRequest,
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "global.upgrade",
          summary: "Upgrade opencode",
          description: "Upgrade opencode to the specified version or latest if not specified.",
        }),
      ),
    )
    .annotateMerge(OpenApi.annotations({ title: "global", description: "Global server routes." })),
)
