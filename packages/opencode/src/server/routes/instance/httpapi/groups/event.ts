import { Schema } from "effect"
import { HttpApi, HttpApiEndpoint, HttpApiError, HttpApiGroup, HttpApiSchema, OpenApi } from "effect/unstable/httpapi"
import { Authorization } from "../middleware/authorization"
import { InstanceContextMiddleware } from "../middleware/instance-context"
import { WorkspaceRoutingMiddleware, WorkspaceRoutingQueryFields } from "../middleware/workspace-routing"
import { SessionID } from "@/session/schema"
import { SelectedEventProjection } from "@/server/shared/selected-event-projection"
import { EventV2 } from "@opencode-ai/core/event"
import { EventManifest } from "@/event-manifest"
import { InstanceDisposed } from "@/server/event"

const SelectedEventSchemas = EventManifest.Latest.values()
  .filter((definition) => SelectedEventProjection.includesEventType(definition.type))
  .map((definition) =>
    Schema.Struct({
      id: EventV2.ID,
      type: Schema.Literal(definition.type),
      properties: definition.data,
    }).annotate({ identifier: `SelectedEvent.${definition.type}` }),
  )
  .toArray()

export const EventSchema = Schema.Union([
  ...SelectedEventSchemas,
  ...EventManifest.Latest.values()
    .filter((definition) => !SelectedEventProjection.includesEventType(definition.type))
    .map((definition) =>
      Schema.Struct({
        id: EventV2.ID,
        type: Schema.Literal(definition.type),
        properties: definition.data,
      }).annotate({ identifier: `Event.${definition.type}` }),
    )
    .toArray(),
  InstanceDisposed,
]).annotate({ identifier: "Event" })

export const EventQuery = Schema.Struct({
  ...WorkspaceRoutingQueryFields,
  sessionID: Schema.optional(SessionID),
  type: Schema.optional(Schema.String),
  [SelectedEventProjection.SelectorQuery]: Schema.optional(Schema.Literal(SelectedEventProjection.Selector)),
})

export const EventPaths = {
  event: "/event",
} as const

export const EventApi = HttpApi.make("event").add(
  HttpApiGroup.make("event")
    .add(
      HttpApiEndpoint.get("subscribe", EventPaths.event, {
        query: EventQuery,
        success: HttpApiSchema.StreamSse({ data: EventSchema }),
        error: HttpApiError.BadRequest,
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "event.subscribe",
          summary: "Subscribe to events",
          description: "Get events",
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
                    schema: { $ref: "#/components/schemas/Event" },
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
    )
    .middleware(InstanceContextMiddleware)
    .middleware(WorkspaceRoutingMiddleware)
    .middleware(Authorization)
    .annotateMerge(OpenApi.annotations({ title: "event", description: "Instance event stream route." })),
)
