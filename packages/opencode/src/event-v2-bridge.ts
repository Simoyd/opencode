// Opencode publish boundary for core events. Attach routed instance location
// so direct EventV2 consumers can isolate directory/workspace streams.
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { InstanceRef, WorkspaceRef } from "@/effect/instance-ref"
import { GlobalBus } from "@/bus/global"
import { EventV2 } from "@opencode-ai/core/event"
import { Location } from "@opencode-ai/core/location"
import { Project } from "@opencode-ai/core/project"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { Context, Effect, Layer } from "effect"
import { CompactionRegionProjection } from "@opencode-ai/core/session/compaction-region"
import { CompactionCatalog } from "@/session/compaction-catalog"
import { SessionID } from "@/session/schema"
import { SessionV1 } from "@opencode-ai/core/v1/session"

export type TaskAdmissionSession =
  | {
      readonly kind: "created"
      readonly info: EventV2.Data<typeof SessionV1.Event.Created>["info"]
    }
  | {
      readonly kind: "updated"
      readonly info: EventV2.Data<typeof SessionV1.Event.Updated>["info"]
    }

export interface Interface extends EventV2.Interface {
  readonly publishTaskAdmission: (input: {
    readonly session: TaskAdmissionSession
    readonly parentPart: EventV2.Data<typeof SessionV1.Event.PartUpdated>["part"]
    readonly childPrompt: SessionV1.WithParts
    readonly time: number
  }) => Effect.Effect<EventV2.Payload[]>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/EventV2Bridge") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const events = yield* EventV2.Service
    const publish: EventV2.Interface["publish"] = (definition, data, options) =>
      Effect.gen(function* () {
        if (options?.location) return yield* events.publish(definition, data, options)
        const ctx = yield* InstanceRef
        if (!ctx) return yield* events.publish(definition, data, options)
        const workspaceID = yield* WorkspaceRef
        return yield* events.publish(definition, data, {
          ...options,
          location: new Location.Info({
            directory: AbsolutePath.make(ctx.directory),
            ...(workspaceID ? { workspaceID } : {}),
            project: { id: Project.ID.make(ctx.project.id), directory: AbsolutePath.make(ctx.worktree) },
          }),
        })
      })

    const publishTransaction: EventV2.Interface["publishTransaction"] = (use) =>
      Effect.gen(function* () {
        const ctx = yield* InstanceRef
        if (!ctx) return yield* events.publishTransaction(use)
        const workspaceID = yield* WorkspaceRef
        const location = new Location.Info({
          directory: AbsolutePath.make(ctx.directory),
          ...(workspaceID ? { workspaceID } : {}),
          project: { id: Project.ID.make(ctx.project.id), directory: AbsolutePath.make(ctx.worktree) },
        })
        return yield* events.publishTransaction((publisher) =>
          use({
            publish: (definition, data, options) =>
              publisher.publish(
                definition,
                data,
                options?.location ? options : { ...options, location },
              ),
          }),
        )
      })

    const publishTaskAdmission: Interface["publishTaskAdmission"] = (input) =>
      publishTransaction((publisher) =>
        Effect.gen(function* () {
          const sessionEvent = input.session.kind === "created"
            ? yield* publisher.publish(SessionV1.Event.Created, {
                sessionID: input.session.info.id,
                info: input.session.info,
              })
            : yield* publisher.publish(SessionV1.Event.Updated, {
                sessionID: input.session.info.id,
                info: input.session.info,
              })
          const parentPartEvent = yield* publisher.publish(SessionV1.Event.PartUpdated, {
            sessionID: input.parentPart.sessionID,
            part: input.parentPart,
            time: input.time,
          })
          const childMessageEvent = yield* publisher.publish(SessionV1.Event.MessageUpdated, {
            sessionID: input.childPrompt.info.sessionID,
            info: input.childPrompt.info,
          })
          const childPartEvents = yield* Effect.forEach(
            input.childPrompt.parts,
            (part) =>
              publisher.publish(SessionV1.Event.PartUpdated, {
                sessionID: part.sessionID,
                part,
                time: input.time,
              }),
            { concurrency: 1 },
          )
          return [sessionEvent, parentPartEvent, childMessageEvent, ...childPartEvents]
        }),
      )

    const unsubscribe = yield* events.listen((event) =>
      Effect.gen(function* () {
        const ctx = yield* InstanceRef
        const workspaceID = (yield* WorkspaceRef) ?? event.location?.workspaceID
        GlobalBus.emit("event", {
          directory: event.location?.directory ?? ctx?.directory,
          project: ctx?.project.id,
          workspace: workspaceID,
          payload: { id: event.id, type: event.type, properties: event.data },
        })
        if (event.durable !== undefined) {
          GlobalBus.emit("event", {
            directory: event.location?.directory ?? ctx?.directory,
            project: ctx?.project.id,
            workspace: workspaceID,
            payload: {
              type: "sync",
              syncEvent: {
                id: event.id,
                type: EventV2.versionedType(event.type, event.durable.version),
                seq: event.durable.seq,
                aggregateID: event.durable.aggregateID,
                data: event.data,
              },
            },
          })
        }
      }),
    )
    const unsubscribeInvalidation = yield* events.afterNotify((event) =>
      Effect.gen(function* () {
        if (!CompactionRegionProjection.takeInvalidation(event.data)) return
        const sessionID = (event.data as Record<string, unknown>).sessionID
        if (typeof sessionID !== "string") return
        yield* events.publish(
          CompactionCatalog.Event.Changed,
          { sessionID: SessionID.make(sessionID) },
          { location: event.location },
        )
      }),
    )
    yield* Effect.addFinalizer(() => Effect.all([unsubscribe, unsubscribeInvalidation], { discard: true }))

    return Service.of({ ...events, publish, publishTransaction, publishTaskAdmission })
  }),
)

export const node = LayerNode.make({ service: Service, layer: layer, deps: [EventV2.node] })

export * as EventV2Bridge from "./event-v2-bridge"
