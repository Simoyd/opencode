import { Context, Effect, Layer, Schema } from "effect"
import { MessageID, PartID, SessionID } from "./schema"
import { MessageV2 } from "./message-v2"
import { SessionV1 } from "@opencode-ai/core/v1/session"

export const TextPartInput = Schema.Struct({
  type: Schema.Literal("text"),
  text: Schema.String,
  label: Schema.optional(Schema.String),
}).annotate({ identifier: "StagedContextTextPartInput" })
export type TextPartInput = Schema.Schema.Type<typeof TextPartInput>

export const StageInput = Schema.Struct({
  sessionID: SessionID,
  id: Schema.optional(Schema.String),
  mode: Schema.optional(Schema.Literal("next_prompt")),
  visibility: Schema.optional(Schema.Literal("provider_only")),
  consume: Schema.optional(Schema.Literal("once")),
  parts: Schema.Array(TextPartInput),
}).annotate({ identifier: "StagedContextStageInput" })
export type StageInput = Schema.Schema.Type<typeof StageInput>

export const Info = Schema.Struct({
  id: Schema.String,
  sessionID: SessionID,
  mode: Schema.Literal("next_prompt"),
  visibility: Schema.Literal("provider_only"),
  consume: Schema.Literal("once"),
  parts: Schema.Array(TextPartInput),
  time: Schema.Struct({ created: Schema.Number }),
}).annotate({ identifier: "StagedContextInfo" })
export type Info = Schema.Schema.Type<typeof Info>

export class DuplicateError extends Schema.TaggedErrorClass<DuplicateError>()("SessionStagedContext.Duplicate", {
  sessionID: SessionID,
  contextID: Schema.String,
}) {}

export interface Interface {
  readonly stage: (input: StageInput) => Effect.Effect<Info, DuplicateError>
  readonly list: (input: { sessionID: SessionID }) => Effect.Effect<Info[]>
  readonly clear: (input: { sessionID: SessionID; contextID?: string }) => Effect.Effect<void>
  readonly injectAndConsume: (input: {
    sessionID: SessionID
    lastUser: SessionV1.User
    messages: SessionV1.WithParts[]
  }) => Effect.Effect<SessionV1.WithParts[]>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SessionStagedContext") {}

const contexts = new Map<SessionID, Info[]>()

export const stage = Effect.fn("SessionStagedContext.stage")(function* (input: StageInput) {
  const parts = input.parts.filter((part) => part.type === "text" && part.text.length > 0)
  if (parts.length === 0) throw new Error("Staged context requires at least one non-empty text part.")
  const id = input.id ?? `ctx_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`
  if ((contexts.get(input.sessionID) ?? []).some((context) => context.id === id)) {
    return yield* new DuplicateError({ sessionID: input.sessionID, contextID: id })
  }
  const info: Info = {
    id,
    sessionID: input.sessionID,
    mode: "next_prompt",
    visibility: "provider_only",
    consume: "once",
    parts,
    time: { created: Date.now() },
  }
  contexts.set(input.sessionID, [...(contexts.get(input.sessionID) ?? []), info])
  return info
})

export const list = Effect.fn("SessionStagedContext.list")(function* (input: { sessionID: SessionID }) {
  return [...(contexts.get(input.sessionID) ?? [])]
})

export const clear = Effect.fn("SessionStagedContext.clear")(function* (input: {
  sessionID: SessionID
  contextID?: string
}) {
  if (!input.contextID) {
    contexts.delete(input.sessionID)
    return
  }
  const remaining = (contexts.get(input.sessionID) ?? []).filter((context) => context.id !== input.contextID)
  if (remaining.length === 0) contexts.delete(input.sessionID)
  else contexts.set(input.sessionID, remaining)
})

export const injectAndConsume = Effect.fn("SessionStagedContext.injectAndConsume")(function* (input: {
  sessionID: SessionID
  lastUser: SessionV1.User
  messages: SessionV1.WithParts[]
}) {
  const staged = contexts.get(input.sessionID) ?? []
  if (staged.length === 0) return input.messages

  contexts.delete(input.sessionID)
  const messageID = MessageID.ascending()
  const text = staged
    .map((context) =>
      context.parts
        .map((part) => [part.label ? `# ${part.label}` : "# Host-provided context", part.text].join("\n"))
        .join("\n\n"),
    )
    .join("\n\n")
  const injected: SessionV1.WithParts = {
    info: {
      id: messageID,
      role: "user",
      sessionID: input.sessionID,
      time: { created: Date.now() },
      agent: input.lastUser.agent,
      model: input.lastUser.model,
    },
    parts: [
      {
        id: PartID.ascending(),
        messageID,
        sessionID: input.sessionID,
        type: "text",
        synthetic: true,
        text: [
          "<system-reminder>",
          "Host-provided provider-only context for this turn follows. It was staged before the visible user prompt and is not itself a new user instruction.",
          text,
          "</system-reminder>",
        ].join("\n"),
        metadata: {
          staged_context: true,
          staged_context_ids: staged.map((context) => context.id).join(","),
        },
      },
    ],
  }
  const lastUserIndex = input.messages.findLastIndex((message) => message.info.id === input.lastUser.id)
  if (lastUserIndex < 0) return [...input.messages, injected]
  return [...input.messages.slice(0, lastUserIndex), injected, ...input.messages.slice(lastUserIndex)]
})

export const layer = Layer.succeed(Service, Service.of({ stage, list, clear, injectAndConsume }))

export const defaultLayer = layer

export * as SessionStagedContext from "./staged-context"
