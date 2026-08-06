import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Context, Effect, Layer, Schema } from "effect"
import { MessageID, PartID, SessionID } from "./schema"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { InstanceState } from "@/effect/instance-state"

export const ContextID = Schema.String.check(Schema.isPattern(/\S/))
export type ContextID = Schema.Schema.Type<typeof ContextID>

export const TextPartInput = Schema.Struct({
  type: Schema.Literal("text"),
  text: Schema.String.check(Schema.isPattern(/\S/)),
  label: Schema.optional(Schema.String),
}).annotate({ identifier: "StagedContextTextPartInput" })
export type TextPartInput = Schema.Schema.Type<typeof TextPartInput>

export const StageInput = Schema.Struct({
  sessionID: SessionID,
  id: Schema.optional(ContextID),
  mode: Schema.optional(Schema.Literal("next_prompt")),
  visibility: Schema.optional(Schema.Literal("provider_only")),
  consume: Schema.optional(Schema.Literal("once")),
  parts: Schema.Array(TextPartInput),
}).annotate({ identifier: "StagedContextStageInput" })
export type StageInput = Schema.Schema.Type<typeof StageInput>

export const Info = Schema.Struct({
  id: ContextID,
  sessionID: SessionID,
  mode: Schema.Literal("next_prompt"),
  visibility: Schema.Literal("provider_only"),
  consume: Schema.Literal("once"),
  parts: Schema.Array(TextPartInput),
  time: Schema.Struct({ created: Schema.Finite }),
}).annotate({ identifier: "StagedContextInfo" })
export type Info = Schema.Schema.Type<typeof Info>

export class DuplicateError extends Schema.TaggedErrorClass<DuplicateError>()("SessionStagedContext.Duplicate", {
  sessionID: SessionID,
  contextID: ContextID,
}) {}

export class InvalidError extends Schema.TaggedErrorClass<InvalidError>()("SessionStagedContext.Invalid", {
  sessionID: SessionID,
}) {}

interface PendingOwner {
  readonly _tag: "Pending"
}

export interface ClaimedOwner {
  readonly _tag: "Claimed"
  readonly messageID: MessageID
}

interface Entry {
  readonly info: Info
  owner: PendingOwner | ClaimedOwner
}

export interface ClaimReceipt {
  readonly sessionID: SessionID
  readonly owner: ClaimedOwner
}

export interface ConsumptionReceipt {
  readonly sessionID: SessionID
  readonly owners: ReadonlySet<ClaimedOwner>
}

export interface Prepared {
  readonly messages: SessionV1.WithParts[]
  readonly receipt?: ConsumptionReceipt
}

export interface Interface {
  readonly stage: (input: StageInput) => Effect.Effect<Info, DuplicateError | InvalidError>
  readonly list: (input: { sessionID: SessionID }) => Effect.Effect<Info[]>
  readonly clear: (input: { sessionID: SessionID; contextID?: ContextID }) => Effect.Effect<void, InvalidError>
  readonly claim: (input: { sessionID: SessionID; messageID: MessageID }) => Effect.Effect<ClaimReceipt | undefined>
  readonly rollbackAdmission: (receipt: ClaimReceipt) => Effect.Effect<void>
  readonly prepare: (input: {
    sessionID: SessionID
    messages: SessionV1.WithParts[]
  }) => Effect.Effect<Prepared>
  readonly consume: (receipt: ConsumptionReceipt) => Effect.Effect<void>
  readonly retire: (input: { sessionID: SessionID }) => Effect.Effect<void>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SessionStagedContext") {}

const pending: PendingOwner = { _tag: "Pending" }
const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const state = yield* InstanceState.make(() =>
      Effect.gen(function* () {
        const contexts = new Map<SessionID, Entry[]>()
        yield* Effect.addFinalizer(() => Effect.sync(() => contexts.clear()))
        return contexts
      }),
    )

    const stage = Effect.fn("SessionStagedContext.stage")(function* (input: StageInput) {
      if (input.id !== undefined && input.id.trim().length === 0) {
        return yield* new InvalidError({ sessionID: input.sessionID })
      }
      if (input.parts.length === 0 || input.parts.some((part) => part.text.trim().length === 0)) {
        return yield* new InvalidError({ sessionID: input.sessionID })
      }
      const contexts = yield* InstanceState.get(state)
      const id = input.id ?? `ctx_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`
      const current = contexts.get(input.sessionID) ?? []
      if (current.some((entry) => entry.info.id === id)) {
        return yield* new DuplicateError({ sessionID: input.sessionID, contextID: id })
      }
      const info: Info = {
        id,
        sessionID: input.sessionID,
        mode: "next_prompt",
        visibility: "provider_only",
        consume: "once",
        parts: input.parts,
        time: { created: Date.now() },
      }
      contexts.set(input.sessionID, [...current, { info, owner: pending }])
      return info
    })

    const list = Effect.fn("SessionStagedContext.list")(function* (input: { sessionID: SessionID }) {
      return ((yield* InstanceState.get(state)).get(input.sessionID) ?? []).map((entry) => entry.info)
    })

    const clear = Effect.fn("SessionStagedContext.clear")(function* (input: {
      sessionID: SessionID
      contextID?: ContextID
    }) {
      if (input.contextID !== undefined && input.contextID.trim().length === 0) {
        return yield* new InvalidError({ sessionID: input.sessionID })
      }
      const contexts = yield* InstanceState.get(state)
      const current = contexts.get(input.sessionID)
      if (!current) return
      const retained = current.filter(
        (entry) => entry.owner._tag === "Claimed" || (input.contextID !== undefined && entry.info.id !== input.contextID),
      )
      if (retained.length === 0) contexts.delete(input.sessionID)
      else contexts.set(input.sessionID, retained)
    })

    const claim = Effect.fn("SessionStagedContext.claim")(function* (input: {
      sessionID: SessionID
      messageID: MessageID
    }) {
      const current = (yield* InstanceState.get(state)).get(input.sessionID)
      if (!current?.some((entry) => entry.owner._tag === "Pending")) return undefined
      const owner: ClaimedOwner = { _tag: "Claimed", messageID: input.messageID }
      for (const entry of current) {
        if (entry.owner._tag === "Pending") entry.owner = owner
      }
      return { sessionID: input.sessionID, owner } satisfies ClaimReceipt
    })

    const rollbackAdmission = Effect.fn("SessionStagedContext.rollbackAdmission")(function* (receipt: ClaimReceipt) {
      const current = (yield* InstanceState.get(state)).get(receipt.sessionID)
      if (!current) return
      for (const entry of current) {
        if (entry.owner === receipt.owner) entry.owner = pending
      }
    })

    const prepare = Effect.fn("SessionStagedContext.prepare")(function* (input: {
      sessionID: SessionID
      messages: SessionV1.WithParts[]
    }) {
      const claimed = ((yield* InstanceState.get(state)).get(input.sessionID) ?? []).filter(
        (entry): entry is Entry & { owner: ClaimedOwner } => entry.owner._tag === "Claimed",
      )
      if (claimed.length === 0) return { messages: input.messages } satisfies Prepared

      const byMessage = new Map<MessageID, Array<Entry & { owner: ClaimedOwner }>>()
      for (const entry of claimed) {
        const entries = byMessage.get(entry.owner.messageID)
        if (entries) entries.push(entry)
        else byMessage.set(entry.owner.messageID, [entry])
      }

      const owners = new Map<MessageID, SessionV1.User>()
      for (const messageID of byMessage.keys()) {
        const matches = input.messages.filter((message) => message.info.id === messageID)
        const match = matches[0]
        if (
          matches.length !== 1 ||
          !match ||
          match.info.role !== "user" ||
          match.info.sessionID !== input.sessionID
        ) {
          return yield* Effect.die(`Staged context owner ${messageID} is missing, duplicated, or malformed`)
        }
        owners.set(messageID, match.info)
      }

      const messages = input.messages.flatMap((message) => {
        const staged = byMessage.get(message.info.id)
        if (!staged) return [message]
        const owner = owners.get(message.info.id)!
        const messageID = MessageID.ascending()
        const text = staged
          .map((entry) =>
            entry.info.parts
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
            agent: owner.agent,
            model: owner.model,
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
                staged_context_ids: staged.map((entry) => entry.info.id).join(","),
              },
            },
          ],
        }
        return [injected, message]
      })

      return {
        messages,
        receipt: {
          sessionID: input.sessionID,
          owners: new Set(claimed.map((entry) => entry.owner)),
        },
      } satisfies Prepared
    })

    const consume = Effect.fn("SessionStagedContext.consume")(function* (receipt: ConsumptionReceipt) {
      const contexts = yield* InstanceState.get(state)
      const current = contexts.get(receipt.sessionID)
      if (!current) return
      const retained = current.filter((entry) => entry.owner._tag === "Pending" || !receipt.owners.has(entry.owner))
      if (retained.length === 0) contexts.delete(receipt.sessionID)
      else contexts.set(receipt.sessionID, retained)
    })

    const retire = Effect.fn("SessionStagedContext.retire")(function* (input: { sessionID: SessionID }) {
      const contexts = yield* InstanceState.get(state)
      contexts.delete(input.sessionID)
    })

    return Service.of({ stage, list, clear, claim, rollbackAdmission, prepare, consume, retire })
  }),
)
export const node = LayerNode.make({ service: Service, layer, deps: [] })

export * as SessionStagedContext from "./staged-context"
