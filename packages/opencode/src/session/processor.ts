import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { PermissionV1 } from "@opencode-ai/core/v1/permission"
import { Image } from "@/image/image"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Cause, Deferred, Effect, Exit, Layer, Context, Schema, Semaphore } from "effect"
import * as Stream from "effect/Stream"
import { Agent } from "@/agent/agent"
import { Config } from "@/config/config"
import { Permission } from "@/permission"
import { Plugin } from "@/plugin"
import { Snapshot } from "@/snapshot"
import { Session } from "./session"
import { LLM } from "./llm"
import { MessageV2 } from "./message-v2"
import { isOverflow } from "./overflow"
import { PartID } from "./schema"
import type { SessionID } from "./schema"
import { SessionRetry } from "./retry"
import { SessionStatus } from "./status"
import { SessionSummary } from "./summary"
import type { Provider } from "@/provider/provider"
import { Question } from "@/question"
import { errorMessage } from "@/util/error"
import { isRecord } from "@/util/record"
import { EventV2Bridge } from "@/event-v2-bridge"
import { Database } from "@opencode-ai/core/database/database"
import { Usage, type LLMEvent } from "@opencode-ai/llm"

const DOOM_LOOP_THRESHOLD = 3
export type Result = "compact" | "stop" | "continue"

export interface Handle {
  readonly message: SessionV1.Assistant
  readonly registerToolCall: (input: { toolCallID: string; toolName: string }) => Effect.Effect<SessionV1.ToolPart>
  readonly updateToolCall: (
    toolCallID: string,
    update: (part: SessionV1.ToolPart) => SessionV1.ToolPart,
  ) => Effect.Effect<SessionV1.ToolPart | undefined>
  readonly admitToolCall: (
    toolCallID: string,
    admit: (part: SessionV1.ToolPart) => Effect.Effect<SessionV1.ToolPart>,
  ) => Effect.Effect<SessionV1.ToolPart | undefined>
  readonly completeToolCall: (
    toolCallID: string,
    output: {
      title: string
      metadata: Record<string, any>
      output: string
      attachments?: SessionV1.FilePart[]
    },
  ) => Effect.Effect<void>
  readonly process: (streamInput: LLM.StreamInput, beforeProviderStream?: Effect.Effect<void>) => Effect.Effect<Result>
}

type Input = {
  assistantMessage: SessionV1.Assistant
  sessionID: SessionID
  model: Provider.Model
}

export interface Interface {
  readonly create: (input: Input) => Effect.Effect<Handle>
}

type ToolCallSlot = {
  readonly gate: Semaphore.Semaphore
  readonly created: Deferred.Deferred<SessionV1.ToolPart>
  readonly done: Deferred.Deferred<void>
  lifecycle: "creating" | "active" | "settled" | "failed"
  part:
    | {
        partID: SessionV1.ToolPart["id"]
        messageID: SessionV1.ToolPart["messageID"]
        sessionID: SessionV1.ToolPart["sessionID"]
      }
    | undefined
  toolName: string
  provisionalName: boolean
  providerExecuted: boolean
}

type ToolCallMatch = {
  slot: ToolCallSlot
  part: SessionV1.ToolPart
}

interface ProcessorContext extends Input {
  toolcalls: Record<string, ToolCallSlot>
  toolcallsClosed: boolean
  shouldBreak: boolean
  snapshot: string | undefined
  blocked: boolean
  needsCompaction: boolean
  currentText: SessionV1.TextPart | undefined
  reasoningMap: Record<string, SessionV1.ReasoningPart>
}

type StreamEvent = LLMEvent

export class Service extends Context.Service<Service, Interface>()("@opencode/SessionProcessor") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const session = yield* Session.Service
    const config = yield* Config.Service
    const snapshot = yield* Snapshot.Service
    const agents = yield* Agent.Service
    const llm = yield* LLM.Service
    const permission = yield* Permission.Service
    const plugin = yield* Plugin.Service
    const summary = yield* SessionSummary.Service
    const status = yield* SessionStatus.Service
    const image = yield* Image.Service
    const events = yield* EventV2Bridge.Service
    const database = yield* Database.Service

    const create = Effect.fn("SessionProcessor.create")(function* (input: Input) {
      // Pre-capture snapshot before the LLM stream starts. The AI SDK
      // may execute tools internally before emitting start-step events,
      // so capturing inside the event handler can be too late.
      const initialSnapshot = yield* snapshot.track()
      const ctx: ProcessorContext = {
        assistantMessage: input.assistantMessage,
        sessionID: input.sessionID,
        model: input.model,
        toolcalls: {},
        toolcallsClosed: false,
        shouldBreak: false,
        snapshot: initialSnapshot,
        blocked: false,
        needsCompaction: false,
        currentText: undefined,
        reasoningMap: {},
      }
      let aborted = false

      const parse = (e: unknown) =>
        MessageV2.fromError(e, {
          providerID: input.model.providerID,
          aborted,
        })

      const settleToolCall = Effect.fnUntraced(function* (slot: ToolCallSlot) {
        if (slot.lifecycle === "settled" || slot.lifecycle === "failed") return
        slot.lifecycle = "settled"
        yield* Deferred.succeed(slot.done, undefined).pipe(Effect.asVoid)
      })

      const readToolCall = Effect.fnUntraced(function* (toolCallID: string, slot: ToolCallSlot) {
        if (ctx.toolcalls[toolCallID] !== slot || !slot.part) return undefined
        const part = yield* session.getPart({
          partID: slot.part.partID,
          messageID: slot.part.messageID,
          sessionID: slot.part.sessionID,
        })
        if (!part || part.type !== "tool") return undefined
        return { slot, part }
      })

      const ownToolCallPart = (slot: ToolCallSlot, part: SessionV1.ToolPart) => {
        slot.part = {
          partID: part.id,
          messageID: part.messageID,
          sessionID: part.sessionID,
        }
        return part
      }

      const persistToolCall = Effect.fnUntraced(function* (slot: ToolCallSlot, part: SessionV1.ToolPart) {
        return ownToolCallPart(slot, yield* session.updatePart(part))
      })

      const useToolCall = <A>(
        toolCallID: string,
        slot: ToolCallSlot,
        includeSettled: boolean,
        use: (match: ToolCallMatch) => Effect.Effect<A>,
      ): Effect.Effect<A | undefined> =>
        Deferred.await(slot.created).pipe(
          Effect.flatMap(() =>
            slot.gate.withPermits(1)(
              Effect.uninterruptible(
                Effect.gen(function* () {
                  if (ctx.toolcalls[toolCallID] !== slot || slot.lifecycle === "failed") return undefined
                  if (!includeSettled && slot.lifecycle !== "active") return undefined
                  const match = yield* readToolCall(toolCallID, slot)
                  if (match) return yield* use(match)
                  yield* settleToolCall(slot)
                  return undefined
                }),
              ),
            ),
          ),
        )

      const updateToolCall = Effect.fn("SessionProcessor.updateToolCall")(function* (
        toolCallID: string,
        update: (part: SessionV1.ToolPart) => SessionV1.ToolPart,
      ) {
        const slot = ctx.toolcalls[toolCallID]
        if (!slot) return undefined
        return yield* useToolCall(toolCallID, slot, false, (match) => persistToolCall(slot, update(match.part)))
      })

      const admitToolCall = Effect.fn("SessionProcessor.admitToolCall")(function* (
        toolCallID: string,
        admit: (part: SessionV1.ToolPart) => Effect.Effect<SessionV1.ToolPart>,
      ) {
        const slot = ctx.toolcalls[toolCallID]
        if (!slot) return undefined
        return yield* useToolCall(toolCallID, slot, false, (match) =>
          admit(match.part).pipe(Effect.map((part) => ownToolCallPart(slot, part))),
        )
      })

      const completeToolCall = Effect.fn("SessionProcessor.completeToolCall")(function* (
        toolCallID: string,
        output: {
          title: string
          metadata: Record<string, any>
          output: string
          attachments?: SessionV1.FilePart[]
        },
      ) {
        const slot = ctx.toolcalls[toolCallID]
        if (!slot) return
        yield* useToolCall(toolCallID, slot, false, (match) =>
          Effect.gen(function* () {
            if (match.part.state.status !== "running") return
            yield* persistToolCall(slot, {
              ...match.part,
              state: {
                status: "completed",
                input: match.part.state.input,
                output: output.output,
                metadata: {
                  ...(isRecord(match.part.state.metadata) ? match.part.state.metadata : {}),
                  ...output.metadata,
                },
                title: output.title,
                time: { start: match.part.state.time.start, end: Date.now() },
                attachments: output.attachments,
              },
            })
            yield* settleToolCall(slot)
          }),
        )
      })

      const failToolCall = Effect.fn("SessionProcessor.failToolCall")(function* (toolCallID: string, error: unknown) {
        const slot = ctx.toolcalls[toolCallID]
        if (!slot) return false
        return (
          (yield* useToolCall(toolCallID, slot, false, (match) =>
            Effect.gen(function* () {
              if (match.part.state.status !== "running") return false
              yield* persistToolCall(slot, {
                ...match.part,
                state: {
                  status: "error",
                  input: match.part.state.input,
                  error: errorMessage(error),
                  // Keep metadata streamed while running so failures retain progress detail (e.g. execute's child calls).
                  metadata: match.part.state.metadata,
                  time: { start: match.part.state.time.start, end: Date.now() },
                },
              })
              if (error instanceof PermissionV1.RejectedError || error instanceof Question.RejectedError) {
                ctx.blocked = ctx.shouldBreak
              }
              yield* settleToolCall(slot)
              return true
            }),
          )) ?? false
        )
      })

      const finishReasoning = Effect.fn("SessionProcessor.finishReasoning")(function* (reasoningID: string) {
        if (!(reasoningID in ctx.reasoningMap)) return
        // oxlint-disable-next-line no-self-assign -- reactivity trigger
        ctx.reasoningMap[reasoningID].text = ctx.reasoningMap[reasoningID].text
        ctx.reasoningMap[reasoningID].time = { ...ctx.reasoningMap[reasoningID].time, end: Date.now() }
        yield* session.updatePart(ctx.reasoningMap[reasoningID])
        delete ctx.reasoningMap[reasoningID]
      })

      const admitToolCallFacts = (
        toolCallID: string,
        slot: ToolCallSlot,
        input: { name: string; provisionalName?: boolean; providerExecuted?: boolean },
      ) => {
        if (!input.provisionalName) {
          if (slot.provisionalName) {
            slot.toolName = input.name
            slot.provisionalName = false
          } else if (slot.toolName !== input.name) {
            throw new Error(`Conflicting tool names for tool call ${toolCallID}`)
          }
        }
        if (input.providerExecuted) slot.providerExecuted = true
      }

      const reconcileToolCallFacts = Effect.fnUntraced(function* (match: ToolCallMatch) {
        match.slot.providerExecuted ||= match.part.metadata?.providerExecuted === true
        const toolName = match.slot.provisionalName ? match.part.tool : match.slot.toolName
        if (match.part.tool === toolName && (!match.slot.providerExecuted || match.part.metadata?.providerExecuted)) {
          return match
        }
        const part = yield* persistToolCall(match.slot, {
          ...match.part,
          tool: toolName,
          metadata: match.slot.providerExecuted
            ? { ...match.part.metadata, providerExecuted: true }
            : match.part.metadata,
        })
        return { slot: match.slot, part }
      })

      const ensureToolCall = Effect.fn("SessionProcessor.ensureToolCall")(function* (input: {
        id: string
        name: string
        provisionalName?: boolean
        providerExecuted?: boolean
      }) {
        return yield* Effect.uninterruptibleMask((restore) =>
          Effect.gen(function* () {
            if (ctx.toolcallsClosed) throw new Error(`Tool call registration is closed for ${input.id}`)
            const existing = ctx.toolcalls[input.id]
            if (existing) {
              const match = yield* restore(
                useToolCall(input.id, existing, true, (current) => {
                  admitToolCallFacts(input.id, existing, input)
                  return reconcileToolCallFacts(current)
                }),
              )
              if (!match) throw new Error(`Tool call ${input.id} lost its durable part`)
              return match
            }

            const slot: ToolCallSlot = {
              gate: Semaphore.makeUnsafe(1),
              created: Deferred.makeUnsafe<SessionV1.ToolPart>(),
              done: Deferred.makeUnsafe<void>(),
              lifecycle: "creating",
              part: undefined,
              toolName: input.name,
              provisionalName: input.provisionalName === true,
              providerExecuted: input.providerExecuted === true,
            }
            ctx.toolcalls[input.id] = slot

            return yield* slot.gate.withPermits(1)(
              Effect.uninterruptible(
                Effect.gen(function* () {
                  const created = yield* session
                    .updatePart({
                      id: PartID.ascending(),
                      messageID: ctx.assistantMessage.id,
                      sessionID: ctx.assistantMessage.sessionID,
                      type: "tool",
                      tool: slot.toolName,
                      callID: input.id,
                      state: { status: "pending", input: {}, raw: "" },
                      metadata: slot.providerExecuted ? { providerExecuted: true } : undefined,
                    } satisfies SessionV1.ToolPart)
                    .pipe(Effect.exit)
                  if (Exit.isFailure(created)) {
                    slot.lifecycle = "failed"
                    if (ctx.toolcalls[input.id] === slot) delete ctx.toolcalls[input.id]
                    yield* Deferred.done(slot.created, created).pipe(Effect.asVoid)
                    yield* Deferred.succeed(slot.done, undefined).pipe(Effect.asVoid)
                    return yield* Effect.failCause(created.cause)
                  }

                  slot.part = {
                    partID: created.value.id,
                    messageID: created.value.messageID,
                    sessionID: created.value.sessionID,
                  }
                  slot.lifecycle = "active"
                  yield* Deferred.succeed(slot.created, created.value).pipe(Effect.asVoid)
                  return yield* reconcileToolCallFacts({ slot, part: created.value })
                }),
              ),
            )
          }),
        )
      })

      const isFilePart = (value: unknown): value is SessionV1.FilePart => Schema.is(SessionV1.FilePart)(value)

      const registerToolCall = Effect.fn("SessionProcessor.registerToolCall")(function* (input: {
        toolCallID: string
        toolName: string
      }) {
        const toolCall = yield* ensureToolCall({ id: input.toolCallID, name: input.toolName })
        return toolCall.part
      })

      const toolResultOutput = (
        value: Extract<StreamEvent, { type: "tool-result" }>,
      ): { title: string; metadata: Record<string, any>; output: string; attachments?: SessionV1.FilePart[] } => {
        if (isRecord(value.result.value) && typeof value.result.value.output === "string") {
          return {
            title: typeof value.result.value.title === "string" ? value.result.value.title : value.name,
            metadata: isRecord(value.result.value.metadata) ? value.result.value.metadata : {},
            output: value.result.value.output,
            attachments: Array.isArray(value.result.value.attachments)
              ? value.result.value.attachments.filter(isFilePart)
              : undefined,
          }
        }
        return {
          title: value.name,
          metadata: value.result.type === "json" && isRecord(value.result.value) ? value.result.value : {},
          output:
            typeof value.result.value === "string" ? value.result.value : (JSON.stringify(value.result.value) ?? ""),
        }
      }

      const handleEvent = Effect.fnUntraced(function* (value: StreamEvent) {
        switch (value.type) {
          case "reasoning-start":
            if (value.id in ctx.reasoningMap) return
            ctx.reasoningMap[value.id] = {
              id: PartID.ascending(),
              messageID: ctx.assistantMessage.id,
              sessionID: ctx.assistantMessage.sessionID,
              type: "reasoning",
              text: "",
              time: { start: Date.now() },
              metadata: value.providerMetadata,
            }
            yield* session.updatePart(ctx.reasoningMap[value.id])
            return

          case "reasoning-delta":
            // Match dev: silently drop orphan deltas (no preceding reasoning-start).
            if (!(value.id in ctx.reasoningMap)) return
            ctx.reasoningMap[value.id].text += value.text
            if (value.providerMetadata) ctx.reasoningMap[value.id].metadata = value.providerMetadata
            yield* session.updatePartDelta({
              sessionID: ctx.reasoningMap[value.id].sessionID,
              messageID: ctx.reasoningMap[value.id].messageID,
              partID: ctx.reasoningMap[value.id].id,
              field: "text",
              delta: value.text,
            })
            return

          case "reasoning-end":
            if (value.providerMetadata && value.id in ctx.reasoningMap) {
              ctx.reasoningMap[value.id].metadata = value.providerMetadata
            }
            yield* finishReasoning(value.id)
            return

          case "tool-input-start":
            if (ctx.assistantMessage.summary) {
              throw new Error(`Tool call not allowed while generating summary: ${value.name}`)
            }
            yield* ensureToolCall({ ...value, provisionalName: true })
            return

          case "tool-input-delta":
            yield* ensureToolCall({ ...value, provisionalName: true })
            return

          case "tool-input-end": {
            yield* ensureToolCall({ ...value, provisionalName: true })
            return
          }

          case "tool-call": {
            if (ctx.assistantMessage.summary) {
              throw new Error(`Tool call not allowed while generating summary: ${value.name}`)
            }
            yield* ensureToolCall(value)
            const input = isRecord(value.input) ? value.input : { value: value.input }
            yield* updateToolCall(value.id, (match) => ({
              ...match,
              tool: value.name,
              state:
                match.state.status === "running"
                  ? { ...match.state, input }
                  : {
                      status: "running",
                      input,
                      time: { start: Date.now() },
                    },
              metadata:
                match.metadata || value.providerMetadata
                  ? {
                      ...match.metadata,
                      ...value.providerMetadata,
                      ...(match.metadata?.providerExecuted ? { providerExecuted: true } : {}),
                    }
                  : undefined,
            }))

            const parts = yield* MessageV2.parts({
              sessionID: ctx.assistantMessage.sessionID,
              messageID: ctx.assistantMessage.id,
            }).pipe(Effect.provideService(Database.Service, database))
            const recentParts = parts.slice(-DOOM_LOOP_THRESHOLD)

            if (
              recentParts.length !== DOOM_LOOP_THRESHOLD ||
              !recentParts.every(
                (part) =>
                  part.type === "tool" &&
                  part.tool === value.name &&
                  part.state.status !== "pending" &&
                  JSON.stringify(part.state.input) === JSON.stringify(input),
              )
            ) {
              return
            }

            const agent = yield* agents.get(ctx.assistantMessage.agent)
            yield* permission.ask({
              permission: "doom_loop",
              patterns: [value.name],
              sessionID: ctx.assistantMessage.sessionID,
              metadata: { tool: value.name, input },
              always: [value.name],
              ruleset: agent.permission,
            })
            return
          }

          case "tool-result": {
            if (value.result.type === "error") {
              yield* failToolCall(value.id, value.result.value)
              return
            }
            const rawOutput = toolResultOutput(value)
            const normalized = yield* Effect.forEach(rawOutput.attachments ?? [], (attachment) =>
              attachment.mime.startsWith("image/")
                ? image.normalize(attachment).pipe(
                    Effect.catchIf(
                      (error) => error instanceof Image.ResizerUnavailableError,
                      () => Effect.succeed(attachment),
                    ),
                    Effect.exit,
                  )
                : Effect.succeed(Exit.succeed<SessionV1.FilePart>(attachment)),
            )
            const omitted = normalized.filter(Exit.isFailure).length
            const attachments = normalized.filter(Exit.isSuccess).map((item) => item.value)
            const output = {
              ...rawOutput,
              output:
                omitted === 0
                  ? rawOutput.output
                  : `${rawOutput.output}\n\n[${omitted} image${omitted === 1 ? "" : "s"} omitted: could not be resized below the image size limit.]`,
              attachments: attachments.length ? attachments : undefined,
            }
            yield* completeToolCall(value.id, output)
            return
          }

          case "tool-error": {
            yield* failToolCall(value.id, value.error ?? new Error(value.message))
            return
          }

          case "provider-error":
            throw new Error(value.message)

          case "step-start":
            if (!ctx.snapshot) ctx.snapshot = yield* snapshot.track()
            yield* session.updatePart({
              id: PartID.ascending(),
              messageID: ctx.assistantMessage.id,
              sessionID: ctx.sessionID,
              snapshot: ctx.snapshot,
              type: "step-start",
            })
            return

          case "step-finish": {
            const completedSnapshot = yield* snapshot.track()
            yield* Effect.forEach(Object.keys(ctx.reasoningMap), finishReasoning)
            const usage = Session.getUsage({
              model: ctx.model,
              usage: value.usage ?? new Usage({}),
              metadata: value.providerMetadata,
            })
            ctx.assistantMessage.finish = value.reason
            ctx.assistantMessage.cost += usage.cost
            ctx.assistantMessage.tokens = usage.tokens
            yield* session.updatePart({
              id: PartID.ascending(),
              reason: value.reason,
              snapshot: completedSnapshot,
              messageID: ctx.assistantMessage.id,
              sessionID: ctx.assistantMessage.sessionID,
              type: "step-finish",
              tokens: usage.tokens,
              cost: usage.cost,
            })
            yield* session.updateMessage(ctx.assistantMessage)
            if (ctx.snapshot) {
              const patch = yield* snapshot.patch(ctx.snapshot)
              if (patch.files.length) {
                yield* session.updatePart({
                  id: PartID.ascending(),
                  messageID: ctx.assistantMessage.id,
                  sessionID: ctx.sessionID,
                  type: "patch",
                  hash: patch.hash,
                  files: patch.files,
                })
              }
              ctx.snapshot = undefined
            }
            yield* summary
              .summarize({
                sessionID: ctx.sessionID,
                messageID: ctx.assistantMessage.parentID,
              })
              .pipe(Effect.ignore)
            if (
              !ctx.assistantMessage.summary &&
              isOverflow({ cfg: yield* config.get(), tokens: usage.tokens, model: ctx.model })
            ) {
              ctx.needsCompaction = true
            }
            return
          }

          case "text-start":
            ctx.currentText = {
              id: PartID.ascending(),
              messageID: ctx.assistantMessage.id,
              sessionID: ctx.assistantMessage.sessionID,
              type: "text",
              text: "",
              time: { start: Date.now() },
              metadata: value.providerMetadata,
            }
            yield* session.updatePart(ctx.currentText)
            return

          case "text-delta":
            if (!ctx.currentText) return
            ctx.currentText.text += value.text
            if (value.providerMetadata) ctx.currentText.metadata = value.providerMetadata
            yield* session.updatePartDelta({
              sessionID: ctx.currentText.sessionID,
              messageID: ctx.currentText.messageID,
              partID: ctx.currentText.id,
              field: "text",
              delta: value.text,
            })
            return

          case "text-end":
            if (!ctx.currentText) return
            // oxlint-disable-next-line no-self-assign -- reactivity trigger
            ctx.currentText.text = ctx.currentText.text
            ctx.currentText.text = (yield* plugin.trigger(
              "experimental.text.complete",
              {
                sessionID: ctx.sessionID,
                messageID: ctx.assistantMessage.id,
                partID: ctx.currentText.id,
              },
              { text: ctx.currentText.text },
            )).text
            {
              const end = Date.now()
              ctx.currentText.time = { start: ctx.currentText.time?.start ?? end, end }
            }
            if (value.providerMetadata) ctx.currentText.metadata = value.providerMetadata
            yield* session.updatePart(ctx.currentText)
            ctx.currentText = undefined
            return

          case "finish":
            return
        }
      })

      const abortToolCall = (toolCallID: string, slot: ToolCallSlot) =>
        slot.gate.withPermits(1)(
          Effect.uninterruptible(
            Effect.gen(function* () {
              if (ctx.toolcalls[toolCallID] !== slot || slot.lifecycle !== "active") return
              const match = yield* readToolCall(toolCallID, slot)
              if (!match) return
              const end = Date.now()
              const metadata =
                "metadata" in match.part.state && isRecord(match.part.state.metadata) ? match.part.state.metadata : {}
              yield* persistToolCall(slot, {
                ...match.part,
                state: {
                  ...match.part.state,
                  status: "error",
                  error: "Tool execution aborted",
                  metadata: { ...metadata, interrupted: true },
                  time: { start: "time" in match.part.state ? match.part.state.time.start : end, end },
                },
              })
            }).pipe(Effect.ensuring(settleToolCall(slot))),
          ),
        )

      const cleanupToolCalls = Effect.fnUntraced(function* () {
        const slots = Object.values(ctx.toolcalls)
        yield* Effect.forEach(slots, (slot) => Deferred.await(slot.created).pipe(Effect.exit), {
          concurrency: "unbounded",
          discard: true,
        })

        yield* Effect.forEach(
          slots,
          (slot) => Deferred.await(slot.done).pipe(Effect.timeout("250 millis"), Effect.ignore),
          { concurrency: "unbounded" },
        )

        const entries = Object.entries(ctx.toolcalls)
        const exits = yield* Effect.forEach(
          entries,
          ([toolCallID, slot]) => abortToolCall(toolCallID, slot).pipe(Effect.exit),
          { concurrency: "unbounded" },
        )
        for (const [toolCallID, slot] of entries) {
          if (ctx.toolcalls[toolCallID] === slot) delete ctx.toolcalls[toolCallID]
        }
        const failure = exits.find(Exit.isFailure)
        if (failure) return yield* Effect.failCause(failure.cause)
        return yield* Effect.void
      })

      const cleanup = Effect.fn("SessionProcessor.cleanup")(function* () {
        ctx.toolcallsClosed = true

        const parts = yield* Effect.gen(function* () {
          if (ctx.snapshot) {
            const patch = yield* snapshot.patch(ctx.snapshot)
            if (patch.files.length) {
              yield* session.updatePart({
                id: PartID.ascending(),
                messageID: ctx.assistantMessage.id,
                sessionID: ctx.sessionID,
                type: "patch",
                hash: patch.hash,
                files: patch.files,
              })
            }
            ctx.snapshot = undefined
          }

          if (ctx.currentText) {
            const end = Date.now()
            ctx.currentText.time = { start: ctx.currentText.time?.start ?? end, end }
            yield* session.updatePart(ctx.currentText)
            ctx.currentText = undefined
          }

          for (const part of Object.values(ctx.reasoningMap)) {
            const end = Date.now()
            yield* session.updatePart({
              ...part,
              time: { start: part.time.start ?? end, end },
            })
          }
          ctx.reasoningMap = {}
        }).pipe(Effect.exit)
        const tools = yield* cleanupToolCalls().pipe(Effect.exit)
        if (Exit.isFailure(parts)) return yield* Effect.failCause(parts.cause)
        if (Exit.isFailure(tools)) return yield* Effect.failCause(tools.cause)
        if (ctx.blocked) ctx.assistantMessage.finish = "stop"
        ctx.assistantMessage.time.completed = Date.now()
        yield* session.updateMessage(ctx.assistantMessage)
        return yield* Effect.void
      })

      const halt = Effect.fn("SessionProcessor.halt")(function* (e: unknown) {
        yield* Effect.logError("process", {
          "session.id": input.sessionID,
          messageID: input.assistantMessage.id,
          error: errorMessage(e),
          stack: e instanceof Error ? e.stack : undefined,
        })
        const error = parse(e)
        if (SessionV1.ContextOverflowError.isInstance(error)) {
          if ((yield* config.get()).compaction?.auto === false && !ctx.assistantMessage.summary) {
            ctx.assistantMessage.error = error
            ctx.assistantMessage.finish = "error"
            yield* events.publish(Session.Event.Error, { sessionID: ctx.sessionID, error })
            return
          }
          ctx.needsCompaction = true
          yield* events.publish(Session.Event.Error, { sessionID: ctx.sessionID, error })
          return
        }
        ctx.assistantMessage.error = error
        yield* events.publish(Session.Event.Error, {
          sessionID: ctx.assistantMessage.sessionID,
          error: ctx.assistantMessage.error,
        })
        yield* status.set(ctx.sessionID, { type: "idle" })
      })

      const process = Effect.fn("SessionProcessor.process")(function* (
        streamInput: LLM.StreamInput,
        beforeProviderStream: Effect.Effect<void> = Effect.void,
      ) {
        yield* Effect.logInfo("process", {
          "session.id": input.sessionID,
          messageID: input.assistantMessage.id,
        })
        ctx.needsCompaction = false
        ctx.shouldBreak = (yield* config.get()).experimental?.continue_loop_on_deny !== true
        const providerStarted = yield* Effect.cached(beforeProviderStream)

        return yield* Effect.gen(function* () {
          yield* Effect.gen(function* () {
            ctx.currentText = undefined
            ctx.reasoningMap = {}
            yield* status.set(ctx.sessionID, { type: "busy" })
            const stream = llm.stream({ ...streamInput, providerStarted })

            yield* stream.pipe(
              Stream.tap((event) => handleEvent(event)),
              Stream.takeUntil(() => ctx.needsCompaction),
              Stream.runDrain,
            )
          }).pipe(
            Effect.onInterrupt(() =>
              Effect.gen(function* () {
                aborted = true
                if (!ctx.assistantMessage.error) {
                  yield* halt(new DOMException("Aborted", "AbortError"))
                }
              }),
            ),
            Effect.catchCauseIf(
              (cause) => !Cause.hasInterruptsOnly(cause),
              (cause) => Effect.fail(Cause.squash(cause)),
            ),
            Effect.retry(
              SessionRetry.policy({
                provider: input.model.providerID,
                parse,
                set: (info) => {
                  return status.set(ctx.sessionID, {
                    type: "retry",
                    attempt: info.attempt,
                    message: info.message,
                    action: info.action,
                    next: info.next,
                  })
                },
              }),
            ),
            Effect.catch(halt),
            Effect.ensuring(cleanup()),
          )

          if (ctx.needsCompaction) return "compact"
          if (ctx.blocked || ctx.assistantMessage.error) return "stop"
          return "continue"
        })
      })

      return {
        get message() {
          return ctx.assistantMessage
        },
        registerToolCall,
        updateToolCall,
        admitToolCall,
        completeToolCall,
        process,
      } satisfies Handle
    })

    return Service.of({ create })
  }),
)

export const node = LayerNode.make({
  service: Service,
  layer: layer,
  deps: [
    Session.node,
    Config.node,
    Snapshot.node,
    Agent.node,
    LLM.node,
    Permission.node,
    Plugin.node,
    SessionSummary.node,
    SessionStatus.node,
    Image.node,
    EventV2Bridge.node,
    Database.node,
  ],
})

export * as SessionProcessor from "./processor"
