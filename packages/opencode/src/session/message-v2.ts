import { SessionID, MessageID } from "./schema"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { ProviderV2 } from "@opencode-ai/core/provider"
import {
  APIError,
  AbortedError,
  Assistant,
  AuthError,
  CompactionPart,
  ContextOverflowError,
  Info,
  OutputLengthError,
  Part,
  SubtaskPart,
  ToolPart,
  User,
  WithParts,
} from "@opencode-ai/core/v1/session"

import { NamedError } from "@opencode-ai/core/util/error"
import { APICallError, convertToModelMessages, LoadAPIKeyError, type ModelMessage, type UIMessage } from "ai"
import { Database } from "@opencode-ai/core/database/database"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { NotFoundError } from "@/storage/storage"
import { and } from "drizzle-orm"
import { desc } from "drizzle-orm"
import { eq } from "drizzle-orm"
import { lt } from "drizzle-orm"
import { or } from "drizzle-orm"
import { MessageTable, PartTable, SessionTable } from "@opencode-ai/core/session/sql"
import { ProviderError } from "@/provider/error"
import { iife } from "@/util/iife"
import { errorMessage } from "@/util/error"
import { isMedia } from "@/util/media"
import type { SystemError } from "bun"
import type { Provider } from "@/provider/provider"
import { Effect, Schema } from "effect"

/** Error shape thrown by Bun's fetch() when gzip/br decompression fails mid-stream */
interface FetchDecompressionError extends Error {
  code: "ZlibError"
  errno: number
  path: string
}

export const SYNTHETIC_ATTACHMENT_PROMPT = "Attached media from tool result:"
export { isMedia }

function truncateToolOutput(text: string, maxChars?: number) {
  if (!maxChars || text.length <= maxChars) return text
  const omitted = text.length - maxChars
  return `${text.slice(0, maxChars)}\n[Tool output truncated for compaction: omitted ${omitted} chars]`
}

export const Event = {
  Updated: SessionV1.Event.MessageUpdated,
  Removed: SessionV1.Event.MessageRemoved,
  PartUpdated: SessionV1.Event.PartUpdated,
  PartDelta: SessionV1.Event.PartDelta,
  PartRemoved: SessionV1.Event.PartRemoved,
}

const Cursor = Schema.Struct({
  id: MessageID,
  time: Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0)),
})
type Cursor = typeof Cursor.Type

const decodeCursor = Schema.decodeUnknownSync(Cursor)

export const cursor = {
  encode(input: Cursor) {
    return Buffer.from(JSON.stringify(input)).toString("base64url")
  },
  decode(input: string) {
    return decodeCursor(JSON.parse(Buffer.from(input, "base64url").toString("utf8")))
  },
}

const info = (row: typeof MessageTable.$inferSelect) =>
  ({
    ...row.data,
    id: row.id,
    sessionID: row.session_id,
  }) as Info

const part = (row: typeof PartTable.$inferSelect) =>
  ({
    ...row.data,
    id: row.id,
    sessionID: row.session_id,
    messageID: row.message_id,
  }) as Part

const older = (row: Cursor) =>
  or(lt(MessageTable.time_created, row.time), and(eq(MessageTable.time_created, row.time), lt(MessageTable.id, row.id)))

function hydrate(db: Database.Interface["db"], rows: (typeof MessageTable.$inferSelect)[]) {
  const partByMessage = new Map<string, Part[]>()
  return Effect.gen(function* () {
    if (rows.length > 0) {
      const partRows = yield* db
        .select()
        .from(PartTable)
        .where(
          or(...rows.map((row) => and(eq(PartTable.message_id, row.id), eq(PartTable.session_id, row.session_id)))),
        )
        .orderBy(PartTable.message_id, PartTable.id)
        .all()
        .pipe(Effect.orDie)
      for (const row of partRows) {
        const next = part(row)
        const list = partByMessage.get(row.message_id)
        if (list) list.push(next)
        else partByMessage.set(row.message_id, [next])
      }
    }

    return rows.map((row) => ({
      info: info(row),
      parts: partByMessage.get(row.id) ?? [],
    }))
  })
}

function providerMeta(metadata: Record<string, any> | undefined) {
  if (!metadata) return undefined
  const { providerExecuted: _, ...rest } = metadata
  return Object.keys(rest).length > 0 ? rest : undefined
}

export const toModelMessagesEffect = Effect.fnUntraced(function* (
  input: WithParts[],
  model: Provider.Model,
  options?: { stripMedia?: boolean; toolOutputMaxChars?: number },
) {
  const result: UIMessage[] = []
  const toolNames = new Set<string>()
  // Track media from tool results that need to be injected as user messages
  // for providers that don't support that media type in tool results.
  //
  // OpenAI-compatible APIs only support string content in tool results, so we need
  // to extract media and inject as user messages. Some SDKs only support a subset
  // of media in tool results; e.g. Bedrock supports images but not PDFs there.
  //
  // Only apply this workaround if the model actually supports that media input -
  // otherwise unsupportedParts() will turn it into a user-visible error.
  const supportsMediaInToolResult = (attachment: { mime: string }) => {
    if (model.api.npm === "@ai-sdk/anthropic") return true
    if (model.api.npm === "@ai-sdk/openai") return true
    if (model.api.npm === "@ai-sdk/amazon-bedrock/mantle") return true
    if (model.api.npm === "@ai-sdk/amazon-bedrock") return attachment.mime.startsWith("image/")
    if (model.api.npm === "@ai-sdk/xai") return attachment.mime.startsWith("image/")
    if (model.api.npm === "@ai-sdk/google-vertex/anthropic") return true
    if (model.api.npm === "@ai-sdk/google") {
      const id = model.api.id.toLowerCase()
      return id.includes("gemini-3") && !id.includes("gemini-2")
    }
    return false
  }

  const toModelOutput = (options: { toolCallId: string; input: unknown; output: unknown }) => {
    const output = options.output
    if (typeof output === "string") {
      return { type: "text", value: output }
    }

    if (typeof output === "object") {
      const outputObject = output as {
        text: string
        attachments?: Array<{ mime: string; url: string }>
      }
      const attachments = (outputObject.attachments ?? []).filter((attachment) => {
        return attachment.url.startsWith("data:") && attachment.url.includes(",")
      })

      return {
        type: "content",
        value: [
          ...(outputObject.text ? [{ type: "text", text: outputObject.text }] : []),
          ...attachments.map((attachment) => ({
            type: "media",
            mediaType: attachment.mime,
            data: iife(() => {
              const commaIndex = attachment.url.indexOf(",")
              return commaIndex === -1 ? attachment.url : attachment.url.slice(commaIndex + 1)
            }),
          })),
        ],
      }
    }

    return { type: "json", value: output as never }
  }

  for (const msg of input) {
    if (msg.parts.length === 0) continue

    if (msg.info.role === "user") {
      const userMessage: UIMessage = {
        id: msg.info.id,
        role: "user",
        parts: [],
      }
      for (const part of msg.parts) {
        // User message parts should never be empty
        if (part.type === "text" && !part.ignored && part.text !== "")
          userMessage.parts.push({
            type: "text",
            text: part.text,
          })
        // text/plain and directory files are converted into text parts, ignore them
        if (part.type === "file" && part.mime !== "text/plain" && part.mime !== "application/x-directory") {
          if (options?.stripMedia && isMedia(part.mime)) {
            userMessage.parts.push({
              type: "text",
              text: `[Attached ${part.mime}: ${part.filename ?? "file"}]`,
            })
          } else {
            userMessage.parts.push({
              type: "file",
              url: part.url,
              mediaType: part.mime,
              filename: part.filename,
            })
          }
        }

        if (part.type === "compaction") {
          userMessage.parts.push({
            type: "text",
            text: "What did we do so far?",
          })
        }
        if (part.type === "subtask") {
          userMessage.parts.push({
            type: "text",
            text: "The following tool was executed by the user",
          })
        }
      }
      if (userMessage.parts.length > 0) result.push(userMessage)
    }

    if (msg.info.role === "assistant") {
      const differentModel = `${model.providerID}/${model.id}` !== `${msg.info.providerID}/${msg.info.modelID}`
      const media: Array<{ mime: string; url: string; filename?: string }> = []

      if (
        msg.info.error &&
        !(
          AbortedError.isInstance(msg.info.error) &&
          msg.parts.some((part) => part.type !== "step-start" && part.type !== "reasoning")
        )
      ) {
        continue
      }
      const assistantMessage: UIMessage = {
        id: msg.info.id,
        role: "assistant",
        parts: [],
      }
      // Anthropic adaptive thinking can persist assistant turns like:
      // step-start, reasoning(signature), text(""), step-start,
      // reasoning(signature). The empty text part is a structural separator,
      // but it does not carry the signature metadata itself. Dropping it shifts
      // signed thinking positions after step-start splitting/provider regrouping;
      // keeping it as "" is filtered by the AI SDK and rejected by Anthropic.
      // It is unclear whether this shape originates in our stream processing,
      // a proxy, or a lower-level library, but preserving a non-empty separator
      // here is the only safe replay point we have.
      // Use a single space so the separator survives replay without changing
      // the neighboring signed reasoning blocks.
      const hasSignedReasoning = msg.parts.some((part) => {
        if (part.type !== "reasoning") return false
        return part.metadata?.anthropic?.signature != null
      })
      for (const part of msg.parts) {
        if (part.type === "text") {
          const text = part.text === "" && hasSignedReasoning ? " " : part.text
          assistantMessage.parts.push({
            type: "text",
            text,
            ...(differentModel ? {} : { providerMetadata: part.metadata }),
          })
        }
        if (part.type === "step-start")
          assistantMessage.parts.push({
            type: "step-start",
          })
        if (part.type === "tool") {
          toolNames.add(part.tool)
          if (part.state.status === "completed") {
            const outputText = part.state.time.compacted
              ? "[Old tool result content cleared]"
              : truncateToolOutput(part.state.output, options?.toolOutputMaxChars)
            const attachments = part.state.time.compacted || options?.stripMedia ? [] : (part.state.attachments ?? [])

            // For providers that don't support media in tool results, extract media files
            // (images, PDFs) to be sent as a separate user message
            const mediaAttachments = attachments.filter((a) => isMedia(a.mime))
            const extractedMedia = mediaAttachments.filter((a) => !supportsMediaInToolResult(a))
            if (extractedMedia.length > 0) {
              media.push(...extractedMedia)
            }
            const finalAttachments = attachments.filter((a) => !isMedia(a.mime) || supportsMediaInToolResult(a))

            const output =
              finalAttachments.length > 0
                ? {
                    text: outputText,
                    attachments: finalAttachments,
                  }
                : outputText

            assistantMessage.parts.push({
              type: ("tool-" + part.tool) as `tool-${string}`,
              state: "output-available",
              toolCallId: part.callID,
              input: part.state.input,
              output,
              ...(part.metadata?.providerExecuted ? { providerExecuted: true } : {}),
              ...(differentModel ? {} : { callProviderMetadata: providerMeta(part.metadata) }),
            })
          }
          if (part.state.status === "error") {
            const output = part.state.metadata?.interrupted === true ? part.state.metadata.output : undefined
            if (typeof output === "string") {
              assistantMessage.parts.push({
                type: ("tool-" + part.tool) as `tool-${string}`,
                state: "output-available",
                toolCallId: part.callID,
                input: part.state.input,
                output,
                ...(part.metadata?.providerExecuted ? { providerExecuted: true } : {}),
                ...(differentModel ? {} : { callProviderMetadata: providerMeta(part.metadata) }),
              })
            } else {
              assistantMessage.parts.push({
                type: ("tool-" + part.tool) as `tool-${string}`,
                state: "output-error",
                toolCallId: part.callID,
                input: part.state.input,
                errorText: part.state.error,
                ...(part.metadata?.providerExecuted ? { providerExecuted: true } : {}),
                ...(differentModel ? {} : { callProviderMetadata: providerMeta(part.metadata) }),
              })
            }
          }
          // Handle pending/running tool calls to prevent dangling tool_use blocks
          // Anthropic/Claude APIs require every tool_use to have a corresponding tool_result
          if (part.state.status === "pending" || part.state.status === "running")
            assistantMessage.parts.push({
              type: ("tool-" + part.tool) as `tool-${string}`,
              state: "output-error",
              toolCallId: part.callID,
              input: part.state.input,
              errorText: "[Tool execution was interrupted]",
              ...(part.metadata?.providerExecuted ? { providerExecuted: true } : {}),
              ...(differentModel ? {} : { callProviderMetadata: providerMeta(part.metadata) }),
            })
        }
        if (part.type === "reasoning") {
          if (differentModel) {
            if (part.text.trim().length > 0)
              assistantMessage.parts.push({
                type: "text",
                text: part.text,
              })
            continue
          }
          assistantMessage.parts.push({
            type: "reasoning",
            text: part.text,
            providerMetadata: part.metadata,
          })
        }
      }
      if (assistantMessage.parts.length > 0) {
        result.push(assistantMessage)
        // Inject pending media as a user message for providers that don't support
        // media (images, PDFs) in tool results
        if (media.length > 0) {
          result.push({
            id: MessageID.ascending(),
            role: "user",
            parts: [
              {
                type: "text" as const,
                text: SYNTHETIC_ATTACHMENT_PROMPT,
              },
              ...media.map((attachment) => ({
                type: "file" as const,
                url: attachment.url,
                mediaType: attachment.mime,
                filename: attachment.filename,
              })),
            ],
          })
        }
      }
    }
  }

  const tools = Object.fromEntries(Array.from(toolNames).map((toolName) => [toolName, { toModelOutput }]))

  return yield* Effect.promise(() =>
    convertToModelMessages(
      result.filter((msg) => msg.parts.some((part) => part.type !== "step-start")),
      {
        //@ts-expect-error (convertToModelMessages expects a ToolSet but only actually needs tools[name]?.toModelOutput)
        tools,
      },
    ),
  )
})

export function toModelMessages(
  input: WithParts[],
  model: Provider.Model,
  options?: { stripMedia?: boolean; toolOutputMaxChars?: number },
): Promise<ModelMessage[]> {
  return Effect.runPromise(toModelMessagesEffect(input, model, options))
}

export const page = Effect.fn("MessageV2.page")(function* (input: {
  sessionID: SessionID
  limit: number
  before?: string
}) {
  const { db } = yield* Database.Service
  return yield* db
    .transaction(() =>
      Effect.gen(function* () {
        const before = input.before ? cursor.decode(input.before) : undefined
        const where = before
          ? and(eq(MessageTable.session_id, input.sessionID), older(before))
          : eq(MessageTable.session_id, input.sessionID)
        const rows = yield* db
          .select()
          .from(MessageTable)
          .where(where)
          .orderBy(desc(MessageTable.time_created), desc(MessageTable.id))
          .limit(input.limit + 1)
          .all()
          .pipe(Effect.orDie)
        if (rows.length === 0) {
          const row = yield* db
            .select({ id: SessionTable.id })
            .from(SessionTable)
            .where(eq(SessionTable.id, input.sessionID))
            .get()
            .pipe(Effect.orDie)
          if (!row) return yield* new NotFoundError({ message: `Session not found: ${input.sessionID}` })
          return { items: [] as WithParts[], more: false }
        }
        const more = rows.length > input.limit
        const slice = more ? rows.slice(0, input.limit) : rows
        const items = yield* hydrate(db, slice)
        items.reverse()
        const tail = slice.at(-1)
        return {
          items,
          more,
          cursor: more && tail ? cursor.encode({ id: tail.id, time: tail.time_created }) : undefined,
        }
      }),
    )
    .pipe(
      Effect.catchIf(
        (error) => !NotFoundError.isInstance(error),
        (error) => Effect.die(error),
      ),
    )
})

export function stream(sessionID: SessionID) {
  const size = 50
  return Effect.gen(function* () {
    const result = [] as WithParts[]
    let before: string | undefined
    while (true) {
      const next = yield* page({ sessionID, limit: size, before }).pipe(
        Effect.catchIf(NotFoundError.isInstance, () =>
          Effect.succeed({ items: [] as WithParts[], more: false, cursor: undefined }),
        ),
      )
      if (next.items.length === 0) break
      for (let i = next.items.length - 1; i >= 0; i--) {
        const item = next.items[i]
        if (item) result.push(item)
      }
      if (!next.more || !next.cursor) break
      before = next.cursor
    }
    return result
  })
}

export function parts(input: { sessionID: SessionID; messageID: MessageID }) {
  return Effect.gen(function* () {
    const { db } = yield* Database.Service
    const rows = yield* db
      .select()
      .from(PartTable)
      .where(and(eq(PartTable.message_id, input.messageID), eq(PartTable.session_id, input.sessionID)))
      .orderBy(PartTable.id)
      .all()
      .pipe(Effect.orDie)
    return rows.map(part)
  })
}

export const get = Effect.fn("MessageV2.get")(function* (input: { sessionID: SessionID; messageID: MessageID }) {
  const { db } = yield* Database.Service
  return yield* db
    .transaction(() =>
      Effect.gen(function* () {
        const row = yield* db
          .select()
          .from(MessageTable)
          .where(and(eq(MessageTable.id, input.messageID), eq(MessageTable.session_id, input.sessionID)))
          .get()
          .pipe(Effect.orDie)
        if (!row) return yield* new NotFoundError({ message: `Message not found: ${input.messageID}` })
        return { info: info(row), parts: yield* parts(input) }
      }),
    )
    .pipe(
      Effect.catchIf(
        (error) => !NotFoundError.isInstance(error),
        (error) => Effect.die(error),
      ),
    )
})

function selectCompacted(physical: WithParts[]) {
  const result = [] as WithParts[]
  const completed = new Set<string>()
  let retain: MessageID | undefined
  for (const msg of [...physical].reverse()) {
    result.push(msg)
    if (retain) {
      if (msg.info.id === retain) break
      continue
    }
    if (msg.info.role === "user" && completed.has(msg.info.id)) {
      const part = msg.parts.find((item): item is CompactionPart => item.type === "compaction")
      if (!part) continue
      if (!part.tail_start_id) break
      retain = part.tail_start_id
      if (msg.info.id === retain) break
      continue
    }
    if (isUsableCompactionSummary(msg)) completed.add(msg.info.parentID)
  }
  result.reverse()
  return result
}

export function filterCompacted(msgs: Iterable<WithParts>) {
  return selectCompacted([...msgs].sort(compareHydratedMessagePhysicalOrder))
}

export const filterCompactedEffect = Effect.fnUntraced(function* (sessionID: SessionID) {
  return filterCompacted(yield* stream(sessionID))
})

export type TerminalClassification = "successful" | "failed" | "pending"

export function classifyAssistant(message: Assistant): TerminalClassification {
  if (message.error || message.finish === "error") return "failed"
  if (message.finish !== undefined) return "successful"
  return "pending"
}

function normalizedSummaryText(message: WithParts) {
  return message.parts
    .filter((part): part is SessionV1.TextPart => part.type === "text")
    .map((part) => part.text.trim())
    .filter(Boolean)
    .join("\n")
    .trim()
}

export function isUsableCompactionSummary(
  message: WithParts,
): message is WithParts & { info: Assistant & { summary: true } } {
  return (
    message.info.role === "assistant" &&
    message.info.summary === true &&
    classifyAssistant(message.info) === "successful" &&
    normalizedSummaryText(message).length > 0
  )
}

export type TaskExecutionState =
  | "unstarted"
  | "attempted-incomplete"
  | "terminal-failure"
  | "terminal-success"
  | "terminal-tool-error"

export interface TaskExecution {
  owner: WithParts & { info: User }
  task: CompactionPart | SubtaskPart
  outputs: WithParts[]
  state: TaskExecutionState
}

interface OwnershipEdge {
  parent: MessageID
  order: number
}

function taskPartsOfOwner(message: WithParts) {
  if (message.info.role !== "user") return []
  const tasks = message.parts.filter(
    (part): part is CompactionPart | SubtaskPart =>
      (part.type === "compaction" || part.type === "subtask") && part.messageID === message.info.id,
  )
  const compactions = tasks.filter((part) => part.type === "compaction")
  return compactions.length <= 1 ? tasks : tasks.filter((part) => part.type === "subtask")
}

function taskToolParts(message: WithParts) {
  return message.parts.filter((part): part is ToolPart => part.type === "tool" && part.tool === "task")
}

function matchedSubtaskTool(execution: TaskExecution & { task: SubtaskPart }, output: WithParts) {
  const tools = taskToolParts(output).filter(
    (part) =>
      part.serverProvenance?.type === "subtask-output" &&
      part.serverProvenance.ownerMessageID === execution.owner.info.id &&
      part.serverProvenance.taskPartID === execution.task.id,
  )
  return tools.length === 1 ? tools[0] : undefined
}

function classifyTask(execution: TaskExecution): TaskExecutionState {
  if (execution.outputs.length === 0) return "unstarted"
  if (
    execution.outputs.some((output) => output.info.role === "assistant" && classifyAssistant(output.info) === "failed")
  ) {
    return "terminal-failure"
  }
  if (execution.task.type === "compaction") {
    return execution.outputs.some(isUsableCompactionSummary) ? "terminal-success" : "attempted-incomplete"
  }
  const output = execution.outputs[0]
  if (!output || output.info.role !== "assistant" || classifyAssistant(output.info) !== "successful") {
    return "attempted-incomplete"
  }
  const tool = matchedSubtaskTool(execution as TaskExecution & { task: SubtaskPart }, output)
  if (tool?.state.status === "completed") return "terminal-success"
  if (tool?.state.status === "error") return "terminal-tool-error"
  return "attempted-incomplete"
}

function deriveTaskExecutions(physical: WithParts[]) {
  const executions = physical.flatMap((message) => {
    if (message.info.role !== "user") return []
    return taskPartsOfOwner(message).map(
      (task): TaskExecution => ({
        owner: message as WithParts & { info: User },
        task,
        outputs: [],
        state: "unstarted",
      }),
    )
  })
  const byOwner = new Map<MessageID, TaskExecution[]>()
  for (const execution of executions) {
    const current = byOwner.get(execution.owner.info.id)
    if (current) current.push(execution)
    else byOwner.set(execution.owner.info.id, [execution])
  }

  for (const [ownerID, local] of byOwner) {
    const compaction = local.find((execution) => execution.task.type === "compaction")
    if (compaction) {
      compaction.outputs.push(
        ...physical.filter(
          (message) =>
            message.info.role === "assistant" && message.info.parentID === ownerID && message.info.summary === true,
        ),
      )
    }

    const subtasks = new Map(
      local
        .filter((execution): execution is TaskExecution & { task: SubtaskPart } => execution.task.type === "subtask")
        .map((execution) => [execution.task.id, execution]),
    )
    for (const output of physical) {
      if (output.info.role !== "assistant" || output.info.parentID !== ownerID) continue
      for (const part of taskToolParts(output)) {
        const provenance = part.serverProvenance
        if (provenance?.type !== "subtask-output" || provenance.ownerMessageID !== ownerID) continue
        const execution = subtasks.get(provenance.taskPartID)
        if (!execution || execution.outputs.some((item) => item.info.id === output.info.id)) continue
        execution.outputs.push(output)
      }
    }
  }
  for (const execution of executions) execution.state = classifyTask(execution)
  return executions
}

function sameProvenance(left: SessionV1.ContinuityProvenance, right: SessionV1.ContinuityProvenance) {
  if (left.type !== right.type || left.ownerMessageID !== right.ownerMessageID) return false
  if (left.type === "compaction-replay" && right.type === "compaction-replay") {
    return left.sourceMessageID === right.sourceMessageID
  }
  if (left.type === "subtask-output" && right.type === "subtask-output") {
    return left.taskPartID === right.taskPartID
  }
  if (left.type === "subtask-continuation" && right.type === "subtask-continuation") {
    return left.taskPartID === right.taskPartID && left.sourceMessageID === right.sourceMessageID
  }
  return left.type === "compaction-continuation" && right.type === "compaction-continuation"
}

function generatedProvenance(message: WithParts & { info: User }) {
  const provenances = message.parts.flatMap((part) =>
    part.type === "text" && part.serverProvenance ? [part.serverProvenance] : [],
  )
  const first = provenances[0]
  if (!first || first.type === "subtask-output") return
  return provenances.every((item) => sameProvenance(item, first)) ? first : undefined
}

function qualifyGenerated(physical: WithParts[], executions: TaskExecution[], indexes: ReadonlyMap<MessageID, number>) {
  const generated = new Map<MessageID, MessageID>()
  const byMessageID = new Map(physical.map((message) => [message.info.id, message]))
  const compactions = new Map(
    executions
      .filter(
        (execution): execution is TaskExecution & { task: CompactionPart } =>
          execution.task.type === "compaction" && execution.state === "terminal-success",
      )
      .map((execution) => [execution.owner.info.id, execution]),
  )
  const subtasks = new Map(
    executions
      .filter((execution): execution is TaskExecution & { task: SubtaskPart } => execution.task.type === "subtask")
      .map((execution) => [execution.task.id, execution]),
  )

  for (const candidate of physical) {
    if (candidate.info.role !== "user") continue
    const provenance = generatedProvenance(candidate as WithParts & { info: User })
    if (!provenance) continue
    const candidateIndex = indexes.get(candidate.info.id)!

    if (provenance.type === "compaction-replay" || provenance.type === "compaction-continuation") {
      const execution = compactions.get(provenance.ownerMessageID)
      const summary = execution?.outputs.find(isUsableCompactionSummary)
      if (!execution || !summary || indexes.get(summary.info.id)! >= candidateIndex) continue
      if (provenance.type === "compaction-replay") {
        const source = byMessageID.get(provenance.sourceMessageID)
        if (
          !source ||
          source.info.role !== "user" ||
          indexes.get(source.info.id)! >= indexes.get(execution.owner.info.id)! ||
          source.parts.some((part) => part.type === "compaction")
        ) {
          continue
        }
      }
      generated.set(candidate.info.id, execution.owner.info.id)
      continue
    }

    const execution = subtasks.get(provenance.taskPartID)
    if (
      !execution ||
      execution.owner.info.id !== provenance.ownerMessageID ||
      !execution.task.command ||
      (execution.state !== "terminal-success" && execution.state !== "terminal-tool-error")
    ) {
      continue
    }
    const output = execution.outputs.find((item) => item.info.id === provenance.sourceMessageID)
    if (!output || indexes.get(output.info.id)! >= candidateIndex) continue
    generated.set(candidate.info.id, execution.owner.info.id)
  }
  return generated
}

interface PhysicalAnalysis {
  physical: WithParts[]
  indexes: Map<MessageID, number>
  executions: TaskExecution[]
  generated: Map<MessageID, MessageID>
  external: Set<MessageID>
  matchedOutputs: Set<MessageID>
  rejectedTaskOutputs: Set<MessageID>
}

function analyzePhysical(messages: WithParts[]): PhysicalAnalysis {
  const physical = [...messages].sort(compareHydratedMessagePhysicalOrder)
  const indexes = new Map(physical.map((message, index) => [message.info.id, index]))
  const candidates = deriveTaskExecutions(physical)
  const generated = qualifyGenerated(physical, candidates, indexes)
  const executions = candidates.filter((execution) => !generated.has(execution.owner.info.id))
  const owners = new Set(executions.map((execution) => execution.owner.info.id))
  const external = new Set(
    physical
      .filter(
        (message) =>
          message.info.role === "user" &&
          !owners.has(message.info.id) &&
          !message.parts.some((part) => part.type === "compaction" || part.type === "subtask") &&
          !generated.has(message.info.id),
      )
      .map((message) => message.info.id),
  )
  const matchedOutputs = new Set(executions.flatMap((execution) => execution.outputs.map((output) => output.info.id)))
  const subtaskOwners = new Set(
    executions.filter((execution) => execution.task.type === "subtask").map((execution) => execution.owner.info.id),
  )
  const rejectedTaskOutputs = new Set(
    physical
      .filter(
        (message) =>
          message.info.role === "assistant" &&
          subtaskOwners.has(message.info.parentID) &&
          taskToolParts(message).length > 0 &&
          !matchedOutputs.has(message.info.id),
      )
      .map((message) => message.info.id),
  )
  return { physical, indexes, executions, generated, external, matchedOutputs, rejectedTaskOutputs }
}

function isTerminalResponse(message: WithParts) {
  if (message.info.role !== "assistant") return false
  const terminal = classifyAssistant(message.info)
  if (terminal === "failed") return true
  return terminal === "successful" && message.info.finish !== "tool-calls" && message.info.finish !== "unknown"
}

function hasUnresolvedModelToolCall(message: WithParts) {
  return message.parts.some(
    (part) =>
      part.type === "tool" &&
      !part.metadata?.providerExecuted &&
      !(part.state.status === "error" && part.state.metadata?.interrupted === true),
  )
}

function isOrdinaryFinalResponse(message: WithParts, analysis: PhysicalAnalysis) {
  if (message.info.role !== "assistant") return false
  return (
    !analysis.matchedOutputs.has(message.info.id) &&
    message.info.summary !== true &&
    !analysis.generated.has(message.info.parentID) &&
    !hasUnresolvedModelToolCall(message) &&
    isTerminalResponse(message)
  )
}

function isGeneratedFinalResponse(message: WithParts, analysis: PhysicalAnalysis) {
  return (
    message.info.role === "assistant" && analysis.generated.has(message.info.parentID) && isTerminalResponse(message)
  )
}

function projectTaskContinuity(selected: WithParts[], analysis: PhysicalAnalysis) {
  const selectedIDs = new Set(selected.map((message) => message.info.id))
  const byID = new Map(selected.map((message) => [message.info.id, message]))
  const edges = new Map<MessageID, OwnershipEdge>()
  const assign = (child: WithParts, parentID: MessageID, order: number) => {
    if (
      child.info.id === parentID ||
      !selectedIDs.has(child.info.id) ||
      !byID.has(parentID) ||
      edges.has(child.info.id)
    ) {
      return false
    }
    edges.set(child.info.id, { parent: parentID, order })
    return true
  }

  for (const execution of analysis.executions) {
    if (!selectedIDs.has(execution.owner.info.id)) continue
    for (const output of execution.outputs) assign(output, execution.owner.info.id, 0)
  }

  for (const [messageID, ownerID] of analysis.generated) {
    const message = byID.get(messageID)
    if (message) assign(message, ownerID, 2)
  }

  for (const message of selected) {
    if (message.info.role !== "assistant" || !analysis.generated.has(message.info.parentID)) continue
    assign(message, message.info.parentID, 0)
  }

  for (const message of selected) {
    if (message.info.role !== "assistant" || !isOrdinaryFinalResponse(message, analysis)) continue
    const parentID = message.info.parentID
    const order = analysis.executions.some((execution) => execution.owner.info.id === parentID) ? 3 : 0
    assign(message, message.info.parentID, order)
  }

  for (const message of selected) {
    const index = analysis.indexes.get(message.info.id)!
    const enclosing = analysis.executions
      .filter((execution) => {
        if (execution.task.type !== "compaction" || execution.state !== "terminal-success") return false
        if (!execution.task.tail_start_id || !selectedIDs.has(execution.owner.info.id)) return false
        const tail = analysis.indexes.get(execution.task.tail_start_id)
        const end = analysis.indexes.get(execution.owner.info.id)
        return tail !== undefined && end !== undefined && tail <= index && index < end
      })
      .sort((left, right) => analysis.indexes.get(left.owner.info.id)! - analysis.indexes.get(right.owner.info.id)!)[0]
    if (enclosing) assign(message, enclosing.owner.info.id, 1)
  }

  const cyclic = new Set<MessageID>()
  for (const message of selected) {
    const path = [] as MessageID[]
    const seen = new Map<MessageID, number>()
    let current: MessageID | undefined = message.info.id
    while (current && edges.has(current)) {
      const found = seen.get(current)
      if (found !== undefined) {
        for (const id of path.slice(found)) cyclic.add(id)
        break
      }
      seen.set(current, path.length)
      path.push(current)
      current = edges.get(current)?.parent
    }
  }
  for (const id of cyclic) edges.delete(id)

  const children = new Map<MessageID, WithParts[]>()
  for (const message of selected) {
    const edge = edges.get(message.info.id)
    if (!edge) continue
    const list = children.get(edge.parent)
    if (list) list.push(message)
    else children.set(edge.parent, [message])
  }
  for (const list of children.values()) {
    list.sort((left, right) => {
      const order = edges.get(left.info.id)!.order - edges.get(right.info.id)!.order
      if (order !== 0) return order
      return analysis.indexes.get(left.info.id)! - analysis.indexes.get(right.info.id)!
    })
  }

  const result = [] as WithParts[]
  const added = new Set<MessageID>()
  const emit = (message: WithParts) => {
    if (added.has(message.info.id)) return
    added.add(message.info.id)
    result.push(message)
    for (const child of children.get(message.info.id) ?? []) emit(child)
  }
  for (const message of selected) {
    if (!edges.has(message.info.id)) emit(message)
  }
  for (const message of selected) emit(message)
  return result
}

const physicalOrderTextEncoder = new TextEncoder()

function compareUtf8Binary(left: string, right: string) {
  const leftBytes = physicalOrderTextEncoder.encode(left)
  const rightBytes = physicalOrderTextEncoder.encode(right)
  const length = Math.min(leftBytes.length, rightBytes.length)
  for (let index = 0; index < length; index++) {
    const order = leftBytes[index] - rightBytes[index]
    if (order !== 0) return order
  }
  return leftBytes.length - rightBytes.length
}

export function compareHydratedMessagePhysicalOrder(left: Info | WithParts, right: Info | WithParts) {
  const leftInfo = "info" in left ? left.info : left
  const rightInfo = "info" in right ? right.info : right
  const time = leftInfo.time.created - rightInfo.time.created
  if (time !== 0) return time
  return compareUtf8Binary(leftInfo.id, rightInfo.id)
}

export function modelTurn(messages: WithParts[]) {
  const analysis = analyzePhysical(messages)
  const selected = selectCompacted(analysis.physical).filter(
    (message) => !analysis.rejectedTaskOutputs.has(message.info.id),
  )
  const selectedIDs = new Set(selected.map((message) => message.info.id))
  const projected = projectTaskContinuity(selected, analysis)
  const ordinaryFinal = (message: WithParts) =>
    selectedIDs.has(message.info.id) && isOrdinaryFinalResponse(message, analysis)
  const answered = new Set(
    selected.flatMap((message) =>
      message.info.role === "assistant" && ordinaryFinal(message) ? [message.info.parentID] : [],
    ),
  )
  const generatedAnswered = new Set(
    selected.flatMap((message) =>
      message.info.role === "assistant" && isGeneratedFinalResponse(message, analysis) ? [message.info.parentID] : [],
    ),
  )
  const boundary = projected.findLastIndex(ordinaryFinal)
  const pendingExternal = projected
    .slice(boundary + 1)
    .filter(
      (message): message is WithParts & { info: User } =>
        message.info.role === "user" && analysis.external.has(message.info.id),
    )
  const pendingGenerated = analysis.physical.filter(
    (message): message is WithParts & { info: User } =>
      selectedIDs.has(message.info.id) &&
      message.info.role === "user" &&
      analysis.generated.has(message.info.id) &&
      !generatedAnswered.has(message.info.id),
  )
  const pendingSubtask = analysis.executions
    .filter(
      (execution) =>
        selectedIDs.has(execution.owner.info.id) &&
        execution.task.type === "subtask" &&
        !execution.task.command &&
        (execution.state === "terminal-success" || execution.state === "terminal-tool-error") &&
        !answered.has(execution.owner.info.id),
    )
    .sort((left, right) => analysis.indexes.get(left.owner.info.id)! - analysis.indexes.get(right.owner.info.id)!)
  const targetMessage = pendingExternal.at(-1) ?? pendingGenerated.at(-1) ?? pendingSubtask.at(-1)?.owner
  const target = targetMessage?.info.role === "user" ? targetMessage.info : undefined
  const assistantMessage = target
    ? analysis.physical.findLast(
        (message) =>
          selectedIDs.has(message.info.id) &&
          message.info.role === "assistant" &&
          message.info.parentID === target.id &&
          !analysis.matchedOutputs.has(message.info.id) &&
          message.info.summary !== true,
      )
    : undefined
  const assistant = assistantMessage?.info.role === "assistant" ? assistantMessage.info : undefined
  const terminalMessage = projected.findLast(ordinaryFinal)
  const terminal = terminalMessage?.info.role === "assistant" ? terminalMessage.info : undefined
  const reminderBoundary = boundary >= 0 ? projected[boundary] : undefined
  const tasks = analysis.executions.filter(
    (execution) => selectedIDs.has(execution.owner.info.id) && execution.state === "unstarted",
  )
  return { messages: projected, tasks, target, assistant, terminal, reminderBoundary, pendingExternal }
}

export function latest(msgs: WithParts[]) {
  const view = modelTurn(msgs)
  return {
    user: view.target,
    assistant: view.assistant,
    finished: view.terminal,
    tasks: view.tasks.map((execution) => execution.task),
  }
}

export const modelTurnEffect = Effect.fnUntraced(function* (sessionID: SessionID) {
  return modelTurn(yield* stream(sessionID))
})

export function fromError(
  e: unknown,
  ctx: { providerID: ProviderV2.ID; aborted?: boolean },
): NonNullable<Assistant["error"]> {
  switch (true) {
    case e instanceof DOMException && e.name === "AbortError":
      return new AbortedError(
        { message: e.message },
        {
          cause: e,
        },
      ).toObject()
    case OutputLengthError.isInstance(e):
      return e
    case LoadAPIKeyError.isInstance(e):
      return new AuthError(
        {
          providerID: ctx.providerID,
          message: e.message,
        },
        { cause: e },
      ).toObject()
    case (e as SystemError)?.code === "ECONNRESET":
      return new APIError(
        {
          message: "Connection reset by server",
          isRetryable: true,
          metadata: {
            code: (e as SystemError).code ?? "",
            syscall: (e as SystemError).syscall ?? "",
            message: (e as SystemError).message ?? "",
          },
        },
        { cause: e },
      ).toObject()
    case e instanceof Error && (e as FetchDecompressionError).code === "ZlibError":
      if (ctx.aborted) {
        return new AbortedError({ message: e.message }, { cause: e }).toObject()
      }
      return new APIError(
        {
          message: "Response decompression failed",
          isRetryable: true,
          metadata: {
            code: (e as FetchDecompressionError).code,
            message: e.message,
          },
        },
        { cause: e },
      ).toObject()
    case e instanceof ProviderError.HeaderTimeoutError:
      return new APIError(
        {
          message: e.message,
          isRetryable: true,
          metadata: {
            code: e.name,
            timeoutMs: String(e.ms),
          },
        },
        { cause: e },
      ).toObject()
    case e instanceof ProviderError.ResponseStreamError:
      return new APIError(
        {
          message: e.message,
          isRetryable: true,
          metadata: {
            code: e.name,
          },
        },
        { cause: e },
      ).toObject()
    case APICallError.isInstance(e):
      const parsed = ProviderError.parseAPICallError({
        providerID: ctx.providerID,
        error: e,
      })
      if (parsed.type === "context_overflow") {
        return new ContextOverflowError(
          {
            message: parsed.message,
            responseBody: parsed.responseBody,
          },
          { cause: e },
        ).toObject()
      }

      return new APIError(
        {
          message: parsed.message,
          statusCode: parsed.statusCode,
          isRetryable: parsed.isRetryable,
          responseHeaders: parsed.responseHeaders,
          responseBody: parsed.responseBody,
          metadata: parsed.metadata,
        },
        { cause: e },
      ).toObject()
    case e instanceof Error:
      return new NamedError.Unknown({ message: errorMessage(e) }, { cause: e }).toObject()
    default:
      try {
        const parsed = ProviderError.parseStreamError(e)
        if (parsed) {
          if (parsed.type === "context_overflow") {
            return new ContextOverflowError(
              {
                message: parsed.message,
                responseBody: parsed.responseBody,
              },
              { cause: e },
            ).toObject()
          }
          return new APIError(
            {
              message: parsed.message,
              isRetryable: parsed.isRetryable,
              responseBody: parsed.responseBody,
            },
            {
              cause: e,
            },
          ).toObject()
        }
      } catch {}
      return new NamedError.Unknown({ message: JSON.stringify(e) }, { cause: e }).toObject()
  }
}

export * as MessageV2 from "./message-v2"
export const node = LayerNode.group([Database.node])
