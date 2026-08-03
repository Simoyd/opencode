import type { Session as SDKSession, Message, Part } from "@opencode-ai/sdk/v2"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Session } from "@/session/session"
import type { MessageID, PartID } from "@/session/schema"
import { MessageV2 } from "../../session/message-v2"
import { CliError, effectCmd } from "../effect-cmd"
import { Database } from "@opencode-ai/core/database/database"
import { SessionTable, MessageTable, PartTable } from "@opencode-ai/core/session/sql"
import { CompactionRegionProjection } from "@opencode-ai/core/session/compaction-region"
import { InstanceRef } from "@/effect/instance-ref"
import { ShareNext } from "@/share/share-next"
import { EOL } from "os"
import path from "path"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { Effect, Schema } from "effect"
import type { InstanceContext } from "@/project/instance-context"
import { eq } from "drizzle-orm"

const decodeMessageInfo = Schema.decodeUnknownSync(SessionV1.Info)
const decodePart = Schema.decodeUnknownSync(SessionV1.Part)

function semanticallyEqual(left: unknown, right: unknown): boolean {
  if (Object.is(left, right)) return true
  if (Array.isArray(left) || Array.isArray(right)) {
    return (
      Array.isArray(left) &&
      Array.isArray(right) &&
      left.length === right.length &&
      left.every((value, index) => semanticallyEqual(value, right[index]))
    )
  }
  if (typeof left !== "object" || left === null || typeof right !== "object" || right === null) return false
  const leftRecord = left as Record<string, unknown>
  const rightRecord = right as Record<string, unknown>
  const leftKeys = Object.keys(leftRecord)
    .filter((key) => leftRecord[key] !== undefined)
    .sort()
  const rightKeys = Object.keys(rightRecord)
    .filter((key) => rightRecord[key] !== undefined)
    .sort()
  return (
    leftKeys.length === rightKeys.length &&
    leftKeys.every((key, index) => key === rightKeys[index] && semanticallyEqual(leftRecord[key], rightRecord[key]))
  )
}

/** Discriminated union returned by the ShareNext API (GET /api/shares/:id/data) */
export type ShareData =
  | { type: "session"; data: SDKSession }
  | { type: "message"; data: Message }
  | { type: "part"; data: Part }
  | { type: "session_diff"; data: unknown }
  | { type: "model"; data: unknown }

/** Extract share ID from a share URL like https://opncd.ai/share/abc123 */
export function parseShareUrl(url: string): string | null {
  const match = url.match(/^https?:\/\/[^/]+\/share\/([a-zA-Z0-9_-]+)$/)
  return match ? match[1] : null
}

export function shouldAttachShareAuthHeaders(shareUrl: string, accountBaseUrl: string): boolean {
  try {
    return new URL(shareUrl).origin === new URL(accountBaseUrl).origin
  } catch {
    return false
  }
}

/**
 * Transform ShareNext API response (flat array) into the nested structure for local file storage.
 *
 * The API returns a flat array: [session, message, message, part, part, ...]
 * Local storage expects: { info: session, messages: [{ info: message, parts: [part, ...] }, ...] }
 *
 * This groups parts by their messageID to reconstruct the hierarchy before writing to disk.
 */
export function transformShareData(shareData: ShareData[]): {
  info: SDKSession
  messages: Array<{ info: Message; parts: Part[] }>
} | null {
  const sessionItem = shareData.find((d) => d.type === "session")
  if (!sessionItem) return null

  const messages: Message[] = []
  const partMap = new Map<string, Part[]>()

  for (const item of shareData) {
    if (item.type === "message") {
      messages.push(item.data)
    } else if (item.type === "part") {
      if (!partMap.has(item.data.messageID)) {
        partMap.set(item.data.messageID, [])
      }
      partMap.get(item.data.messageID)!.push(item.data)
    }
  }

  if (messages.length === 0) return null
  const messageIDs = new Set(messages.map((message) => message.id))
  for (const messageID of partMap.keys()) {
    if (!messageIDs.has(messageID)) {
      throw new Error(`Shared part references an absent containing message: ${messageID}`)
    }
  }

  return {
    info: sessionItem.data,
    messages: messages.map((msg) => ({
      info: msg,
      parts: partMap.get(msg.id) ?? [],
    })),
  }
}

export type ExportData = { info: SDKSession; messages: Array<{ info: Message; parts: Part[] }> }

function decodeImportedMessages(exportData: ExportData, sessionID: Session.Info["id"]) {
  const messages = exportData.messages.map((message) => ({
    info: decodeMessageInfo(message.info) as SessionV1.Info,
    parts: message.parts.map((part) => decodePart(part) as SessionV1.Part),
  }))
  const messageIDs = new Set<string>()
  const partIDs = new Set<string>()
  const indexes = new Map<string, number>()
  const byMessageID = new Map(messages.map((message) => [message.info.id, message]))
  const byPartID = new Map<string, SessionV1.Part>()
  let previous: SessionV1.Info | undefined

  for (const [index, message] of messages.entries()) {
    if (message.info.sessionID !== sessionID || message.info.time?.created === undefined) {
      throw new Error("Imported message owner and physical time must match its containing session")
    }
    if (
      messageIDs.has(message.info.id) ||
      (previous && MessageV2.compareHydratedMessagePhysicalOrder(message.info, previous) <= 0)
    ) {
      throw new Error("Imported messages contradict canonical physical order")
    }
    messageIDs.add(message.info.id)
    indexes.set(message.info.id, index)
    previous = message.info
    for (const part of message.parts) {
      if (part.sessionID !== sessionID || part.messageID !== message.info.id || partIDs.has(part.id)) {
        throw new Error("Imported part ownership must match its containing message and session")
      }
      partIDs.add(part.id)
      byPartID.set(part.id, part)
      if (part.type === "tool" && part.state.status === "completed" && part.state.attachments) {
        if (message.info.role !== "assistant") {
          throw new Error("Imported completed-tool attachments require an assistant containing message")
        }
        for (const attachment of part.state.attachments) {
          if (
            attachment.sessionID !== sessionID ||
            attachment.messageID !== message.info.id ||
            partIDs.has(attachment.id)
          ) {
            throw new Error("Imported nested attachment identity contradicts its containing ToolPart")
          }
          partIDs.add(attachment.id)
        }
      }
    }
  }

  const requireOwner = (ownerMessageID: MessageID, part: SessionV1.Part) => {
    const owner = byMessageID.get(ownerMessageID)
    const containing = byMessageID.get(part.messageID)!
    if (!owner || owner.info.role !== "user" || indexes.get(owner.info.id)! >= indexes.get(containing.info.id)!) {
      throw new Error("Imported continuity owner must be an earlier user message in the same session")
    }
    return { owner, containing }
  }
  const requireCompaction = (owner: SessionV1.WithParts) => {
    const parts = owner.parts.filter(
      (part): part is SessionV1.CompactionPart => part.type === "compaction" && part.messageID === owner.info.id,
    )
    if (parts.length !== 1) throw new Error("Imported compaction continuity must target one exact CompactionPart owner")
    return parts[0]
  }
  const requireSubtask = (owner: SessionV1.WithParts, taskPartID: PartID) => {
    const task = byPartID.get(taskPartID)
    if (!task || task.type !== "subtask" || task.messageID !== owner.info.id) {
      throw new Error("Imported subtask continuity must target an exact SubtaskPart owner")
    }
    return task
  }

  const sameProvenance = (left: SessionV1.ContinuityProvenance, right: SessionV1.ContinuityProvenance) => {
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

  for (const message of messages) {
    const claims = message.parts.flatMap((part) =>
      part.type === "text" && part.serverProvenance ? [part.serverProvenance] : [],
    )
    const first = claims[0]
    if (first && !claims.every((claim) => sameProvenance(first, claim))) {
      throw new Error(`Imported generated message ${message.info.id} has contradictory continuity relationships`)
    }
  }

  type ExactSubtaskOutput = {
    owner: SessionV1.WithParts
    task: SessionV1.SubtaskPart
    message: SessionV1.WithParts
    part: SessionV1.ToolPart
  }
  const outputByTaskPartID = new Map<string, ExactSubtaskOutput>()
  const taskPartIDByOutputMessageID = new Map<string, string>()

  for (const message of messages) {
    for (const part of message.parts) {
      const provenance = (part.type === "text" || part.type === "tool") && part.serverProvenance
      if (!provenance || provenance.type !== "subtask-output") continue
      const { owner, containing } = requireOwner(provenance.ownerMessageID, part)
      const task = requireSubtask(owner, provenance.taskPartID)
      if (
        part.type !== "tool" ||
        part.tool !== "task" ||
        containing.info.role !== "assistant" ||
        containing.info.parentID !== owner.info.id ||
        MessageV2.classifyAssistant(containing.info) !== "successful" ||
        (part.state.status !== "completed" && part.state.status !== "error")
      ) {
        throw new Error("Imported Subtask output provenance does not identify an exact terminal task result")
      }
      if (outputByTaskPartID.has(task.id) || taskPartIDByOutputMessageID.has(containing.info.id)) {
        throw new Error("Imported Subtask output provenance has duplicate or conflicting exact result claims")
      }
      const output = { owner, task, message: containing, part } satisfies ExactSubtaskOutput
      outputByTaskPartID.set(task.id, output)
      taskPartIDByOutputMessageID.set(containing.info.id, task.id)
    }
  }

  const requireCompletedCompactionBefore = (owner: SessionV1.WithParts, containing: SessionV1.WithParts) => {
    requireCompaction(owner)
    const ownerIndex = indexes.get(owner.info.id)!
    const containingIndex = indexes.get(containing.info.id)!
    const summary = messages.find(
      (candidate) =>
        candidate.info.role === "assistant" &&
        candidate.info.parentID === owner.info.id &&
        ownerIndex < indexes.get(candidate.info.id)! &&
        indexes.get(candidate.info.id)! < containingIndex &&
        MessageV2.isUsableCompactionSummary(candidate),
    )
    if (!summary) {
      throw new Error("Imported compaction continuity requires a usable completed owner summary before its carrier")
    }
  }

  for (const message of messages) {
    for (const part of message.parts) {
      if (part.type !== "text" && part.type !== "tool") continue
      const provenance = part.serverProvenance
      if (!provenance) continue
      const { owner, containing } = requireOwner(provenance.ownerMessageID, part)
      switch (provenance.type) {
        case "compaction-replay": {
          requireCompletedCompactionBefore(owner, containing)
          const source = byMessageID.get(provenance.sourceMessageID)
          if (
            part.type !== "text" ||
            containing.info.role !== "user" ||
            !source ||
            source.info.role !== "user" ||
            indexes.get(source.info.id)! >= indexes.get(owner.info.id)! ||
            source.parts.some((candidate) => candidate.type === "compaction")
          ) {
            throw new Error("Imported compaction replay provenance has an invalid source or physical direction")
          }
          break
        }
        case "compaction-continuation":
          requireCompletedCompactionBefore(owner, containing)
          if (part.type !== "text" || containing.info.role !== "user") {
            throw new Error("Imported compaction continuation provenance has an invalid target kind")
          }
          break
        case "subtask-output":
          break
        case "subtask-continuation": {
          const task = requireSubtask(owner, provenance.taskPartID)
          const source = byMessageID.get(provenance.sourceMessageID)
          const output = outputByTaskPartID.get(task.id)
          if (
            part.type !== "text" ||
            containing.info.role !== "user" ||
            !task.command ||
            !source ||
            source.info.role !== "assistant" ||
            source.info.parentID !== owner.info.id ||
            !output ||
            output.owner.info.id !== owner.info.id ||
            output.message.info.id !== source.info.id ||
            indexes.get(owner.info.id)! >= indexes.get(source.info.id)! ||
            indexes.get(source.info.id)! >= indexes.get(containing.info.id)!
          ) {
            throw new Error("Imported Subtask continuation provenance has an invalid source or physical direction")
          }
          break
        }
      }
    }
  }
  return messages
}

export const persistImportedSession = Effect.fn("Cli.import.persist")(function* (
  exportData: ExportData,
  ctx: InstanceContext,
) {
  const { db } = yield* Database.Service
  const info = Schema.decodeUnknownSync(Session.Info)({
    ...exportData.info,
    projectID: ctx.project.id,
    directory: ctx.directory,
    path: path.relative(path.resolve(ctx.worktree), ctx.directory).replaceAll("\\", "/"),
  }) as Session.Info
  const row = Session.toRow(info)
  const importedMessages = decodeImportedMessages(exportData, info.id)
  const importedParts = importedMessages.flatMap((message) => message.parts)
  const nestedAttachments = importedParts.flatMap((part) =>
    part.type === "tool" && part.state.status === "completed"
      ? (part.state.attachments ?? []).map((attachment) => ({ attachment, containingPart: part }))
      : [],
  )
  const validateCollisions = Effect.gen(function* () {
    const existingSession = yield* db
      .select()
      .from(SessionTable)
      .where(eq(SessionTable.id, row.id))
      .get()
      .pipe(Effect.orDie)
    if (existingSession && !semanticallyEqual(Session.fromRow(existingSession), info)) {
      return yield* Effect.die(`Imported session ${row.id} collides with different persisted data`)
    }

    if (existingSession) {
      const existingMessages = yield* db
        .select({ id: MessageTable.id })
        .from(MessageTable)
        .where(eq(MessageTable.session_id, row.id))
        .all()
        .pipe(Effect.orDie)
      const importedMessageIDs = new Set(importedMessages.map((message) => message.info.id))
      if (
        existingMessages.length > 0 &&
        (existingMessages.length !== importedMessageIDs.size ||
          existingMessages.some((message) => !importedMessageIDs.has(message.id)))
      ) {
        return yield* Effect.die(`Imported session ${row.id} has a different persisted message graph`)
      }

      const existingParts = yield* db
        .select({ id: PartTable.id })
        .from(PartTable)
        .where(eq(PartTable.session_id, row.id))
        .all()
        .pipe(Effect.orDie)
      const importedPartIDs = new Set(importedParts.map((part) => part.id))
      if (
        existingParts.length > 0 &&
        (existingParts.length !== importedPartIDs.size || existingParts.some((part) => !importedPartIDs.has(part.id)))
      ) {
        return yield* Effect.die(`Imported session ${row.id} has a different persisted part graph`)
      }
    }

    const persistedParts = yield* db
      .select({
        id: PartTable.id,
        sessionID: PartTable.session_id,
        messageID: PartTable.message_id,
        data: PartTable.data,
      })
      .from(PartTable)
      .all()
      .pipe(Effect.orDie)
    const persistedTopLevelIDs = new Set(persistedParts.map((part) => part.id))
    const persistedNestedByID = new Map<
      string,
      Array<{
        containingPartID: string
        sessionID: string
        messageID: string
        attachment: SessionV1.FilePart
      }>
    >()
    for (const part of persistedParts) {
      const data = part.data as Partial<SessionV1.ToolPart>
      if (data.type !== "tool" || data.state?.status !== "completed") continue
      for (const attachment of data.state.attachments ?? []) {
        const existing = persistedNestedByID.get(attachment.id)
        const location = {
          containingPartID: part.id,
          sessionID: part.sessionID,
          messageID: part.messageID,
          attachment,
        }
        if (existing) existing.push(location)
        else persistedNestedByID.set(attachment.id, [location])
      }
    }

    for (const part of importedParts) {
      if (persistedNestedByID.has(part.id)) {
        return yield* Effect.die(`Imported top-level part ${part.id} collides with a persisted nested attachment`)
      }
    }

    for (const msg of importedMessages) {
      const msgInfo = msg.info
      const { id, sessionID: _, ...msgData } = msgInfo
      const timeCreated = msgInfo.time.created
      const existingMessage = yield* db
        .select({ sessionID: MessageTable.session_id, timeCreated: MessageTable.time_created, data: MessageTable.data })
        .from(MessageTable)
        .where(eq(MessageTable.id, id))
        .get()
        .pipe(Effect.orDie)
      if (
        existingMessage &&
        (existingMessage.sessionID !== row.id ||
          existingMessage.timeCreated !== timeCreated ||
          !semanticallyEqual(existingMessage.data, msgData))
      ) {
        return yield* Effect.die(`Imported message ${id} collides with different persisted identity or data`)
      }

      for (const part of msg.parts) {
        const { id: partId, sessionID: _s, messageID, ...partData } = part
        const existingPart = yield* db
          .select({ sessionID: PartTable.session_id, messageID: PartTable.message_id, data: PartTable.data })
          .from(PartTable)
          .where(eq(PartTable.id, partId))
          .get()
          .pipe(Effect.orDie)
        if (
          existingPart &&
          (existingPart.sessionID !== row.id ||
            existingPart.messageID !== messageID ||
            !semanticallyEqual(existingPart.data, partData))
        ) {
          return yield* Effect.die(`Imported part ${partId} collides with different persisted identity or data`)
        }
      }
    }

    for (const { attachment, containingPart } of nestedAttachments) {
      if (persistedTopLevelIDs.has(attachment.id)) {
        return yield* Effect.die(`Imported nested attachment ${attachment.id} collides with a persisted top-level part`)
      }
      const existingNested = persistedNestedByID.get(attachment.id)
      if (
        existingNested?.some(
          (persisted) =>
            persisted.containingPartID !== containingPart.id ||
            persisted.sessionID !== containingPart.sessionID ||
            persisted.messageID !== containingPart.messageID ||
            !semanticallyEqual(persisted.attachment, attachment),
        )
      ) {
        return yield* Effect.die(
          `Imported nested attachment ${attachment.id} collides with a different persisted parent or data`,
        )
      }
    }
  })

  const writeImportedRows = Effect.gen(function* () {
    yield* db.insert(SessionTable).values(row).onConflictDoNothing().run().pipe(Effect.orDie)
    for (const msg of importedMessages) {
      const { id, sessionID: _, ...msgData } = msg.info
      yield* db
        .insert(MessageTable)
        .values({
          id,
          session_id: row.id,
          time_created: msg.info.time.created,
          data: msgData as never,
        })
        .onConflictDoNothing()
        .run()
        .pipe(Effect.orDie)

      for (const part of msg.parts) {
        const { id: partId, sessionID: _s, messageID, ...partData } = part
        yield* db
          .insert(PartTable)
          .values({
            id: partId,
            message_id: messageID,
            session_id: row.id,
            data: partData,
          })
          .onConflictDoNothing()
          .run()
          .pipe(Effect.orDie)
      }
    }
    yield* CompactionRegionProjection.reconcile(db, { sessionID: row.id })
  })
  yield* db
    .transaction(() => validateCollisions.pipe(Effect.andThen(writeImportedRows)), { behavior: "immediate" })
    .pipe(Effect.orDie)

  return info
})

export const ImportCommand = effectCmd({
  command: "import <file>",
  describe: "import session data from JSON file or URL",
  builder: (yargs) =>
    yargs.positional("file", {
      describe: "path to JSON file or share URL",
      type: "string",
      demandOption: true,
    }),
  handler: Effect.fn("Cli.import")(function* (args) {
    const ctx = yield* InstanceRef
    if (!ctx) return yield* Effect.die("InstanceRef not provided")
    return yield* runImport(args.file, ctx)
  }),
})

const runImport = Effect.fn("Cli.import.body")(function* (file: string, ctx: InstanceContext) {
  const share = yield* ShareNext.Service
  const fs = yield* FSUtil.Service

  let exportData: ExportData | undefined

  const isUrl = file.startsWith("http://") || file.startsWith("https://")

  if (isUrl) {
    const slug = parseShareUrl(file)
    if (!slug) {
      const baseUrl = yield* Effect.orDie(share.url())
      process.stdout.write(`Invalid URL format. Expected: ${baseUrl}/share/<slug>`)
      process.stdout.write(EOL)
      return
    }

    const baseUrl = new URL(file).origin
    const req = yield* Effect.orDie(share.request())
    const headers = shouldAttachShareAuthHeaders(file, req.baseUrl) ? req.headers : {}

    const tryFetch = (url: string) =>
      Effect.tryPromise({
        try: () => fetch(url, { headers }),
        catch: (e) =>
          new CliError({
            message: `Failed to fetch share data: ${e instanceof Error ? e.message : String(e)}`,
          }),
      })

    const dataPath = req.api.data(slug)
    let response = yield* tryFetch(`${baseUrl}${dataPath}`)

    if (!response.ok && dataPath !== `/api/share/${slug}/data`) {
      response = yield* tryFetch(`${baseUrl}/api/share/${slug}/data`)
    }

    if (!response.ok) {
      process.stdout.write(`Failed to fetch share data: ${response.statusText}`)
      process.stdout.write(EOL)
      return
    }

    const shareData = yield* Effect.tryPromise({
      try: () => response.json() as Promise<ShareData[]>,
      catch: () => new CliError({ message: "Share data was not valid JSON" }),
    })
    const transformed = transformShareData(shareData)

    if (!transformed) {
      process.stdout.write(`Share not found or empty: ${slug}`)
      process.stdout.write(EOL)
      return
    }

    exportData = transformed
  } else {
    exportData = (yield* fs.readJson(file).pipe(Effect.orElseSucceed(() => undefined))) as
      | NonNullable<typeof exportData>
      | undefined
    if (!exportData) {
      process.stdout.write(`File not found: ${file}`)
      process.stdout.write(EOL)
      return
    }
  }

  if (!exportData) {
    process.stdout.write(`Failed to read session data`)
    process.stdout.write(EOL)
    return
  }

  yield* persistImportedSession(exportData, ctx)

  process.stdout.write(`Imported session: ${exportData.info.id}`)
  process.stdout.write(EOL)
})
