import type { Session as SDKSession, Message, Part } from "@opencode-ai/sdk/v2"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Session } from "@/session/session"
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
import { isDeepStrictEqual } from "node:util"

const decodeMessageInfo = Schema.decodeUnknownSync(SessionV1.Info)
const decodePart = Schema.decodeUnknownSync(SessionV1.Part)

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

export function formatImportFileError(file: string, error: FSUtil.Error) {
  if (error._tag === "PlatformError") {
    if (error.reason._tag === "NotFound") return `File not found: ${file}`
    if (error.reason._tag === "PermissionDenied") return `Failed to read file: Permission denied`
    return `Failed to read file: ${error.message}`
  }

  const detail = error.cause instanceof Error ? error.cause.message : error.message
  return `Invalid JSON in ${file}: ${detail}`
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
  if (Array.from(partMap.keys()).some((messageID) => !messageIDs.has(messageID))) return null

  return {
    info: sessionItem.data,
    messages: messages.map((msg) => ({
      info: msg,
      parts: partMap.get(msg.id) ?? [],
    })),
  }
}

export type ExportData = { info: SDKSession; messages: Array<{ info: Message; parts: Part[] }> }

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
  const messageIDs = new Set<string>()
  const partIDs = new Set<string>()
  let previousMessage: SessionV1.Info | undefined
  const imported = exportData.messages.map((message) => {
    const messageInfo = decodeMessageInfo(message.info) as SessionV1.Info
    if (messageInfo.sessionID !== info.id || messageInfo.time?.created === undefined) {
      throw new Error("Imported message owner and physical time must match its containing session")
    }
    if (
      messageIDs.has(messageInfo.id) ||
      (previousMessage && MessageV2.compareHydratedMessagePhysicalOrder(messageInfo, previousMessage) <= 0)
    ) {
      throw new Error("Imported messages contradict canonical physical order")
    }
    messageIDs.add(messageInfo.id)
    previousMessage = messageInfo
    const { id, sessionID: _, ...data } = messageInfo
    return {
      id,
      timeCreated: messageInfo.time.created,
      data,
      parts: message.parts.map((part) => {
        const partInfo = decodePart(part) as SessionV1.Part
        if (partInfo.sessionID !== info.id || partInfo.messageID !== id || !partIDs.add(partInfo.id)) {
          throw new Error("Imported part ownership must match its containing message and session")
        }
        const { id: partID, sessionID: _sessionID, messageID, ...partData } = partInfo
        return { id: partID, messageID, data: partData }
      }),
    }
  })

  const persist = Effect.gen(function* () {
    const existingSession = yield* db
      .select({
        projectID: SessionTable.project_id,
        parentID: SessionTable.parent_id,
        workspaceID: SessionTable.workspace_id,
      })
      .from(SessionTable)
      .where(eq(SessionTable.id, row.id))
      .get()
      .pipe(Effect.orDie)
    if (
      existingSession &&
      (existingSession.projectID !== row.project_id ||
        (existingSession.parentID ?? null) !== (row.parent_id ?? null) ||
        (existingSession.workspaceID ?? null) !== (row.workspace_id ?? null))
    ) {
      return yield* Effect.die(`Imported session ${row.id} contradicts persisted identity or ownership`)
    }
    for (const message of imported) {
      const existingMessage = yield* db
        .select({ sessionID: MessageTable.session_id, timeCreated: MessageTable.time_created, data: MessageTable.data })
        .from(MessageTable)
        .where(eq(MessageTable.id, message.id))
        .get()
        .pipe(Effect.orDie)
      if (
        existingMessage &&
        (existingMessage.sessionID !== row.id ||
          existingMessage.timeCreated !== message.timeCreated ||
          !isDeepStrictEqual(existingMessage.data, message.data))
      ) {
        return yield* Effect.die(`Imported message ${message.id} collides with different persisted content or ownership`)
      }
      for (const part of message.parts) {
        const existingPart = yield* db
          .select({ sessionID: PartTable.session_id, messageID: PartTable.message_id, data: PartTable.data })
          .from(PartTable)
          .where(eq(PartTable.id, part.id))
          .get()
          .pipe(Effect.orDie)
        if (
          existingPart &&
          (existingPart.sessionID !== row.id ||
            existingPart.messageID !== part.messageID ||
            !isDeepStrictEqual(existingPart.data, part.data))
        ) {
          return yield* Effect.die(`Imported part ${part.id} collides with different persisted content or ownership`)
        }
      }
    }

    yield* db
      .insert(SessionTable)
      .values(row)
      .onConflictDoUpdate({
        target: SessionTable.id,
        set: { project_id: row.project_id, directory: row.directory, path: row.path },
      })
      .run()
      .pipe(Effect.orDie)
    for (const message of imported) {
      yield* db
        .insert(MessageTable)
        .values({
          id: message.id,
          session_id: row.id,
          time_created: message.timeCreated,
          data: message.data as never,
        })
        .onConflictDoNothing()
        .run()
        .pipe(Effect.orDie)
      for (const part of message.parts) {
        yield* db
          .insert(PartTable)
          .values({ id: part.id, message_id: part.messageID, session_id: row.id, data: part.data })
          .onConflictDoNothing()
          .run()
          .pipe(Effect.orDie)
      }
    }
    yield* CompactionRegionProjection.reconcile(db, { sessionID: row.id })
  })
  yield* db.transaction(() => persist, { behavior: "immediate" }).pipe(Effect.orDie)
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
    exportData = (yield* fs
      .readJson(file)
      .pipe(Effect.mapError((error) => new CliError({ message: formatImportFileError(file, error) })))) as ExportData
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
