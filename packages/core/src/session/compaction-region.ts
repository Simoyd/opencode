export * as CompactionRegionProjection from "./compaction-region"

import { Effect } from "effect"
import { and, asc, eq } from "drizzle-orm"
import type { Database } from "../database/database"
import { SessionV1, type MessageID } from "../v1/session"
import type { SessionSchema } from "./schema"
import { CompactionRegionTable, MessageTable, PartTable } from "./sql"

type DatabaseService = Database.Interface["db"]
const changed = Symbol("compaction-region-changed")
type MarkedData = Record<PropertyKey, unknown>

export function markInvalidation(data: unknown) {
  if (typeof data === "object" && data !== null) (data as MarkedData)[changed] = true
}

export function takeInvalidation(data: unknown) {
  if (typeof data !== "object" || data === null) return false
  const marked = data as MarkedData
  if (marked[changed] !== true) return false
  delete marked[changed]
  return true
}

export function create(_db: DatabaseService, _input: { sessionID: SessionSchema.ID }) {
  return Effect.void
}

export const reconcile = Effect.fn("CompactionRegionProjection.reconcile")(function* (
  db: DatabaseService,
  input: { sessionID: SessionSchema.ID },
) {
  const messageRows = yield* db
    .select()
    .from(MessageTable)
    .where(eq(MessageTable.session_id, input.sessionID))
    .orderBy(asc(MessageTable.time_created), asc(MessageTable.id))
    .all()
    .pipe(Effect.orDie)
  const partRows = yield* db
    .select()
    .from(PartTable)
    .where(eq(PartTable.session_id, input.sessionID))
    .orderBy(PartTable.message_id, PartTable.id)
    .all()
    .pipe(Effect.orDie)
  const parts = new Map<MessageID, SessionV1.Part[]>()
  for (const row of partRows) {
    const part = { ...row.data, id: row.id, sessionID: row.session_id, messageID: row.message_id } as SessionV1.Part
    const list = parts.get(row.message_id)
    if (list) list.push(part)
    else parts.set(row.message_id, [part])
  }
  const messages = messageRows.map(
    (row) =>
      ({
        info: { ...row.data, id: row.id, sessionID: row.session_id } as SessionV1.Info,
        parts: parts.get(row.id) ?? [],
      }) satisfies SessionV1.WithParts,
  )
  const next = derive(input.sessionID, messages)
  const current = yield* db
    .select()
    .from(CompactionRegionTable)
    .where(eq(CompactionRegionTable.session_id, input.sessionID))
    .orderBy(CompactionRegionTable.marker_id)
    .all()
    .pipe(Effect.orDie)
  if (same(current, next)) return false

  const nextByMarker = new Map(next.map((row) => [row.marker_id, row]))
  for (const row of current) {
    if (nextByMarker.has(row.marker_id)) continue
    yield* db
      .delete(CompactionRegionTable)
      .where(
        and(
          eq(CompactionRegionTable.session_id, input.sessionID),
          eq(CompactionRegionTable.marker_id, row.marker_id),
        ),
      )
      .run()
      .pipe(Effect.orDie)
  }
  const currentByMarker = new Map(current.map((row) => [row.marker_id, row]))
  for (const row of next) {
    const existing = currentByMarker.get(row.marker_id)
    if (existing && sameRow(existing, row)) continue
    yield* db
      .insert(CompactionRegionTable)
      .values(row)
      .onConflictDoUpdate({
        target: [CompactionRegionTable.session_id, CompactionRegionTable.marker_id],
        set: {
          start_message_id: row.start_message_id,
          summary_message_id: row.summary_message_id,
          summary_preview: row.summary_preview,
          physical_message_count: row.physical_message_count,
          semantic_message_count: row.semantic_message_count,
          part_count: row.part_count,
        },
      })
      .run()
      .pipe(Effect.orDie)
  }
  return true
})

function derive(sessionID: SessionSchema.ID, messages: SessionV1.WithParts[]) {
  const rows: (typeof CompactionRegionTable.$inferInsert)[] = []
  let start = 0
  for (let markerIndex = 0; markerIndex < messages.length; markerIndex++) {
    const marker = messages[markerIndex]!
    if (!isMarker(marker)) continue
    const nextMarker = messages.findIndex((message, index) => index > markerIndex && isMarker(message))
    const summaryCandidates = messages
      .slice(markerIndex + 1, nextMarker < 0 ? messages.length : nextMarker)
      .filter(
        (message) =>
          message.info.role === "assistant" &&
          message.info.parentID === marker.info.id &&
          !!message.info.summary &&
          !!message.info.finish &&
          !message.info.error &&
          messageText(message).length > 0,
      )
    if (summaryCandidates.length !== 1 || markerIndex <= start) continue
    const body = messages.slice(start, markerIndex)
    const summary = summaryCandidates[0]!
    const normalized = messageText(summary).split(/\s+/).filter(Boolean).join(" ")
    rows.push({
      session_id: sessionID,
      marker_id: marker.info.id,
      start_message_id: body[0]!.info.id,
      summary_message_id: summary.info.id,
      summary_preview: normalized.length <= 80 ? normalized : `${normalized.slice(0, 77)}...`,
      physical_message_count: body.length,
      semantic_message_count: body.filter(isSemantic).length,
      part_count: body.reduce((count, message) => count + message.parts.length, 0),
    })
    start = markerIndex + 1
  }
  return rows
}

function isMarker(message: SessionV1.WithParts) {
  return message.info.role === "user" && message.parts.some((part) => part.type === "compaction")
}

function isSemantic(message: SessionV1.WithParts) {
  if (isMarker(message)) return false
  if (message.info.role === "assistant" && message.info.summary) return false
  if (
    message.info.role === "user" &&
    message.parts.some(
      (part) =>
        part.type === "text" &&
        (part.metadata?.compaction_replay === true || (part.synthetic && part.metadata?.compaction_continue === true)),
    )
  )
    return false
  return true
}

function messageText(message: SessionV1.WithParts) {
  return message.parts
    .filter(
      (part): part is SessionV1.TextPart | SessionV1.ReasoningPart => part.type === "text" || part.type === "reasoning",
    )
    .map((part) => part.text.trim())
    .filter(Boolean)
    .join("\n")
    .trim()
}

function same(
  left: (typeof CompactionRegionTable.$inferSelect)[],
  right: (typeof CompactionRegionTable.$inferInsert)[],
) {
  if (left.length !== right.length) return false
  return left.every((row, index) => {
    const candidate = right[index]!
    return sameRow(row, candidate)
  })
}

function sameRow(
  row: typeof CompactionRegionTable.$inferSelect,
  candidate: typeof CompactionRegionTable.$inferInsert,
) {
  return (
    row.session_id === candidate.session_id &&
    row.marker_id === candidate.marker_id &&
    row.start_message_id === candidate.start_message_id &&
    row.summary_message_id === candidate.summary_message_id &&
    row.summary_preview === candidate.summary_preview &&
    row.physical_message_count === candidate.physical_message_count &&
    row.semantic_message_count === candidate.semantic_message_count &&
    row.part_count === candidate.part_count
  )
}
