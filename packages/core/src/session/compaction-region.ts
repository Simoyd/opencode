export * as CompactionRegionProjection from "./compaction-region"

import { Effect } from "effect"
import { and, asc, desc, eq, gt, inArray, lt, lte, or, sql } from "drizzle-orm"
import type { Database } from "../database/database"
import { SessionV1, type MessageID } from "../v1/session"
import type { SessionSchema } from "./schema"
import { CompactionRegionTable, MessageTable, PartTable, SessionTable } from "./sql"

type DatabaseService = Omit<Database.Interface["db"], "$client">
type Position = { id: MessageID; time_created: number }
type ReconcileInput = {
  sessionID: SessionSchema.ID
  messageID?: MessageID
  removed?: Position & { marker: boolean }
}
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

export const reconcile = Effect.fn("CompactionRegionProjection.reconcile")(function* (
  db: DatabaseService,
  input: ReconcileInput,
) {
  if (!input.messageID && !input.removed) return yield* reconcileAll(db, input.sessionID)

  const currentPosition = input.messageID
    ? yield* db
        .select({ id: MessageTable.id, time_created: MessageTable.time_created })
        .from(MessageTable)
        .where(and(eq(MessageTable.session_id, input.sessionID), eq(MessageTable.id, input.messageID)))
        .get()
        .pipe(Effect.orDie)
    : undefined
  const anchor = currentPosition ?? input.removed
  if (!anchor) return false

  const markers = new Map<MessageID, Position>()
  const before = yield* markerAtOrBefore(db, input.sessionID, anchor)
  const predecessor = currentPosition ? yield* markerBefore(db, input.sessionID, anchor) : undefined
  const after = yield* markerAfter(db, input.sessionID, anchor)
  const completedAfter = yield* completedMarkerAfter(db, input.sessionID, anchor)
  if (before) markers.set(before.id, before)
  if (predecessor) markers.set(predecessor.id, predecessor)
  if (after) {
    markers.set(after.id, after)
    const neighbor = yield* markerAfter(db, input.sessionID, after)
    if (neighbor) markers.set(neighbor.id, neighbor)
  }
  if (completedAfter) markers.set(completedAfter.id, completedAfter)

  let didChange = false
  if (input.removed?.marker) didChange = (yield* deleteRow(db, input.sessionID, input.removed.id)) || didChange
  for (const marker of [...markers.values()].sort(comparePosition)) {
    const next = yield* deriveMarker(db, input.sessionID, marker.id)
    didChange = (yield* replaceRow(db, input.sessionID, marker.id, next)) || didChange
  }
  return didChange
})

export const backfill = Effect.fn("CompactionRegionProjection.backfill")(function* (db: DatabaseService) {
  const sessions = yield* db.select({ id: SessionTable.id }).from(SessionTable).all().pipe(Effect.orDie)
  for (const session of sessions) yield* reconcileAll(db, session.id)
})

function reconcileAll(db: DatabaseService, sessionID: SessionSchema.ID) {
  return Effect.gen(function* () {
    const messages = yield* loadMessages(db, sessionID)
    const next = yield* derive(sessionID, messages)
    const current = yield* db
      .select()
      .from(CompactionRegionTable)
      .where(eq(CompactionRegionTable.session_id, sessionID))
      .all()
      .pipe(Effect.orDie)
    if (same(current, next)) return false
    const nextByMarker = new Map(next.map((row) => [row.marker_id, row]))
    for (const row of current) {
      if (nextByMarker.has(row.marker_id)) continue
      yield* deleteRow(db, sessionID, row.marker_id)
    }
    for (const row of next) yield* replaceRow(db, sessionID, row.marker_id, row)
    return true
  })
}

function markerAtOrBefore(db: DatabaseService, sessionID: SessionSchema.ID, position: Position) {
  return db
    .select({ id: MessageTable.id, time_created: MessageTable.time_created })
    .from(MessageTable)
    .innerJoin(
      PartTable,
      and(
        eq(PartTable.message_id, MessageTable.id),
        eq(PartTable.session_id, MessageTable.session_id),
        sql`json_extract(${PartTable.data}, '$.type') = 'compaction'`,
      ),
    )
    .where(
      and(
        eq(MessageTable.session_id, sessionID),
        or(
          lt(MessageTable.time_created, position.time_created),
          and(eq(MessageTable.time_created, position.time_created), lte(MessageTable.id, position.id)),
        ),
      ),
    )
    .groupBy(MessageTable.id, MessageTable.time_created)
    .orderBy(desc(MessageTable.time_created), desc(MessageTable.id))
    .limit(1)
    .get()
    .pipe(Effect.orDie)
}

function markerAfter(db: DatabaseService, sessionID: SessionSchema.ID, position: Position) {
  return db
    .select({ id: MessageTable.id, time_created: MessageTable.time_created })
    .from(MessageTable)
    .innerJoin(
      PartTable,
      and(
        eq(PartTable.message_id, MessageTable.id),
        eq(PartTable.session_id, MessageTable.session_id),
        sql`json_extract(${PartTable.data}, '$.type') = 'compaction'`,
      ),
    )
    .where(
      and(
        eq(MessageTable.session_id, sessionID),
        or(
          gt(MessageTable.time_created, position.time_created),
          and(eq(MessageTable.time_created, position.time_created), gt(MessageTable.id, position.id)),
        ),
      ),
    )
    .groupBy(MessageTable.id, MessageTable.time_created)
    .orderBy(asc(MessageTable.time_created), asc(MessageTable.id))
    .limit(1)
    .get()
    .pipe(Effect.orDie)
}

function markerBefore(db: DatabaseService, sessionID: SessionSchema.ID, position: Position) {
  return db
    .select({ id: MessageTable.id, time_created: MessageTable.time_created })
    .from(MessageTable)
    .innerJoin(
      PartTable,
      and(
        eq(PartTable.message_id, MessageTable.id),
        eq(PartTable.session_id, MessageTable.session_id),
        sql`json_extract(${PartTable.data}, '$.type') = 'compaction'`,
      ),
    )
    .where(
      and(
        eq(MessageTable.session_id, sessionID),
        or(
          lt(MessageTable.time_created, position.time_created),
          and(eq(MessageTable.time_created, position.time_created), lt(MessageTable.id, position.id)),
        ),
      ),
    )
    .groupBy(MessageTable.id, MessageTable.time_created)
    .orderBy(desc(MessageTable.time_created), desc(MessageTable.id))
    .limit(1)
    .get()
    .pipe(Effect.orDie)
}

function comparePosition(left: Position, right: Position) {
  const time = left.time_created - right.time_created
  return time !== 0 ? time : Buffer.compare(Buffer.from(left.id), Buffer.from(right.id))
}

function completedMarkerBefore(db: DatabaseService, sessionID: SessionSchema.ID, position: Position) {
  return db
    .select({ id: MessageTable.id, time_created: MessageTable.time_created })
    .from(CompactionRegionTable)
    .innerJoin(
      MessageTable,
      and(
        eq(MessageTable.id, CompactionRegionTable.marker_id),
        eq(MessageTable.session_id, CompactionRegionTable.session_id),
      ),
    )
    .where(
      and(
        eq(CompactionRegionTable.session_id, sessionID),
        or(
          lt(MessageTable.time_created, position.time_created),
          and(eq(MessageTable.time_created, position.time_created), lt(MessageTable.id, position.id)),
        ),
      ),
    )
    .groupBy(MessageTable.id, MessageTable.time_created)
    .orderBy(desc(MessageTable.time_created), desc(MessageTable.id))
    .limit(1)
    .get()
    .pipe(Effect.orDie)
}

function completedMarkerAfter(db: DatabaseService, sessionID: SessionSchema.ID, position: Position) {
  return db
    .select({ id: MessageTable.id, time_created: MessageTable.time_created })
    .from(CompactionRegionTable)
    .innerJoin(
      MessageTable,
      and(
        eq(MessageTable.id, CompactionRegionTable.marker_id),
        eq(MessageTable.session_id, CompactionRegionTable.session_id),
      ),
    )
    .where(
      and(
        eq(CompactionRegionTable.session_id, sessionID),
        or(
          gt(MessageTable.time_created, position.time_created),
          and(eq(MessageTable.time_created, position.time_created), gt(MessageTable.id, position.id)),
        ),
      ),
    )
    .orderBy(asc(MessageTable.time_created), asc(MessageTable.id))
    .limit(1)
    .get()
    .pipe(Effect.orDie)
}

function deriveMarker(db: DatabaseService, sessionID: SessionSchema.ID, markerID: MessageID) {
  return Effect.gen(function* () {
    const marker = yield* db
      .select({ id: MessageTable.id, time_created: MessageTable.time_created })
      .from(MessageTable)
      .innerJoin(
        PartTable,
        and(
          eq(PartTable.message_id, MessageTable.id),
          eq(PartTable.session_id, MessageTable.session_id),
          sql`json_extract(${PartTable.data}, '$.type') = 'compaction'`,
        ),
      )
      .where(and(eq(MessageTable.session_id, sessionID), eq(MessageTable.id, markerID)))
      .groupBy(MessageTable.id, MessageTable.time_created)
      .get()
      .pipe(Effect.orDie)
    if (!marker) return undefined

    const previous = yield* completedMarkerBefore(db, sessionID, marker)
    const next = yield* completedMarkerAfter(db, sessionID, marker)
    const rows = yield* db
      .select()
      .from(MessageTable)
      .where(
        and(
          eq(MessageTable.session_id, sessionID),
          previous
            ? or(
                gt(MessageTable.time_created, previous.time_created),
                and(eq(MessageTable.time_created, previous.time_created), gt(MessageTable.id, previous.id)),
              )
            : undefined,
          next
            ? or(
                lt(MessageTable.time_created, next.time_created),
                and(eq(MessageTable.time_created, next.time_created), lt(MessageTable.id, next.id)),
              )
            : undefined,
        ),
      )
      .orderBy(asc(MessageTable.time_created), asc(MessageTable.id))
      .all()
      .pipe(Effect.orDie)
    const messages = yield* hydrateMessages(db, rows)
    const markerIndex = messages.findIndex((message) => message.info.id === markerID)
    if (markerIndex < 1) return undefined
    const sources = replaySources(messages.slice(0, markerIndex))
    const existingSources =
      sources.length === 0
        ? new Set<MessageID>()
        : new Set(
            (yield* db
              .select({ id: MessageTable.id })
              .from(MessageTable)
              .where(and(eq(MessageTable.session_id, sessionID), inArray(MessageTable.id, sources)))
              .all()
              .pipe(Effect.orDie)).map((row) => row.id),
          )
    return yield* deriveCompletedRegion(sessionID, messages, markerIndex, previous?.id, existingSources)
  })
}

function loadMessages(db: DatabaseService, sessionID: SessionSchema.ID) {
  return Effect.gen(function* () {
    const rows = yield* db
      .select()
      .from(MessageTable)
      .where(eq(MessageTable.session_id, sessionID))
      .orderBy(asc(MessageTable.time_created), asc(MessageTable.id))
      .all()
      .pipe(Effect.orDie)
    return yield* hydrateMessages(db, rows)
  })
}

function hydrateMessages(db: DatabaseService, messageRows: (typeof MessageTable.$inferSelect)[]) {
  return Effect.gen(function* () {
    if (messageRows.length === 0) return []
    const messageIDs = new Set(messageRows.map((row) => row.id))
    const sessionID = messageRows[0]!.session_id
    if (messageRows.some((row) => row.session_id !== sessionID)) {
      return yield* Effect.die("Compaction hydration crossed persisted session owners")
    }
    const partRows = yield* db
      .select()
      .from(PartTable)
      .where(eq(PartTable.session_id, sessionID))
      .orderBy(PartTable.message_id, PartTable.id)
      .all()
      .pipe(Effect.orDie)
    const parts = new Map<MessageID, SessionV1.Part[]>()
    for (const row of partRows) {
      if (!messageIDs.has(row.message_id)) continue
      const part = { ...row.data, id: row.id, sessionID: row.session_id, messageID: row.message_id } as SessionV1.Part
      const list = parts.get(row.message_id)
      if (list) list.push(part)
      else parts.set(row.message_id, [part])
    }
    return messageRows.map(
      (row) =>
        ({
          info: { ...row.data, id: row.id, sessionID: row.session_id } as SessionV1.Info,
          parts: parts.get(row.id) ?? [],
        }) satisfies SessionV1.WithParts,
    )
  })
}

function derive(sessionID: SessionSchema.ID, messages: SessionV1.WithParts[]) {
  return Effect.gen(function* () {
    const rows: (typeof CompactionRegionTable.$inferInsert)[] = []
    const existingMessageIDs = new Set(messages.map((message) => message.info.id))
    let start = 0
    let previousMarkerID: MessageID | undefined
    for (let markerIndex = 0; markerIndex < messages.length; markerIndex++) {
      const marker = messages[markerIndex]!
      if (!isMarker(marker)) continue
      const row = yield* deriveCompletedRegion(
        sessionID,
        messages.slice(start),
        markerIndex - start,
        previousMarkerID,
        existingMessageIDs,
      )
      if (row) {
        rows.push(row)
        start = markerIndex + 1
        previousMarkerID = marker.info.id
      }
    }
    return rows
  })
}

function deriveCompletedRegion(
  sessionID: SessionSchema.ID,
  messages: SessionV1.WithParts[],
  markerIndex: number,
  previousMarkerID: MessageID | undefined,
  existingMessageIDs: ReadonlySet<MessageID>,
) {
  return Effect.sync(() => {
    const marker = messages[markerIndex]
    if (!marker || !isMarker(marker) || markerIndex < 1) return undefined
    const body = messages.slice(0, markerIndex)
    const afterMarker = messages.slice(markerIndex + 1)
    const nextMarkerIndex = afterMarker.findIndex(isMarker)
    const completedTail = nextMarkerIndex < 0 ? afterMarker : afterMarker.slice(0, nextMarkerIndex)
    const declaredSummaries = completedTail.filter(
      (message): message is SessionV1.WithParts & { info: SessionV1.Assistant } =>
        message.info.role === "assistant" && message.info.summary === true && message.info.parentID === marker.info.id,
    )
    if (declaredSummaries.length > 1) {
      throw new Error(`Compaction region ${marker.info.id} has contradictory terminal summaries`)
    }
    const summary = declaredSummaries[0]
    if (!summary) return undefined
    if (!summary.info.finish || summary.info.error || summaryText(summary).length === 0) return undefined

    const normalized = summaryText(summary).split(/\s+/).filter(Boolean).join(" ")
    return {
      session_id: sessionID,
      marker_id: marker.info.id,
      start_message_id: body[0]!.info.id,
      summary_message_id: summary.info.id,
      summary_preview: normalized.length <= 80 ? normalized : `${normalized.slice(0, 77)}...`,
      physical_message_count: body.length,
      semantic_message_count: semanticCount(body, previousMarkerID, existingMessageIDs),
    } satisfies typeof CompactionRegionTable.$inferInsert
  })
}

function semanticCount(
  messages: SessionV1.WithParts[],
  ownerMarkerID: MessageID | undefined,
  existingMessageIDs: ReadonlySet<MessageID>,
) {
  const canonicalReplaySources = new Set<MessageID>()
  const replaySourcesByMessageID = new Map<MessageID, MessageID>()
  for (const message of messages) {
    const value = protocol(message)
    if (!value.replay) continue
    replaySourcesByMessageID.set(message.info.id, value.replay.source)
    if (existingMessageIDs.has(value.replay.source)) canonicalReplaySources.add(value.replay.source)
  }
  const identities = new Set<string>()
  for (const message of messages) {
    if (isMarker(message) || (message.info.role === "assistant" && message.info.summary)) continue
    const value = protocol(message)
    if (value.replay) {
      if (!ownerMarkerID || value.replay.owner !== ownerMarkerID) {
        throw new Error(`Compaction replay ${message.info.id} has contradictory marker ownership`)
      }
      if (canonicalReplaySources.has(value.replay.source)) identities.add(`message:${value.replay.source}`)
      continue
    }
    if (value.continuation) {
      if (!ownerMarkerID || value.continuation.owner !== ownerMarkerID) {
        throw new Error(`Compaction continuation ${message.info.id} has contradictory marker ownership`)
      }
      continue
    }
    if (
      message.info.role === "assistant" &&
      replaySourcesByMessageID.has(message.info.parentID) &&
      !canonicalReplaySources.has(replaySourcesByMessageID.get(message.info.parentID)!)
    )
      continue
    if (!canonicalReplaySources.has(message.info.id)) identities.add(`message:${message.info.id}`)
  }
  return identities.size
}

function replaySources(messages: SessionV1.WithParts[]) {
  return [
    ...new Set(
      messages
        .map((message) => protocol(message).replay?.source)
        .filter((source): source is MessageID => source !== undefined),
    ),
  ]
}

function protocol(message: SessionV1.WithParts) {
  let replay: { owner: MessageID; source: MessageID } | undefined
  let continuation: { owner: MessageID } | undefined
  for (const part of message.parts) {
    if (part.type !== "text") continue
    const provenance = part.serverProvenance
    if (provenance?.type === "compaction-replay") {
      if (
        (replay && (replay.owner !== provenance.ownerMessageID || replay.source !== provenance.sourceMessageID)) ||
        continuation
      ) {
        throw new Error(`Compaction replay ${message.info.id} has contradictory part provenance`)
      }
      replay = { owner: provenance.ownerMessageID, source: provenance.sourceMessageID }
    }
    if (provenance?.type === "compaction-continuation") {
      if (part.synthetic !== true || (continuation && continuation.owner !== provenance.ownerMessageID) || replay) {
        throw new Error(`Compaction continuation ${message.info.id} has contradictory part provenance`)
      }
      continuation = { owner: provenance.ownerMessageID }
    }
  }
  return { replay, continuation }
}

function isMarker(message: SessionV1.WithParts) {
  return message.info.role === "user" && message.parts.some((part) => part.type === "compaction")
}

function summaryText(message: SessionV1.WithParts) {
  return message.parts
    .filter((part): part is SessionV1.TextPart => part.type === "text")
    .map((part) => part.text.trim())
    .filter(Boolean)
    .join("\n")
    .trim()
}

function deleteRow(db: DatabaseService, sessionID: SessionSchema.ID, markerID: MessageID) {
  return Effect.gen(function* () {
    const existing = yield* db
      .select({ marker_id: CompactionRegionTable.marker_id })
      .from(CompactionRegionTable)
      .where(and(eq(CompactionRegionTable.session_id, sessionID), eq(CompactionRegionTable.marker_id, markerID)))
      .get()
      .pipe(Effect.orDie)
    if (!existing) return false
    yield* db
      .delete(CompactionRegionTable)
      .where(and(eq(CompactionRegionTable.session_id, sessionID), eq(CompactionRegionTable.marker_id, markerID)))
      .run()
      .pipe(Effect.orDie)
    return true
  })
}

function replaceRow(
  db: DatabaseService,
  sessionID: SessionSchema.ID,
  markerID: MessageID,
  next: typeof CompactionRegionTable.$inferInsert | undefined,
) {
  return Effect.gen(function* () {
    const existing = yield* db
      .select()
      .from(CompactionRegionTable)
      .where(and(eq(CompactionRegionTable.session_id, sessionID), eq(CompactionRegionTable.marker_id, markerID)))
      .get()
      .pipe(Effect.orDie)
    if (!next) return existing ? yield* deleteRow(db, sessionID, markerID) : false
    if (existing && sameRow(existing, next)) return false
    yield* db
      .insert(CompactionRegionTable)
      .values(next)
      .onConflictDoUpdate({
        target: [CompactionRegionTable.session_id, CompactionRegionTable.marker_id],
        set: {
          start_message_id: next.start_message_id,
          summary_message_id: next.summary_message_id,
          summary_preview: next.summary_preview,
          physical_message_count: next.physical_message_count,
          semantic_message_count: next.semantic_message_count,
        },
      })
      .run()
      .pipe(Effect.orDie)
    return true
  })
}

function same(
  left: (typeof CompactionRegionTable.$inferSelect)[],
  right: (typeof CompactionRegionTable.$inferInsert)[],
) {
  if (left.length !== right.length) return false
  const rightByMarker = new Map(right.map((row) => [row.marker_id, row]))
  return left.every((row) => {
    const candidate = rightByMarker.get(row.marker_id)
    return candidate !== undefined && sameRow(row, candidate)
  })
}

function sameRow(row: typeof CompactionRegionTable.$inferSelect, candidate: typeof CompactionRegionTable.$inferInsert) {
  return (
    row.session_id === candidate.session_id &&
    row.marker_id === candidate.marker_id &&
    row.start_message_id === candidate.start_message_id &&
    row.summary_message_id === candidate.summary_message_id &&
    row.summary_preview === candidate.summary_preview &&
    row.physical_message_count === candidate.physical_message_count &&
    row.semantic_message_count === candidate.semantic_message_count
  )
}
