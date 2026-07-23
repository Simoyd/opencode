import { EventV2 } from "@opencode-ai/core/event"
import { Database } from "@opencode-ai/core/database/database"
import { CompactionRegionTable, MessageTable } from "@opencode-ai/core/session/sql"
import { and, asc, desc, eq, gt, lt, or } from "drizzle-orm"
import { Effect, Schema } from "effect"
import { MessageV2 } from "./message-v2"
import { MessageID, SessionID } from "./schema"

export const Event = {
  Changed: EventV2.define({
    type: "compaction.catalog.changed",
    schema: { sessionID: SessionID },
  }),
}

const CatalogCursor = Schema.Struct({ markerID: MessageID, timeCreated: Schema.Number })
const decodeCursor = Schema.decodeUnknownSync(CatalogCursor)
const PageSize = 50

export const page = Effect.fn("CompactionCatalog.page")(function* (input: { sessionID: SessionID; cursor?: string }) {
  const { db } = yield* Database.Service
  const after = input.cursor
    ? decodeCursor(JSON.parse(Buffer.from(input.cursor, "base64url").toString("utf8")))
    : undefined
  return yield* db.transaction(() => Effect.gen(function* () {
  const rows = yield* db
    .select({ region: CompactionRegionTable, markerTime: MessageTable.time_created })
    .from(CompactionRegionTable)
    .innerJoin(MessageTable, and(eq(MessageTable.session_id, CompactionRegionTable.session_id), eq(MessageTable.id, CompactionRegionTable.marker_id)))
    .where(and(
      eq(CompactionRegionTable.session_id, input.sessionID),
      after ? or(gt(MessageTable.time_created, after.timeCreated), and(eq(MessageTable.time_created, after.timeCreated), gt(MessageTable.id, after.markerID))) : undefined,
    ))
    .orderBy(asc(MessageTable.time_created), asc(MessageTable.id))
    .limit(PageSize + 1)
    .all()
    .pipe(Effect.orDie)
  const selected = rows.slice(0, PageSize)
  let precedingSummaryMessageID: MessageID | undefined
  if (selected[0]) {
    precedingSummaryMessageID = (yield* db
      .select({ summaryMessageID: CompactionRegionTable.summary_message_id })
      .from(CompactionRegionTable)
      .innerJoin(MessageTable, and(eq(MessageTable.session_id, CompactionRegionTable.session_id), eq(MessageTable.id, CompactionRegionTable.marker_id)))
      .where(and(
        eq(CompactionRegionTable.session_id, input.sessionID),
        or(lt(MessageTable.time_created, selected[0].markerTime), and(eq(MessageTable.time_created, selected[0].markerTime), lt(MessageTable.id, selected[0].region.marker_id))),
      ))
      .orderBy(desc(MessageTable.time_created), desc(MessageTable.id))
      .limit(1)
      .get()
      .pipe(Effect.orDie))?.summaryMessageID
  }
  const items = selected.map(({ region: row, markerTime }) => {
    const item = {
      startMessageID: row.start_message_id,
      markerID: row.marker_id,
      endExclusiveCursor: MessageV2.cursor.encode({ id: row.marker_id, time: markerTime }),
      physicalMessageCount: row.physical_message_count,
      semanticMessageCount: row.semantic_message_count,
      summaryMessageID: row.summary_message_id,
      summaryPreview: row.summary_preview,
      ...(precedingSummaryMessageID ? { precedingSummaryMessageID } : {}),
    }
    precedingSummaryMessageID = row.summary_message_id
    return item
  })
  const tail = selected.at(-1)
  return {
    items,
    ...(rows.length > PageSize && tail
      ? { nextCursor: Buffer.from(JSON.stringify({ markerID: tail.region.marker_id, timeCreated: tail.markerTime })).toString("base64url") }
      : {}),
  }
  })).pipe(Effect.orDie)
})

export * as CompactionCatalog from "./compaction-catalog"
