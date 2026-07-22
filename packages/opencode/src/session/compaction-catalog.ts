import { EventV2 } from "@opencode-ai/core/event"
import { Database } from "@opencode-ai/core/database/database"
import { CompactionRegionTable, MessageTable } from "@opencode-ai/core/session/sql"
import { and, asc, eq, gt, inArray, lt } from "drizzle-orm"
import { Effect, Schema } from "effect"
import { MessageV2 } from "./message-v2"
import { MessageID, SessionID } from "./schema"

export const Event = {
  Changed: EventV2.define({
    type: "compaction.catalog.changed",
    schema: { sessionID: SessionID },
  }),
}

const CatalogCursor = Schema.Struct({ markerID: MessageID })
const decodeCursor = Schema.decodeUnknownSync(CatalogCursor)
const PageSize = 50

export const page = Effect.fn("CompactionCatalog.page")(function* (input: {
  sessionID: SessionID
  cursor?: string
}) {
  const { db } = yield* Database.Service
  const after = input.cursor
    ? decodeCursor(JSON.parse(Buffer.from(input.cursor, "base64url").toString("utf8"))).markerID
    : undefined
  const rows = yield* db
    .select()
    .from(CompactionRegionTable)
    .where(
      after
        ? and(eq(CompactionRegionTable.session_id, input.sessionID), gt(CompactionRegionTable.marker_id, after))
        : eq(CompactionRegionTable.session_id, input.sessionID),
    )
    .orderBy(asc(CompactionRegionTable.marker_id))
    .limit(PageSize + 1)
    .all()
    .pipe(Effect.orDie)
  const selected = rows.slice(0, PageSize)
  const ids = selected.flatMap((row) => [row.marker_id, row.summary_message_id])
  const orders =
    ids.length === 0
      ? []
      : yield* db
          .select({ id: MessageTable.id, time: MessageTable.time_created })
          .from(MessageTable)
          .where(and(eq(MessageTable.session_id, input.sessionID), inArray(MessageTable.id, ids)))
          .all()
          .pipe(Effect.orDie)
  const order = new Map(orders.map((row) => [row.id, row.time]))
  let precedingSummaryMessageID: MessageID | undefined
  if (selected[0]) {
    const previous = yield* db
      .select({ summaryMessageID: CompactionRegionTable.summary_message_id })
      .from(CompactionRegionTable)
      .where(
        and(
          eq(CompactionRegionTable.session_id, input.sessionID),
          // marker ids are ascending identifiers in the selected V1 store
          lt(CompactionRegionTable.marker_id, selected[0].marker_id),
        ),
      )
      .orderBy(CompactionRegionTable.marker_id)
      .all()
      .pipe(Effect.orDie)
    precedingSummaryMessageID = previous.at(-1)?.summaryMessageID
  }
  const items = selected.map((row) => {
    const markerTime = order.get(row.marker_id)
    if (markerTime === undefined) throw new Error("Compaction catalog marker is missing from ordinary messages")
    const item = {
      startMessageID: row.start_message_id,
      markerID: row.marker_id,
      endExclusiveCursor: MessageV2.cursor.encode({ id: row.marker_id, time: markerTime }),
      physicalMessageCount: row.physical_message_count,
      semanticMessageCount: row.semantic_message_count,
      partCount: row.part_count,
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
      ? { nextCursor: Buffer.from(JSON.stringify({ markerID: tail.marker_id })).toString("base64url") }
      : {}),
  }
})

export * as CompactionCatalog from "./compaction-catalog"
