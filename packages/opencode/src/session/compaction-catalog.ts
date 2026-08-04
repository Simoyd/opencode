import { Database } from "@opencode-ai/core/database/database"
import { CompactionRegionTable, MessageTable } from "@opencode-ai/core/session/sql"
import { SessionCompactionEvent } from "@opencode-ai/schema/session-compaction-event"
import { and, asc, desc, eq, gt, lt, or } from "drizzle-orm"
import { Effect, Schema } from "effect"
import { MessageV2 } from "./message-v2"
import { MessageID, SessionID } from "./schema"

export const Event = {
  Changed: SessionCompactionEvent.CatalogChanged,
}

const CatalogCursor = Schema.Struct({
  markerID: MessageID,
  timeCreated: Schema.Number.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0)),
})
const decodeCursor = Schema.decodeUnknownSync(CatalogCursor)
const PageSize = 50

export class InvalidCursorError extends Schema.TaggedErrorClass<InvalidCursorError>()(
  "CompactionCatalog.InvalidCursor",
  {},
) {}

const decodePageCursor = Effect.fn("CompactionCatalog.decodeCursor")((cursor: string) =>
  Effect.try({
    try: () => {
      if (!/^[A-Za-z0-9_-]+$/.test(cursor)) throw new Error("Invalid base64url cursor")
      const bytes = Buffer.from(cursor, "base64url")
      if (bytes.toString("base64url") !== cursor) throw new Error("Non-canonical base64url cursor")
      return decodeCursor(JSON.parse(bytes.toString("utf8")) as unknown)
    },
    catch: () => new InvalidCursorError(),
  }),
)

export const page = Effect.fn("CompactionCatalog.page")(function* (input: { sessionID: SessionID; cursor?: string }) {
  const { db } = yield* Database.Service
  const after = input.cursor ? yield* decodePageCursor(input.cursor) : undefined
  return yield* db
    .transaction(() =>
      Effect.gen(function* () {
        const rows = yield* db
          .select({ region: CompactionRegionTable, markerTime: MessageTable.time_created })
          .from(CompactionRegionTable)
          .innerJoin(
            MessageTable,
            and(
              eq(MessageTable.session_id, CompactionRegionTable.session_id),
              eq(MessageTable.id, CompactionRegionTable.marker_id),
            ),
          )
          .where(
            and(
              eq(CompactionRegionTable.session_id, input.sessionID),
              after
                ? or(
                    gt(MessageTable.time_created, after.timeCreated),
                    and(eq(MessageTable.time_created, after.timeCreated), gt(MessageTable.id, after.markerID)),
                  )
                : undefined,
            ),
          )
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
            .innerJoin(
              MessageTable,
              and(
                eq(MessageTable.session_id, CompactionRegionTable.session_id),
                eq(MessageTable.id, CompactionRegionTable.marker_id),
              ),
            )
            .where(
              and(
                eq(CompactionRegionTable.session_id, input.sessionID),
                or(
                  lt(MessageTable.time_created, selected[0].markerTime),
                  and(
                    eq(MessageTable.time_created, selected[0].markerTime),
                    lt(MessageTable.id, selected[0].region.marker_id),
                  ),
                ),
              ),
            )
            .orderBy(desc(MessageTable.time_created), desc(MessageTable.id))
            .limit(1)
            .get()
            .pipe(Effect.orDie))?.summaryMessageID
        }
        const items = [] as Array<{
          startMessageID: MessageID
          startTimeCreated: number
          markerID: MessageID
          markerTimeCreated: number
          endExclusiveCursor: string
          physicalMessageCount: number
          semanticMessageCount: number
          summaryMessageID: MessageID
          summaryPreview: string
          precedingSummaryMessageID?: MessageID
        }>
        for (const { region: row, markerTime } of selected) {
          const start = yield* db
            .select({ timeCreated: MessageTable.time_created })
            .from(MessageTable)
            .where(and(eq(MessageTable.session_id, input.sessionID), eq(MessageTable.id, row.start_message_id)))
            .get()
            .pipe(Effect.orDie)
          if (!start) return yield* Effect.die(`Compaction region ${row.marker_id} has no canonical start position`)
          items.push({
            startMessageID: row.start_message_id,
            startTimeCreated: start.timeCreated,
            markerID: row.marker_id,
            markerTimeCreated: markerTime,
            endExclusiveCursor: MessageV2.cursor.encode({ id: row.marker_id, time: markerTime }),
            physicalMessageCount: row.physical_message_count,
            semanticMessageCount: row.semantic_message_count,
            summaryMessageID: row.summary_message_id,
            summaryPreview: row.summary_preview,
            ...(precedingSummaryMessageID ? { precedingSummaryMessageID } : {}),
          })
          precedingSummaryMessageID = row.summary_message_id
        }
        const tail = selected.at(-1)
        return {
          items,
          ...(rows.length > PageSize && tail
            ? {
                nextCursor: Buffer.from(
                  JSON.stringify({ markerID: tail.region.marker_id, timeCreated: tail.markerTime }),
                ).toString("base64url"),
              }
            : {}),
        }
      }),
    )
    .pipe(Effect.orDie)
})

export * as CompactionCatalog from "./compaction-catalog"
