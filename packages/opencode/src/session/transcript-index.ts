export * as SessionTranscriptIndex from "./transcript-index"

import { Database } from "@opencode-ai/core/database/database"
import { EventSequenceTable } from "@opencode-ai/core/event/sql"
import { SessionMaintenance } from "@opencode-ai/core/session/maintenance"
import {
  CompactionArchiveManifestTable,
  CompactionArchiveStagingTable,
  MessageTable,
  PartTable,
  SessionMaintenanceTable,
  TranscriptWindowStateTable,
} from "@opencode-ai/core/session/sql"
import { TranscriptWindowProjection } from "@opencode-ai/core/session/transcript-window"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { and, asc, eq, gt, inArray, or, sql } from "drizzle-orm"
import { Cause, Effect, Exit } from "effect"
import { MessageID, SessionID } from "./schema"

const BatchMessages = 256
const BatchMetadataBytes = 8 * 1024 * 1024
const MaxSingleRowBytes = 32 * 1024 * 1024
const MaxTransientBytes = 64 * 1024 * 1024

type Counts = { messages: number; parts: number; textUnits: number; decodedBytes: number }
type State = {
  ordinal: number
  latestTailStartID?: MessageID
  previous?: { summaryMessageID: MessageID; replayMessageIDs: MessageID[] }
  current?: {
    sourceMessageID: MessageID
    counts: Counts
    markerID?: MessageID
    tailStartID?: MessageID
    overflow?: boolean
    summaryMessageID?: MessageID
    summaryPreview?: string
    replayMessageIDs: MessageID[]
    continuationCount: number
    published?: boolean
  }
}

const runIndex = Effect.fn("SessionTranscriptIndex.runIndex")(function* (input: {
  sessionID: SessionID
  ownerID: string
  maxBatches?: number
}) {
  const { db } = yield* Database.Service
  const lease = yield* SessionMaintenance.acquireTranscriptIndex(db, input)
  const stateRow = yield* db
    .select()
    .from(TranscriptWindowStateTable)
    .where(eq(TranscriptWindowStateTable.session_id, input.sessionID))
    .get()
    .pipe(Effect.orDie)
  if (!stateRow) return yield* Effect.die("Transcript index state was not created")
  const state = decodeState(stateRow.index_state)
  let batches = 0

  while (true) {
    const page = yield* loadBatch(db, {
      sessionID: input.sessionID,
      cursorTime: stateRow.index_cursor_time ?? undefined,
      cursorID: stateRow.index_cursor_id ?? undefined,
    })
    if (page.messages.length === 0) break
    for (const message of page.messages) {
      if (isPrompt(message)) {
        state.current = {
          sourceMessageID: message.info.id,
          counts: emptyCounts(),
          replayMessageIDs: [],
          continuationCount: 0,
        }
      }
      const current = state.current
      if (!current) continue
      const marker = message.parts.find((part): part is SessionV1.CompactionPart => part.type === "compaction")
      if (marker && message.info.role === "user") {
        current.markerID = message.info.id
        current.tailStartID = marker.tail_start_id
        current.overflow = marker.overflow
      } else if (!current.markerID) {
        add(current.counts, message)
      }
      if (
        current.markerID &&
        message.info.role === "assistant" &&
        message.info.parentID === current.markerID &&
        message.info.summary &&
        message.info.finish &&
        !message.info.error
      ) {
        const text = messageText(message)
        if (text) {
          current.summaryMessageID = message.info.id
          const normalized = text.split(/\s+/).filter(Boolean).join(" ")
          current.summaryPreview =
            normalized.length <= TranscriptWindowProjection.Limits.summaryCodeUnits
              ? normalized
              : `${normalized.slice(0, TranscriptWindowProjection.Limits.summaryCodeUnits - 3)}...`
        }
      }
      if (
        message.info.role === "user" &&
        message.parts.some((part) => part.type === "text" && part.metadata?.compaction_replay === true)
      )
        current.replayMessageIDs.push(message.info.id)
      if (
        message.info.role === "user" &&
        message.parts.some(
          (part) => part.type === "text" && part.synthetic && part.metadata?.compaction_continue === true,
        )
      )
        current.continuationCount++

      if (
        !current.published &&
        current.markerID &&
        current.summaryMessageID &&
        current.summaryPreview &&
        (current.continuationCount > 0 || current.replayMessageIDs.length > 0) &&
        (!current.overflow || current.replayMessageIDs.length > 0) &&
        isFinalAssistant(message)
      ) {
        const continuity = state.previous ? [state.previous.summaryMessageID, ...state.previous.replayMessageIDs] : []
        const continuityCounts = yield* countMessages(db, input.sessionID, continuity)
        const counts = finalizeCounts(sum(current.counts, continuityCounts))
        if (
          counts.messages > TranscriptWindowProjection.Limits.messages ||
          counts.parts > TranscriptWindowProjection.Limits.parts ||
          counts.textUnits > TranscriptWindowProjection.Limits.textCodeUnits ||
          counts.decodedBytes > TranscriptWindowProjection.Limits.decodedBytes ||
          state.ordinal >= TranscriptWindowProjection.Limits.descriptors
        )
          return yield* Effect.die("Transcript archive exceeds the safe index limit")
        yield* db
          .insert(CompactionArchiveStagingTable)
          .values({
            session_id: input.sessionID,
            owner_id: input.ownerID,
            archive_id: current.markerID,
            archive_revision: stateRow.window_revision,
            ordinal: state.ordinal++,
            marker_id: current.markerID,
            tail_start_id: current.tailStartID,
            source_message_id: current.sourceMessageID,
            summary_message_id: current.summaryMessageID,
            range_start_id: current.sourceMessageID,
            range_end_id: current.markerID,
            summary_preview: current.summaryPreview,
            continuity_message_ids: continuity,
            replay_message_ids: current.replayMessageIDs,
            message_count: counts.messages,
            part_count: counts.parts,
            text_units: counts.textUnits,
            decoded_bytes: counts.decodedBytes,
          })
          .onConflictDoUpdate({
            target: [
              CompactionArchiveStagingTable.session_id,
              CompactionArchiveStagingTable.owner_id,
              CompactionArchiveStagingTable.archive_id,
            ],
            set: {
              archive_revision: stateRow.window_revision,
              summary_preview: current.summaryPreview,
              message_count: counts.messages,
              part_count: counts.parts,
              text_units: counts.textUnits,
              decoded_bytes: counts.decodedBytes,
            },
          })
          .run()
          .pipe(Effect.orDie)
        current.published = true
        state.latestTailStartID = current.tailStartID
        state.previous = {
          summaryMessageID: current.summaryMessageID,
          replayMessageIDs: current.replayMessageIDs,
        }
      }
    }

    const last = page.identities.at(-1)!
    yield* db.transaction(
      () =>
        Effect.gen(function* () {
          yield* SessionMaintenance.verifyOwner(db, input)
          yield* db
            .update(TranscriptWindowStateTable)
            .set({
              index_cursor_time: last.time,
              index_cursor_id: last.id,
              index_state: state,
            })
            .where(
              and(
                eq(TranscriptWindowStateTable.session_id, input.sessionID),
                eq(TranscriptWindowStateTable.index_owner_id, input.ownerID),
              ),
            )
            .run()
            .pipe(Effect.orDie)
          yield* db
            .update(SessionMaintenanceTable)
            .set({ time_updated: Date.now() })
            .where(
              and(
                eq(SessionMaintenanceTable.session_id, input.sessionID),
                eq(SessionMaintenanceTable.owner_id, input.ownerID),
              ),
            )
            .run()
            .pipe(Effect.orDie)
        }),
      { behavior: "immediate" },
    )
    stateRow.index_cursor_time = last.time
    stateRow.index_cursor_id = last.id
    batches++
    if (input.maxBatches !== undefined && batches >= input.maxBatches)
      return {
        ownerID: input.ownerID,
        resumed: lease.resumed,
        descriptors: state.ordinal,
        complete: false as const,
      }
  }

  yield* db.transaction(
    () =>
      Effect.gen(function* () {
        yield* SessionMaintenance.verifyOwner(db, input)
        const sequence = yield* db
          .select({ seq: EventSequenceTable.seq })
          .from(EventSequenceTable)
          .where(eq(EventSequenceTable.aggregate_id, input.sessionID))
          .get()
          .pipe(Effect.orDie)
        if ((sequence?.seq ?? -1) !== stateRow.window_revision)
          return yield* Effect.die("Transcript source changed during indexing")
        const staging = yield* db
          .select()
          .from(CompactionArchiveStagingTable)
          .where(
            and(
              eq(CompactionArchiveStagingTable.session_id, input.sessionID),
              eq(CompactionArchiveStagingTable.owner_id, input.ownerID),
            ),
          )
          .orderBy(CompactionArchiveStagingTable.ordinal)
          .limit(TranscriptWindowProjection.Limits.descriptors + 1)
          .all()
          .pipe(Effect.orDie)
        if (staging.length > TranscriptWindowProjection.Limits.descriptors)
          return yield* Effect.die("Transcript descriptor index exceeds the safe limit")
        yield* db
          .delete(CompactionArchiveManifestTable)
          .where(eq(CompactionArchiveManifestTable.session_id, input.sessionID))
          .run()
          .pipe(Effect.orDie)
        for (const row of staging) {
          const { owner_id: _, ...manifest } = row
          yield* db.insert(CompactionArchiveManifestTable).values(manifest).run().pipe(Effect.orDie)
        }
        yield* db
          .update(TranscriptWindowStateTable)
          .set({
            index_status: "complete",
            tail_start_id: state.latestTailStartID,
            index_owner_id: null,
            index_cursor_time: null,
            index_cursor_id: null,
            index_state: null,
          })
          .where(eq(TranscriptWindowStateTable.session_id, input.sessionID))
          .run()
          .pipe(Effect.orDie)
        yield* TranscriptWindowProjection.refresh(db, {
          sessionID: input.sessionID,
          revision: stateRow.window_revision,
        })
        yield* db
          .delete(CompactionArchiveStagingTable)
          .where(
            and(
              eq(CompactionArchiveStagingTable.session_id, input.sessionID),
              eq(CompactionArchiveStagingTable.owner_id, input.ownerID),
            ),
          )
          .run()
          .pipe(Effect.orDie)
        yield* db
          .delete(SessionMaintenanceTable)
          .where(
            and(
              eq(SessionMaintenanceTable.session_id, input.sessionID),
              eq(SessionMaintenanceTable.owner_id, input.ownerID),
            ),
          )
          .run()
          .pipe(Effect.orDie)
      }),
    { behavior: "immediate" },
  )
  return { ownerID: input.ownerID, resumed: lease.resumed, descriptors: state.ordinal, complete: true as const }
})

export const run = Effect.fn("SessionTranscriptIndex.run")(function* (input: {
  sessionID: SessionID
  ownerID: string
  maxBatches?: number
}) {
  const { db } = yield* Database.Service
  return yield* runIndex(input).pipe(
    Effect.onExit((exit) =>
      Exit.isFailure(exit) && !Cause.hasInterruptsOnly(exit.cause) ? markIndexFailed(db, input) : Effect.void,
    ),
  )
})

function markIndexFailed(db: Database.Interface["db"], input: { sessionID: SessionID; ownerID: string }) {
  return db.transaction(
    () =>
      Effect.gen(function* () {
        yield* db
          .update(TranscriptWindowStateTable)
          .set({ index_status: "index_failed", index_owner_id: null })
          .where(
            and(
              eq(TranscriptWindowStateTable.session_id, input.sessionID),
              eq(TranscriptWindowStateTable.index_owner_id, input.ownerID),
            ),
          )
          .run()
          .pipe(Effect.orDie)
        yield* db
          .delete(CompactionArchiveStagingTable)
          .where(
            and(
              eq(CompactionArchiveStagingTable.session_id, input.sessionID),
              eq(CompactionArchiveStagingTable.owner_id, input.ownerID),
            ),
          )
          .run()
          .pipe(Effect.orDie)
        yield* db
          .delete(SessionMaintenanceTable)
          .where(
            and(
              eq(SessionMaintenanceTable.session_id, input.sessionID),
              eq(SessionMaintenanceTable.owner_id, input.ownerID),
            ),
          )
          .run()
          .pipe(Effect.orDie)
      }),
    { behavior: "immediate" },
  )
}

function loadBatch(
  db: Database.Interface["db"],
  input: { sessionID: SessionID; cursorTime?: number; cursorID?: MessageID },
) {
  return Effect.gen(function* () {
    const after =
      input.cursorTime !== undefined && input.cursorID
        ? or(
            gt(MessageTable.time_created, input.cursorTime),
            and(eq(MessageTable.time_created, input.cursorTime), gt(MessageTable.id, input.cursorID)),
          )
        : undefined
    const identities = yield* db
      .select({
        id: MessageTable.id,
        time: MessageTable.time_created,
        bytes: sql<number>`length(cast(${MessageTable.data} as blob))`,
      })
      .from(MessageTable)
      .where(
        after ? and(eq(MessageTable.session_id, input.sessionID), after) : eq(MessageTable.session_id, input.sessionID),
      )
      .orderBy(asc(MessageTable.time_created), asc(MessageTable.id))
      .limit(BatchMessages + 1)
      .all()
      .pipe(Effect.orDie)
    const page = identities.slice(0, BatchMessages)
    if (
      page.some((row) => row.bytes > MaxSingleRowBytes) ||
      page.reduce((count, row) => count + row.bytes, 0) > BatchMetadataBytes
    )
      return yield* Effect.die("Transcript message batch exceeds the safe index limit")
    if (page.length === 0) return { identities: page, messages: [] as SessionV1.WithParts[] }
    const ids = page.map((row) => row.id)
    const partIdentities = yield* db
      .select({ id: PartTable.id, bytes: sql<number>`length(cast(${PartTable.data} as blob))` })
      .from(PartTable)
      .where(and(eq(PartTable.session_id, input.sessionID), inArray(PartTable.message_id, ids)))
      .limit(TranscriptWindowProjection.Limits.parts + 1)
      .all()
      .pipe(Effect.orDie)
    const bytes =
      page.reduce((count, row) => count + row.bytes, 0) + partIdentities.reduce((count, row) => count + row.bytes, 0)
    if (
      partIdentities.length > TranscriptWindowProjection.Limits.parts ||
      partIdentities.some((row) => row.bytes > MaxSingleRowBytes) ||
      bytes > MaxTransientBytes
    )
      return yield* Effect.die("Transcript part batch exceeds the safe index limit")
    const rows = yield* db
      .select()
      .from(MessageTable)
      .where(and(eq(MessageTable.session_id, input.sessionID), inArray(MessageTable.id, ids)))
      .orderBy(asc(MessageTable.time_created), asc(MessageTable.id))
      .all()
      .pipe(Effect.orDie)
    const partRows = yield* db
      .select()
      .from(PartTable)
      .where(and(eq(PartTable.session_id, input.sessionID), inArray(PartTable.message_id, ids)))
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
    return {
      identities: page,
      messages: rows.map(
        (row) =>
          ({
            info: { ...row.data, id: row.id, sessionID: row.session_id } as SessionV1.Info,
            parts: parts.get(row.id) ?? [],
          }) satisfies SessionV1.WithParts,
      ),
    }
  })
}

function countMessages(db: Database.Interface["db"], sessionID: SessionID, ids: MessageID[]) {
  if (ids.length === 0) return Effect.succeed(emptyCounts())
  return Effect.gen(function* () {
    const rows = yield* db
      .select()
      .from(MessageTable)
      .where(and(eq(MessageTable.session_id, sessionID), inArray(MessageTable.id, ids)))
      .orderBy(asc(MessageTable.time_created), asc(MessageTable.id))
      .all()
      .pipe(Effect.orDie)
    const parts = yield* db
      .select()
      .from(PartTable)
      .where(and(eq(PartTable.session_id, sessionID), inArray(PartTable.message_id, ids)))
      .orderBy(PartTable.message_id, PartTable.id)
      .all()
      .pipe(Effect.orDie)
    const partsByMessage = new Map<MessageID, SessionV1.Part[]>()
    for (const row of parts) {
      const part = { ...row.data, id: row.id, sessionID: row.session_id, messageID: row.message_id } as SessionV1.Part
      const list = partsByMessage.get(row.message_id)
      if (list) list.push(part)
      else partsByMessage.set(row.message_id, [part])
    }
    const counts = emptyCounts()
    for (const row of rows) {
      add(counts, {
        info: { ...row.data, id: row.id, sessionID: row.session_id } as SessionV1.Info,
        parts: partsByMessage.get(row.id) ?? [],
      })
    }
    return counts
  })
}

function decodeState(value: Record<string, unknown> | null): State {
  if (!value) return { ordinal: 0 }
  if (typeof value.ordinal !== "number" || value.ordinal < 0 || !Number.isSafeInteger(value.ordinal))
    throw new Error("Stored transcript index state is invalid")
  return value as State
}

function emptyCounts(): Counts {
  return { messages: 0, parts: 0, textUnits: 0, decodedBytes: 0 }
}

function add(counts: Counts, message: SessionV1.WithParts) {
  const wireMessage = TranscriptWindowProjection.encodeMessagesForTransport([message])[0]!
  const encoded = JSON.stringify(wireMessage)
  counts.messages++
  counts.parts += wireMessage.parts.length
  counts.textUnits += encoded.length
  counts.decodedBytes += Buffer.byteLength(encoded, "utf8")
}

function sum(left: Counts, right: Counts): Counts {
  return {
    messages: left.messages + right.messages,
    parts: left.parts + right.parts,
    textUnits: left.textUnits + right.textUnits,
    decodedBytes: left.decodedBytes + right.decodedBytes,
  }
}

function finalizeCounts(counts: Counts): Counts {
  const separators = Math.max(0, counts.messages - 1)
  return {
    ...counts,
    textUnits: counts.textUnits + separators + 2,
    decodedBytes: counts.decodedBytes + separators + 2,
  }
}

function isPrompt(message: SessionV1.WithParts) {
  return (
    message.info.role === "user" &&
    !message.parts.some((part) => part.type === "compaction") &&
    !message.parts.some((part) => part.type === "text" && part.metadata?.compaction_replay === true) &&
    !message.parts.every((part) => "synthetic" in part && !!part.synthetic)
  )
}

function isFinalAssistant(message: SessionV1.WithParts) {
  return (
    message.info.role === "assistant" &&
    !!message.info.finish &&
    !message.info.error &&
    !message.info.summary &&
    message.info.finish !== "tool-calls" &&
    message.info.finish !== "tool_calls" &&
    messageText(message).length > 0
  )
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
