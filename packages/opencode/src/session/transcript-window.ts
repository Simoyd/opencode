export * as SessionTranscriptWindow from "./transcript-window"

import { Database } from "@opencode-ai/core/database/database"
import {
  CompactionArchiveManifestTable,
  MessageTable,
  PartTable,
  TranscriptWindowStateTable,
} from "@opencode-ai/core/session/sql"
import { TranscriptWindowProjection } from "@opencode-ai/core/session/transcript-window"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { and, asc, eq, inArray, or, sql } from "drizzle-orm"
import { Effect } from "effect"
import { MessageID, SessionID } from "./schema"

type DatabaseService = Database.Interface["db"]

export class TooLarge extends Error {}
export class Stale extends Error {}

export const loadWindow = Effect.fn("SessionTranscriptWindow.loadWindow")(function* (sessionID: SessionID) {
  const { db } = yield* Database.Service
  return yield* db.transaction(() =>
    Effect.gen(function* () {
      const state = yield* db
        .select()
        .from(TranscriptWindowStateTable)
        .where(eq(TranscriptWindowStateTable.session_id, sessionID))
        .get()
        .pipe(Effect.orDie)
      if (!state) {
        return {
          status: "index_required" as const,
          sessionID,
          archiveDescriptors: [],
          tail: [],
          counts: { descriptors: 0, messages: 0, parts: 0, textUnits: 2, decodedBytes: 2 },
        }
      }
      if (state.index_status !== "complete") {
        return {
          status: state.index_status,
          sessionID,
          sourceGeneration: state.source_generation,
          windowRevision: state.window_revision.toString(),
          archiveDescriptors: [],
          tail: [],
          counts: { descriptors: 0, messages: 0, parts: 0, textUnits: 2, decodedBytes: 2 },
        }
      }

      const manifestSizes = yield* db
        .select({
          bytes: sql<number>`length(cast(${CompactionArchiveManifestTable.archive_id} as blob)) + length(cast(${CompactionArchiveManifestTable.marker_id} as blob)) + length(cast(coalesce(${CompactionArchiveManifestTable.tail_start_id}, '') as blob)) + length(cast(${CompactionArchiveManifestTable.source_message_id} as blob)) + length(cast(${CompactionArchiveManifestTable.summary_preview} as blob)) + length(cast(${CompactionArchiveManifestTable.continuity_message_ids} as blob)) + length(cast(${CompactionArchiveManifestTable.replay_message_ids} as blob))`,
        })
        .from(CompactionArchiveManifestTable)
        .where(eq(CompactionArchiveManifestTable.session_id, sessionID))
        .limit(TranscriptWindowProjection.Limits.descriptors + 1)
        .all()
        .pipe(Effect.orDie)
      if (
        manifestSizes.length > TranscriptWindowProjection.Limits.descriptors ||
        manifestSizes.reduce((count, row) => count + row.bytes, 0) > TranscriptWindowProjection.Limits.descriptorBytes
      )
        return yield* Effect.fail(new TooLarge())

      const manifests = yield* db
        .select()
        .from(CompactionArchiveManifestTable)
        .where(eq(CompactionArchiveManifestTable.session_id, sessionID))
        .orderBy(CompactionArchiveManifestTable.ordinal)
        .limit(TranscriptWindowProjection.Limits.descriptors + 1)
        .all()
        .pipe(Effect.orDie)
      if (manifests.length > TranscriptWindowProjection.Limits.descriptors) return yield* Effect.fail(new TooLarge())

      const tail = yield* loadRange(db, {
        sessionID,
        startID: state.tail_start_id ?? undefined,
      })
      const encoded = JSON.stringify(tail)
      const counts = {
        descriptors: manifests.length,
        messages: tail.length,
        parts: tail.reduce((count, message) => count + message.parts.length, 0),
        textUnits: encoded.length,
        decodedBytes: Buffer.byteLength(encoded, "utf8"),
      }
      if (
        counts.messages !== state.message_count ||
        counts.parts !== state.part_count ||
        counts.textUnits !== state.text_units ||
        counts.decodedBytes !== state.decoded_bytes ||
        counts.descriptors !== state.descriptor_count
      )
        return yield* Effect.fail(new Stale())

      const archiveDescriptors = manifests.map((row) => ({
        archiveID: row.archive_id,
        archiveRevision: row.archive_revision.toString(),
        markerID: row.marker_id,
        tailStartID: row.tail_start_id ?? undefined,
        sourceMessageID: row.source_message_id,
        summaryPreview: row.summary_preview,
        messageCount: row.message_count,
        partCount: row.part_count,
        textUnits: row.text_units,
        decodedBytes: row.decoded_bytes,
      }))
      if (
        Buffer.byteLength(JSON.stringify(archiveDescriptors), "utf8") >
        TranscriptWindowProjection.Limits.descriptorBytes
      )
        return yield* Effect.fail(new TooLarge())

      return {
        status: "complete" as const,
        sessionID,
        sourceGeneration: state.source_generation,
        windowRevision: state.window_revision.toString(),
        tailStartID: state.tail_start_id ?? undefined,
        archiveDescriptors,
        tail,
        counts,
      }
    }),
  )
})

export const loadArchive = Effect.fn("SessionTranscriptWindow.loadArchive")(function* (input: {
  sessionID: SessionID
  markerID: MessageID
  sourceGeneration?: string
  archiveID?: string
  archiveRevision?: string
  tailStartID?: MessageID
  messageID?: MessageID
  sourceMessageID?: MessageID
}) {
  const { db } = yield* Database.Service
  return yield* db.transaction(() =>
    Effect.gen(function* () {
      const state = yield* db
        .select()
        .from(TranscriptWindowStateTable)
        .where(eq(TranscriptWindowStateTable.session_id, input.sessionID))
        .get()
        .pipe(Effect.orDie)
      if (!state || state.index_status !== "complete") return yield* Effect.fail(new Stale())
      if (input.sourceGeneration && input.sourceGeneration !== state.source_generation)
        return yield* Effect.fail(new Stale())

      const rowSize = yield* db
        .select({
          bytes: sql<number>`length(cast(${CompactionArchiveManifestTable.continuity_message_ids} as blob)) + length(cast(${CompactionArchiveManifestTable.replay_message_ids} as blob)) + length(cast(${CompactionArchiveManifestTable.summary_preview} as blob))`,
        })
        .from(CompactionArchiveManifestTable)
        .where(
          and(
            eq(CompactionArchiveManifestTable.session_id, input.sessionID),
            eq(CompactionArchiveManifestTable.marker_id, input.markerID),
          ),
        )
        .get()
        .pipe(Effect.orDie)
      if (rowSize && rowSize.bytes > TranscriptWindowProjection.Limits.descriptorBytes)
        return yield* Effect.fail(new TooLarge())

      const row = yield* db
        .select()
        .from(CompactionArchiveManifestTable)
        .where(
          and(
            eq(CompactionArchiveManifestTable.session_id, input.sessionID),
            eq(CompactionArchiveManifestTable.marker_id, input.markerID),
          ),
        )
        .get()
        .pipe(Effect.orDie)
      if (
        !row ||
        (input.archiveID && input.archiveID !== row.archive_id) ||
        (input.archiveRevision && input.archiveRevision !== row.archive_revision.toString()) ||
        (input.tailStartID && input.tailStartID !== row.tail_start_id) ||
        (input.messageID && input.messageID !== row.marker_id) ||
        (input.sourceMessageID && input.sourceMessageID !== row.source_message_id)
      )
        return yield* Effect.fail(new Stale())

      const messages = yield* loadRange(db, {
        sessionID: input.sessionID,
        startID: row.range_start_id,
        endID: row.range_end_id,
        continuityIDs: row.continuity_message_ids,
      })
      const encoded = JSON.stringify(messages)
      const partCount = messages.reduce((count, message) => count + message.parts.length, 0)
      if (
        messages.length !== row.message_count ||
        partCount !== row.part_count ||
        encoded.length !== row.text_units ||
        Buffer.byteLength(encoded, "utf8") !== row.decoded_bytes
      )
        return yield* Effect.fail(new Stale())

      return {
        sourceGeneration: state.source_generation,
        archiveID: row.archive_id,
        archiveRevision: row.archive_revision.toString(),
        markerID: row.marker_id,
        tailStartID: row.tail_start_id ?? undefined,
        sourceMessageID: row.source_message_id,
        messages,
      }
    }),
  )
})

function loadRange(
  db: DatabaseService,
  input: {
    sessionID: SessionID
    startID?: MessageID
    endID?: MessageID
    continuityIDs?: MessageID[]
  },
) {
  return Effect.gen(function* () {
    const orders = yield* TranscriptWindowProjection.loadMessageOrders(
      db,
      input.sessionID,
      [input.startID, input.endID].filter((id): id is MessageID => !!id),
    )
    const start = input.startID ? orders.get(input.startID) : undefined
    const end = input.endID ? orders.get(input.endID) : undefined
    if ((input.startID && !start) || (input.endID && !end)) return yield* Effect.fail(new Stale())
    const range = start
      ? TranscriptWindowProjection.orderedRange(input.sessionID, start, end)
      : eq(MessageTable.session_id, input.sessionID)
    const continuity = input.continuityIDs?.length
      ? and(eq(MessageTable.session_id, input.sessionID), inArray(MessageTable.id, input.continuityIDs))
      : undefined
    const identities = yield* db
      .select({
        id: MessageTable.id,
        time: MessageTable.time_created,
        bytes: sql<number>`length(cast(${MessageTable.data} as blob))`,
      })
      .from(MessageTable)
      .where(continuity ? or(range, continuity) : range)
      .orderBy(asc(MessageTable.time_created), asc(MessageTable.id))
      .limit(TranscriptWindowProjection.Limits.messages + 1)
      .all()
      .pipe(Effect.orDie)
    if (
      identities.length > TranscriptWindowProjection.Limits.messages ||
      identities.some((row) => row.id.length > TranscriptWindowProjection.Limits.identityCodeUnits) ||
      identities.reduce((count, row) => count + row.bytes, 0) > TranscriptWindowProjection.Limits.decodedBytes
    )
      return yield* Effect.fail(new TooLarge())
    if (identities.length === 0) return [] as SessionV1.WithParts[]

    const ids = identities.map((row) => row.id)
    const partIdentities = yield* db
      .select({
        id: PartTable.id,
        messageID: PartTable.message_id,
        bytes: sql<number>`length(cast(${PartTable.data} as blob))`,
      })
      .from(PartTable)
      .where(and(eq(PartTable.session_id, input.sessionID), inArray(PartTable.message_id, ids)))
      .orderBy(PartTable.message_id, PartTable.id)
      .limit(TranscriptWindowProjection.Limits.parts + 1)
      .all()
      .pipe(Effect.orDie)
    if (
      partIdentities.length > TranscriptWindowProjection.Limits.parts ||
      partIdentities.some(
        (row) =>
          row.id.length > TranscriptWindowProjection.Limits.identityCodeUnits ||
          row.messageID.length > TranscriptWindowProjection.Limits.identityCodeUnits,
      ) ||
      identities.reduce((count, row) => count + row.bytes, 0) +
        partIdentities.reduce((count, row) => count + row.bytes, 0) >
        TranscriptWindowProjection.Limits.decodedBytes
    )
      return yield* Effect.fail(new TooLarge())

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
      const value = { ...row.data, id: row.id, sessionID: row.session_id, messageID: row.message_id } as SessionV1.Part
      const list = parts.get(row.message_id)
      if (list) list.push(value)
      else parts.set(row.message_id, [value])
    }
    const messages = TranscriptWindowProjection.encodeMessagesForTransport(
      rows.map(
        (row) =>
          ({
            info: { ...row.data, id: row.id, sessionID: row.session_id } as SessionV1.Info,
            parts: parts.get(row.id) ?? [],
          }) satisfies SessionV1.WithParts,
      ),
    )
    const encoded = JSON.stringify(messages)
    if (
      encoded.length > TranscriptWindowProjection.Limits.textCodeUnits ||
      Buffer.byteLength(encoded, "utf8") > TranscriptWindowProjection.Limits.decodedBytes
    )
      return yield* Effect.fail(new TooLarge())
    return messages
  })
}
