export * as TranscriptWindowProjection from "./transcript-window"

import { Effect, Schema } from "effect"
import { and, asc, desc, eq, gte, inArray, lt, or, sql } from "drizzle-orm"
import type { Database } from "../database/database"
import { SessionV1, type MessageID } from "../v1/session"
import type { SessionSchema } from "./schema"
import { CompactionArchiveManifestTable, MessageTable, PartTable, TranscriptWindowStateTable } from "./sql"

type DatabaseService = Database.Interface["db"]

export const Limits = {
  descriptors: 512,
  descriptorBytes: 2 * 1024 * 1024,
  identityCodeUnits: 4096,
  summaryCodeUnits: 512,
  messages: 4096,
  parts: 32768,
  textCodeUnits: 8_000_000,
  decodedBytes: 32 * 1024 * 1024,
} as const

const encodeMessages = Schema.encodeSync(Schema.Array(SessionV1.WithParts))

export function encodeMessagesForTransport(messages: SessionV1.WithParts[]) {
  return encodeMessages(messages) as SessionV1.WithParts[]
}

export function create(db: DatabaseService, input: { sessionID: SessionSchema.ID; revision: number }) {
  return db
    .insert(TranscriptWindowStateTable)
    .values({
      session_id: input.sessionID,
      source_generation: crypto.randomUUID(),
      window_revision: input.revision,
      index_status: "complete",
      text_units: 2,
      decoded_bytes: 2,
    })
    .run()
    .pipe(Effect.orDie)
}

export function touch(
  db: DatabaseService,
  input: { sessionID: SessionSchema.ID; messageID: MessageID; revision: number; structural?: boolean },
) {
  return Effect.gen(function* () {
    const state = yield* db
      .select({ status: TranscriptWindowStateTable.index_status })
      .from(TranscriptWindowStateTable)
      .where(eq(TranscriptWindowStateTable.session_id, input.sessionID))
      .get()
      .pipe(Effect.orDie)
    if (!state) return

    yield* db
      .update(TranscriptWindowStateTable)
      .set({ window_revision: input.revision })
      .where(eq(TranscriptWindowStateTable.session_id, input.sessionID))
      .run()
      .pipe(Effect.orDie)
    if (state.status !== "complete") return

    const rows = yield* db
      .select()
      .from(CompactionArchiveManifestTable)
      .where(eq(CompactionArchiveManifestTable.session_id, input.sessionID))
      .limit(Limits.descriptors + 1)
      .all()
      .pipe(Effect.orDie)
    if (rows.length > Limits.descriptors) {
      yield* db
        .update(TranscriptWindowStateTable)
        .set({ index_status: "too_large" })
        .where(eq(TranscriptWindowStateTable.session_id, input.sessionID))
        .run()
        .pipe(Effect.orDie)
      return
    }

    let requiresIndex = false
    for (const row of rows) {
      const inRange = input.messageID >= row.range_start_id && input.messageID < row.range_end_id
      const continuity = row.continuity_message_ids.includes(input.messageID)
      const structuralIdentity =
        input.messageID === row.marker_id ||
        input.messageID === row.tail_start_id ||
        input.messageID === row.source_message_id ||
        input.messageID === row.summary_message_id ||
        input.messageID === row.range_start_id ||
        input.messageID === row.range_end_id ||
        row.replay_message_ids.includes(input.messageID)
      const structural = structuralIdentity || (input.structural === true && (inRange || continuity))
      if (structural) {
        yield* db
          .update(CompactionArchiveManifestTable)
          .set({ archive_revision: input.revision })
          .where(
            and(
              eq(CompactionArchiveManifestTable.session_id, input.sessionID),
              eq(CompactionArchiveManifestTable.archive_id, row.archive_id),
            ),
          )
          .run()
          .pipe(Effect.orDie)
        requiresIndex = true
        continue
      }
      if (!inRange && !continuity) continue
      const recomputed = yield* recomputeArchive(db, row)
      if (!recomputed) {
        yield* failTooLarge(db, input.sessionID, input.revision)
        return
      }
      yield* db
        .update(CompactionArchiveManifestTable)
        .set({
          archive_revision: input.revision,
          message_count: recomputed.messageCount,
          part_count: recomputed.partCount,
          text_units: recomputed.textUnits,
          decoded_bytes: recomputed.decodedBytes,
        })
        .where(
          and(
            eq(CompactionArchiveManifestTable.session_id, input.sessionID),
            eq(CompactionArchiveManifestTable.archive_id, row.archive_id),
          ),
        )
        .run()
        .pipe(Effect.orDie)
    }
    if (requiresIndex) {
      yield* db
        .update(TranscriptWindowStateTable)
        .set({ index_status: "index_required" })
        .where(eq(TranscriptWindowStateTable.session_id, input.sessionID))
        .run()
        .pipe(Effect.orDie)
    }
  })
}

function recomputeArchive(db: DatabaseService, row: typeof CompactionArchiveManifestTable.$inferSelect) {
  return Effect.gen(function* () {
    const range = and(
      eq(MessageTable.session_id, row.session_id),
      gte(MessageTable.id, row.range_start_id),
      lt(MessageTable.id, row.range_end_id),
    )
    const continuity = row.continuity_message_ids.length
      ? and(eq(MessageTable.session_id, row.session_id), inArray(MessageTable.id, row.continuity_message_ids))
      : undefined
    const identities = yield* db
      .select({
        id: MessageTable.id,
        bytes: sql<number>`length(cast(${MessageTable.data} as blob))`,
      })
      .from(MessageTable)
      .where(continuity ? or(range, continuity) : range)
      .orderBy(asc(MessageTable.time_created), asc(MessageTable.id))
      .limit(Limits.messages + 1)
      .all()
      .pipe(Effect.orDie)
    if (
      identities.length > Limits.messages ||
      identities.some((identity) => identity.id.length > Limits.identityCodeUnits) ||
      identities.reduce((total, identity) => total + identity.bytes, 0) > Limits.decodedBytes
    )
      return undefined
    if (identities.length === 0) return { messageCount: 0, partCount: 0, textUnits: 2, decodedBytes: 2 }

    const ids = identities.map((identity) => identity.id)
    const partIdentities = yield* db
      .select({
        id: PartTable.id,
        messageID: PartTable.message_id,
        bytes: sql<number>`length(cast(${PartTable.data} as blob))`,
      })
      .from(PartTable)
      .where(and(eq(PartTable.session_id, row.session_id), inArray(PartTable.message_id, ids)))
      .limit(Limits.parts + 1)
      .all()
      .pipe(Effect.orDie)
    if (
      partIdentities.length > Limits.parts ||
      partIdentities.some(
        (part) =>
          part.id.length > Limits.identityCodeUnits ||
          part.messageID.length > Limits.identityCodeUnits,
      ) ||
      identities.reduce((total, identity) => total + identity.bytes, 0) +
          partIdentities.reduce((total, part) => total + part.bytes, 0) >
        Limits.decodedBytes
    )
      return undefined

    const messages = yield* db
      .select()
      .from(MessageTable)
      .where(and(eq(MessageTable.session_id, row.session_id), inArray(MessageTable.id, ids)))
      .orderBy(asc(MessageTable.time_created), asc(MessageTable.id))
      .all()
      .pipe(Effect.orDie)
    const partRows = yield* db
      .select()
      .from(PartTable)
      .where(and(eq(PartTable.session_id, row.session_id), inArray(PartTable.message_id, ids)))
      .orderBy(PartTable.message_id, PartTable.id)
      .all()
      .pipe(Effect.orDie)
    const parts = new Map<MessageID, SessionV1.Part[]>()
    for (const partRow of partRows) {
      const part = {
        ...partRow.data,
        id: partRow.id,
        sessionID: partRow.session_id,
        messageID: partRow.message_id,
      } as SessionV1.Part
      const list = parts.get(partRow.message_id)
      if (list) list.push(part)
      else parts.set(partRow.message_id, [part])
    }
    const archive = encodeMessagesForTransport(
      messages.map(
        (message) =>
          ({
            info: { ...message.data, id: message.id, sessionID: message.session_id } as SessionV1.Info,
            parts: parts.get(message.id) ?? [],
          }) satisfies SessionV1.WithParts,
      ),
    )
    const encoded = JSON.stringify(archive)
    const decodedBytes = Buffer.byteLength(encoded, "utf8")
    if (encoded.length > Limits.textCodeUnits || decodedBytes > Limits.decodedBytes) return undefined
    return {
      messageCount: archive.length,
      partCount: archive.reduce((total, message) => total + message.parts.length, 0),
      textUnits: encoded.length,
      decodedBytes,
    }
  })
}

export function refresh(db: DatabaseService, input: { sessionID: SessionSchema.ID; revision: number }) {
  return Effect.gen(function* () {
    const state = yield* db
      .select()
      .from(TranscriptWindowStateTable)
      .where(eq(TranscriptWindowStateTable.session_id, input.sessionID))
      .get()
      .pipe(Effect.orDie)
    if (!state || state.index_status !== "complete") return

    const identities = yield* db
      .select({ id: MessageTable.id, bytes: sql<number>`length(cast(${MessageTable.data} as blob))` })
      .from(MessageTable)
      .where(
        state.tail_start_id
          ? and(eq(MessageTable.session_id, input.sessionID), gte(MessageTable.id, state.tail_start_id))
          : eq(MessageTable.session_id, input.sessionID),
      )
      .orderBy(desc(MessageTable.time_created), desc(MessageTable.id))
      .limit(Limits.messages + 1)
      .all()
      .pipe(Effect.orDie)
    if (
      identities.length > Limits.messages ||
      identities.some((row) => row.id.length > Limits.identityCodeUnits || row.bytes > Limits.decodedBytes) ||
      identities.reduce((count, row) => count + row.bytes, 0) > Limits.decodedBytes
    )
      return yield* failTooLarge(db, input.sessionID, input.revision)

    const ids = identities.map((row) => row.id)
    const partIdentities =
      ids.length === 0
        ? []
        : yield* db
            .select({
              id: PartTable.id,
              messageID: PartTable.message_id,
              bytes: sql<number>`length(cast(${PartTable.data} as blob))`,
            })
            .from(PartTable)
            .where(and(eq(PartTable.session_id, input.sessionID), inArray(PartTable.message_id, ids)))
            .orderBy(PartTable.message_id, PartTable.id)
            .limit(Limits.parts + 1)
            .all()
            .pipe(Effect.orDie)
    if (
      partIdentities.length > Limits.parts ||
      partIdentities.some(
        (row) =>
          row.id.length > Limits.identityCodeUnits ||
          row.messageID.length > Limits.identityCodeUnits ||
          row.bytes > Limits.decodedBytes,
      ) ||
      identities.reduce((count, row) => count + row.bytes, 0) +
        partIdentities.reduce((count, row) => count + row.bytes, 0) >
        Limits.decodedBytes
    )
      return yield* failTooLarge(db, input.sessionID, input.revision)

    const rows =
      ids.length === 0
        ? []
        : yield* db
            .select()
            .from(MessageTable)
            .where(and(eq(MessageTable.session_id, input.sessionID), inArray(MessageTable.id, ids)))
            .orderBy(desc(MessageTable.time_created), desc(MessageTable.id))
            .all()
            .pipe(Effect.orDie)
    const partRows =
      ids.length === 0
        ? []
        : yield* db
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
    const messages = encodeMessagesForTransport(
      rows.toReversed().map(
        (row) =>
          ({
            info: { ...row.data, id: row.id, sessionID: row.session_id } as SessionV1.Info,
            parts: parts.get(row.id) ?? [],
          }) satisfies SessionV1.WithParts,
      ),
    )
    const encoded = JSON.stringify(messages)
    const textUnits = encoded.length
    const decodedBytes = Buffer.byteLength(encoded, "utf8")
    if (textUnits > Limits.textCodeUnits || decodedBytes > Limits.decodedBytes)
      return yield* failTooLarge(db, input.sessionID, input.revision)

    const existing = yield* db
      .select()
      .from(CompactionArchiveManifestTable)
      .where(eq(CompactionArchiveManifestTable.session_id, input.sessionID))
      .orderBy(CompactionArchiveManifestTable.ordinal)
      .limit(Limits.descriptors + 1)
      .all()
      .pipe(Effect.orDie)
    if (existing.length > Limits.descriptors) return yield* failTooLarge(db, input.sessionID, input.revision)

    const derived = deriveCompactions(messages)
    const byMarker = new Map(existing.map((row) => [row.marker_id, row]))
    let nextOrdinal = (existing.at(-1)?.ordinal ?? -1) + 1
    for (const item of derived) {
      const prior = [...existing, ...derived.map((candidate) => byMarker.get(candidate.markerID)).filter(Boolean)]
        .filter((candidate) => candidate && candidate.marker_id < item.markerID)
        .sort((a, b) => a!.ordinal - b!.ordinal)
        .at(-1)
      const continuity = prior ? [prior.summary_message_id, ...prior.replay_message_ids] : []
      const own = messages.filter((message) => message.info.id >= item.rangeStartID && message.info.id < item.markerID)
      const continuityMessages = continuity
        .map((id) => messages.find((message) => message.info.id === id))
        .filter((message): message is SessionV1.WithParts => !!message)
      if (continuityMessages.length !== continuity.length) continue
      const archive = [...continuityMessages, ...own].filter(
        (message, index, values) => values.findIndex((candidate) => candidate.info.id === message.info.id) === index,
      )
      const archiveEncoded = JSON.stringify(archive)
      const archivePartCount = archive.reduce((count, message) => count + message.parts.length, 0)
      if (
        archive.length > Limits.messages ||
        archivePartCount > Limits.parts ||
        archiveEncoded.length > Limits.textCodeUnits ||
        Buffer.byteLength(archiveEncoded, "utf8") > Limits.decodedBytes
      )
        return yield* failTooLarge(db, input.sessionID, input.revision)

      const current = byMarker.get(item.markerID)
      const ordinal = current?.ordinal ?? nextOrdinal++
      yield* db
        .insert(CompactionArchiveManifestTable)
        .values({
          session_id: input.sessionID,
          archive_id: item.markerID,
          archive_revision: input.revision,
          ordinal,
          marker_id: item.markerID,
          tail_start_id: item.tailStartID,
          source_message_id: item.sourceMessageID,
          summary_message_id: item.summaryMessageID,
          range_start_id: item.rangeStartID,
          range_end_id: item.markerID,
          summary_preview: item.summaryPreview,
          continuity_message_ids: continuity,
          replay_message_ids: item.replayMessageIDs,
          message_count: archive.length,
          part_count: archivePartCount,
          text_units: archiveEncoded.length,
          decoded_bytes: Buffer.byteLength(archiveEncoded, "utf8"),
        })
        .onConflictDoUpdate({
          target: [CompactionArchiveManifestTable.session_id, CompactionArchiveManifestTable.archive_id],
          set: {
            archive_revision: input.revision,
            tail_start_id: item.tailStartID,
            source_message_id: item.sourceMessageID,
            summary_message_id: item.summaryMessageID,
            range_start_id: item.rangeStartID,
            range_end_id: item.markerID,
            summary_preview: item.summaryPreview,
            continuity_message_ids: continuity,
            replay_message_ids: item.replayMessageIDs,
            message_count: archive.length,
            part_count: archivePartCount,
            text_units: archiveEncoded.length,
            decoded_bytes: Buffer.byteLength(archiveEncoded, "utf8"),
          },
        })
        .run()
        .pipe(Effect.orDie)
      byMarker.set(item.markerID, {
        ...(current ?? ({} as typeof CompactionArchiveManifestTable.$inferSelect)),
        session_id: input.sessionID,
        archive_id: item.markerID,
        archive_revision: input.revision,
        ordinal,
        marker_id: item.markerID,
        tail_start_id: item.tailStartID ?? null,
        source_message_id: item.sourceMessageID,
        summary_message_id: item.summaryMessageID,
        range_start_id: item.rangeStartID,
        range_end_id: item.markerID,
        summary_preview: item.summaryPreview,
        continuity_message_ids: continuity,
        replay_message_ids: item.replayMessageIDs,
        message_count: archive.length,
        part_count: archivePartCount,
        text_units: archiveEncoded.length,
        decoded_bytes: Buffer.byteLength(archiveEncoded, "utf8"),
      })
    }

    const latest = derived.filter((item) => item.tailStartID).at(-1)
    const tail = latest?.tailStartID ? messages.filter((message) => message.info.id >= latest.tailStartID!) : messages
    const tailEncoded = JSON.stringify(tail)
    const descriptorCount = new Set([...existing.map((row) => row.marker_id), ...derived.map((item) => item.markerID)])
      .size
    if (descriptorCount > Limits.descriptors) return yield* failTooLarge(db, input.sessionID, input.revision)
    yield* db
      .update(TranscriptWindowStateTable)
      .set({
        window_revision: input.revision,
        tail_start_id: latest?.tailStartID ?? state.tail_start_id,
        descriptor_count: descriptorCount,
        message_count: tail.length,
        part_count: tail.reduce((count, message) => count + message.parts.length, 0),
        text_units: tailEncoded.length,
        decoded_bytes: Buffer.byteLength(tailEncoded, "utf8"),
      })
      .where(eq(TranscriptWindowStateTable.session_id, input.sessionID))
      .run()
      .pipe(Effect.orDie)
  })
}

function failTooLarge(db: DatabaseService, sessionID: SessionSchema.ID, revision: number) {
  return db
    .update(TranscriptWindowStateTable)
    .set({ index_status: "too_large", window_revision: revision })
    .where(eq(TranscriptWindowStateTable.session_id, sessionID))
    .run()
    .pipe(Effect.orDie)
}

function deriveCompactions(messages: SessionV1.WithParts[]) {
  return messages.flatMap((prompt, start) => {
    if (!isPrompt(prompt)) return []
    const next = messages.findIndex((message, index) => index > start && isPrompt(message))
    const segment = messages.slice(start, next < 0 ? messages.length : next)
    const final = segment.filter(isFinalAssistant).at(-1)
    if (!final) return []
    const markers = segment.filter(
      (message) => message.info.role === "user" && message.parts.some((part) => part.type === "compaction"),
    )
    if (markers.length !== 1) return []
    const marker = markers[0]!
    const markerPart = marker.parts.find((part): part is SessionV1.CompactionPart => part.type === "compaction")
    if (!markerPart) return []
    const summaries = segment.filter(
      (message) =>
        message.info.role === "assistant" &&
        message.info.parentID === marker.info.id &&
        !!message.info.summary &&
        !!message.info.finish &&
        !message.info.error,
    )
    if (summaries.length !== 1) return []
    const summary = summaries[0]!
    const fullSummary = messageText(summary)
    if (!fullSummary) return []
    const replayMessageIDs = segment
      .filter(
        (message) =>
          message.info.role === "user" &&
          message.parts.some((part) => part.type === "text" && part.metadata?.compaction_replay === true),
      )
      .map((message) => message.info.id)
    const synthetic = segment.filter(
      (message) =>
        message.info.role === "user" &&
        message.parts.some(
          (part) => part.type === "text" && part.synthetic && part.metadata?.compaction_continue === true,
        ),
    )
    if (synthetic.length + replayMessageIDs.length === 0 || (markerPart.overflow && replayMessageIDs.length === 0))
      return []
    const normalized = fullSummary.split(/\s+/).filter(Boolean).join(" ")
    return [
      {
        markerID: marker.info.id,
        tailStartID: markerPart.tail_start_id,
        sourceMessageID: prompt.info.id,
        summaryMessageID: summary.info.id,
        rangeStartID: prompt.info.id,
        summaryPreview:
          normalized.length <= Limits.summaryCodeUnits
            ? normalized
            : `${normalized.slice(0, Limits.summaryCodeUnits - 3)}...`,
        replayMessageIDs,
      },
    ]
  })
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
