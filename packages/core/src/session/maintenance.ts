export * as SessionMaintenance from "./maintenance"

import { Effect, Schema } from "effect"
import { and, eq, isNull } from "drizzle-orm"
import type { Database } from "../database/database"
import type { EventV2 } from "../event"
import { EventSequenceTable } from "../event/sql"
import { SessionAdmissionTable, SessionInputTable, SessionMaintenanceTable, TranscriptWindowStateTable } from "./sql"
import type { SessionSchema } from "./schema"

type DatabaseService = Database.Interface["db"]

export class BlockedError extends Schema.TaggedErrorClass<BlockedError>()("SessionMaintenance.Blocked", {
  sessionID: Schema.String,
  reason: Schema.String,
}) {}

export class OwnerMismatchError extends Schema.TaggedErrorClass<OwnerMismatchError>()(
  "SessionMaintenance.OwnerMismatch",
  {
    sessionID: Schema.String,
  },
) {}

export function guardEvent(db: DatabaseService, event: EventV2.Payload) {
  const sessionID = (event.data as Record<string, unknown>).sessionID
  if (typeof sessionID !== "string") return Effect.void
  return Effect.gen(function* () {
    const lease = yield* db
      .select({ ownerID: SessionMaintenanceTable.owner_id })
      .from(SessionMaintenanceTable)
      .where(eq(SessionMaintenanceTable.session_id, sessionID as SessionSchema.ID))
      .get()
      .pipe(Effect.orDie)
    if (lease)
      return yield* Effect.die(
        new BlockedError({
          sessionID,
          reason: "Session is locked for transcript indexing.",
        }),
      )
  })
}

export function withAdmission<A, E, R>(
  db: DatabaseService,
  input: { sessionID: SessionSchema.ID; kind: string },
  effect: Effect.Effect<A, E, R>,
) {
  const operationID = crypto.randomUUID()
  const acquire = db
    .transaction(
      () =>
        Effect.gen(function* () {
        const lease = yield* db
          .select({ ownerID: SessionMaintenanceTable.owner_id })
          .from(SessionMaintenanceTable)
          .where(eq(SessionMaintenanceTable.session_id, input.sessionID))
          .get()
          .pipe(Effect.orDie)
        if (lease)
          return yield* Effect.die(
            new BlockedError({
              sessionID: input.sessionID,
              reason: "Session is locked for transcript indexing.",
            }),
          )
        yield* db
          .insert(SessionAdmissionTable)
          .values({
            session_id: input.sessionID,
            operation_id: operationID,
            kind: input.kind,
            time_started: Date.now(),
          })
          .run()
          .pipe(Effect.orDie)
        }),
      { behavior: "immediate" },
    )
    .pipe(Effect.orDie)
  const release = db
    .delete(SessionAdmissionTable)
    .where(
      and(
        eq(SessionAdmissionTable.session_id, input.sessionID),
        eq(SessionAdmissionTable.operation_id, operationID),
      ),
    )
    .run()
    .pipe(Effect.orDie)
  return acquire.pipe(Effect.andThen(effect), Effect.ensuring(release))
}

export function acquireTranscriptIndex(
  db: DatabaseService,
  input: { sessionID: SessionSchema.ID; ownerID: string },
) {
  return db.transaction(
    () =>
      Effect.gen(function* () {
        const existing = yield* db
          .select()
          .from(SessionMaintenanceTable)
          .where(eq(SessionMaintenanceTable.session_id, input.sessionID))
          .get()
          .pipe(Effect.orDie)
        if (existing) {
          if (existing.owner_id !== input.ownerID) {
            return yield* new BlockedError({
              sessionID: input.sessionID,
              reason: "Another transcript index owner holds the maintenance lease.",
            })
          }
          return { epoch: existing.epoch, resumed: true }
        }

        const admission = yield* db
          .select({ operationID: SessionAdmissionTable.operation_id })
          .from(SessionAdmissionTable)
          .where(eq(SessionAdmissionTable.session_id, input.sessionID))
          .limit(1)
          .get()
          .pipe(Effect.orDie)
        if (admission)
          return yield* new BlockedError({
            sessionID: input.sessionID,
            reason: "A session operation is still admitted.",
          })

        const pending = yield* db
          .select({ id: SessionInputTable.id })
          .from(SessionInputTable)
          .where(and(eq(SessionInputTable.session_id, input.sessionID), isNull(SessionInputTable.promoted_seq)))
          .limit(1)
          .get()
          .pipe(Effect.orDie)
        if (pending)
          return yield* new BlockedError({
            sessionID: input.sessionID,
            reason: "The session has pending admitted input.",
          })

        const sequence = yield* db
          .select({ seq: EventSequenceTable.seq })
          .from(EventSequenceTable)
          .where(eq(EventSequenceTable.aggregate_id, input.sessionID))
          .get()
          .pipe(Effect.orDie)
        const epoch = (sequence?.seq ?? -1) + 1
        const now = Date.now()
        yield* db
          .insert(SessionMaintenanceTable)
          .values({
            session_id: input.sessionID,
            owner_id: input.ownerID,
            epoch,
            kind: "transcript_index",
            time_started: now,
            time_updated: now,
          })
          .run()
          .pipe(Effect.orDie)
        yield* db
          .insert(TranscriptWindowStateTable)
          .values({
            session_id: input.sessionID,
            source_generation: crypto.randomUUID(),
            window_revision: sequence?.seq ?? -1,
            index_status: "indexing",
            index_owner_id: input.ownerID,
          })
          .onConflictDoUpdate({
            target: TranscriptWindowStateTable.session_id,
            set: {
              index_status: "indexing",
              index_owner_id: input.ownerID,
              window_revision: sequence?.seq ?? -1,
              index_cursor_time: null,
              index_cursor_id: null,
              index_state: null,
            },
          })
          .run()
          .pipe(Effect.orDie)
        return { epoch, resumed: false }
      }),
    { behavior: "immediate" },
  )
}

export function verifyOwner(db: DatabaseService, input: { sessionID: SessionSchema.ID; ownerID: string }) {
  return Effect.gen(function* () {
    const row = yield* db
      .select({ ownerID: SessionMaintenanceTable.owner_id, epoch: SessionMaintenanceTable.epoch })
      .from(SessionMaintenanceTable)
      .where(eq(SessionMaintenanceTable.session_id, input.sessionID))
      .get()
      .pipe(Effect.orDie)
    if (!row || row.ownerID !== input.ownerID) return yield* new OwnerMismatchError({ sessionID: input.sessionID })
    return row
  })
}

export function releaseTranscriptIndex(db: DatabaseService, input: { sessionID: SessionSchema.ID; ownerID: string }) {
  return db.transaction(
    () =>
      Effect.gen(function* () {
        yield* verifyOwner(db, input)
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
