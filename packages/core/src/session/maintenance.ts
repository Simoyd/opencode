export * as SessionMaintenance from "./maintenance"

import { Effect, Schema } from "effect"
import type { Database } from "../database/database"
import type { EventV2 } from "../event"
import type { SessionSchema } from "./schema"

type DatabaseService = Database.Interface["db"]

export class BlockedError extends Schema.TaggedErrorClass<BlockedError>()("SessionMaintenance.Blocked", {
  sessionID: Schema.String,
  reason: Schema.String,
}) {}

export class OwnerMismatchError extends Schema.TaggedErrorClass<OwnerMismatchError>()(
  "SessionMaintenance.OwnerMismatch",
  { sessionID: Schema.String },
) {}

export function guardEvent(_db: DatabaseService, _event: EventV2.Payload) {
  return Effect.void
}

export function withAdmission<A, E, R>(
  _db: DatabaseService,
  _input: { sessionID: SessionSchema.ID; kind: string },
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> {
  return effect
}
