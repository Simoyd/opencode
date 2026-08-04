import { NotFoundError as StorageNotFoundError } from "@/storage/storage"
import { Session } from "@/session/session"
import { Effect } from "effect"
import * as ApiError from "../errors"

export function mapStorageNotFound<A, E, R>(
  self: Effect.Effect<A, E | StorageNotFoundError, R>,
) {
  return self.pipe(
    Effect.catchTag("NotFoundError", (error) =>
      "message" in error && typeof error.message === "string"
        ? Effect.fail(ApiError.notFound(error.message))
        : Effect.die(new Error("NotFoundError requires a string message")),
    ),
  )
}

export const mapNotFound = (error: StorageNotFoundError) => ApiError.notFound(error.message)

export function mapBusy<A, E, R>(
  self: Effect.Effect<A, E | Session.BusyError, R>,
) {
  return self.pipe(
    Effect.catchTag("SessionBusyError", (error) =>
      "sessionID" in error && typeof error.sessionID === "string"
        ? Effect.fail(
            new ApiError.SessionBusyError({
              sessionID: error.sessionID,
              message: `Session is busy: ${error.sessionID}`,
            }),
          )
        : Effect.die(new Error("SessionBusyError requires a string sessionID")),
    ),
  )
}

export function mapLifecycle<A, E, R>(
  self: Effect.Effect<A, E | StorageNotFoundError | Session.BusyError, R>,
) {
  return mapBusy(mapStorageNotFound(self))
}
