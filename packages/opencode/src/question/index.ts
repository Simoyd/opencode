import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Deferred, Effect, Layer, Schema, Context } from "effect"
import { InstanceState } from "@/effect/instance-state"
import { SessionID } from "@/session/schema"
import { QuestionID } from "./schema"
import { EventV2Bridge } from "@/event-v2-bridge"
import { QuestionV1 } from "@opencode-ai/schema/question-v1"

export const Option = QuestionV1.Option
export type Option = typeof Option.Type
export const Info = QuestionV1.Info
export type Info = typeof Info.Type
export const Prompt = QuestionV1.Prompt
export type Prompt = typeof Prompt.Type
export const Tool = QuestionV1.Tool
export type Tool = typeof Tool.Type
export const Request = QuestionV1.Request
export type Request = typeof Request.Type
export const Answer = QuestionV1.Answer
export type Answer = typeof Answer.Type
export const Reply = QuestionV1.Reply
export type Reply = typeof Reply.Type
export const Replied = QuestionV1.Replied
export const Rejected = QuestionV1.Rejected
export const Event = QuestionV1.Event

export class RejectedError extends Schema.TaggedErrorClass<RejectedError>()("QuestionRejectedError", {}) {
  override get message() {
    return "The user dismissed this question"
  }
}

export class NotFoundError extends Schema.TaggedErrorClass<NotFoundError>()("Question.NotFoundError", {
  requestID: QuestionID,
}) {}

interface PendingEntry {
  info: Request
  deferred: Deferred.Deferred<ReadonlyArray<Answer>, RejectedError>
}

interface State {
  pending: Map<QuestionID, PendingEntry>
  closed: boolean
}

// Service

export interface Interface {
  readonly ask: (input: {
    sessionID: SessionID
    questions: ReadonlyArray<Info>
    tool?: Tool
  }) => Effect.Effect<ReadonlyArray<Answer>, RejectedError>
  readonly reply: (input: {
    requestID: QuestionID
    answers: ReadonlyArray<Answer>
  }) => Effect.Effect<void, NotFoundError>
  readonly reject: (requestID: QuestionID) => Effect.Effect<void, NotFoundError>
  readonly list: () => Effect.Effect<ReadonlyArray<Request>>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/Question") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const events = yield* EventV2Bridge.Service
    const state = yield* InstanceState.make<State>(
      Effect.fn("Question.state")(function* () {
        const state = {
          pending: new Map<QuestionID, PendingEntry>(),
          closed: false,
        }

        yield* Effect.addFinalizer(() =>
          Effect.uninterruptible(
            Effect.gen(function* () {
              const cancelled = yield* Effect.sync(() => {
                state.closed = true
                const entries = [...state.pending.values()]
                state.pending.clear()
                return entries
              })
              for (const item of cancelled) {
                yield* Deferred.fail(item.deferred, new RejectedError())
              }
            }),
          ),
        )

        return state
      }),
    )

    const ask = Effect.fn("Question.ask")(function* (input: {
      sessionID: SessionID
      questions: ReadonlyArray<Info>
      tool?: Tool
    }) {
      const current = yield* InstanceState.get(state)
      if (current.closed) return yield* new RejectedError()
      const pending = current.pending
      const id = QuestionID.ascending()
      const deferred = yield* Deferred.make<ReadonlyArray<Answer>, RejectedError>()
      const info: Request = {
        id,
        sessionID: input.sessionID,
        questions: input.questions,
        tool: input.tool,
      }
      const entry: PendingEntry = { info, deferred }
      return yield* Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const admitted = yield* Effect.sync(() => {
            if (current.closed) return false
            pending.set(id, entry)
            return true
          })
          if (!admitted) return yield* new RejectedError()
          return yield* restore(
            Effect.gen(function* () {
              if (yield* Effect.sync(() => current.closed)) return yield* new RejectedError()
              yield* Effect.logInfo("asking", { id, questions: input.questions.length })
              yield* Effect.raceFirst(events.publish(Event.Asked, info), Deferred.await(deferred))
              return yield* Deferred.await(deferred)
            }),
          ).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                if (pending.get(id) === entry) pending.delete(id)
              }),
            ),
          )
        }),
      )
    })

    const reply = Effect.fn("Question.reply")(function* (input: {
      requestID: QuestionID
      answers: ReadonlyArray<Answer>
    }) {
      const pending = (yield* InstanceState.get(state)).pending
      const existing = yield* Effect.uninterruptible(
        Effect.gen(function* () {
          const entry = yield* Effect.sync(() => {
            const item = pending.get(input.requestID)
            if (!item) return
            pending.delete(input.requestID)
            return item
          })
          if (!entry) return
          yield* Deferred.succeed(entry.deferred, input.answers)
          return entry
        }),
      )
      if (!existing) {
        yield* Effect.logWarning("reply for unknown request", { requestID: input.requestID })
        return yield* new NotFoundError({ requestID: input.requestID })
      }
      yield* Effect.logInfo("replied", { requestID: input.requestID, answers: input.answers })
      yield* events.publish(Event.Replied, {
        sessionID: existing.info.sessionID,
        requestID: existing.info.id,
        answers: input.answers.map((a) => [...a]),
      })
    })

    const reject = Effect.fn("Question.reject")(function* (requestID: QuestionID) {
      const pending = (yield* InstanceState.get(state)).pending
      const existing = yield* Effect.uninterruptible(
        Effect.gen(function* () {
          const entry = yield* Effect.sync(() => {
            const item = pending.get(requestID)
            if (!item) return
            pending.delete(requestID)
            return item
          })
          if (!entry) return
          yield* Deferred.fail(entry.deferred, new RejectedError())
          return entry
        }),
      )
      if (!existing) {
        yield* Effect.logWarning("reject for unknown request", { requestID })
        return yield* new NotFoundError({ requestID })
      }
      yield* Effect.logInfo("rejected", { requestID })
      yield* events.publish(Event.Rejected, {
        sessionID: existing.info.sessionID,
        requestID: existing.info.id,
      })
    })

    const list = Effect.fn("Question.list")(function* () {
      const pending = (yield* InstanceState.get(state)).pending
      return Array.from(pending.values(), (x) => x.info)
    })

    return Service.of({ ask, reply, reject, list })
  }),
)

export const node = LayerNode.make({ service: Service, layer: layer, deps: [EventV2Bridge.node] })

export * as Question from "."
