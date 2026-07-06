import { afterEach, describe, expect } from "bun:test"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Effect, Layer } from "effect"
import { HttpClientResponse } from "effect/unstable/http"
import { Session as SessionNs } from "@/session/session"
import { MessageV2 } from "../../src/session/message-v2"

import { MessageID, PartID, type SessionID } from "../../src/session/schema"
import * as Log from "@opencode-ai/core/util/log"
import { disposeAllInstances, TestInstance } from "../fixture/fixture"
import { testEffect } from "../lib/effect"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"
import { httpApiLayer, requestInDirectory } from "./httpapi-layer"

void Log.init({ print: false })

const it = testEffect(Layer.mergeAll(SessionNs.defaultLayer, httpApiLayer))

const model = {
  providerID: ProviderV2.ID.make("test"),
  modelID: ModelV2.ID.make("test"),
}

afterEach(async () => {
  await disposeAllInstances()
})

const withoutWatcher = <A, E, R>(effect: Effect.Effect<A, E, R>) => {
  if (process.platform !== "win32") return effect
  return Effect.acquireUseRelease(
    Effect.sync(() => {
      const previous = process.env.OPENCODE_EXPERIMENTAL_DISABLE_FILEWATCHER
      process.env.OPENCODE_EXPERIMENTAL_DISABLE_FILEWATCHER = "true"
      return previous
    }),
    () => effect,
    (previous) =>
      Effect.sync(() => {
        if (previous === undefined) delete process.env.OPENCODE_EXPERIMENTAL_DISABLE_FILEWATCHER
        else process.env.OPENCODE_EXPERIMENTAL_DISABLE_FILEWATCHER = previous
      }),
  )
}

const sessionScoped = Effect.acquireRelease(SessionNs.use.create({}), (session) =>
  SessionNs.use.remove(session.id).pipe(Effect.ignore),
)

const fill = Effect.fn("SessionMessagesTest.fill")(function* (
  sessionID: SessionID,
  count: number,
  time = (i: number) => Date.now() + i,
) {
  const session = yield* SessionNs.Service
  return yield* Effect.forEach(
    Array.from({ length: count }, (_, i) => i),
    (i) =>
      Effect.gen(function* () {
        const id = MessageID.ascending()
        yield* session.updateMessage({
          id,
          sessionID,
          role: "user",
          time: { created: time(i) },
          agent: "test",
          model,
          tools: {},
        } satisfies SessionV1.User)
        yield* session.updatePart({
          id: PartID.ascending(),
          sessionID,
          messageID: id,
          type: "text",
          text: `m${i}`,
        } satisfies SessionV1.TextPart)
        return id
      }),
  )
})

const addUser = Effect.fn("SessionMessagesTest.addUser")(function* (
  sessionID: SessionID,
  text: string,
  opts?: { replay?: boolean; replaySourceMessageID?: MessageID; syntheticContinue?: boolean },
) {
  const session = yield* SessionNs.Service
  const id = MessageID.ascending()
  const metadata = opts?.replay
    ? { compaction_replay: true, compaction_replay_source_message_id: opts.replaySourceMessageID }
    : opts?.syntheticContinue
      ? { compaction_continue: true }
      : undefined
  yield* session.updateMessage({
    id,
    sessionID,
    role: "user",
    time: { created: Date.now() },
    agent: "test",
    model,
    tools: {},
    mode: "",
  } as unknown as SessionV1.Info)
  yield* session.updatePart({
    id: PartID.ascending(),
    sessionID,
    messageID: id,
    type: "text",
    text,
    synthetic: opts?.syntheticContinue ? true : undefined,
    metadata,
  } as any)
  return id
})

type CompactedRangeBody = {
  reference: { markerID: string; tailStartID?: string; messageID?: string }
  messages: SessionV1.WithParts[]
  complete: boolean
  notice?: string
}

const addAssistant = Effect.fn("SessionMessagesTest.addAssistant")(function* (
  sessionID: SessionID,
  parentID: MessageID,
  text: string,
  opts?: { summary?: boolean; finish?: string },
) {
  const session = yield* SessionNs.Service
  const id = MessageID.ascending()
  yield* session.updateMessage({
    id,
    sessionID,
    role: "assistant",
    time: { created: Date.now() },
    parentID,
    modelID: ModelV2.ID.make("test"),
    providerID: ProviderV2.ID.make("test"),
    mode: "",
    agent: "test",
    path: { cwd: "/", root: "/" },
    cost: 0,
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    summary: opts?.summary,
    finish: opts?.finish,
  } as unknown as SessionV1.Info)
  yield* session.updatePart({
    id: PartID.ascending(),
    sessionID,
    messageID: id,
    type: "text",
    text,
  })
  return id
})

const addCompaction = Effect.fn("SessionMessagesTest.addCompaction")(function* (
  sessionID: SessionID,
  tailStartID: MessageID,
) {
  const session = yield* SessionNs.Service
  const id = MessageID.ascending()
  yield* session.updateMessage({
    id,
    sessionID,
    role: "user",
    time: { created: Date.now() },
    agent: "test",
    model,
    tools: {},
    mode: "",
  } as unknown as SessionV1.Info)
  yield* session.updatePart({
    id: PartID.ascending(),
    sessionID,
    messageID: id,
    type: "compaction",
    auto: true,
    tail_start_id: tailStartID,
  } as any)
  return id
})

function request(path: string) {
  return TestInstance.pipe(Effect.flatMap((test) => requestInDirectory(path, test.directory)))
}

function json<T>(response: HttpClientResponse.HttpClientResponse) {
  return response.json.pipe(Effect.map((body) => body as T))
}

describe("session messages endpoint", () => {
  it.instance(
    "returns cursor headers for older pages",
    withoutWatcher(
      Effect.gen(function* () {
        const session = yield* sessionScoped
        const ids = yield* fill(session.id, 5)

        const a = yield* request(`/session/${session.id}/message?limit=2`)
        expect(a.status).toBe(200)
        const aBody = yield* json<SessionV1.WithParts[]>(a)
        expect(aBody.map((item) => item.info.id)).toEqual(ids.slice(-2))
        const cursor = a.headers["x-next-cursor"]
        expect(cursor).toBeTruthy()
        expect(a.headers["link"]).toContain('rel="next"')

        const b = yield* request(`/session/${session.id}/message?limit=2&before=${encodeURIComponent(cursor!)}`)
        expect(b.status).toBe(200)
        const bBody = yield* json<SessionV1.WithParts[]>(b)
        expect(bBody.map((item) => item.info.id)).toEqual(ids.slice(-4, -2))
      }),
    ),
    { git: true },
  )

  it.instance(
    "keeps full-history responses when limit is omitted",
    withoutWatcher(
      Effect.gen(function* () {
        const session = yield* sessionScoped
        const ids = yield* fill(session.id, 3)

        const res = yield* request(`/session/${session.id}/message`)
        expect(res.status).toBe(200)
        const body = yield* json<SessionV1.WithParts[]>(res)
        expect(body.map((item) => item.info.id)).toEqual(ids)
      }),
    ),
    { git: true },
  )

  it.instance(
    "rejects invalid cursors and missing sessions",
    withoutWatcher(
      Effect.gen(function* () {
        const session = yield* sessionScoped

        const bad = yield* request(`/session/${session.id}/message?limit=2&before=bad`)
        expect(bad.status).toBe(400)

        const miss = yield* request(`/session/ses_missing/message?limit=2`)
        expect(miss.status).toBe(404)
      }),
    ),
    { git: true },
  )

  it.instance(
    "does not truncate large legacy limit requests",
    withoutWatcher(
      Effect.gen(function* () {
        const session = yield* sessionScoped
        yield* fill(session.id, 520)

        const res = yield* request(`/session/${session.id}/message?limit=510`)
        expect(res.status).toBe(200)
        const body = yield* json<SessionV1.WithParts[]>(res)
        expect(body).toHaveLength(510)
      }),
    ),
    { git: true },
  )

  it.instance(
    "accepts directory query used by workspace routing",
    withoutWatcher(
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        const session = yield* sessionScoped
        yield* fill(session.id, 1)

        const res = yield* request(
          `/session/${session.id}/message?limit=80&directory=${encodeURIComponent(tmp.directory)}`,
        )
        expect(res.status).toBe(200)
        const body = yield* json<unknown[]>(res)
        expect(Array.isArray(body)).toBe(true)
        expect(body).toHaveLength(1)
      }),
    ),
    { git: true },
  )

  it.instance(
    "returns row-local compacted range for latest compaction marker",
    withoutWatcher(
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        const session = yield* sessionScoped

        const start1 = yield* addUser(session.id, "first prompt")
        yield* addAssistant(session.id, start1, "first raw work", { finish: "tool-calls" })
        const compact1 = yield* addCompaction(session.id, start1)
        yield* addAssistant(session.id, compact1, "first summary", { summary: true, finish: "end_turn" })
        const replay1 = yield* addUser(session.id, "first replay", { replay: true, replaySourceMessageID: start1 })
        yield* addAssistant(session.id, replay1, "first final", { finish: "end_turn" })

        const start2 = yield* addUser(session.id, "second prompt")
        yield* addAssistant(session.id, start2, "second raw work", { finish: "tool-calls" })
        const compact2 = yield* addCompaction(session.id, start2)
        const summary2 = yield* addAssistant(session.id, compact2, "second summary", { summary: true, finish: "end_turn" })
        const replay2 = yield* addUser(session.id, "second replay", { replay: true, replaySourceMessageID: start2 })
        yield* addAssistant(session.id, replay2, "second final", { finish: "end_turn" })

        const start3 = yield* addUser(session.id, "third prompt")
        const pre3 = yield* addAssistant(session.id, start3, "third raw work", { finish: "tool-calls" })
        const compact3 = yield* addCompaction(session.id, start3)
        yield* addAssistant(session.id, compact3, "third summary", { summary: true, finish: "end_turn" })
        const replay3 = yield* addUser(session.id, "third replay", { replay: true, replaySourceMessageID: start3 })
        yield* addAssistant(session.id, replay3, "third final", { finish: "end_turn" })

        const res = yield* requestInDirectory(
          `/session/${session.id}/compacted_range?marker=${encodeURIComponent(compact3)}&tail_start_id=${encodeURIComponent(start3)}&message_id=${encodeURIComponent(compact3)}`,
          tmp.directory,
        )
        expect(res.status).toBe(200)
        const body = (yield* res.json) as CompactedRangeBody
        const ids = body.messages.map((message) => message.info.id)

        expect(body.reference).toEqual({ markerID: compact3, tailStartID: start3, messageID: compact3 })
        expect(body.complete).toBe(true)
        expect(ids).toContain(summary2)
        expect(ids).toContain(replay2)
        expect(ids).toContain(start3)
        expect(ids).toContain(pre3)
        expect(ids).not.toContain(start1)
        expect(ids).not.toContain(start2)
        expect(ids).not.toContain(replay1)
        expect(ids).not.toContain(replay3)
      }),
    ),
    { git: true },
  )

  it.instance(
    "fails closed for marker-present compacted ranges without a derivable turn",
    withoutWatcher(
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        const session = yield* sessionScoped

        const leakedPrefix = yield* addUser(session.id, "must not leak")
        const nonCompactionMarker = yield* addUser(session.id, "not a compaction marker")

        const nonCompaction = yield* requestInDirectory(
          `/session/${session.id}/compacted_range?marker=${encodeURIComponent(nonCompactionMarker)}&tail_start_id=${encodeURIComponent(leakedPrefix)}&message_id=${encodeURIComponent(nonCompactionMarker)}`,
          tmp.directory,
        )
        expect(nonCompaction.status).toBe(200)
        const nonCompactionBody = (yield* nonCompaction.json) as CompactedRangeBody
        expect(nonCompactionBody.reference).toEqual({
          markerID: nonCompactionMarker,
          tailStartID: leakedPrefix,
          messageID: nonCompactionMarker,
        })
        expect(nonCompactionBody.messages.map((message) => message.info.id)).toEqual([])
        expect(nonCompactionBody.complete).toBe(false)
        expect(nonCompactionBody.notice).toBe("Requested marker is not a compaction message.")

        const orphanCompaction = yield* addCompaction(session.id, leakedPrefix)
        const orphan = yield* requestInDirectory(
          `/session/${session.id}/compacted_range?marker=${encodeURIComponent(orphanCompaction)}&tail_start_id=${encodeURIComponent(leakedPrefix)}&message_id=${encodeURIComponent(orphanCompaction)}`,
          tmp.directory,
        )
        expect(orphan.status).toBe(200)
        const orphanBody = (yield* orphan.json) as CompactedRangeBody
        expect(orphanBody.reference).toEqual({
          markerID: orphanCompaction,
          tailStartID: leakedPrefix,
          messageID: orphanCompaction,
        })
        expect(orphanBody.messages.map((message) => message.info.id)).toEqual([])
        expect(orphanBody.complete).toBe(false)
        expect(orphanBody.notice).toBe("Compaction marker did not belong to a complete derived compaction turn.")
      }),
    ),
    { git: true },
  )

  it.instance(
    "excludes synthetic continue prompts from compacted range continuity",
    withoutWatcher(
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        const session = yield* sessionScoped

        const start1 = yield* addUser(session.id, "first prompt")
        const compact1 = yield* addCompaction(session.id, start1)
        const summary1 = yield* addAssistant(session.id, compact1, "first summary", { summary: true, finish: "end_turn" })
        const synthetic1 = yield* addUser(session.id, "synthetic continue", { syntheticContinue: true })
        const replay1 = yield* addUser(session.id, "first replay", { replay: true, replaySourceMessageID: start1 })
        yield* addAssistant(session.id, replay1, "first final", { finish: "end_turn" })

        const start2 = yield* addUser(session.id, "second prompt")
        const compact2 = yield* addCompaction(session.id, start2)
        yield* addAssistant(session.id, compact2, "second summary", { summary: true, finish: "end_turn" })
        const replay2 = yield* addUser(session.id, "second replay", { replay: true, replaySourceMessageID: start2 })
        yield* addAssistant(session.id, replay2, "second final", { finish: "end_turn" })

        const res = yield* requestInDirectory(
          `/session/${session.id}/compacted_range?marker=${encodeURIComponent(compact2)}&tail_start_id=${encodeURIComponent(start2)}&message_id=${encodeURIComponent(compact2)}`,
          tmp.directory,
        )
        expect(res.status).toBe(200)
        const body = (yield* res.json) as CompactedRangeBody
        const ids = body.messages.map((message) => message.info.id)

        expect(body.complete).toBe(true)
        expect(ids).toContain(summary1)
        expect(ids).toContain(replay1)
        expect(ids).toContain(start2)
        expect(ids).not.toContain(synthetic1)
        expect(ids).not.toContain(start1)
        expect(ids).not.toContain(replay2)
      }),
    ),
    { git: true },
  )
})
