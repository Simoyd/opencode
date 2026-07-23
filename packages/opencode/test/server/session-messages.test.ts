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
import { Database } from "@opencode-ai/core/database/database"
import { CompactionRegionTable } from "@opencode-ai/core/session/sql"
import { eq } from "drizzle-orm"

void Log.init({ print: false })

const it = testEffect(Layer.mergeAll(Database.defaultLayer, SessionNs.defaultLayer, httpApiLayer))

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
  opts?: {
    replay?: boolean
    replaySourceMessageID?: MessageID
    syntheticContinue?: boolean
    ownerMarkerID?: MessageID
    id?: MessageID
    created?: number
  },
) {
  const session = yield* SessionNs.Service
  const id = opts?.id ?? MessageID.ascending()
  const metadata = opts?.replay
    ? {
        compaction_replay: true,
        compaction_owner_marker_id: opts.ownerMarkerID,
        compaction_replay_source_message_id: opts.replaySourceMessageID,
      }
    : opts?.syntheticContinue
      ? { compaction_continue: true, compaction_owner_marker_id: opts.ownerMarkerID }
      : undefined
  yield* session.updateMessage({
    id,
    sessionID,
    role: "user",
    time: { created: opts?.created ?? Date.now() },
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

const addAssistant = Effect.fn("SessionMessagesTest.addAssistant")(function* (
  sessionID: SessionID,
  parentID: MessageID,
  text: string,
  opts?: { summary?: boolean; finish?: string; id?: MessageID; created?: number; reasoning?: string },
) {
  const session = yield* SessionNs.Service
  const id = opts?.id ?? MessageID.ascending()
  yield* session.updateMessage({
    id,
    sessionID,
    role: "assistant",
    time: { created: opts?.created ?? Date.now() },
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
  if (opts?.reasoning) {
    yield* session.updatePart({
      id: PartID.ascending(),
      sessionID,
      messageID: id,
      type: "reasoning",
      text: opts.reasoning,
      time: { start: Date.now(), end: Date.now() },
    })
  }
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
  opts?: { auto?: boolean; id?: MessageID; created?: number },
) {
  const session = yield* SessionNs.Service
  const id = opts?.id ?? MessageID.ascending()
  yield* session.updateMessage({
    id,
    sessionID,
    role: "user",
    time: { created: opts?.created ?? Date.now() },
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
    auto: opts?.auto ?? true,
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

describe("session messages and compaction catalog", () => {
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
    "publishes metadata-only completed regions with persisted 80-code-unit preview",
    withoutWatcher(
      Effect.gen(function* () {
        const session = yield* sessionScoped
        const prompt = yield* addUser(session.id, "catalog prompt")
        const answer = yield* addAssistant(session.id, prompt, "catalog answer", { finish: "end_turn" })
        const marker = yield* addCompaction(session.id, prompt, { auto: false })
        const longSummary = "word ".repeat(30)
        const summary = yield* addAssistant(session.id, marker, longSummary, { summary: true, finish: "end_turn" })

        const response = yield* request(`/session/${session.id}/compaction`)
        expect(response.status).toBe(200)
        const page = yield* json<{
          items: Array<{
            startMessageID: string
            markerID: string
            endExclusiveCursor: string
            physicalMessageCount: number
            semanticMessageCount: number
            summaryMessageID: string
            summaryPreview: string
          }>
          nextCursor?: string
        }>(response)
        expect(page.items).toHaveLength(1)
        const descriptor = page.items[0]!
        expect(descriptor.startMessageID).toBe(prompt)
        expect(descriptor.markerID).toBe(marker)
        expect(descriptor.summaryMessageID).toBe(summary)
        expect(descriptor.summaryPreview).toHaveLength(80)
        expect(descriptor.summaryPreview.endsWith("...")).toBe(true)
        expect(descriptor.physicalMessageCount).toBe(2)
        expect(descriptor.semanticMessageCount).toBe(2)
        expect(page.nextCursor).toBeUndefined()

        const ordinary = yield* request(
          `/session/${session.id}/message?limit=50&before=${encodeURIComponent(descriptor.endExclusiveCursor)}`,
        )
        const messages = yield* json<SessionV1.WithParts[]>(ordinary)
        expect(messages.map((message) => message.info.id)).toEqual([prompt, answer])
      }),
    ),
    { git: true },
  )

  it.instance(
    "paginates every completed region in fixed pages of 50",
    withoutWatcher(
      Effect.gen(function* () {
        const session = yield* sessionScoped
        const markers: MessageID[] = []
        for (let index = 0; index < 51; index++) {
          const prompt = yield* addUser(session.id, `prompt ${index}`)
          yield* addAssistant(session.id, prompt, `answer ${index}`, { finish: "end_turn" })
          const marker = yield* addCompaction(session.id, prompt, { auto: false })
          yield* addAssistant(session.id, marker, `summary ${index}`, { summary: true, finish: "end_turn" })
          markers.push(marker)
        }

        const firstResponse = yield* request(`/session/${session.id}/compaction`)
        const first = yield* json<{ items: Array<{ markerID: MessageID }>; nextCursor?: string }>(firstResponse)
        expect(first.items).toHaveLength(50)
        expect(first.items.map((item) => item.markerID)).toEqual(markers.slice(0, 50))
        expect(first.nextCursor).toBeTruthy()

        const secondResponse = yield* request(
          `/session/${session.id}/compaction?cursor=${encodeURIComponent(first.nextCursor!)}`,
        )
        const second = yield* json<{
          items: Array<{ markerID: MessageID; precedingSummaryMessageID?: MessageID }>
          nextCursor?: string
        }>(secondResponse)
        expect(second.items).toHaveLength(1)
        expect(second.items[0]!.markerID).toBe(markers[50])
        expect(second.items[0]!.precedingSummaryMessageID).toBeTruthy()
        expect(second.nextCursor).toBeUndefined()
      }),
    ),
    { git: true },
  )

  it.instance(
    "stores normalized previews at and above the 80-code-unit boundary",
    withoutWatcher(
      Effect.gen(function* () {
        const session = yield* sessionScoped
        const exactPrompt = yield* addUser(session.id, "exact preview")
        const exactMarker = yield* addCompaction(session.id, exactPrompt, { auto: false })
        yield* addAssistant(session.id, exactMarker, "x".repeat(80), { summary: true, finish: "end_turn" })
        const longPrompt = yield* addUser(session.id, "long preview")
        const longMarker = yield* addCompaction(session.id, longPrompt, { auto: false })
        yield* addAssistant(session.id, longMarker, `  ${"y".repeat(81)}  `, { summary: true, finish: "end_turn" })

        const response = yield* request(`/session/${session.id}/compaction`)
        const page = yield* json<{ items: Array<{ summaryPreview: string }> }>(response)
        expect(page.items.map((item) => item.summaryPreview)).toEqual(["x".repeat(80), `${"y".repeat(77)}...`])
      }),
    ),
    { git: true },
  )

  it.instance(
    "classifies owned replay once while hiding summary and continuation protocol rows",
    withoutWatcher(
      Effect.gen(function* () {
        const session = yield* sessionScoped
        const service = yield* SessionNs.Service
        const source = yield* addUser(session.id, "original prompt")
        yield* addAssistant(session.id, source, "original answer", { finish: "end_turn" })
        const firstMarker = yield* addCompaction(session.id, source, { auto: false })
        yield* addAssistant(session.id, firstMarker, "first summary", { summary: true, finish: "end_turn" })
        yield* addUser(session.id, "replayed prompt", {
          replay: true,
          replaySourceMessageID: source,
          ownerMarkerID: firstMarker,
        })
        yield* addUser(session.id, "duplicate replay", {
          replay: true,
          replaySourceMessageID: source,
          ownerMarkerID: firstMarker,
        })
        const continuation = yield* addUser(session.id, "continue", {
          syntheticContinue: true,
          ownerMarkerID: firstMarker,
        })
        const zeroPart = MessageID.ascending()
        yield* service.updateMessage({
          id: zeroPart,
          sessionID: session.id,
          role: "user",
          time: { created: Date.now() },
          agent: "test",
          model,
          tools: {},
        } satisfies SessionV1.User)
        yield* addAssistant(session.id, continuation, "continued output", { finish: "end_turn" })
        const secondMarker = yield* addCompaction(session.id, zeroPart, { auto: false })
        yield* addAssistant(session.id, secondMarker, "second summary", { summary: true, finish: "end_turn" })

        const response = yield* request(`/session/${session.id}/compaction`)
        const page = yield* json<{
          items: Array<{
            markerID: MessageID
            physicalMessageCount: number
            semanticMessageCount: number
            partCount: number
          }>
        }>(response)
        const second = page.items.find((item) => item.markerID === secondMarker)!
        expect(second.physicalMessageCount).toBe(6)
        expect(second.semanticMessageCount).toBe(3)
        expect(second.partCount).toBe(5)

        yield* service.removeMessage({ sessionID: session.id, messageID: source })
        const afterDelete = yield* request(`/session/${session.id}/compaction`).pipe(
          Effect.flatMap(
            json<{
              items: Array<{
                markerID: MessageID
                semanticMessageCount: number
              }>
            }>,
          ),
        )
        expect(afterDelete.items.find((item) => item.markerID === secondMarker)?.semanticMessageCount).toBe(2)
      }),
    ),
    { git: true },
  )

  it.instance(
    "rejects contradictory protocol ownership without publishing a partial region",
    withoutWatcher(
      Effect.gen(function* () {
        const session = yield* sessionScoped
        const source = yield* addUser(session.id, "source")
        const firstMarker = yield* addCompaction(session.id, source, { auto: false })
        yield* addAssistant(session.id, firstMarker, "first summary", { summary: true, finish: "end_turn" })
        yield* addUser(session.id, "bad continuation", {
          syntheticContinue: true,
          ownerMarkerID: MessageID.ascending(),
        })
        const secondMarker = yield* addCompaction(session.id, source, { auto: false })
        const attempted = yield* Effect.exit(
          addAssistant(session.id, secondMarker, "must not publish", { summary: true, finish: "end_turn" }),
        )
        expect(attempted._tag).toBe("Failure")

        const response = yield* request(`/session/${session.id}/compaction`)
        const page = yield* json<{ items: Array<{ markerID: MessageID }> }>(response)
        expect(page.items.map((item) => item.markerID)).toEqual([firstMarker])
      }),
    ),
    { git: true },
  )

  it.instance(
    "retires metadata when its canonical summary is removed",
    withoutWatcher(
      Effect.gen(function* () {
        const session = yield* sessionScoped
        const prompt = yield* addUser(session.id, "retire region")
        const marker = yield* addCompaction(session.id, prompt, { auto: false })
        const summary = yield* addAssistant(session.id, marker, "summary", { summary: true, finish: "end_turn" })
        const before = yield* request(`/session/${session.id}/compaction`).pipe(
          Effect.flatMap(json<{ items: unknown[] }>),
        )
        expect(before.items).toHaveLength(1)

        const service = yield* SessionNs.Service
        yield* service.removeMessage({ sessionID: session.id, messageID: summary })

        const after = yield* request(`/session/${session.id}/compaction`).pipe(
          Effect.flatMap(json<{ items: unknown[] }>),
        )
        expect(after.items).toEqual([])
      }),
    ),
    { git: true },
  )

  it.instance(
    "cascades compact-region metadata when the session is removed",
    withoutWatcher(
      Effect.gen(function* () {
        const session = yield* SessionNs.Service
        const db = (yield* Database.Service).db
        const info = yield* session.create({})
        const prompt = yield* addUser(info.id, "cascade region")
        const marker = yield* addCompaction(info.id, prompt, { auto: false })
        yield* addAssistant(info.id, marker, "summary", { summary: true, finish: "end_turn" })

        expect(
          yield* db.select().from(CompactionRegionTable).where(eq(CompactionRegionTable.session_id, info.id)).all(),
        ).toHaveLength(1)

        yield* session.remove(info.id)

        expect(
          yield* db.select().from(CompactionRegionTable).where(eq(CompactionRegionTable.session_id, info.id)).all(),
        ).toEqual([])
      }),
    ),
    { git: true },
  )
})
