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
import { CompactionArchiveManifestTable, TranscriptWindowStateTable } from "@opencode-ai/core/session/sql"
import { SessionTranscriptIndex } from "@/session/transcript-index"
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
    id?: MessageID
    created?: number
  },
) {
  const session = yield* SessionNs.Service
  const id = opts?.id ?? MessageID.ascending()
  const metadata = opts?.replay
    ? { compaction_replay: true, compaction_replay_source_message_id: opts.replaySourceMessageID }
    : opts?.syntheticContinue
      ? { compaction_continue: true }
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

type CompactedRangeBody = {
  reference: { markerID: string; tailStartID?: string; messageID?: string; sourceMessageID?: string }
  messages: SessionV1.WithParts[]
  turns: Array<{
    startMessageID: string
    intermediateMessageIDs: string[]
    finalOutputMessageID?: string
    status: "complete" | "incomplete"
  }>
  complete: boolean
  notice?: string
  status: "complete" | "stale" | "unavailable" | "too_large"
}

const addAssistant = Effect.fn("SessionMessagesTest.addAssistant")(function* (
  sessionID: SessionID,
  parentID: MessageID,
  text: string,
  opts?: { summary?: boolean; finish?: string; id?: MessageID; created?: number },
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

describe("session messages endpoint", () => {
  it.instance(
    "indexes a completed manual compaction before returning its transcript window",
    withoutWatcher(
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        const session = yield* sessionScoped
        const start = yield* addUser(session.id, "manual source")
        const work = yield* addAssistant(session.id, start, "manual work", { finish: "tool-calls" })
        const final = yield* addAssistant(session.id, start, "manual answer", { finish: "end_turn" })
        const marker = yield* addCompaction(session.id, start, { auto: false })
        const summary = yield* addAssistant(session.id, marker, "manual summary", {
          summary: true,
          finish: "end_turn",
        })

        const response = yield* requestInDirectory(`/session/${session.id}/transcript_window`, tmp.directory)
        const window = (yield* response.json) as {
          status: string
          sourceGeneration: string
          archiveDescriptors: Array<{
            archiveID: string
            archiveRevision: string
            markerID: string
            sourceMessageID: string
          }>
          tail: SessionV1.WithParts[]
        }

        expect(window.status).toBe("complete")
        expect(window.archiveDescriptors).toHaveLength(1)
        expect(window.archiveDescriptors[0]).toEqual(
          expect.objectContaining({ markerID: marker, sourceMessageID: start }),
        )
        expect(window.tail.map((message) => message.info.id)).toEqual([summary])

        const descriptor = window.archiveDescriptors[0]!
        const recall = yield* requestInDirectory(
          `/session/${session.id}/compacted_range?marker=${encodeURIComponent(marker)}&tail_start_id=${encodeURIComponent(start)}&message_id=${encodeURIComponent(marker)}&source_generation=${encodeURIComponent(window.sourceGeneration)}&archive_id=${encodeURIComponent(descriptor.archiveID)}&archive_revision=${encodeURIComponent(descriptor.archiveRevision)}&source_message_id=${encodeURIComponent(start)}`,
          tmp.directory,
        )
        const range = (yield* recall.json) as CompactedRangeBody
        expect(range.status).toBe("complete")
        expect(range.messages.map((message) => message.info.id)).toEqual([start, work, final])
        expect(range.turns).toEqual([
          expect.objectContaining({
            startMessageID: start,
            intermediateMessageIDs: [work],
            finalOutputMessageID: final,
            status: "complete",
          }),
        ])
      }),
    ),
    { git: true },
  )

  it.instance(
    "declares exact empty transcript window body counts",
    withoutWatcher(
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        const session = yield* sessionScoped

        const complete = yield* requestInDirectory(`/session/${session.id}/transcript_window`, tmp.directory)
        const completeBody = (yield* complete.json) as {
          status: string
          tail: unknown[]
          turns: unknown[]
          counts: { textUnits: number; decodedBytes: number; turnTextUnits: number; turnDecodedBytes: number }
        }
        expect(completeBody.status).toBe("complete")
        expect(completeBody.tail).toEqual([])
        expect(completeBody.turns).toEqual([])
        expect(completeBody.counts.textUnits).toBe(JSON.stringify(completeBody.tail).length)
        expect(completeBody.counts.decodedBytes).toBe(Buffer.byteLength(JSON.stringify(completeBody.tail), "utf8"))
        expect(completeBody.counts.turnTextUnits).toBe(JSON.stringify(completeBody.turns).length)
        expect(completeBody.counts.turnDecodedBytes).toBe(Buffer.byteLength(JSON.stringify(completeBody.turns), "utf8"))

        const { db } = yield* Database.Service
        yield* db
          .update(TranscriptWindowStateTable)
          .set({ text_units: 3 })
          .where(eq(TranscriptWindowStateTable.session_id, session.id))
          .run()
          .pipe(Effect.orDie)
        const stale = yield* requestInDirectory(`/session/${session.id}/transcript_window`, tmp.directory)
        const staleBody = (yield* stale.json) as typeof completeBody
        expect(staleBody.status).toBe("stale")
        expect(staleBody.tail).toEqual([])
        expect(staleBody.turns).toEqual([])
        expect(staleBody.counts.textUnits).toBe(2)
        expect(staleBody.counts.decodedBytes).toBe(2)
        expect(staleBody.counts.turnTextUnits).toBe(2)
        expect(staleBody.counts.turnDecodedBytes).toBe(2)
      }),
    ),
    { git: true },
  )

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
    "resumes bounded legacy transcript indexing without publishing a partial window",
    withoutWatcher(
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        const session = yield* sessionScoped
        yield* fill(session.id, 257)
        const { db } = yield* Database.Service
        yield* db
          .delete(CompactionArchiveManifestTable)
          .where(eq(CompactionArchiveManifestTable.session_id, session.id))
          .run()
          .pipe(Effect.orDie)
        yield* db
          .delete(TranscriptWindowStateTable)
          .where(eq(TranscriptWindowStateTable.session_id, session.id))
          .run()
          .pipe(Effect.orDie)

        const ownerID = crypto.randomUUID()
        const interrupted = yield* SessionTranscriptIndex.run({ sessionID: session.id, ownerID, maxBatches: 1 })
        expect(interrupted.complete).toBe(false)

        const during = yield* requestInDirectory(`/session/${session.id}/transcript_window`, tmp.directory)
        const duringBody = (yield* during.json) as {
          status: string
          tail: unknown[]
          turns: unknown[]
          counts: { textUnits: number; decodedBytes: number; turnTextUnits: number; turnDecodedBytes: number }
        }
        expect(duringBody.status).toBe("indexing")
        expect(duringBody.tail).toEqual([])
        expect(duringBody.turns).toEqual([])
        expect(duringBody.counts.textUnits).toBe(2)
        expect(duringBody.counts.decodedBytes).toBe(2)
        expect(duringBody.counts.turnTextUnits).toBe(2)
        expect(duringBody.counts.turnDecodedBytes).toBe(2)

        const resumed = yield* SessionTranscriptIndex.run({ sessionID: session.id, ownerID })
        expect(resumed.resumed).toBe(true)
        expect(resumed.complete).toBe(true)

        const complete = yield* requestInDirectory(`/session/${session.id}/transcript_window`, tmp.directory)
        const completeBody = (yield* complete.json) as { status: string; tail: SessionV1.WithParts[] }
        expect(completeBody.status).toBe("complete")
        expect(completeBody.tail).toHaveLength(257)
      }),
    ),
    { git: true },
  )

  it.instance(
    "indexes and recalls legacy transcript ranges by created order instead of lexical message id",
    withoutWatcher(
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        const session = yield* sessionScoped
        const suffix = crypto.randomUUID().replaceAll("-", "")
        const id = (prefix: string) => MessageID.make(`msg_${prefix}_${suffix}`)
        const created = Date.now() - 10_000

        const start = yield* addUser(session.id, "legacy prompt", { id: id("z"), created })
        const work = yield* addAssistant(session.id, start, "legacy work", {
          id: id("y"),
          created: created + 1,
          finish: "tool-calls",
        })
        const firstMarker = yield* addCompaction(session.id, start, { id: id("x"), created: created + 2 })
        const firstSummary = yield* addAssistant(session.id, firstMarker, "first legacy summary", {
          id: id("w"),
          created: created + 3,
          summary: true,
          finish: "end_turn",
        })
        const laterWork = yield* addAssistant(session.id, start, "later legacy work", {
          id: id("v"),
          created: created + 4,
          finish: "tool-calls",
        })
        const marker = yield* addCompaction(session.id, start, { id: id("u"), created: created + 5 })
        yield* addAssistant(session.id, marker, "final legacy summary", {
          id: id("t"),
          created: created + 6,
          summary: true,
          finish: "end_turn",
        })
        const replay = yield* addUser(session.id, "legacy replay", {
          id: id("s"),
          created: created + 7,
          replay: true,
          replaySourceMessageID: start,
        })
        yield* addAssistant(session.id, replay, "legacy final", {
          id: id("r"),
          created: created + 8,
          finish: "end_turn",
        })

        const { db } = yield* Database.Service
        yield* db
          .delete(CompactionArchiveManifestTable)
          .where(eq(CompactionArchiveManifestTable.session_id, session.id))
          .run()
          .pipe(Effect.orDie)
        yield* db
          .delete(TranscriptWindowStateTable)
          .where(eq(TranscriptWindowStateTable.session_id, session.id))
          .run()
          .pipe(Effect.orDie)

        const indexed = yield* SessionTranscriptIndex.run({ sessionID: session.id, ownerID: crypto.randomUUID() })
        expect(indexed.complete).toBe(true)

        const windowResponse = yield* requestInDirectory(`/session/${session.id}/transcript_window`, tmp.directory)
        const window = (yield* windowResponse.json) as {
          status: string
          sourceGeneration: string
          archiveDescriptors: Array<{
            archiveID: string
            archiveRevision: string
            markerID: string
            sourceMessageID: string
          }>
        }
        expect(window.status).toBe("complete")
        expect(window.archiveDescriptors).toHaveLength(1)
        const descriptor = window.archiveDescriptors[0]!

        const recall = yield* requestInDirectory(
          `/session/${session.id}/compacted_range?marker=${encodeURIComponent(marker)}&tail_start_id=${encodeURIComponent(start)}&message_id=${encodeURIComponent(marker)}&source_generation=${encodeURIComponent(window.sourceGeneration)}&archive_id=${encodeURIComponent(descriptor.archiveID)}&archive_revision=${encodeURIComponent(descriptor.archiveRevision)}&source_message_id=${encodeURIComponent(start)}`,
          tmp.directory,
        )
        const body = (yield* recall.json) as CompactedRangeBody
        expect(body.status).toBe("complete")
        expect(body.complete).toBe(true)
        expect(body.messages.map((message) => message.info.id)).toEqual([
          start,
          work,
          firstMarker,
          firstSummary,
          laterWork,
        ])
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
        const pre1 = yield* addAssistant(session.id, start1, "first raw work", { finish: "tool-calls" })
        const compact1 = yield* addCompaction(session.id, start1)
        yield* addAssistant(session.id, compact1, "first summary", { summary: true, finish: "end_turn" })
        const replay1 = yield* addUser(session.id, "first replay", { replay: true, replaySourceMessageID: start1 })
        yield* addAssistant(session.id, replay1, "first final", { finish: "end_turn" })

        const start2 = yield* addUser(session.id, "second prompt")
        yield* addAssistant(session.id, start2, "second raw work", { finish: "tool-calls" })
        const compact2 = yield* addCompaction(session.id, start2)
        const summary2 = yield* addAssistant(session.id, compact2, "second summary", {
          summary: true,
          finish: "end_turn",
        })
        const replay2 = yield* addUser(session.id, "second replay", { replay: true, replaySourceMessageID: start2 })
        yield* addAssistant(session.id, replay2, "second final", { finish: "end_turn" })

        const start3 = yield* addUser(session.id, "third prompt")
        const pre3 = yield* addAssistant(session.id, start3, "third raw work", { finish: "tool-calls" })
        const compact3 = yield* addCompaction(session.id, start3)
        yield* addAssistant(session.id, compact3, "third summary", { summary: true, finish: "end_turn" })
        const replay3 = yield* addUser(session.id, "third replay", { replay: true, replaySourceMessageID: start3 })
        yield* addAssistant(session.id, replay3, "third final", { finish: "end_turn" })

        const start4 = yield* addUser(session.id, "fourth prompt")
        yield* addAssistant(session.id, start4, "fourth final", { finish: "end_turn" })

        const { db } = yield* Database.Service
        yield* db
          .delete(CompactionArchiveManifestTable)
          .where(eq(CompactionArchiveManifestTable.session_id, session.id))
          .run()
          .pipe(Effect.orDie)
        yield* db
          .delete(TranscriptWindowStateTable)
          .where(eq(TranscriptWindowStateTable.session_id, session.id))
          .run()
          .pipe(Effect.orDie)
        const indexed = yield* SessionTranscriptIndex.run({ sessionID: session.id, ownerID: crypto.randomUUID() })
        expect(indexed.complete).toBe(true)

        const res = yield* requestInDirectory(
          `/session/${session.id}/compacted_range?marker=${encodeURIComponent(compact3)}&tail_start_id=${encodeURIComponent(start3)}&message_id=${encodeURIComponent(compact3)}`,
          tmp.directory,
        )
        expect(res.status).toBe(200)
        const body = (yield* res.json) as CompactedRangeBody
        const ids = body.messages.map((message) => message.info.id)

        expect(body.reference).toEqual({
          markerID: compact3,
          tailStartID: start3,
          messageID: compact3,
          sourceMessageID: start3,
        })
        expect(body.complete).toBe(true)
        expect(ids).toContain(summary2)
        expect(ids).toContain(replay2)
        expect(ids).toContain(start3)
        expect(ids).toContain(pre3)
        expect(ids).not.toContain(start1)
        expect(ids).not.toContain(start2)
        expect(ids).not.toContain(replay1)
        expect(ids).not.toContain(replay3)

        const windowResponse = yield* requestInDirectory(`/session/${session.id}/transcript_window`, tmp.directory)
        expect(windowResponse.status).toBe(200)
        const window = (yield* windowResponse.json) as {
          status: string
          sourceGeneration: string
          archiveDescriptors: Array<{
            archiveID: string
          archiveRevision: string
          markerID: string
          sourceMessageID: string
          messageCount: number
          partCount: number
          textUnits: number
          decodedBytes: number
          }>
          tail: SessionV1.WithParts[]
          turns: Array<{ startMessageID: string; finalOutputMessageID?: string }>
          counts: {
            textUnits: number
            decodedBytes: number
            turnTextUnits: number
            turnDecodedBytes: number
          }
        }
        expect(window.status).toBe("complete")
        expect(window.archiveDescriptors).toHaveLength(3)
        expect(window.tail.map((message) => message.info.id)).toContain(start3)
        expect(window.tail.map((message) => message.info.id)).not.toContain(start2)
        expect(window.turns).toEqual([
          expect.objectContaining({ startMessageID: start3 }),
          expect.objectContaining({ startMessageID: start4 }),
        ])
        const encodedTail = JSON.stringify(window.tail)
        const encodedTurns = JSON.stringify(window.turns)
        expect(window.counts.textUnits).toBe(encodedTail.length)
        expect(window.counts.decodedBytes).toBe(Buffer.byteLength(encodedTail, "utf8"))
        expect(window.counts.turnTextUnits).toBe(encodedTurns.length)
        expect(window.counts.turnDecodedBytes).toBe(Buffer.byteLength(encodedTurns, "utf8"))

        const firstBeforeMutation = window.archiveDescriptors[0]!
        const sessionService = yield* SessionNs.Service
        const mutationPartID = PartID.ascending()
        yield* sessionService.updatePart({
          id: mutationPartID,
          sessionID: session.id,
          messageID: pre1,
          type: "text",
          text: "first archived mutation",
        })
        const changedWindowResponse = yield* requestInDirectory(`/session/${session.id}/transcript_window`, tmp.directory)
        const changedWindow = (yield* changedWindowResponse.json) as typeof window
        const firstAfterMutation = changedWindow.archiveDescriptors[0]!
        expect(changedWindow.status).toBe("complete")
        expect(firstAfterMutation.archiveRevision).not.toBe(firstBeforeMutation.archiveRevision)
        expect(firstAfterMutation.messageCount).toBe(firstBeforeMutation.messageCount)
        expect(firstAfterMutation.partCount).toBe(firstBeforeMutation.partCount + 1)
        expect(firstAfterMutation.textUnits).toBeGreaterThan(firstBeforeMutation.textUnits)

        const staleRecall = yield* requestInDirectory(
          `/session/${session.id}/compacted_range?marker=${encodeURIComponent(compact1)}&source_generation=${encodeURIComponent(window.sourceGeneration)}&archive_id=${encodeURIComponent(firstBeforeMutation.archiveID)}&archive_revision=${encodeURIComponent(firstBeforeMutation.archiveRevision)}&source_message_id=${encodeURIComponent(start1)}`,
          tmp.directory,
        )
        expect(((yield* staleRecall.json) as CompactedRangeBody).status).toBe("stale")

        window.archiveDescriptors = changedWindow.archiveDescriptors

        yield* sessionService.removePart({ sessionID: session.id, messageID: pre1, partID: mutationPartID })
        const removedPartResponse = yield* requestInDirectory(`/session/${session.id}/transcript_window`, tmp.directory)
        const removedPartWindow = (yield* removedPartResponse.json) as typeof window
        const firstAfterPartRemoval = removedPartWindow.archiveDescriptors[0]!
        expect(firstAfterPartRemoval.archiveRevision).not.toBe(firstAfterMutation.archiveRevision)
        expect(firstAfterPartRemoval.messageCount).toBe(firstAfterMutation.messageCount)
        expect(firstAfterPartRemoval.partCount).toBe(firstBeforeMutation.partCount)

        yield* sessionService.removeMessage({ sessionID: session.id, messageID: pre1 })
        const removedMessageResponse = yield* requestInDirectory(`/session/${session.id}/transcript_window`, tmp.directory)
        const removedMessageWindow = (yield* removedMessageResponse.json) as typeof window
        const firstAfterMessageRemoval = removedMessageWindow.archiveDescriptors[0]!
        expect(firstAfterMessageRemoval.archiveRevision).not.toBe(firstAfterPartRemoval.archiveRevision)
        expect(firstAfterMessageRemoval.messageCount).toBe(firstAfterPartRemoval.messageCount - 1)
        expect(firstAfterMessageRemoval.partCount).toBe(firstAfterPartRemoval.partCount - 1)
        window.archiveDescriptors = removedMessageWindow.archiveDescriptors

        for (const [index, marker] of [compact1, compact2, compact3].entries()) {
          const indexedDescriptor = window.archiveDescriptors[index]!
          const indexedRecall = yield* requestInDirectory(
            `/session/${session.id}/compacted_range?marker=${encodeURIComponent(marker)}&source_generation=${encodeURIComponent(window.sourceGeneration)}&archive_id=${encodeURIComponent(indexedDescriptor.archiveID)}&archive_revision=${encodeURIComponent(indexedDescriptor.archiveRevision)}&source_message_id=${encodeURIComponent([start1, start2, start3][index]!)}`,
            tmp.directory,
          )
          const indexedBody = (yield* indexedRecall.json) as CompactedRangeBody
          expect(indexedBody.status).toBe("complete")
          expect(indexedBody.complete).toBe(true)
          expect(indexedBody.messages.length).toBeGreaterThan(0)
          const indexedBodyEncoded = JSON.stringify(indexedBody.messages)
          expect(indexedDescriptor.textUnits).toBe(indexedBodyEncoded.length)
          expect(indexedDescriptor.decodedBytes).toBe(Buffer.byteLength(indexedBodyEncoded, "utf8"))
        }

        const descriptor = window.archiveDescriptors[2]!
        const strong = yield* requestInDirectory(
          `/session/${session.id}/compacted_range?marker=${encodeURIComponent(compact3)}&tail_start_id=${encodeURIComponent(start3)}&message_id=${encodeURIComponent(compact3)}&source_generation=${encodeURIComponent(window.sourceGeneration)}&archive_id=${encodeURIComponent(descriptor.archiveID)}&archive_revision=${encodeURIComponent(descriptor.archiveRevision)}&source_message_id=${encodeURIComponent(start3)}`,
          tmp.directory,
        )
        const strongBody = (yield* strong.json) as CompactedRangeBody
        expect(strongBody.status).toBe("complete")
        expect(strongBody.messages.map((message) => message.info.id)).toEqual(ids)

        yield* sessionService.updatePart({
          id: PartID.ascending(),
          sessionID: session.id,
          messageID: compact3,
          type: "text",
          text: "structural mutation",
        })
        const invalidatedState = yield* db
          .select({ status: TranscriptWindowStateTable.index_status })
          .from(TranscriptWindowStateTable)
          .where(eq(TranscriptWindowStateTable.session_id, session.id))
          .get()
          .pipe(Effect.orDie)
        const invalidatedDescriptor = yield* db
          .select({ revision: CompactionArchiveManifestTable.archive_revision })
          .from(CompactionArchiveManifestTable)
          .where(eq(CompactionArchiveManifestTable.archive_id, descriptor.archiveID))
          .get()
          .pipe(Effect.orDie)
        expect(invalidatedState?.status).toBe("index_required")
        expect(String(invalidatedDescriptor?.revision)).not.toBe(descriptor.archiveRevision)
      }),
    ),
    { git: true },
  )

  it.instance(
    "preserves ordinary zero-part messages in compacted ranges",
    withoutWatcher(
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        const session = yield* sessionScoped
        const sessionService = yield* SessionNs.Service
        const start = yield* addUser(session.id, "source prompt")
        const empty = MessageID.ascending()
        yield* sessionService.updateMessage({
          id: empty,
          sessionID: session.id,
          role: "assistant",
          parentID: start,
          time: { created: Date.now() },
          modelID: ModelV2.ID.make("test"),
          providerID: ProviderV2.ID.make("test"),
          mode: "build",
          agent: "build",
          path: { cwd: "/", root: "/" },
          cost: 0,
          tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
          finish: "tool-calls",
        } satisfies SessionV1.Assistant)
        const marker = yield* addCompaction(session.id, start)
        yield* addAssistant(session.id, marker, "summary", { summary: true, finish: "end_turn" })
        const replay = yield* addUser(session.id, "replay", { replay: true, replaySourceMessageID: start })
        yield* addAssistant(session.id, replay, "final", { finish: "end_turn" })

        const windowResponse = yield* requestInDirectory(`/session/${session.id}/transcript_window`, tmp.directory)
        const window = (yield* windowResponse.json) as {
          status: string
          sourceGeneration: string
          archiveDescriptors: Array<{ archiveID: string; archiveRevision: string; markerID: string }>
        }
        const descriptor = window.archiveDescriptors.find((item) => item.markerID === marker)
        expect(window.status).toBe("complete")
        expect(descriptor).toBeDefined()

        const recall = yield* requestInDirectory(
          `/session/${session.id}/compacted_range?marker=${encodeURIComponent(marker)}&tail_start_id=${encodeURIComponent(start)}&message_id=${encodeURIComponent(marker)}&source_generation=${encodeURIComponent(window.sourceGeneration)}&archive_id=${encodeURIComponent(descriptor!.archiveID)}&archive_revision=${encodeURIComponent(descriptor!.archiveRevision)}&source_message_id=${encodeURIComponent(start)}`,
          tmp.directory,
        )
        const body = (yield* recall.json) as CompactedRangeBody
        expect(body.status).toBe("complete")
        expect(body.reference.sourceMessageID).toBe(start)
        expect(body.messages.find((message) => message.info.id === empty)).toEqual(
          expect.objectContaining({ info: expect.objectContaining({ id: empty }), parts: [] }),
        )
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
        expect(nonCompactionBody.notice).toBe("Compacted transcript range is stale or unavailable.")
        expect(nonCompactionBody.status).toBe("stale")

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
        expect(orphanBody.notice).toBe("Compacted transcript range is stale or unavailable.")
        expect(orphanBody.status).toBe("stale")
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
        const summary1 = yield* addAssistant(session.id, compact1, "first summary", {
          summary: true,
          finish: "end_turn",
        })
        const synthetic1 = yield* addUser(session.id, "synthetic continue", { syntheticContinue: true })
        const replay1 = yield* addUser(session.id, "first replay", { replay: true, replaySourceMessageID: start1 })
        yield* addAssistant(session.id, replay1, "first final", { finish: "end_turn" })

        const start2 = yield* addUser(session.id, "second prompt")
        const compact2 = yield* addCompaction(session.id, start2)
        yield* addAssistant(session.id, compact2, "second summary", { summary: true, finish: "end_turn" })
        const replay2 = yield* addUser(session.id, "second replay", { replay: true, replaySourceMessageID: start2 })
        yield* addAssistant(session.id, replay2, "second final", { finish: "end_turn" })

        const indexed = yield* requestInDirectory(`/session/${session.id}/transcript_window`, tmp.directory)
        expect(((yield* indexed.json) as { status: string }).status).toBe("complete")
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
