import { PermissionV1 } from "@opencode-ai/core/v1/permission"
import { TranscriptWindowProjection } from "@opencode-ai/core/session/transcript-window"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { EventV2Bridge } from "@/event-v2-bridge"
import { Command } from "@/command"
import { Permission } from "@/permission"
import { SessionShare } from "@/share/session"
import { Session } from "@/session/session"
import { MessageV2 } from "@/session/message-v2"
import { SessionPrompt } from "@/session/prompt"
import { SessionRevert } from "@/session/revert"
import { SessionRunState } from "@/session/run-state"
import { SessionStagedContext } from "@/session/staged-context"
import { SessionStatus } from "@/session/status"
import { SessionTranscriptWindow } from "@/session/transcript-window"
import { SessionTranscriptIndex } from "@/session/transcript-index"
import { SessionSummary } from "@/session/summary"
import { Todo } from "@/session/todo"
import { MessageID, PartID, SessionID } from "@/session/schema"
import { StreamDiagnostics } from "@/diagnostic/stream"
import { CompactionDiagnostics } from "@/diagnostic/compaction"
import { NamedError } from "@opencode-ai/core/util/error"
import { Cause, Deferred, Effect, Option, Schema, Scope } from "effect"
import * as Stream from "effect/Stream"
import { HttpServerRequest, HttpServerResponse } from "effect/unstable/http"
import { HttpApiBuilder, HttpApiError, HttpApiSchema } from "effect/unstable/httpapi"
import { InstanceHttpApi } from "../api"
import {
  CommandPayload,
  CompactedRangeQuery,
  DiffQuery,
  ForkPayload,
  InitPayload,
  ListQuery,
  MessagesQuery,
  PermissionResponsePayload,
  PromptPayload,
  RevertPayload,
  SessionTurn,
  ShellPayload,
  SummarizePayload,
  StagedContextPayload,
  UpdatePayload,
} from "../groups/session"
import { PermissionNotFoundError } from "../errors"
import * as SessionError from "./session-errors"

const tryParseJson = (text: string) =>
  Effect.try({
    try: () => JSON.parse(text) as unknown,
    catch: () => new HttpApiError.BadRequest({}),
  })

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

function isCompactionMessage(message: SessionV1.WithParts) {
  return message.info.role === "user" && message.parts.some((part) => part.type === "compaction")
}

function isCompactionReplayMessage(message: SessionV1.WithParts) {
  return (
    message.info.role === "user" &&
    message.parts.some((part) => part.type === "text" && part.metadata?.compaction_replay === true)
  )
}

function isPromptMessage(message: SessionV1.WithParts) {
  return (
    message.info.role === "user" &&
    !isCompactionMessage(message) &&
    !isCompactionReplayMessage(message) &&
    !message.parts.every((part) => "synthetic" in part && !!part.synthetic)
  )
}

function isFinalAssistant(message: SessionV1.WithParts) {
  if (message.info.role !== "assistant" || !message.info.finish || message.info.error || message.info.summary)
    return false
  if (message.info.finish === "tool-calls" || message.info.finish === "tool_calls") return false
  return !!messageText(message).trim()
}

function createSummaryPreview(summary: string) {
  const normalized = summary.split(/\s+/).filter(Boolean).join(" ")
  return normalized.length <= 80 ? normalized : `${normalized.slice(0, 77)}...`
}

function buildTurnCompaction(
  promptMessageID: MessageID,
  segment: SessionV1.WithParts[],
  finalOutputMessageID: MessageID | undefined,
) {
  if (!finalOutputMessageID) return undefined
  const compactionItems = segment
    .map((message, index) => ({
      message,
      index,
      part: message.parts.find((part): part is SessionV1.CompactionPart => part.type === "compaction"),
    }))
    .filter((item) => item.part)
  if (compactionItems.length !== 1) return undefined

  const compaction = compactionItems[0]
  const compactionPart = compaction.part
  if (!compactionPart) return undefined
  const summaryItems = segment
    .map((message, index) => ({ message, index }))
    .filter(
      (item) =>
        item.message.info.role === "assistant" &&
        item.message.info.parentID === compaction.message.info.id &&
        !!item.message.info.summary &&
        !!item.message.info.finish &&
        !item.message.info.error,
    )
  if (summaryItems.length !== 1) return undefined

  const summary = summaryItems[0]
  const fullSummary = messageText(summary.message)
  if (!fullSummary) return undefined

  const syntheticPromptMessageIDs = segment
    .filter(
      (message) =>
        message.info.role === "user" &&
        message.parts.some(
          (part) => part.type === "text" && part.synthetic && part.metadata?.compaction_continue === true,
        ),
    )
    .map((message) => message.info.id)
  const replayPromptMessageIDs = segment.filter(isCompactionReplayMessage).map((message) => message.info.id)
  if (syntheticPromptMessageIDs.length + replayPromptMessageIDs.length === 0) return undefined
  if (compactionPart.overflow && replayPromptMessageIDs.length === 0) return undefined

  const finalIndex = segment.findIndex((message) => message.info.id === finalOutputMessageID)
  const represented = new Set<MessageID>([
    compaction.message.info.id,
    summary.message.info.id,
    ...syntheticPromptMessageIDs,
    ...replayPromptMessageIDs,
  ])
  const allowedParents = new Set<MessageID>([promptMessageID, ...syntheticPromptMessageIDs, ...replayPromptMessageIDs])
  const isCollapsibleOperationalMessage = (message: SessionV1.WithParts) =>
    message.info.role === "assistant" &&
    allowedParents.has(message.info.parentID) &&
    message.info.id !== finalOutputMessageID &&
    !represented.has(message.info.id)

  return {
    compactionMessageID: compaction.message.info.id,
    summaryMessageID: summary.message.info.id,
    summaryPreview: createSummaryPreview(fullSummary),
    preCompactionMessageIDs: segment
      .slice(0, compaction.index)
      .filter(isCollapsibleOperationalMessage)
      .map((message) => message.info.id),
    postCompactionMessageIDs: segment
      .slice(summary.index + 1, finalIndex >= 0 ? finalIndex : segment.length)
      .filter(isCollapsibleOperationalMessage)
      .map((message) => message.info.id),
    syntheticPromptMessageIDs,
    replayPromptMessageIDs,
    recallMarkerID: compaction.message.info.id,
    ...(compactionPart.tail_start_id ? { recallTailStartMessageID: compactionPart.tail_start_id } : {}),
  }
}

function buildSessionTurns(sessionID: SessionID, messages: SessionV1.WithParts[]) {
  return {
    sessionID,
    turns: messages.flatMap((prompt, start) => {
      if (!isPromptMessage(prompt)) return []
      const nextPrompt = messages.findIndex((message, index) => index > start && isPromptMessage(message))
      const segment = messages.slice(start, nextPrompt < 0 ? messages.length : nextPrompt)
      const finalOutputMessageID = segment.filter(isFinalAssistant).at(-1)?.info.id
      const intermediateMessageIDs = finalOutputMessageID
        ? segment
            .slice(
              1,
              segment.findIndex((message) => message.info.id === finalOutputMessageID),
            )
            .map((message) => message.info.id)
        : segment.slice(1).map((message) => message.info.id)
      const compaction = buildTurnCompaction(prompt.info.id, segment, finalOutputMessageID)
      return [
        {
          id: `turn:${prompt.info.id}`,
          startMessageID: prompt.info.id,
          messageIDs: segment.map((message) => message.info.id),
          intermediateMessageIDs: compaction
            ? [...compaction.preCompactionMessageIDs, ...compaction.postCompactionMessageIDs]
            : intermediateMessageIDs,
          compactionBoundaryMessageIDs: compaction ? [compaction.compactionMessageID] : [],
          ...(finalOutputMessageID ? { finalOutputMessageID } : {}),
          status: finalOutputMessageID ? ("complete" as const) : ("incomplete" as const),
          ...(compaction ? { compaction } : {}),
        },
      ]
    }),
  }
}

export const sessionHandlers = HttpApiBuilder.group(InstanceHttpApi, "session", (handlers) =>
  Effect.gen(function* () {
    const session = yield* Session.Service
    const shareSvc = yield* SessionShare.Service
    const promptSvc = yield* SessionPrompt.Service
    const revertSvc = yield* SessionRevert.Service
    const runState = yield* SessionRunState.Service
    const stagedContext = yield* SessionStagedContext.Service
    const permissionSvc = yield* Permission.Service
    const statusSvc = yield* SessionStatus.Service
    const todoSvc = yield* Todo.Service
    const summary = yield* SessionSummary.Service
    const events = yield* EventV2Bridge.Service
    const scope = yield* Scope.Scope

    const list = Effect.fn("SessionHttpApi.list")(function* (ctx: { query: typeof ListQuery.Type }) {
      return yield* session.list({
        directory: ctx.query.scope === "project" ? undefined : ctx.query.directory,
        scope: ctx.query.scope,
        path: ctx.query.path,
        roots: ctx.query.roots,
        start: ctx.query.start,
        search: ctx.query.search,
        limit: ctx.query.limit,
      })
    })

    const status = Effect.fn("SessionHttpApi.status")(function* () {
      return Object.fromEntries(yield* statusSvc.list())
    })

    const requireSession = Effect.fn("SessionHttpApi.requireSession")(function* (sessionID: SessionID) {
      return yield* SessionError.mapStorageNotFound(session.get(sessionID))
    })

    const get = Effect.fn("SessionHttpApi.get")(function* (ctx: { params: { sessionID: SessionID } }) {
      return yield* requireSession(ctx.params.sessionID)
    })

    const children = Effect.fn("SessionHttpApi.children")(function* (ctx: { params: { sessionID: SessionID } }) {
      yield* requireSession(ctx.params.sessionID)
      return yield* session.children(ctx.params.sessionID)
    })

    const todo = Effect.fn("SessionHttpApi.todo")(function* (ctx: { params: { sessionID: SessionID } }) {
      yield* requireSession(ctx.params.sessionID)
      return yield* todoSvc.get(ctx.params.sessionID)
    })

    const turns = Effect.fn("SessionHttpApi.turns")(function* (ctx: { params: { sessionID: SessionID } }) {
      yield* requireSession(ctx.params.sessionID)
      return buildSessionTurns(
        ctx.params.sessionID,
        yield* SessionError.mapStorageNotFound(session.messages({ sessionID: ctx.params.sessionID })),
      )
    })

    const diff = Effect.fn("SessionHttpApi.diff")(function* (ctx: {
      params: { sessionID: SessionID }
      query: typeof DiffQuery.Type
    }) {
      return yield* summary.diff({ sessionID: ctx.params.sessionID, messageID: ctx.query.messageID })
    })

    const messages = Effect.fn("SessionHttpApi.messages")(function* (ctx: {
      params: { sessionID: SessionID }
      query: typeof MessagesQuery.Type
    }) {
      if (ctx.query.before && ctx.query.limit === undefined) return yield* new HttpApiError.BadRequest({})
      if (ctx.query.before) {
        const before = ctx.query.before
        yield* Effect.try({
          try: () => MessageV2.cursor.decode(before),
          catch: () => new HttpApiError.BadRequest({}),
        })
      }
      yield* requireSession(ctx.params.sessionID)
      if (ctx.query.limit === undefined || ctx.query.limit === 0) {
        return yield* SessionError.mapStorageNotFound(session.messages({ sessionID: ctx.params.sessionID }))
      }

      const page = yield* SessionError.mapStorageNotFound(
        MessageV2.page({
          sessionID: ctx.params.sessionID,
          limit: ctx.query.limit,
          before: ctx.query.before,
        }),
      )
      if (!page.cursor) return page.items

      const request = yield* HttpServerRequest.HttpServerRequest
      // toURL() honors the Host + x-forwarded-proto headers, so the Link
      // header echoes the real origin instead of a hard-coded localhost.
      const url = Option.getOrElse(HttpServerRequest.toURL(request), () => new URL(request.url, "http://localhost"))
      url.searchParams.set("limit", ctx.query.limit.toString())
      url.searchParams.set("before", page.cursor)
      return HttpServerResponse.jsonUnsafe(page.items, {
        headers: {
          "Access-Control-Expose-Headers": "Link, X-Next-Cursor",
          Link: `<${url.toString()}>; rel="next"`,
          "X-Next-Cursor": page.cursor,
        },
      })
    })

    const message = Effect.fn("SessionHttpApi.message")(function* (ctx: {
      params: { sessionID: SessionID; messageID: MessageID }
    }) {
      return yield* SessionError.mapStorageNotFound(
        MessageV2.get({ sessionID: ctx.params.sessionID, messageID: ctx.params.messageID }),
      )
    })

    const contextStage = Effect.fn("SessionHttpApi.contextStage")(function* (ctx: {
      params: { sessionID: SessionID }
      payload: typeof StagedContextPayload.Type
    }) {
      yield* requireSession(ctx.params.sessionID)
      if (!ctx.payload.parts.some((part) => part.type === "text" && part.text.length > 0)) {
        return yield* new HttpApiError.BadRequest({})
      }
      return yield* stagedContext
        .stage({ ...ctx.payload, sessionID: ctx.params.sessionID })
        .pipe(Effect.catchTag("SessionStagedContext.Duplicate", () => Effect.fail(new HttpApiError.BadRequest({}))))
    })

    const contextListStaged = Effect.fn("SessionHttpApi.contextListStaged")(function* (ctx: {
      params: { sessionID: SessionID }
    }) {
      yield* requireSession(ctx.params.sessionID)
      return yield* stagedContext.list({ sessionID: ctx.params.sessionID })
    })

    const contextClearStaged = Effect.fn("SessionHttpApi.contextClearStaged")(function* (ctx: {
      params: { sessionID: SessionID; contextID?: string }
    }) {
      yield* requireSession(ctx.params.sessionID)
      yield* stagedContext.clear({ sessionID: ctx.params.sessionID, contextID: ctx.params.contextID })
      return HttpApiSchema.NoContent.make()
    })

    const compactedRange = Effect.fn("SessionHttpApi.compactedRange")(function* (ctx: {
      params: { sessionID: SessionID }
      query: typeof CompactedRangeQuery.Type
    }) {
      yield* requireSession(ctx.params.sessionID)
      const result = yield* SessionTranscriptWindow.loadArchive({
        sessionID: ctx.params.sessionID,
        markerID: ctx.query.marker,
        sourceGeneration: ctx.query.source_generation,
        archiveID: ctx.query.archive_id,
        archiveRevision: ctx.query.archive_revision,
        tailStartID: ctx.query.tail_start_id,
        messageID: ctx.query.message_id,
        sourceMessageID: ctx.query.source_message_id,
      }).pipe(
        Effect.map((value) => ({ value })),
        Effect.catch((error) => Effect.succeed({ error })),
      )
      if ("error" in result) {
        const tooLarge = result.error instanceof SessionTranscriptWindow.TooLarge
        return {
          reference: {
            markerID: ctx.query.marker,
            tailStartID: ctx.query.tail_start_id,
            messageID: ctx.query.message_id,
            ...(ctx.query.source_message_id ? { sourceMessageID: ctx.query.source_message_id } : {}),
          },
          messages: [],
          precedingSummary: undefined,
          turns: [],
          complete: false,
          notice: tooLarge
            ? "Compacted transcript range exceeds the safe recall limit."
            : "Compacted transcript range is stale or unavailable.",
          status: tooLarge ? ("too_large" as const) : ("stale" as const),
          sourceGeneration: ctx.query.source_generation,
          archiveID: ctx.query.archive_id,
          archiveRevision: ctx.query.archive_revision,
        }
      }
      const turns = buildSessionTurns(ctx.params.sessionID, result.value.messages).turns
      return {
        reference: {
          markerID: result.value.markerID,
          tailStartID: result.value.tailStartID,
          messageID: result.value.markerID,
          sourceMessageID: result.value.sourceMessageID,
        },
        messages: result.value.messages,
        ...(result.value.precedingSummary ? { precedingSummary: result.value.precedingSummary } : {}),
        turns,
        complete: true,
        status: "complete" as const,
        sourceGeneration: result.value.sourceGeneration,
        archiveID: result.value.archiveID,
        archiveRevision: result.value.archiveRevision,
      }
    })

    const transcriptWindow = Effect.fn("SessionHttpApi.transcriptWindow")(function* (ctx: {
      params: { sessionID: SessionID }
    }) {
      yield* requireSession(ctx.params.sessionID)
      const load = () => SessionTranscriptWindow.loadWindow(ctx.params.sessionID).pipe(
        Effect.catch((error) =>
          Effect.succeed({
            status: error instanceof SessionTranscriptWindow.TooLarge ? ("too_large" as const) : ("stale" as const),
            sessionID: ctx.params.sessionID,
            archiveDescriptors: [],
            tail: [],
            counts: { descriptors: 0, messages: 0, parts: 0, textUnits: 2, decodedBytes: 2 },
          }),
        ),
      )
      let window = yield* load()
      if (window.status === "index_required") {
        yield* SessionTranscriptIndex.run({
          sessionID: ctx.params.sessionID,
          ownerID: crypto.randomUUID(),
        }).pipe(Effect.catch(() => Effect.void))
        window = yield* load()
      }
      const activeActionToken = CompactionDiagnostics.activeToken(ctx.params.sessionID)
      CompactionDiagnostics.recordSession(ctx.params.sessionID, "transcript.window", "loaded", {
        facts: {
          status: window.status,
          descriptors: window.counts.descriptors,
          messages: window.counts.messages,
          ...("windowRevision" in window && window.windowRevision && activeActionToken
            ? { revisionToken: CompactionDiagnostics.opaque(activeActionToken, "revision", window.windowRevision)! }
            : {}),
        },
      })
      const turns = window.status === "complete" ? buildSessionTurns(ctx.params.sessionID, window.tail).turns : []
      const transportTurns = Schema.encodeSync(Schema.Array(SessionTurn))(turns)
      const turnEncoded = JSON.stringify(transportTurns)
      const turnIdentities = transportTurns.reduce(
        (count, turn) =>
          count +
          2 +
          turn.messageIDs.length +
          turn.intermediateMessageIDs.length +
          turn.compactionBoundaryMessageIDs.length +
          (turn.finalOutputMessageID ? 1 : 0) +
          (turn.compaction
            ? 2 +
              turn.compaction.preCompactionMessageIDs.length +
              turn.compaction.postCompactionMessageIDs.length +
              turn.compaction.syntheticPromptMessageIDs.length +
              turn.compaction.replayPromptMessageIDs.length +
              (turn.compaction.recallMarkerID ? 1 : 0) +
              (turn.compaction.recallTailStartMessageID ? 1 : 0)
            : 0),
        0,
      )
      const turnDecodedBytes = Buffer.byteLength(turnEncoded, "utf8")
      if (
        turnIdentities > TranscriptWindowProjection.Limits.parts ||
        turnEncoded.length > TranscriptWindowProjection.Limits.textCodeUnits ||
        turnDecodedBytes > TranscriptWindowProjection.Limits.decodedBytes
      )
        return {
          status: "too_large" as const,
          sessionID: ctx.params.sessionID,
          archiveDescriptors: [],
          tail: [],
          turns: [],
          counts: {
            descriptors: 0,
            messages: 0,
            parts: 0,
            textUnits: 2,
            decodedBytes: 2,
            turns: 0,
            turnIdentities: 0,
            turnTextUnits: 2,
            turnDecodedBytes: 2,
          },
        }
      return {
        ...window,
        turns,
        counts: {
          ...window.counts,
          turns: turns.length,
          turnIdentities,
          turnTextUnits: turnEncoded.length,
          turnDecodedBytes,
        },
      }
    })

    const create = Effect.fn("SessionHttpApi.create")(function* (ctx: { payload?: Session.CreateInput }) {
      return yield* shareSvc.create(ctx.payload)
    })

    const createRaw = Effect.fn("SessionHttpApi.createRaw")(function* (ctx: {
      request: HttpServerRequest.HttpServerRequest
    }) {
      const body = yield* Effect.orDie(ctx.request.text)
      if (body.trim().length === 0) return yield* create({})

      const json = yield* tryParseJson(body)
      const decoded = yield* Schema.decodeUnknownEffect(Session.CreateInput)(json).pipe(
        Effect.mapError(() => new HttpApiError.BadRequest({})),
      )
      const payload = decoded
        ? {
            ...decoded,
            permission: decoded.permission ? [...decoded.permission] : undefined,
          }
        : decoded
      return yield* create({ payload })
    })

    const remove = Effect.fn("SessionHttpApi.remove")(function* (ctx: { params: { sessionID: SessionID } }) {
      yield* SessionError.mapStorageNotFound(session.remove(ctx.params.sessionID))
      return true
    })

    const update = Effect.fn("SessionHttpApi.update")(function* (ctx: {
      params: { sessionID: SessionID }
      payload: typeof UpdatePayload.Type
    }) {
      const current = yield* requireSession(ctx.params.sessionID)
      if (ctx.payload.title !== undefined) {
        yield* session.setTitle({ sessionID: ctx.params.sessionID, title: ctx.payload.title })
      }
      if (ctx.payload.metadata !== undefined) {
        yield* session.setMetadata({ sessionID: ctx.params.sessionID, metadata: ctx.payload.metadata })
      }
      if (ctx.payload.permission !== undefined) {
        yield* session.setPermission({
          sessionID: ctx.params.sessionID,
          permission: Permission.merge(current.permission ?? [], ctx.payload.permission),
        })
      }
      if (ctx.payload.time?.archived !== undefined) {
        yield* session.setArchived({ sessionID: ctx.params.sessionID, time: ctx.payload.time.archived })
      }
      return yield* requireSession(ctx.params.sessionID)
    })

    const fork = Effect.fn("SessionHttpApi.fork")(function* (ctx: {
      params: { sessionID: SessionID }
      payload?: typeof ForkPayload.Type
    }) {
      return yield* SessionError.mapStorageNotFound(
        session.fork({
          sessionID: ctx.params.sessionID,
          messageID: ctx.payload?.messageID,
        }),
      )
    })

    const forkRaw = Effect.fn("SessionHttpApi.forkRaw")(function* (ctx: {
      params: { sessionID: SessionID }
      request: HttpServerRequest.HttpServerRequest
    }) {
      const body = yield* Effect.orDie(ctx.request.text)
      if (body.trim().length === 0) return yield* fork({ params: ctx.params })

      const json = yield* tryParseJson(body)
      const payload = yield* Schema.decodeUnknownEffect(ForkPayload)(json).pipe(
        Effect.mapError(() => new HttpApiError.BadRequest({})),
      )
      return yield* fork({ params: ctx.params, payload })
    })

    const abort = Effect.fn("SessionHttpApi.abort")(function* (ctx: { params: { sessionID: SessionID } }) {
      yield* promptSvc.cancel(ctx.params.sessionID)
      return true
    })

    const init = Effect.fn("SessionHttpApi.init")(function* (ctx: {
      params: { sessionID: SessionID }
      payload: typeof InitPayload.Type
    }) {
      yield* requireSession(ctx.params.sessionID)
      yield* promptSvc
        .command({
          sessionID: ctx.params.sessionID,
          messageID: ctx.payload.messageID,
          model: `${ctx.payload.providerID}/${ctx.payload.modelID}`,
          command: Command.Default.INIT,
          arguments: "",
        })
        .pipe(Effect.mapError(() => new HttpApiError.BadRequest({})))
      return true
    })

    // share/unshare errors aren't all client-induced — storage and network
    // failures from SessionShare are real possibilities. Map to a typed 500
    // (matches the legacy route behavior which routed any failure through
    // ErrorMiddleware → NamedError.Unknown 500) instead of blanket-mapping
    // every failure to a 400 BadRequest.
    const share = Effect.fn("SessionHttpApi.share")(function* (ctx: { params: { sessionID: SessionID } }) {
      yield* requireSession(ctx.params.sessionID)
      yield* shareSvc.share(ctx.params.sessionID).pipe(Effect.mapError(() => new HttpApiError.InternalServerError({})))
      return yield* requireSession(ctx.params.sessionID)
    })

    const unshare = Effect.fn("SessionHttpApi.unshare")(function* (ctx: { params: { sessionID: SessionID } }) {
      yield* requireSession(ctx.params.sessionID)
      yield* shareSvc
        .unshare(ctx.params.sessionID)
        .pipe(Effect.mapError(() => new HttpApiError.InternalServerError({})))
      return yield* requireSession(ctx.params.sessionID)
    })

    const summarize = Effect.fn("SessionHttpApi.summarize")(function* (ctx: {
      params: { sessionID: SessionID }
      payload: typeof SummarizePayload.Type
    }) {
      yield* requireSession(ctx.params.sessionID)
      const request = yield* HttpServerRequest.HttpServerRequest
      const actionToken = CompactionDiagnostics.tokenFromHeader(
        request.headers["x-opencode-avalonia-stream-diagnostic"],
      )
      if (actionToken) {
        CompactionDiagnostics.begin(ctx.params.sessionID, actionToken)
        StreamDiagnostics.bindCorrelation(ctx.params.sessionID, actionToken)
        CompactionDiagnostics.recordSession(ctx.params.sessionID, "summarize.route", "accepted", {
          facts: { auto: ctx.payload.auto },
        })
      }
      yield* SessionError.mapBusy(
        promptSvc.summarize({
          sessionID: ctx.params.sessionID,
          providerID: ctx.payload.providerID,
          modelID: ctx.payload.modelID,
          auto: ctx.payload.auto,
        }).pipe(
          Effect.tapError(() =>
            Effect.sync(() =>
              CompactionDiagnostics.recordSession(ctx.params.sessionID, "summarize.route", "failed"),
            ),
          ),
        ),
      )
      CompactionDiagnostics.recordSession(ctx.params.sessionID, "summarize.route", "returned")
      return true
    })

    const prompt = Effect.fn("SessionHttpApi.prompt")(function* (ctx: {
      params: { sessionID: SessionID }
      payload: typeof PromptPayload.Type
    }) {
      yield* requireSession(ctx.params.sessionID)
      const request = yield* HttpServerRequest.HttpServerRequest
      const correlation = StreamDiagnostics.correlationFromHeader(
        request.headers["x-opencode-avalonia-stream-diagnostic"],
      )
      StreamDiagnostics.bindCorrelation(ctx.params.sessionID, correlation)
      StreamDiagnostics.record({
        stage: "prompt.route",
        action: "accepted",
        routeMode: "prompt-sync",
        correlation,
      })
      const message = yield* promptSvc
        .prompt({
          ...ctx.payload,
          sessionID: ctx.params.sessionID,
        })
        .pipe(Effect.mapError(() => new HttpApiError.BadRequest({})))
      StreamDiagnostics.record({
        stage: "prompt.route",
        action: "returned",
        routeMode: "prompt-sync",
        count: message.parts.length,
        correlation,
        match: true,
      })
      return HttpServerResponse.stream(Stream.make(JSON.stringify(message)).pipe(Stream.encodeText), {
        contentType: "application/json",
      })
    })

    const promptAsync = Effect.fn("SessionHttpApi.promptAsync")(function* (ctx: {
      params: { sessionID: SessionID }
      payload: typeof PromptPayload.Type
    }) {
      yield* requireSession(ctx.params.sessionID)
      const request = yield* HttpServerRequest.HttpServerRequest
      const correlation = StreamDiagnostics.correlationFromHeader(
        request.headers["x-opencode-avalonia-stream-diagnostic"],
      )
      StreamDiagnostics.bindCorrelation(ctx.params.sessionID, correlation)
      StreamDiagnostics.record({
        stage: "prompt.route",
        action: "accepted",
        routeMode: "prompt-async",
        correlation,
      })
      const committed = yield* Deferred.make<void, HttpApiError.BadRequest>()
      const operation = promptSvc
        .prompt(
          { ...ctx.payload, sessionID: ctx.params.sessionID },
          Deferred.succeed(committed, undefined).pipe(Effect.asVoid),
        )
        .pipe(
          Effect.onError((cause) =>
            Effect.gen(function* () {
              yield* Deferred.fail(committed, new HttpApiError.BadRequest({})).pipe(Effect.asVoid)
              yield* Effect.logError("prompt_async failed").pipe(
                Effect.annotateLogs({ sessionID: ctx.params.sessionID, cause }),
              )
              yield* events.publish(Session.Event.Error, {
                sessionID: ctx.params.sessionID,
                error: new NamedError.Unknown({ message: Cause.pretty(cause) }).toObject(),
              })
              StreamDiagnostics.record({
                stage: "prompt.route",
                action: "error",
                routeMode: "prompt-async",
                correlation,
                match: false,
              })
            }),
          ),
          Effect.mapError(() => new HttpApiError.BadRequest({})),
        )
      yield* operation.pipe(Effect.forkIn(scope, { startImmediately: true }))
      yield* Deferred.await(committed)
      StreamDiagnostics.record({
        stage: "prompt.route",
        action: "returned",
        routeMode: "prompt-async",
        correlation,
        match: true,
      })
      return HttpApiSchema.NoContent.make()
    })

    const command = Effect.fn("SessionHttpApi.command")(function* (ctx: {
      params: { sessionID: SessionID }
      payload: typeof CommandPayload.Type
    }) {
      yield* requireSession(ctx.params.sessionID)
      return yield* promptSvc
        .command({ ...ctx.payload, sessionID: ctx.params.sessionID })
        .pipe(Effect.mapError(() => new HttpApiError.BadRequest({})))
    })

    const shell = Effect.fn("SessionHttpApi.shell")(function* (ctx: {
      params: { sessionID: SessionID }
      payload: typeof ShellPayload.Type
    }) {
      yield* requireSession(ctx.params.sessionID)
      return yield* SessionError.mapBusy(promptSvc.shell({ ...ctx.payload, sessionID: ctx.params.sessionID }))
    })

    const revert = Effect.fn("SessionHttpApi.revert")(function* (ctx: {
      params: { sessionID: SessionID }
      payload: typeof RevertPayload.Type
    }) {
      yield* requireSession(ctx.params.sessionID)
      return yield* SessionError.mapBusy(revertSvc.revert({ sessionID: ctx.params.sessionID, ...ctx.payload }))
    })

    const unrevert = Effect.fn("SessionHttpApi.unrevert")(function* (ctx: { params: { sessionID: SessionID } }) {
      yield* requireSession(ctx.params.sessionID)
      return yield* SessionError.mapBusy(revertSvc.unrevert({ sessionID: ctx.params.sessionID }))
    })

    const permissionRespond = Effect.fn("SessionHttpApi.permissionRespond")(function* (ctx: {
      params: { sessionID: SessionID; permissionID: PermissionV1.ID }
      payload: typeof PermissionResponsePayload.Type
    }) {
      yield* requireSession(ctx.params.sessionID)
      yield* permissionSvc.reply({ requestID: ctx.params.permissionID, reply: ctx.payload.response }).pipe(
        Effect.catchTag("Permission.NotFoundError", (error) =>
          Effect.fail(
            new PermissionNotFoundError({
              requestID: String(error.requestID),
              message: `Permission request not found: ${error.requestID}`,
            }),
          ),
        ),
      )
      return true
    })

    const deleteMessage = Effect.fn("SessionHttpApi.deleteMessage")(function* (ctx: {
      params: { sessionID: SessionID; messageID: MessageID }
    }) {
      yield* requireSession(ctx.params.sessionID)
      yield* SessionError.mapBusy(runState.assertNotBusy(ctx.params.sessionID))
      yield* session.removeMessage(ctx.params)
      return true
    })

    const deletePart = Effect.fn("SessionHttpApi.deletePart")(function* (ctx: {
      params: { sessionID: SessionID; messageID: MessageID; partID: PartID }
    }) {
      yield* requireSession(ctx.params.sessionID)
      yield* session.removePart(ctx.params)
      return true
    })

    const updatePart = Effect.fn("SessionHttpApi.updatePart")(function* (ctx: {
      params: { sessionID: SessionID; messageID: MessageID; partID: PartID }
      payload: typeof SessionV1.Part.Type
    }) {
      yield* requireSession(ctx.params.sessionID)
      const payload = ctx.payload as SessionV1.Part
      if (
        payload.id !== ctx.params.partID ||
        payload.messageID !== ctx.params.messageID ||
        payload.sessionID !== ctx.params.sessionID
      ) {
        return yield* new HttpApiError.BadRequest({})
      }
      return yield* session.updatePart(payload)
    })

    return handlers
      .handle("list", list)
      .handle("status", status)
      .handle("get", get)
      .handle("children", children)
      .handle("todo", todo)
      .handle("turns", turns)
      .handle("diff", diff)
      .handle("messages", messages)
      .handle("message", message)
      .handle("contextStage", contextStage)
      .handle("contextListStaged", contextListStaged)
      .handle("contextClearStaged", contextClearStaged)
      .handle("contextClearStagedItem", contextClearStaged)
      .handle("compactedRange", compactedRange)
      .handle("transcriptWindow", transcriptWindow)
      .handleRaw("create", createRaw)
      .handle("remove", remove)
      .handle("update", update)
      .handleRaw("fork", forkRaw)
      .handle("abort", abort)
      .handle("init", init)
      .handle("share", share)
      .handle("unshare", unshare)
      .handle("summarize", summarize)
      .handle("prompt", prompt)
      .handle("promptAsync", promptAsync)
      .handle("command", command)
      .handle("shell", shell)
      .handle("revert", revert)
      .handle("unrevert", unrevert)
      .handle("permissionRespond", permissionRespond)
      .handle("deleteMessage", deleteMessage)
      .handle("deletePart", deletePart)
      .handle("updatePart", updatePart)
  }),
)
