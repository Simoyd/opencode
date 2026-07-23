import { Flag } from "@opencode-ai/core/flag/flag"

const SCHEMA = "opencode.avalonia.stream.diagnostics.v1"
const DEFAULT_LIMIT = 512
const DEFAULT_SUMMARY_LIMIT = 128
const STAGES = new Set([
  "prompt.route",
  "provider.stream",
  "processor.delta",
  "session.updatePartDelta",
  "bus.publish",
  "bus.global",
  "global.bus",
  "route.event",
  "route.global",
])
const ACTIONS = new Set([
  "accepted",
  "attempt",
  "chunk",
  "connect",
  "disconnect",
  "dispatched",
  "emit",
  "emit-attempt",
  "emit-returned",
  "end",
  "envelope",
  "error",
  "fault",
  "observed",
  "published",
  "queue",
  "returned",
  "skipped",
  "start",
  "updated",
  "write",
])
const EVENT_TYPES = new Set([
  "error",
  "event.filtered-fallback",
  "final",
  "message.part.delta",
  "message.part.removed",
  "message.part.updated",
  "message.removed",
  "message.updated",
  "permission.asked",
  "permission.replied",
  "permission.updated",
  "question.asked",
  "question.rejected",
  "question.replied",
  "reasoning-delta",
  "server.connected",
  "server.heartbeat",
  "server.instance.disposed",
  "session.compacted",
  "session.created",
  "session.deleted",
  "session.diff",
  "session.error",
  "session.idle",
  "session.status",
  "session.updated",
  "todo.updated",
  "tool-call",
  "tool-result",
])
const READINESS = new Set(["connected", "control", "error", "final", "reasoning-delta", "text-delta", "tool-control"])
const ROUTE_MODES = new Set([
  "global",
  "instance-event-filtered",
  "instance-event-unfiltered",
  "prompt-async",
  "prompt-sync",
  "provider-stream",
])
const SHAPES = new Set([
  "bus-payload",
  "control",
  "error",
  "event-label",
  "final",
  "global-envelope",
  "reasoning-delta",
  "text-delta",
  "tool-control",
  "unknown",
])
export type StreamDiagnosticEvent = {
  seq: number
  relativeMs: number
  stage: string
  action?: string
  eventType?: string
  count?: number
  length?: number
  readiness?: string
  overflow?: boolean
  routeMode?: string
  shape?: string
  correlation?: string
  match?: boolean
}

type Input = Omit<StreamDiagnosticEvent, "seq" | "relativeMs">

export type StreamDiagnosticStageSummary = {
  stage: string
  action: string
  eventType: string
  shape: string
  routeMode: string
  correlation?: string
  match?: boolean
  count: number
  firstRelativeMs: number
  lastRelativeMs: number
  minRelativeMs: number
  maxRelativeMs: number
  minCount?: number
  maxCount?: number
  minLength?: number
  maxLength?: number
  lengthTotal: number
}

const state = {
  started: Date.now(),
  seq: 0,
  limit: DEFAULT_LIMIT,
  summaryLimit: DEFAULT_SUMMARY_LIMIT,
  dropped: 0,
  droppedSummaries: 0,
  events: [] as StreamDiagnosticEvent[],
  summaries: new Map<string, StreamDiagnosticStageSummary>(),
  correlations: new Map<string, string>(),
}

function allowlisted(value: unknown, allowed: Set<string>) {
  if (typeof value !== "string") return undefined
  if (!allowed.has(value)) return undefined
  return value
}

function number(value: unknown) {
  return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : undefined
}

function safeCorrelation(value: unknown) {
  if (typeof value !== "string") return undefined
  if (!/^[A-Za-z0-9._:-]{1,64}$/.test(value)) return undefined
  return value
}

function sanitize(input: Input): Input | undefined {
  const stage = allowlisted(input.stage, STAGES)
  if (!stage) return
  return {
    stage,
    action: allowlisted(input.action, ACTIONS),
    eventType: allowlisted(input.eventType, EVENT_TYPES),
    count: number(input.count),
    length: number(input.length),
    readiness: allowlisted(input.readiness, READINESS),
    overflow: typeof input.overflow === "boolean" ? input.overflow : undefined,
    routeMode: allowlisted(input.routeMode, ROUTE_MODES),
    shape: allowlisted(input.shape, SHAPES),
    correlation: safeCorrelation(input.correlation),
    match: typeof input.match === "boolean" ? input.match : undefined,
  }
}

function enabled() {
  return Flag.OPENCODE_AVALONIA_STREAM_DIAGNOSTICS
}

function record(input: Input) {
  if (!enabled()) return false
  const safe = sanitize(input)
  if (!safe) return false
  const relativeMs = Math.max(0, Date.now() - state.started)
  const event: StreamDiagnosticEvent = {
    seq: ++state.seq,
    relativeMs,
    ...safe,
  }
  recordSummary(safe, relativeMs)
  if (state.events.length >= state.limit) {
    state.events.shift()
    state.dropped++
    event.overflow = true
  }
  state.events.push(event)
  return true
}

function recordSummary(input: Input, relativeMs: number) {
  const summaryKey = [
    input.stage,
    input.action ?? "unknown",
    input.eventType ?? "unknown",
    input.shape ?? "unknown",
    input.routeMode ?? "unknown",
    input.correlation ?? "none",
    input.match === undefined ? "unknown" : String(input.match),
  ].join("\u001f")
  let summary = state.summaries.get(summaryKey)
  if (!summary) {
    if (state.summaries.size >= state.summaryLimit) {
      state.droppedSummaries++
      return
    }

    summary = {
      stage: input.stage,
      action: input.action ?? "unknown",
      eventType: input.eventType ?? "unknown",
      shape: input.shape ?? "unknown",
      routeMode: input.routeMode ?? "unknown",
      correlation: input.correlation,
      match: input.match,
      count: 0,
      firstRelativeMs: relativeMs,
      lastRelativeMs: relativeMs,
      minRelativeMs: relativeMs,
      maxRelativeMs: relativeMs,
      minCount: input.count,
      maxCount: input.count,
      minLength: input.length,
      maxLength: input.length,
      lengthTotal: 0,
    }
    state.summaries.set(summaryKey, summary)
  }

  summary.count++
  summary.lastRelativeMs = relativeMs
  summary.minRelativeMs = Math.min(summary.minRelativeMs, relativeMs)
  summary.maxRelativeMs = Math.max(summary.maxRelativeMs, relativeMs)
  if (input.count !== undefined) {
    summary.minCount = summary.minCount === undefined ? input.count : Math.min(summary.minCount, input.count)
    summary.maxCount = summary.maxCount === undefined ? input.count : Math.max(summary.maxCount, input.count)
  }
  if (input.length !== undefined) {
    summary.minLength = summary.minLength === undefined ? input.length : Math.min(summary.minLength, input.length)
    summary.maxLength = summary.maxLength === undefined ? input.length : Math.max(summary.maxLength, input.length)
    summary.lengthTotal += input.length
  }
}

function bindCorrelation(sessionID: string | undefined, correlation: string | undefined) {
  if (!enabled()) return
  const safe = safeCorrelation(correlation)
  if (!sessionID || !safe) return
  state.correlations.set(sessionID, safe)
}

function correlationForSession(sessionID: string | undefined) {
  if (!enabled() || !sessionID) return undefined
  return state.correlations.get(sessionID)
}

function sessionIDFromPayload(value: unknown) {
  if (!value || typeof value !== "object") return undefined
  const record = value as Record<string, any>
  const properties =
    record.payload && typeof record.payload === "object" ? record.payload.properties : record.properties
  if (!properties || typeof properties !== "object") return undefined
  return typeof properties.sessionID === "string" ? properties.sessionID : undefined
}

function correlationForPayload(value: unknown) {
  return correlationForSession(sessionIDFromPayload(value))
}

function correlationFromHeader(value: string | undefined) {
  if (!enabled()) return undefined
  return safeCorrelation(value)
}

function eventType(value: unknown) {
  if (!value || typeof value !== "object") return undefined
  const record = value as Record<string, any>
  return allowlisted(record.payload?.type, EVENT_TYPES) ?? allowlisted(record.type, EVENT_TYPES)
}

function shape(value: unknown) {
  if (!value || typeof value !== "object") return "unknown"
  const record = value as Record<string, any>
  if (record.payload && typeof record.payload === "object") return "global-envelope"
  if (typeof record.type === "string" && record.properties && typeof record.properties === "object")
    return "bus-payload"
  if (typeof record.type === "string") return "event-label"
  return "unknown"
}

function snapshot() {
  return {
    schema: SCHEMA,
    enabled: enabled(),
    limit: state.limit,
    summaryLimit: state.summaryLimit,
    dropped: state.dropped,
    droppedSummaries: state.droppedSummaries,
    overflow: state.dropped > 0,
    summaryOverflow: state.droppedSummaries > 0,
    stageSummaries: enabled()
      ? [...state.summaries.values()].sort(
          (left, right) =>
            right.count - left.count ||
            left.stage.localeCompare(right.stage) ||
            left.action.localeCompare(right.action) ||
            left.eventType.localeCompare(right.eventType) ||
            left.shape.localeCompare(right.shape) ||
            left.routeMode.localeCompare(right.routeMode),
        )
      : [],
    events: enabled() ? [...state.events] : [],
  }
}

function resetForTest(limit = DEFAULT_LIMIT, summaryLimit = DEFAULT_SUMMARY_LIMIT) {
  state.started = Date.now()
  state.seq = 0
  state.limit = Math.max(1, Math.floor(limit))
  state.summaryLimit = Math.max(1, Math.floor(summaryLimit))
  state.dropped = 0
  state.droppedSummaries = 0
  state.events = []
  state.summaries.clear()
  state.correlations.clear()
}

export const StreamDiagnostics = {
  schema: SCHEMA,
  enabled,
  record,
  bindCorrelation,
  correlationForSession,
  correlationForPayload,
  correlationFromHeader,
  eventType,
  shape,
  snapshot,
  resetForTest,
}
