import { Global } from "@opencode-ai/core/global"
import { createHash } from "node:crypto"
import { appendFileSync, mkdirSync, readFileSync } from "node:fs"
import path from "node:path"

const SCHEMA = "opencode.avalonia.compaction-incident.v1"
const TOKEN = /^ocac-[a-f0-9]{24}$/
const SAFE_TEXT = /^[A-Za-z0-9._:-]{1,96}$/
const EVENT_TYPES = new Set([
  "session.status",
  "session.compacted",
  "session.transcript.reconciled",
])

type SafeFact = string | number | boolean | null

export type Record = {
  schema: typeof SCHEMA
  actionToken: string
  sequence: number
  elapsedMs: number
  layer: string
  action: string
  eventType?: string
  eventToken?: string
  facts?: globalThis.Record<string, SafeFact>
}

type Incident = {
  actionToken: string
  started: number
  sequence: number
  persistedThroughSequence: number
  persistenceFailed: boolean
  records: Record[]
}

const incidents = new Map<string, Incident>()
const activeBySession = new Map<string, string>()

function tokenFromHeader(value: string | undefined) {
  if (!value) return undefined
  const normalized = value.trim().toLowerCase()
  return TOKEN.test(normalized) ? normalized : undefined
}

function activeToken(sessionID: string | undefined) {
  if (!sessionID) return undefined
  return activeBySession.get(sessionID)
}

function eventType(value: unknown) {
  if (!value || typeof value !== "object") return undefined
  const candidate = value as globalThis.Record<string, any>
  const type = candidate.payload?.type ?? candidate.type
  return typeof type === "string" && EVENT_TYPES.has(type) ? type : undefined
}

function eventID(value: unknown) {
  if (!value || typeof value !== "object") return undefined
  const candidate = value as globalThis.Record<string, any>
  const id = candidate.payload?.id ?? candidate.id
  return typeof id === "string" && id.length > 0 ? id : undefined
}

function sessionID(value: unknown) {
  if (!value || typeof value !== "object") return undefined
  const candidate = value as globalThis.Record<string, any>
  const properties = candidate.payload?.properties ?? candidate.properties ?? candidate.data
  return properties && typeof properties === "object" && typeof properties.sessionID === "string"
    ? properties.sessionID
    : undefined
}

function opaque(actionToken: string, kind: string, raw: string | undefined) {
  if (!raw) return undefined
  return `${kind}:${createHash("sha256").update(actionToken).update("\0").update(kind).update("\0").update(raw).digest("hex").slice(0, 16)}`
}

function sanitizeFacts(facts: globalThis.Record<string, SafeFact> | undefined) {
  if (!facts) return undefined
  const safe: globalThis.Record<string, SafeFact> = {}
  for (const [key, value] of Object.entries(facts)) {
    if (!SAFE_TEXT.test(key)) continue
    if (typeof value === "string") {
      if (!SAFE_TEXT.test(value)) continue
      safe[key] = value
      continue
    }
    if (typeof value === "number") {
      if (!Number.isFinite(value) || value < 0) continue
      safe[key] = value
      continue
    }
    if (typeof value === "boolean" || value === null) safe[key] = value
  }
  return Object.keys(safe).length === 0 ? undefined : safe
}

function root() {
  return path.join(Global.Path.state, "compaction-incidents")
}

function file(actionToken: string) {
  return path.join(root(), `${actionToken}.sidecar.jsonl`)
}

function ensureIncident(actionToken: string, rawSessionID: string) {
  const current = incidents.get(actionToken)
  if (current) return current
  const incident: Incident = {
    actionToken,
    started: performance.now(),
    sequence: 0,
    persistedThroughSequence: 0,
    persistenceFailed: false,
    records: [],
  }
  incidents.set(actionToken, incident)
  return incident
}

function append(
  incident: Incident,
  layer: string,
  action: string,
  input?: {
    eventType?: string
    rawEventID?: string
    facts?: globalThis.Record<string, SafeFact>
  },
) {
  if (!SAFE_TEXT.test(layer) || !SAFE_TEXT.test(action)) return
  const type = input?.eventType && EVENT_TYPES.has(input.eventType) ? input.eventType : undefined
  const facts = sanitizeFacts(input?.facts)
  const record: Record = {
    schema: SCHEMA,
    actionToken: incident.actionToken,
    sequence: ++incident.sequence,
    elapsedMs: Math.max(0, Math.round((performance.now() - incident.started) * 1000) / 1000),
    layer,
    action,
    ...(type ? { eventType: type } : {}),
    ...(input?.rawEventID ? { eventToken: opaque(incident.actionToken, "event", input.rawEventID) } : {}),
    ...(facts ? { facts } : {}),
  }
  incident.records.push(record)
  try {
    mkdirSync(root(), { recursive: true })
    appendFileSync(file(incident.actionToken), `${JSON.stringify(record)}\n`, { encoding: "utf8", mode: 0o600 })
    incident.persistedThroughSequence = record.sequence
  } catch {
    incident.persistenceFailed = true
  }
}

function begin(rawSessionID: string, actionToken: string) {
  const token = tokenFromHeader(actionToken)
  if (!token || !rawSessionID) return false
  const incident = ensureIncident(token, rawSessionID)
  activeBySession.set(rawSessionID, token)
  append(incident, "compact.action", "armed", {
    facts: {
      armedBeforeRequest: true,
      persistenceFailed: incident.persistenceFailed,
    },
  })
  return true
}

function recordSession(
  rawSessionID: string | undefined,
  layer: string,
  action: string,
  input?: {
    eventType?: string
    rawEventID?: string
    facts?: globalThis.Record<string, SafeFact>
  },
) {
  const token = activeToken(rawSessionID)
  if (!token || !rawSessionID) return
  append(ensureIncident(token, rawSessionID), layer, action, input)
}

function recordPayload(
  value: unknown,
  layer: string,
  action: string,
  facts?: globalThis.Record<string, SafeFact>,
) {
  const rawSessionID = sessionID(value)
  const type = eventType(value)
  if (!rawSessionID || !type) return
  recordSession(rawSessionID, layer, action, {
    eventType: type,
    rawEventID: eventID(value),
    facts,
  })
}

function parseFile(actionToken: string) {
  try {
    return readFileSync(file(actionToken), "utf8")
      .split("\n")
      .filter(Boolean)
      .map((line) => JSON.parse(line) as Record)
  } catch {
    return undefined
  }
}

function snapshot(actionToken: string) {
  const token = tokenFromHeader(actionToken)
  if (!token) return undefined
  const incident = incidents.get(token)
  const persisted = parseFile(token)
  const records = persisted ?? incident?.records ?? []
  if (!incident && records.length === 0) return undefined
  const lastSequence = records.at(-1)?.sequence ?? incident?.sequence ?? 0
  return {
    schema: SCHEMA,
    actionToken: token,
    available: true,
    persistenceFailed: incident?.persistenceFailed ?? false,
    firstSequence: records.at(0)?.sequence ?? 0,
    lastSequence,
    persistedThroughSequence: persisted?.at(-1)?.sequence ?? incident?.persistedThroughSequence ?? 0,
    records,
  }
}

function resetForTest() {
  incidents.clear()
  activeBySession.clear()
}

export const CompactionDiagnostics = {
  schema: SCHEMA,
  tokenFromHeader,
  activeToken,
  opaque,
  begin,
  recordSession,
  recordPayload,
  snapshot,
  resetForTest,
}
