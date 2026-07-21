import { afterEach, beforeEach, describe, expect, test } from "bun:test"
import { Global } from "@opencode-ai/core/global"
import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { CompactionDiagnostics } from "../../src/diagnostic/compaction"

describe("compaction diagnostics", () => {
  const previousState = Global.Path.state
  let root = ""

  beforeEach(async () => {
    root = await mkdtemp(path.join(tmpdir(), "opencode-compaction-diagnostics-"))
    Global.Path.state = root
    CompactionDiagnostics.resetForTest()
  })

  afterEach(async () => {
    CompactionDiagnostics.resetForTest()
    Global.Path.state = previousState
    await rm(root, { recursive: true, force: true })
  })

  test("persists a sanitized causal fragment that survives recorder reset", () => {
    const actionToken = "ocac-0123456789abcdef01234567"
    const rawSessionID = "session-secret-123"
    const rawEventID = "event-secret-456"

    expect(CompactionDiagnostics.begin(rawSessionID, actionToken)).toBeTrue()
    CompactionDiagnostics.recordSession(rawSessionID, "session.status", "set", {
      eventType: "session.status",
      facts: { status: "busy" },
    })
    CompactionDiagnostics.recordPayload(
      {
        directory: "/secret/workspace",
        payload: {
          id: rawEventID,
          type: "session.status",
          properties: { sessionID: rawSessionID, status: { type: "busy", message: "private text" } },
        },
      },
      "global.bus",
      "emit",
      { listeners: 2 },
    )
    CompactionDiagnostics.recordPayload(
      {
        payload: {
          id: "ignored-event",
          type: "message.part.updated",
          properties: { sessionID: rawSessionID, text: "private prompt" },
        },
      },
      "global.bus",
      "emit",
    )

    const live = CompactionDiagnostics.snapshot(actionToken)
    expect(live?.persistenceFailed).toBeFalse()
    expect(live?.persistedThroughSequence).toBe(3)
    expect(live?.records.map((record) => record.sequence)).toEqual([1, 2, 3])
    expect(live?.records[2]?.eventToken).toMatch(/^event:[a-f0-9]{16}$/)

    CompactionDiagnostics.resetForTest()
    const recovered = CompactionDiagnostics.snapshot(actionToken)
    expect(recovered?.records).toEqual(live?.records)
    const encoded = JSON.stringify(recovered)
    expect(encoded).not.toContain(rawSessionID)
    expect(encoded).not.toContain(rawEventID)
    expect(encoded).not.toContain("/secret/workspace")
    expect(encoded).not.toContain("private text")
    expect(encoded).not.toContain("private prompt")
    expect(encoded).not.toContain("ignored-event")
  })

  test("rejects malformed action tokens", () => {
    expect(CompactionDiagnostics.begin("session", "not-a-token")).toBeFalse()
    expect(CompactionDiagnostics.snapshot("not-a-token")).toBeUndefined()
  })
})
