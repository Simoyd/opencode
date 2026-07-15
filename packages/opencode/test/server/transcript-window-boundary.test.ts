import { expect, test } from "bun:test"
import path from "path"

test("selected transcript window handlers cannot call exhaustive history APIs", async () => {
  const handler = await Bun.file(
    path.join(import.meta.dir, "../../src/server/routes/instance/httpapi/handlers/session.ts"),
  ).text()
  const start = handler.indexOf('const compactedRange = Effect.fn("SessionHttpApi.compactedRange")')
  const end = handler.indexOf('const create = Effect.fn("SessionHttpApi.create")', start)
  expect(start).toBeGreaterThanOrEqual(0)
  expect(end).toBeGreaterThan(start)

  const selectedHandlers = handler.slice(start, end)
  const helper = await Bun.file(path.join(import.meta.dir, "../../src/session/transcript-window.ts")).text()
  for (const source of [selectedHandlers, helper]) {
    expect(source).not.toContain("session.messages(")
    expect(source).not.toContain("MessageV2.stream")
  }
})
