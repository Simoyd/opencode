import { describe, expect, test } from "bun:test"
import { ConfigMarkdown } from "@opencode-ai/core/config/markdown"

describe("ConfigMarkdown.parse", () => {
  test("returns sanitized frontmatter on repeated same-process parses", () => {
    const content = `---
name: cache-colon-skill
description: Build UI with MVVM: thin bindable view models.
---

# Cache Colon Skill
`

    const first = ConfigMarkdown.parse(content)
    const second = ConfigMarkdown.parse(content)

    expect(first.data.name).toBe("cache-colon-skill")
    expect(first.data.description).toBe("Build UI with MVVM: thin bindable view models.")
    expect(second.data.name).toBe("cache-colon-skill")
    expect(second.data.description).toBe("Build UI with MVVM: thin bindable view models.")
  })
})
