import { describe, expect, test } from "bun:test"
import { Schema } from "effect"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { APICallError } from "ai"
import { MessageV2 } from "../../src/session/message-v2"
import { SessionPrompt } from "../../src/session/prompt"
import { ProviderTransform } from "@/provider/transform"
import type { Provider } from "@/provider/provider"

import { SessionID, MessageID, PartID } from "../../src/session/schema"
import { Question } from "../../src/question"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"

const sessionID = SessionID.make("session")
const providerID = ProviderV2.ID.make("test")
const model: Provider.Model = {
  id: ModelV2.ID.make("test-model"),
  providerID,
  api: {
    id: "test-model",
    url: "https://example.com",
    npm: "@ai-sdk/openai",
  },
  name: "Test Model",
  capabilities: {
    temperature: true,
    reasoning: false,
    attachment: false,
    toolcall: true,
    input: {
      text: true,
      audio: false,
      image: false,
      video: false,
      pdf: false,
    },
    output: {
      text: true,
      audio: false,
      image: false,
      video: false,
      pdf: false,
    },
    interleaved: false,
  },
  cost: {
    input: 0,
    output: 0,
    cache: {
      read: 0,
      write: 0,
    },
  },
  limit: {
    context: 0,
    input: 0,
    output: 0,
  },
  status: "active",
  options: {},
  headers: {},
  release_date: "2026-01-01",
}

function userInfo(id: string): SessionV1.User {
  return {
    id,
    sessionID,
    role: "user",
    time: { created: 0 },
    agent: "user",
    model: { providerID, modelID: ModelV2.ID.make("test") },
    tools: {},
    mode: "",
  } as unknown as SessionV1.User
}

function assistantInfo(
  id: string,
  parentID: string,
  error?: SessionV1.Assistant["error"],
  meta?: { providerID: string; modelID: string },
): SessionV1.Assistant {
  const infoModel = meta ?? { providerID: model.providerID, modelID: model.api.id }
  return {
    id,
    sessionID,
    role: "assistant",
    time: { created: 0 },
    error,
    parentID,
    modelID: infoModel.modelID,
    providerID: infoModel.providerID,
    mode: "",
    agent: "agent",
    path: { cwd: "/", root: "/" },
    cost: 0,
    tokens: {
      input: 0,
      output: 0,
      reasoning: 0,
      cache: { read: 0, write: 0 },
    },
  } as unknown as SessionV1.Assistant
}

function basePart(messageID: string, id: string) {
  return {
    id: PartID.make(id.startsWith("prt") ? id : `prt_${id}`),
    sessionID,
    messageID: MessageID.make(messageID.startsWith("msg") ? messageID : `msg_${messageID}`),
  }
}

describe("session.message-v2.toModelMessage", () => {
  test("filters out messages with no parts", async () => {
    const input: SessionV1.WithParts[] = [
      {
        info: userInfo("m-empty"),
        parts: [],
      },
      {
        info: userInfo("m-user"),
        parts: [
          {
            ...basePart("m-user", "p1"),
            type: "text",
            text: "hello",
          },
        ] as SessionV1.Part[],
      },
    ]

    expect(await MessageV2.toModelMessages(input, model)).toStrictEqual([
      {
        role: "user",
        content: [{ type: "text", text: "hello" }],
      },
    ])
  })

  test("filters out messages with only ignored parts", async () => {
    const messageID = "m-user"

    const input: SessionV1.WithParts[] = [
      {
        info: userInfo(messageID),
        parts: [
          {
            ...basePart(messageID, "p1"),
            type: "text",
            text: "ignored",
            ignored: true,
          },
        ] as SessionV1.Part[],
      },
    ]

    expect(await MessageV2.toModelMessages(input, model)).toStrictEqual([])
  })

  test("filters out user messages with only empty text parts", async () => {
    const messageID = "m-user"

    const input: SessionV1.WithParts[] = [
      {
        info: userInfo(messageID),
        parts: [
          {
            ...basePart(messageID, "p1"),
            type: "text",
            text: "",
          },
        ] as SessionV1.Part[],
      },
    ]

    expect(await MessageV2.toModelMessages(input, model)).toStrictEqual([])
  })

  test("filters empty user text parts while keeping non-empty parts", async () => {
    const messageID = "m-user"

    const input: SessionV1.WithParts[] = [
      {
        info: userInfo(messageID),
        parts: [
          {
            ...basePart(messageID, "p1"),
            type: "text",
            text: "",
          },
          {
            ...basePart(messageID, "p2"),
            type: "text",
            text: "hello",
          },
        ] as SessionV1.Part[],
      },
    ]

    expect(await MessageV2.toModelMessages(input, model)).toStrictEqual([
      {
        role: "user",
        content: [{ type: "text", text: "hello" }],
      },
    ])
  })

  test("includes synthetic text parts", async () => {
    const messageID = "m-user"

    const input: SessionV1.WithParts[] = [
      {
        info: userInfo(messageID),
        parts: [
          {
            ...basePart(messageID, "p1"),
            type: "text",
            text: "hello",
            synthetic: true,
          },
        ] as SessionV1.Part[],
      },
      {
        info: assistantInfo("m-assistant", messageID),
        parts: [
          {
            ...basePart("m-assistant", "a1"),
            type: "text",
            text: "assistant",
            synthetic: true,
          },
        ] as SessionV1.Part[],
      },
    ]

    expect(await MessageV2.toModelMessages(input, model)).toStrictEqual([
      {
        role: "user",
        content: [{ type: "text", text: "hello" }],
      },
      {
        role: "assistant",
        content: [{ type: "text", text: "assistant" }],
      },
    ])
  })

  test("converts user text/file parts and injects compaction/subtask prompts", async () => {
    const messageID = "m-user"

    const input: SessionV1.WithParts[] = [
      {
        info: userInfo(messageID),
        parts: [
          {
            ...basePart(messageID, "p1"),
            type: "text",
            text: "hello",
          },
          {
            ...basePart(messageID, "p2"),
            type: "text",
            text: "ignored",
            ignored: true,
          },
          {
            ...basePart(messageID, "p3"),
            type: "file",
            mime: "image/png",
            filename: "img.png",
            url: "https://example.com/img.png",
          },
          {
            ...basePart(messageID, "p4"),
            type: "file",
            mime: "text/plain",
            filename: "note.txt",
            url: "https://example.com/note.txt",
          },
          {
            ...basePart(messageID, "p5"),
            type: "file",
            mime: "application/x-directory",
            filename: "dir",
            url: "https://example.com/dir",
          },
          {
            ...basePart(messageID, "p6"),
            type: "compaction",
            auto: true,
          },
          {
            ...basePart(messageID, "p7"),
            type: "subtask",
            prompt: "prompt",
            description: "desc",
            agent: "agent",
          },
        ] as SessionV1.Part[],
      },
    ]

    expect(await MessageV2.toModelMessages(input, model)).toStrictEqual([
      {
        role: "user",
        content: [
          { type: "text", text: "hello" },
          {
            type: "file",
            mediaType: "image/png",
            filename: "img.png",
            data: "https://example.com/img.png",
          },
          { type: "text", text: "What did we do so far?" },
          { type: "text", text: "The following tool was executed by the user" },
        ],
      },
    ])
  })

  test("converts assistant tool completion into tool-call + tool-result messages with attachments", async () => {
    const userID = "m-user"
    const assistantID = "m-assistant"

    const input: SessionV1.WithParts[] = [
      {
        info: userInfo(userID),
        parts: [
          {
            ...basePart(userID, "u1"),
            type: "text",
            text: "run tool",
          },
        ] as SessionV1.Part[],
      },
      {
        info: assistantInfo(assistantID, userID),
        parts: [
          {
            ...basePart(assistantID, "a1"),
            type: "text",
            text: "done",
            metadata: { openai: { assistant: "meta" } },
          },
          {
            ...basePart(assistantID, "a2"),
            type: "tool",
            callID: "call-1",
            tool: "bash",
            state: {
              status: "completed",
              input: { cmd: "ls" },
              output: "ok",
              title: "Bash",
              metadata: {},
              time: { start: 0, end: 1 },
              attachments: [
                {
                  ...basePart(assistantID, "file-1"),
                  type: "file",
                  mime: "image/png",
                  filename: "attachment.png",
                  url: "data:image/png;base64,Zm9v",
                },
              ],
            },
            metadata: { openai: { tool: "meta" } },
          },
        ] as SessionV1.Part[],
      },
    ]

    expect(await MessageV2.toModelMessages(input, model)).toStrictEqual([
      {
        role: "user",
        content: [{ type: "text", text: "run tool" }],
      },
      {
        role: "assistant",
        content: [
          { type: "text", text: "done", providerOptions: { openai: { assistant: "meta" } } },
          {
            type: "tool-call",
            toolCallId: "call-1",
            toolName: "bash",
            input: { cmd: "ls" },
            providerExecuted: undefined,
            providerOptions: { openai: { tool: "meta" } },
          },
        ],
      },
      {
        role: "tool",
        content: [
          {
            type: "tool-result",
            toolCallId: "call-1",
            toolName: "bash",
            output: {
              type: "content",
              value: [
                { type: "text", text: "ok" },
                { type: "media", mediaType: "image/png", data: "Zm9v" },
              ],
            },
            providerOptions: { openai: { tool: "meta" } },
          },
        ],
      },
    ])
  })

  test("preserves jpeg tool-result media for anthropic models", async () => {
    const anthropicModel: Provider.Model = {
      ...model,
      id: ModelV2.ID.make("anthropic/claude-opus-4-7"),
      providerID: ProviderV2.ID.make("anthropic"),
      api: {
        id: "claude-opus-4-7-20250805",
        url: "https://api.anthropic.com",
        npm: "@ai-sdk/anthropic",
      },
      capabilities: {
        ...model.capabilities,
        attachment: true,
        input: {
          ...model.capabilities.input,
          image: true,
          pdf: true,
        },
      },
    }
    const jpeg = Buffer.from([0xff, 0xd8, 0xff, 0xe0, 0x00, 0x10, 0x4a, 0x46, 0x49, 0x46, 0x00, 0x01]).toString(
      "base64",
    )
    const userID = "m-user-anthropic"
    const assistantID = "m-assistant-anthropic"
    const input: SessionV1.WithParts[] = [
      {
        info: userInfo(userID),
        parts: [
          {
            ...basePart(userID, "u1-anthropic"),
            type: "text",
            text: "run tool",
          },
        ] as SessionV1.Part[],
      },
      {
        info: assistantInfo(assistantID, userID),
        parts: [
          {
            ...basePart(assistantID, "a1-anthropic"),
            type: "tool",
            callID: "call-anthropic-1",
            tool: "read",
            state: {
              status: "completed",
              input: { filePath: "/tmp/rails-demo.png" },
              output: "Image read successfully",
              title: "Read",
              metadata: {},
              time: { start: 0, end: 1 },
              attachments: [
                {
                  ...basePart(assistantID, "file-anthropic-1"),
                  type: "file",
                  mime: "image/jpeg",
                  filename: "rails-demo.png",
                  url: `data:image/jpeg;base64,${jpeg}`,
                },
              ],
            },
          },
        ] as SessionV1.Part[],
      },
    ]

    const result = ProviderTransform.message(await MessageV2.toModelMessages(input, anthropicModel), anthropicModel, {})
    expect(result).toHaveLength(3)
    expect(result[2].role).toBe("tool")
    expect(result[2].content[0]).toMatchObject({
      type: "tool-result",
      toolCallId: "call-anthropic-1",
      toolName: "read",
      output: {
        type: "content",
        value: [
          { type: "text", text: "Image read successfully" },
          { type: "media", mediaType: "image/jpeg", data: jpeg },
        ],
      },
    })
  })

  test("moves bedrock pdf tool-result media into a separate user message", async () => {
    const bedrockModel: Provider.Model = {
      ...model,
      id: ModelV2.ID.make("amazon-bedrock/anthropic.claude-sonnet-4-6"),
      providerID: ProviderV2.ID.make("amazon-bedrock"),
      api: {
        id: "anthropic.claude-sonnet-4-6",
        url: "https://bedrock-runtime.us-east-1.amazonaws.com",
        npm: "@ai-sdk/amazon-bedrock",
      },
      capabilities: {
        ...model.capabilities,
        attachment: true,
        input: {
          ...model.capabilities.input,
          image: true,
          pdf: true,
        },
      },
    }
    const pdf = Buffer.from("%PDF-1.4\n").toString("base64")
    const userID = "m-user-bedrock-pdf"
    const assistantID = "m-assistant-bedrock-pdf"
    const input: SessionV1.WithParts[] = [
      {
        info: userInfo(userID),
        parts: [
          {
            ...basePart(userID, "u1-bedrock-pdf"),
            type: "text",
            text: "run tool",
          },
        ] as SessionV1.Part[],
      },
      {
        info: assistantInfo(assistantID, userID),
        parts: [
          {
            ...basePart(assistantID, "a1-bedrock-pdf"),
            type: "tool",
            callID: "call-bedrock-pdf-1",
            tool: "read",
            state: {
              status: "completed",
              input: { filePath: "/tmp/example.pdf" },
              output: "PDF read successfully",
              title: "Read",
              metadata: {},
              time: { start: 0, end: 1 },
              attachments: [
                {
                  ...basePart(assistantID, "file-bedrock-pdf-1"),
                  type: "file",
                  mime: "application/pdf",
                  filename: "example.pdf",
                  url: `data:application/pdf;base64,${pdf}`,
                },
              ],
            },
          },
        ] as SessionV1.Part[],
      },
    ]

    expect(await MessageV2.toModelMessages(input, bedrockModel)).toStrictEqual([
      {
        role: "user",
        content: [{ type: "text", text: "run tool" }],
      },
      {
        role: "assistant",
        content: [
          {
            type: "tool-call",
            toolCallId: "call-bedrock-pdf-1",
            toolName: "read",
            input: { filePath: "/tmp/example.pdf" },
            providerExecuted: undefined,
          },
        ],
      },
      {
        role: "tool",
        content: [
          {
            type: "tool-result",
            toolCallId: "call-bedrock-pdf-1",
            toolName: "read",
            output: { type: "text", value: "PDF read successfully" },
          },
        ],
      },
      {
        role: "user",
        content: [
          { type: "text", text: "Attached media from tool result:" },
          {
            type: "file",
            mediaType: "application/pdf",
            filename: "example.pdf",
            data: `data:application/pdf;base64,${pdf}`,
          },
        ],
      },
    ])
  })

  test("omits provider metadata when assistant model differs", async () => {
    const userID = "m-user"
    const assistantID = "m-assistant"

    const input: SessionV1.WithParts[] = [
      {
        info: userInfo(userID),
        parts: [
          {
            ...basePart(userID, "u1"),
            type: "text",
            text: "run tool",
          },
        ] as SessionV1.Part[],
      },
      {
        info: assistantInfo(assistantID, userID, undefined, { providerID: "other", modelID: "other" }),
        parts: [
          {
            ...basePart(assistantID, "a1"),
            type: "text",
            text: "done",
            metadata: { openai: { assistant: "meta" } },
          },
          {
            ...basePart(assistantID, "a2"),
            type: "reasoning",
            text: "thinking",
            metadata: { openai: { reasoning: "meta" } },
            time: { start: 0 },
          },
          {
            ...basePart(assistantID, "a3"),
            type: "tool",
            callID: "call-1",
            tool: "bash",
            state: {
              status: "completed",
              input: { cmd: "ls" },
              output: "ok",
              title: "Bash",
              metadata: {},
              time: { start: 0, end: 1 },
            },
            metadata: { openai: { tool: "meta" } },
          },
        ] as SessionV1.Part[],
      },
    ]

    expect(await MessageV2.toModelMessages(input, model)).toStrictEqual([
      {
        role: "user",
        content: [{ type: "text", text: "run tool" }],
      },
      {
        role: "assistant",
        content: [
          { type: "text", text: "done" },
          { type: "text", text: "thinking" },
          {
            type: "tool-call",
            toolCallId: "call-1",
            toolName: "bash",
            input: { cmd: "ls" },
            providerExecuted: undefined,
          },
        ],
      },
      {
        role: "tool",
        content: [
          {
            type: "tool-result",
            toolCallId: "call-1",
            toolName: "bash",
            output: { type: "text", value: "ok" },
          },
        ],
      },
    ])
  })

  test("replaces compacted tool output with placeholder", async () => {
    const userID = "m-user"
    const assistantID = "m-assistant"

    const input: SessionV1.WithParts[] = [
      {
        info: userInfo(userID),
        parts: [
          {
            ...basePart(userID, "u1"),
            type: "text",
            text: "run tool",
          },
        ] as SessionV1.Part[],
      },
      {
        info: assistantInfo(assistantID, userID),
        parts: [
          {
            ...basePart(assistantID, "a1"),
            type: "tool",
            callID: "call-1",
            tool: "bash",
            state: {
              status: "completed",
              input: { cmd: "ls" },
              output: "this should be cleared",
              title: "Bash",
              metadata: {},
              time: { start: 0, end: 1, compacted: 1 },
            },
          },
        ] as SessionV1.Part[],
      },
    ]

    expect(await MessageV2.toModelMessages(input, model)).toStrictEqual([
      {
        role: "user",
        content: [{ type: "text", text: "run tool" }],
      },
      {
        role: "assistant",
        content: [
          {
            type: "tool-call",
            toolCallId: "call-1",
            toolName: "bash",
            input: { cmd: "ls" },
            providerExecuted: undefined,
          },
        ],
      },
      {
        role: "tool",
        content: [
          {
            type: "tool-result",
            toolCallId: "call-1",
            toolName: "bash",
            output: { type: "text", value: "[Old tool result content cleared]" },
          },
        ],
      },
    ])
  })

  test("truncates tool output when requested", async () => {
    const userID = "m-user"
    const assistantID = "m-assistant"

    const input: SessionV1.WithParts[] = [
      {
        info: userInfo(userID),
        parts: [
          {
            ...basePart(userID, "u1"),
            type: "text",
            text: "run tool",
          },
        ] as SessionV1.Part[],
      },
      {
        info: assistantInfo(assistantID, userID),
        parts: [
          {
            ...basePart(assistantID, "a1"),
            type: "tool",
            callID: "call-1",
            tool: "bash",
            state: {
              status: "completed",
              input: { cmd: "ls" },
              output: "abcdefghij",
              title: "Shell",
              metadata: {},
              time: { start: 0, end: 1 },
            },
          },
        ] as SessionV1.Part[],
      },
    ]

    expect(await MessageV2.toModelMessages(input, model, { toolOutputMaxChars: 4 })).toStrictEqual([
      {
        role: "user",
        content: [{ type: "text", text: "run tool" }],
      },
      {
        role: "assistant",
        content: [
          {
            type: "tool-call",
            toolCallId: "call-1",
            toolName: "bash",
            input: { cmd: "ls" },
            providerExecuted: undefined,
          },
        ],
      },
      {
        role: "tool",
        content: [
          {
            type: "tool-result",
            toolCallId: "call-1",
            toolName: "bash",
            output: {
              type: "text",
              value: "abcd\n[Tool output truncated for compaction: omitted 6 chars]",
            },
          },
        ],
      },
    ])
  })

  test("converts assistant tool error into error-text tool result", async () => {
    const userID = "m-user"
    const assistantID = "m-assistant"

    const input: SessionV1.WithParts[] = [
      {
        info: userInfo(userID),
        parts: [
          {
            ...basePart(userID, "u1"),
            type: "text",
            text: "run tool",
          },
        ] as SessionV1.Part[],
      },
      {
        info: assistantInfo(assistantID, userID),
        parts: [
          {
            ...basePart(assistantID, "a1"),
            type: "tool",
            callID: "call-1",
            tool: "bash",
            state: {
              status: "error",
              input: { cmd: "ls" },
              error: "nope",
              time: { start: 0, end: 1 },
              metadata: {},
            },
            metadata: { openai: { tool: "meta" } },
          },
        ] as SessionV1.Part[],
      },
    ]

    expect(await MessageV2.toModelMessages(input, model)).toStrictEqual([
      {
        role: "user",
        content: [{ type: "text", text: "run tool" }],
      },
      {
        role: "assistant",
        content: [
          {
            type: "tool-call",
            toolCallId: "call-1",
            toolName: "bash",
            input: { cmd: "ls" },
            providerExecuted: undefined,
            providerOptions: { openai: { tool: "meta" } },
          },
        ],
      },
      {
        role: "tool",
        content: [
          {
            type: "tool-result",
            toolCallId: "call-1",
            toolName: "bash",
            output: { type: "error-text", value: "nope" },
            providerOptions: { openai: { tool: "meta" } },
          },
        ],
      },
    ])
  })

  test("forwards partial bash output for aborted tool calls", async () => {
    const userID = "m-user"
    const assistantID = "m-assistant"
    const output = [
      "31403",
      "12179",
      "4575",
      "",
      "<shell_metadata>",
      "User aborted the command",
      "</shell_metadata>",
    ].join("\n")

    const input: SessionV1.WithParts[] = [
      {
        info: userInfo(userID),
        parts: [
          {
            ...basePart(userID, "u1"),
            type: "text",
            text: "run tool",
          },
        ] as SessionV1.Part[],
      },
      {
        info: assistantInfo(assistantID, userID),
        parts: [
          {
            ...basePart(assistantID, "a1"),
            type: "tool",
            callID: "call-1",
            tool: "bash",
            state: {
              status: "error",
              input: { command: "for i in {1..20}; do print -- $RANDOM; sleep 1; done" },
              error: "Tool execution aborted",
              metadata: { interrupted: true, output },
              time: { start: 0, end: 1 },
            },
          },
        ] as SessionV1.Part[],
      },
    ]

    expect(await MessageV2.toModelMessages(input, model)).toStrictEqual([
      {
        role: "user",
        content: [{ type: "text", text: "run tool" }],
      },
      {
        role: "assistant",
        content: [
          {
            type: "tool-call",
            toolCallId: "call-1",
            toolName: "bash",
            input: { command: "for i in {1..20}; do print -- $RANDOM; sleep 1; done" },
            providerExecuted: undefined,
          },
        ],
      },
      {
        role: "tool",
        content: [
          {
            type: "tool-result",
            toolCallId: "call-1",
            toolName: "bash",
            output: { type: "text", value: output },
          },
        ],
      },
    ])
  })

  test("filters assistant messages with non-abort errors", async () => {
    const assistantID = "m-assistant"

    const input: SessionV1.WithParts[] = [
      {
        info: assistantInfo(
          assistantID,
          "m-parent",
          new SessionV1.APIError({ message: "boom", isRetryable: true }).toObject() as SessionV1.APIError,
        ),
        parts: [
          {
            ...basePart(assistantID, "a1"),
            type: "text",
            text: "should not render",
          },
        ] as SessionV1.Part[],
      },
    ]

    expect(await MessageV2.toModelMessages(input, model)).toStrictEqual([])
  })

  test("includes aborted assistant messages only when they have non-step-start/reasoning content", async () => {
    const assistantID1 = "m-assistant-1"
    const assistantID2 = "m-assistant-2"

    const aborted = new SessionV1.AbortedError({
      message: "aborted",
    }).toObject() as SessionV1.Assistant["error"]

    const input: SessionV1.WithParts[] = [
      {
        info: assistantInfo(assistantID1, "m-parent", aborted),
        parts: [
          {
            ...basePart(assistantID1, "a1"),
            type: "reasoning",
            text: "thinking",
            time: { start: 0 },
          },
          {
            ...basePart(assistantID1, "a2"),
            type: "text",
            text: "partial answer",
          },
        ] as SessionV1.Part[],
      },
      {
        info: assistantInfo(assistantID2, "m-parent", aborted),
        parts: [
          {
            ...basePart(assistantID2, "b1"),
            type: "step-start",
          },
          {
            ...basePart(assistantID2, "b2"),
            type: "reasoning",
            text: "thinking",
            time: { start: 0 },
          },
        ] as SessionV1.Part[],
      },
    ]

    expect(await MessageV2.toModelMessages(input, model)).toStrictEqual([
      {
        role: "assistant",
        content: [
          { type: "reasoning", text: "thinking", providerOptions: undefined },
          { type: "text", text: "partial answer" },
        ],
      },
    ])
  })

  test("preserves OpenRouter reasoning details through provider transform", async () => {
    const assistantID = "m-assistant"
    const openrouterModel: Provider.Model = {
      ...model,
      id: ModelV2.ID.make("deepseek/deepseek-v4-pro"),
      providerID: ProviderV2.ID.make("openrouter"),
      api: {
        id: "deepseek/deepseek-v4-pro",
        url: "https://openrouter.ai/api/v1",
        npm: "@openrouter/ai-sdk-provider",
      },
      capabilities: {
        ...model.capabilities,
        reasoning: true,
        interleaved: { field: "reasoning_details" },
      },
    }
    const reasoningDetails = [
      {
        type: "reasoning.text",
        text: "thinking",
        format: "unknown",
        index: 0,
      },
    ]
    const input: SessionV1.WithParts[] = [
      {
        info: assistantInfo(assistantID, "m-parent", undefined, {
          providerID: openrouterModel.providerID,
          modelID: openrouterModel.id,
        }),
        parts: [
          {
            ...basePart(assistantID, "a1"),
            type: "reasoning",
            text: "thinking",
            time: { start: 0 },
            metadata: {
              openrouter: {
                reasoning_details: reasoningDetails,
              },
            },
          },
          {
            ...basePart(assistantID, "a2"),
            type: "text",
            text: "answer",
          },
        ] as SessionV1.Part[],
      },
    ]

    expect(
      ProviderTransform.message(await MessageV2.toModelMessages(input, openrouterModel), openrouterModel, {}),
    ).toStrictEqual([
      {
        role: "assistant",
        content: [
          {
            type: "reasoning",
            text: "thinking",
            providerOptions: {
              openrouter: {
                reasoning_details: reasoningDetails,
              },
            },
          },
          { type: "text", text: "answer" },
        ],
      },
    ])
  })

  test("splits assistant messages on step-start boundaries", async () => {
    const assistantID = "m-assistant"

    const input: SessionV1.WithParts[] = [
      {
        info: assistantInfo(assistantID, "m-parent"),
        parts: [
          {
            ...basePart(assistantID, "p1"),
            type: "text",
            text: "first",
          },
          {
            ...basePart(assistantID, "p2"),
            type: "step-start",
          },
          {
            ...basePart(assistantID, "p3"),
            type: "text",
            text: "second",
          },
        ] as SessionV1.Part[],
      },
    ]

    expect(await MessageV2.toModelMessages(input, model)).toStrictEqual([
      {
        role: "assistant",
        content: [{ type: "text", text: "first" }],
      },
      {
        role: "assistant",
        content: [{ type: "text", text: "second" }],
      },
    ])
  })

  test("drops messages that only contain step-start parts", async () => {
    const assistantID = "m-assistant"

    const input: SessionV1.WithParts[] = [
      {
        info: assistantInfo(assistantID, "m-parent"),
        parts: [
          {
            ...basePart(assistantID, "p1"),
            type: "step-start",
          },
        ] as SessionV1.Part[],
      },
    ]

    expect(await MessageV2.toModelMessages(input, model)).toStrictEqual([])
  })

  test("converts pending/running tool calls to error results to prevent dangling tool_use", async () => {
    const userID = "m-user"
    const assistantID = "m-assistant"

    const input: SessionV1.WithParts[] = [
      {
        info: userInfo(userID),
        parts: [
          {
            ...basePart(userID, "u1"),
            type: "text",
            text: "run tool",
          },
        ] as SessionV1.Part[],
      },
      {
        info: assistantInfo(assistantID, userID),
        parts: [
          {
            ...basePart(assistantID, "a1"),
            type: "tool",
            callID: "call-pending",
            tool: "bash",
            state: {
              status: "pending",
              input: { cmd: "ls" },
              raw: "",
            },
          },
          {
            ...basePart(assistantID, "a2"),
            type: "tool",
            callID: "call-running",
            tool: "read",
            state: {
              status: "running",
              input: { path: "/tmp" },
              time: { start: 0 },
            },
          },
        ] as SessionV1.Part[],
      },
    ]

    const result = await MessageV2.toModelMessages(input, model)

    expect(result).toStrictEqual([
      {
        role: "user",
        content: [{ type: "text", text: "run tool" }],
      },
      {
        role: "assistant",
        content: [
          {
            type: "tool-call",
            toolCallId: "call-pending",
            toolName: "bash",
            input: { cmd: "ls" },
            providerExecuted: undefined,
          },
          {
            type: "tool-call",
            toolCallId: "call-running",
            toolName: "read",
            input: { path: "/tmp" },
            providerExecuted: undefined,
          },
        ],
      },
      {
        role: "tool",
        content: [
          {
            type: "tool-result",
            toolCallId: "call-pending",
            toolName: "bash",
            output: { type: "error-text", value: "[Tool execution was interrupted]" },
          },
          {
            type: "tool-result",
            toolCallId: "call-running",
            toolName: "read",
            output: { type: "error-text", value: "[Tool execution was interrupted]" },
          },
        ],
      },
    ])
  })

  test("substitutes space for empty text between signed reasoning blocks", async () => {
    // Reproduces the bug pattern: [reasoning(sig), text(""), reasoning(sig), text(full)]
    const assistantID = "m-assistant"
    const input: SessionV1.WithParts[] = [
      {
        info: assistantInfo(assistantID, "m-parent"),
        parts: [
          { ...basePart(assistantID, "p1"), type: "step-start" },
          {
            ...basePart(assistantID, "p2"),
            type: "reasoning",
            text: "thinking-one",
            metadata: { anthropic: { signature: "sig1" } },
          },
          { ...basePart(assistantID, "p3"), type: "text", text: "" },
          { ...basePart(assistantID, "p4"), type: "step-start" },
          {
            ...basePart(assistantID, "p5"),
            type: "reasoning",
            text: "thinking-two",
            metadata: { anthropic: { signature: "sig2" } },
          },
          { ...basePart(assistantID, "p6"), type: "text", text: "the answer" },
        ] as SessionV1.Part[],
      },
    ]

    const result = await MessageV2.toModelMessages(input, model)

    // step-start splits into two assistant messages; SDK's groupIntoBlocks merges them later
    expect(result).toHaveLength(2)
    expect((result[0].content as any[]).find((p) => p.type === "text").text).toBe(" ")
    expect((result[1].content as any[]).find((p) => p.type === "text").text).toBe("the answer")
  })

  test("leaves empty text alone when reasoning signature is under 'bedrock' namespace", async () => {
    // Bedrock signed reasoning is preserved as reasoning metadata, but unlike the
    // direct Anthropic path we do not preserve empty text separators for Bedrock.
    const assistantID = "m-assistant-bedrock"
    const input: SessionV1.WithParts[] = [
      {
        info: assistantInfo(assistantID, "m-parent"),
        parts: [
          {
            ...basePart(assistantID, "p1"),
            type: "reasoning",
            text: "thinking-bedrock",
            metadata: { bedrock: { signature: "bedrock-sig" } },
          },
          { ...basePart(assistantID, "p2"), type: "text", text: "" },
          { ...basePart(assistantID, "p3"), type: "text", text: "answer" },
        ] as SessionV1.Part[],
      },
    ]

    const result = await MessageV2.toModelMessages(input, model)

    expect(result).toHaveLength(1)
    const texts = (result[0].content as any[]).filter((p) => p.type === "text")
    expect(texts.map((t) => t.text)).toStrictEqual(["", "answer"])
  })

  test("leaves empty text alone when reasoning has no Anthropic signature", async () => {
    // Non-Anthropic providers' reasoning doesn't position-validate, so empty text
    // should be filtered normally rather than substituted.
    const assistantID = "m-assistant-unsigned"
    const input: SessionV1.WithParts[] = [
      {
        info: assistantInfo(assistantID, "m-parent"),
        parts: [
          { ...basePart(assistantID, "p1"), type: "reasoning", text: "thinking" },
          { ...basePart(assistantID, "p2"), type: "text", text: "" },
          { ...basePart(assistantID, "p3"), type: "text", text: "answer" },
        ] as SessionV1.Part[],
      },
    ]

    const result = await MessageV2.toModelMessages(input, model)

    expect(result).toHaveLength(1)
    const texts = (result[0].content as any[]).filter((p) => p.type === "text")
    expect(texts.map((t) => t.text)).toStrictEqual(["", "answer"])
  })

  test("leaves empty text alone in assistant messages without reasoning", async () => {
    const assistantID = "m-assistant-no-reasoning"
    const input: SessionV1.WithParts[] = [
      {
        info: assistantInfo(assistantID, "m-parent"),
        parts: [
          { ...basePart(assistantID, "p1"), type: "text", text: "" },
          { ...basePart(assistantID, "p2"), type: "text", text: "hello" },
        ] as SessionV1.Part[],
      },
    ]

    const result = await MessageV2.toModelMessages(input, model)

    expect(result).toHaveLength(1)
    const texts = (result[0].content as any[]).filter((p) => p.type === "text")
    expect(texts.map((t) => t.text)).toStrictEqual(["", "hello"])
  })
})

describe("session.message-v2.fromError", () => {
  test("serializes context_length_exceeded as ContextOverflowError", () => {
    const input = {
      type: "error",
      error: {
        code: "context_length_exceeded",
      },
    }
    const result = MessageV2.fromError(input, { providerID })

    expect(result).toStrictEqual({
      name: "ContextOverflowError",
      data: {
        message: "Input exceeds context window of this model",
        responseBody: JSON.stringify(input),
      },
    })
  })

  test("serializes response error codes", () => {
    const cases = [
      {
        code: "insufficient_quota",
        message: "Quota exceeded. Check your plan and billing details.",
      },
      {
        code: "usage_not_included",
        message: "To use Codex with your ChatGPT plan, upgrade to Plus: https://chatgpt.com/explore/plus.",
      },
      {
        code: "invalid_prompt",
        message: "Invalid prompt from test",
      },
    ]

    cases.forEach((item) => {
      const input = {
        type: "error",
        error: {
          code: item.code,
          message: item.code === "invalid_prompt" ? item.message : undefined,
        },
      }
      const result = MessageV2.fromError(input, { providerID })

      expect(result).toStrictEqual({
        name: "APIError",
        data: {
          message: item.message,
          isRetryable: false,
          responseBody: JSON.stringify(input),
        },
      })
    })
  })

  test("serializes OpenAI response server_error stream chunks as retryable APIError", () => {
    const body = {
      type: "error",
      sequence_number: 2,
      error: {
        type: "server_error",
        code: "server_error",
        message:
          "An error occurred while processing your request. You can retry your request, or contact us through our help center at help.openai.com if the error persists. Please include the request ID req_77eccd008d984bf6bf82d1b2c2b68715 in your message.",
        param: null,
      },
    }
    const result = MessageV2.fromError({ message: JSON.stringify(body) }, { providerID })

    expect(result).toStrictEqual({
      name: "APIError",
      data: {
        message: body.error.message,
        isRetryable: true,
        responseBody: JSON.stringify(body),
      },
    })
  })

  test("detects context overflow from APICallError provider messages", () => {
    const cases = [
      "prompt is too long: 213462 tokens > 200000 maximum",
      "Your input exceeds the context window of this model",
      "The input token count (1196265) exceeds the maximum number of tokens allowed (1048575)",
      "Please reduce the length of the messages or completion",
      "400 status code (no body)",
      "413 status code (no body)",
    ]

    cases.forEach((message) => {
      const error = new APICallError({
        message,
        url: "https://example.com",
        requestBodyValues: {},
        statusCode: 400,
        responseHeaders: { "content-type": "application/json" },
        isRetryable: false,
      })
      const result = MessageV2.fromError(error, { providerID })
      expect(SessionV1.ContextOverflowError.isInstance(result)).toBe(true)
    })
  })

  test("detects context overflow from context_length_exceeded code in response body", () => {
    const error = new APICallError({
      message: "Request failed",
      url: "https://example.com",
      requestBodyValues: {},
      statusCode: 422,
      responseHeaders: { "content-type": "application/json" },
      responseBody: JSON.stringify({
        error: {
          message: "Some message",
          type: "invalid_request_error",
          code: "context_length_exceeded",
        },
      }),
      isRetryable: false,
    })
    const result = MessageV2.fromError(error, { providerID })
    expect(SessionV1.ContextOverflowError.isInstance(result)).toBe(true)
  })

  test("does not classify 429 no body as context overflow", () => {
    const result = MessageV2.fromError(
      new APICallError({
        message: "429 status code (no body)",
        url: "https://example.com",
        requestBodyValues: {},
        statusCode: 429,
        responseHeaders: { "content-type": "application/json" },
        isRetryable: false,
      }),
      { providerID },
    )
    expect(SessionV1.ContextOverflowError.isInstance(result)).toBe(false)
    expect(SessionV1.APIError.isInstance(result)).toBe(true)
  })

  test("serializes unknown inputs", () => {
    const result = MessageV2.fromError(123, { providerID })

    expect(result).toStrictEqual({
      name: "UnknownError",
      data: {
        message: "123",
      },
    })
  })

  test("serializes tagged errors with their message", () => {
    const result = MessageV2.fromError(new Question.RejectedError(), { providerID })

    expect(result).toStrictEqual({
      name: "UnknownError",
      data: {
        message: "The user dismissed this question",
      },
    })
  })

  test("classifies ZlibError from fetch as retryable APIError", () => {
    const zlibError = new Error(
      'ZlibError fetching "https://opencode.cloudflare.dev/anthropic/messages". For more information, pass `verbose: true` in the second argument to fetch()',
    )
    ;(zlibError as any).code = "ZlibError"
    ;(zlibError as any).errno = 0
    ;(zlibError as any).path = ""

    const result = MessageV2.fromError(zlibError, { providerID })

    expect(SessionV1.APIError.isInstance(result)).toBe(true)
    expect((result as SessionV1.APIError).data.isRetryable).toBe(true)
    expect((result as SessionV1.APIError).data.message).toInclude("decompression")
  })

  test("classifies ZlibError as AbortedError when abort context is provided", () => {
    const zlibError = new Error(
      'ZlibError fetching "https://opencode.cloudflare.dev/anthropic/messages". For more information, pass `verbose: true` in the second argument to fetch()',
    )
    ;(zlibError as any).code = "ZlibError"
    ;(zlibError as any).errno = 0

    const result = MessageV2.fromError(zlibError, { providerID, aborted: true })

    expect(result.name).toBe("MessageAbortedError")
  })
})

describe("session.message-v2.modelTurn continuity", () => {
  const text = (messageID: MessageID, id: string, value: string, extra?: Partial<SessionV1.TextPart>) =>
    ({ ...basePart(messageID, id), type: "text", text: value, ...extra }) as SessionV1.TextPart
  const completedAssistant = (id: MessageID, parentID: MessageID, extra?: Partial<SessionV1.Assistant>) =>
    ({
      ...assistantInfo(id, parentID),
      time: { created: Number(id.slice(-3)), completed: Number(id.slice(-3)) },
      finish: "stop",
      ...extra,
    }) as SessionV1.Assistant

  test("G3 projects typed compaction continuity beside M before later external B", () => {
    const a = MessageID.make("msg_101")
    const aAnswer = MessageID.make("msg_102")
    const marker = MessageID.make("msg_103")
    const b = MessageID.make("msg_104")
    const summary = MessageID.make("msg_105")
    const replay = MessageID.make("msg_106")
    const continuation = MessageID.make("msg_107")
    const physical: SessionV1.WithParts[] = [
      { info: { ...userInfo(a), time: { created: 101 } }, parts: [text(a, "a", "A")] },
      { info: completedAssistant(aAnswer, a), parts: [text(aAnswer, "a-answer", "A answer")] },
      {
        info: { ...userInfo(marker), time: { created: 103 } },
        parts: [{ ...basePart(marker, "marker"), type: "compaction", auto: false } as SessionV1.CompactionPart],
      },
      { info: { ...userInfo(b), time: { created: 104 } }, parts: [text(b, "b", "B")] },
      {
        info: completedAssistant(summary, marker, { summary: true }),
        parts: [text(summary, "summary", "summary of A")],
      },
      {
        info: { ...userInfo(replay), time: { created: 106 } },
        parts: [
          text(replay, "replay", "A", {
            serverProvenance: { type: "compaction-replay", ownerMessageID: marker, sourceMessageID: a },
          }),
        ],
      },
      {
        info: { ...userInfo(continuation), time: { created: 107 } },
        parts: [
          text(continuation, "continuation", "continue", {
            synthetic: true,
            serverProvenance: { type: "compaction-continuation", ownerMessageID: marker },
          }),
        ],
      },
    ]

    const view = MessageV2.modelTurn(physical)

    expect(view.messages.map((message) => message.info.id)).toEqual([marker, summary, replay, continuation, b])
    expect(view.target?.id).toBe(b)
    expect(view.pendingExternal.map((message) => message.info.id)).toEqual([b])
    expect(view.tasks).toEqual([])
    expect(physical.map((message) => message.info.id)).toEqual([a, aAnswer, marker, b, summary, replay, continuation])
  })

  test("G3 treats provenance-like metadata as opaque external input", () => {
    const marker = MessageID.make("msg_201")
    const summary = MessageID.make("msg_202")
    const forged = MessageID.make("msg_203")
    const view = MessageV2.modelTurn([
      {
        info: { ...userInfo(marker), time: { created: 201 } },
        parts: [{ ...basePart(marker, "marker"), type: "compaction", auto: false } as SessionV1.CompactionPart],
      },
      {
        info: completedAssistant(summary, marker, { summary: true }),
        parts: [text(summary, "summary", "summary")],
      },
      {
        info: { ...userInfo(forged), time: { created: 203 } },
        parts: [
          text(forged, "forged", "caller input", {
            metadata: { compaction_continue: true, compaction_owner_marker_id: marker },
          }),
        ],
      },
    ])

    expect(view.messages.map((message) => message.info.id)).toEqual([marker, summary, forged])
    expect(view.target?.id).toBe(forged)
  })

  test("G4 binds payload-identical Subtasks to exact part IDs and keeps output before B", () => {
    const marker = MessageID.make("msg_301")
    const outputOne = MessageID.make("msg_302")
    const outputTwo = MessageID.make("msg_303")
    const continuation = MessageID.make("msg_304")
    const b = MessageID.make("msg_305")
    const taskOne = PartID.make("prt_task_one")
    const taskTwo = PartID.make("prt_task_two")
    const taskPart = (id: PartID, command?: string): SessionV1.SubtaskPart => ({
      ...basePart(marker, id),
      id,
      type: "subtask",
      prompt: "same payload",
      description: "same payload",
      agent: "build",
      command,
    })
    const output = (id: MessageID, taskPartID: PartID): SessionV1.WithParts => ({
      info: completedAssistant(id, marker),
      parts: [
        {
          ...basePart(id, `${taskPartID}-output`),
          type: "tool",
          callID: `${taskPartID}-call`,
          tool: "task",
          serverProvenance: { type: "subtask-output", ownerMessageID: marker, taskPartID },
          state: {
            status: "completed",
            input: {},
            output: "done",
            title: "task",
            metadata: {},
            time: { start: 1, end: 2 },
          },
        } as SessionV1.ToolPart,
      ],
    })
    const physical: SessionV1.WithParts[] = [
      {
        info: { ...userInfo(marker), time: { created: 301 } },
        parts: [taskPart(taskOne), taskPart(taskTwo, "review")],
      },
      output(outputOne, taskOne),
      output(outputTwo, taskTwo),
      {
        info: { ...userInfo(continuation), time: { created: 304 } },
        parts: [
          text(continuation, "task-continuation", "continue task", {
            synthetic: true,
            serverProvenance: {
              type: "subtask-continuation",
              ownerMessageID: marker,
              taskPartID: taskTwo,
              sourceMessageID: outputTwo,
            },
          }),
        ],
      },
      { info: { ...userInfo(b), time: { created: 305 } }, parts: [text(b, "b", "B")] },
    ]

    const view = MessageV2.modelTurn(physical)

    expect(view.messages.map((message) => message.info.id)).toEqual([marker, outputOne, outputTwo, continuation, b])
    expect(view.target?.id).toBe(b)
    expect(view.tasks).toEqual([])
    expect(
      view.messages
        .flatMap((message) => message.parts)
        .filter((part): part is SessionV1.ToolPart => part.type === "tool")
        .map((part) => part.serverProvenance),
    ).toEqual([
      { type: "subtask-output", ownerMessageID: marker, taskPartID: taskOne },
      { type: "subtask-output", ownerMessageID: marker, taskPartID: taskTwo },
    ])
  })
})

describe("server provenance caller-write boundaries", () => {
  const forged = {
    type: "compaction-replay",
    ownerMessageID: "msg_owner",
    sourceMessageID: "msg_source",
  }

  test.each([
    [
      "prompt text",
      SessionPrompt.PromptInput,
      {
        sessionID,
        agent: "build",
        model: { providerID, modelID: ModelV2.ID.make("test") },
        parts: [{ type: "text", text: "hello", metadata: { keep: "prompt" }, serverProvenance: forged }],
      },
    ],
    [
      "command attachment",
      SessionPrompt.CommandInput,
      {
        sessionID,
        command: "review",
        arguments: "",
        parts: [
          {
            type: "file",
            mime: "text/plain",
            filename: "note.txt",
            url: "data:text/plain,hello",
            serverProvenance: forged,
          },
        ],
      },
    ],
    [
      "text update",
      SessionV1.PartUpdateInput,
      {
        id: PartID.make("prt_text_update"),
        sessionID,
        messageID: MessageID.make("msg_text_update"),
        type: "text",
        text: "updated",
        metadata: { keep: "text-update" },
        serverProvenance: forged,
      },
    ],
    [
      "tool update and nested attachment",
      SessionV1.PartUpdateInput,
      {
        id: PartID.make("prt_tool_update"),
        sessionID,
        messageID: MessageID.make("msg_tool_update"),
        type: "tool",
        callID: "call-update",
        tool: "task",
        metadata: { keep: "tool-update" },
        serverProvenance: forged,
        state: {
          status: "completed",
          input: {},
          output: "done",
          title: "task",
          metadata: { keep: "state" },
          time: { start: 1, end: 2 },
          attachments: [
            {
              id: PartID.make("prt_nested_update"),
              sessionID,
              messageID: MessageID.make("msg_tool_update"),
              type: "file",
              mime: "text/plain",
              url: "data:text/plain,nested",
              serverProvenance: forged,
            },
          ],
        },
      },
    ],
  ] as const)("strips %s provenance while preserving ordinary metadata", (_name, schema, input) => {
    const decoded = Schema.decodeUnknownSync(schema)(input)
    expect(JSON.stringify(decoded)).not.toContain("serverProvenance")
    if ("metadata" in decoded && decoded.metadata && "metadata" in input)
      expect(decoded.metadata).toEqual(input.metadata)
    if ("state" in decoded && decoded.state.status === "completed") {
      expect(decoded.state.metadata).toEqual({ keep: "state" })
    }
  })
})
