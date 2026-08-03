import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Effect } from "effect"
import type { Provider } from "@/provider/provider"
import { Plugin } from "@/plugin"
import { MessageV2 } from "./message-v2"

export function stripServerProvenance<T>(part: T): T {
  if (typeof part !== "object" || part === null) return part
  const value = { ...part } as Record<string, unknown>
  delete value.serverProvenance

  if (value.type !== "tool" || typeof value.state !== "object" || value.state === null) return value as T
  const state = value.state as Record<string, unknown>
  if (state.status !== "completed" || !Array.isArray(state.attachments)) return value as T
  value.state = {
    ...state,
    attachments: state.attachments.map((attachment) => {
      if (typeof attachment !== "object" || attachment === null) return attachment
      const nested = { ...attachment } as Record<string, unknown>
      delete nested.serverProvenance
      return nested
    }),
  }
  return value as T
}

function withoutServerProvenance(messages: SessionV1.WithParts[]) {
  return messages.map((message) => ({
    ...message,
    parts: message.parts.map(stripServerProvenance),
  }))
}

export const toPluginTransformedModelMessages = Effect.fnUntraced(function* (input: {
  messages: SessionV1.WithParts[]
  model: Provider.Model
  plugin: Plugin.Interface
  options?: { stripMedia?: boolean; toolOutputMaxChars?: number }
}) {
  const output = { messages: withoutServerProvenance(structuredClone(input.messages)) }
  yield* input.plugin.trigger("experimental.chat.messages.transform", {}, output)
  const sanitized = withoutServerProvenance(output.messages)
  return yield* MessageV2.toModelMessagesEffect(sanitized, input.model, input.options)
})
