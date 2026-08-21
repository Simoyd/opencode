import { SessionV1 } from "@opencode-ai/core/v1/session"
import { PermissionV1 } from "@opencode-ai/core/v1/permission"
import { Database } from "@opencode-ai/core/database/database"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { EventV2Bridge } from "@/event-v2-bridge"
import { expect } from "bun:test"
import { tool } from "ai"
import { Cause, Context, Deferred, Effect, Exit, Fiber, Layer, Scope, Stream } from "effect"
import path from "path"
import z from "zod"
import type { Agent } from "../../src/agent/agent"
import { Provider } from "@/provider/provider"
import { Question } from "@/question"

import { Session } from "@/session/session"
import { LLM } from "../../src/session/llm"
import { MessageV2 } from "../../src/session/message-v2"
import { SessionProcessor } from "../../src/session/processor"
import { MessageID, PartID, SessionID } from "../../src/session/schema"
import { SessionStatus } from "../../src/session/status"
import { SessionSummary } from "../../src/session/summary"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { provideTmpdirInstance, provideTmpdirServer, TestInstance } from "../fixture/fixture"
import { testEffect } from "../lib/effect"
import { raw, reply, TestLLMServer } from "../lib/llm-server"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { LLMEvent } from "@opencode-ai/llm"

const summary = Layer.succeed(
  SessionSummary.Service,
  SessionSummary.Service.of({
    summarize: () => Effect.void,
    diff: () => Effect.succeed([]),
    computeDiff: () => Effect.succeed([]),
  }),
)

const ref = {
  providerID: ProviderV2.ID.make("test"),
  modelID: ModelV2.ID.make("test-model"),
}

const cfg = {
  provider: {
    test: {
      name: "Test",
      id: "test",
      env: [],
      npm: "@ai-sdk/openai-compatible",
      models: {
        "test-model": {
          id: "test-model",
          name: "Test Model",
          attachment: false,
          reasoning: false,
          temperature: false,
          tool_call: true,
          release_date: "2025-01-01",
          limit: { context: 100000, output: 10000 },
          cost: { input: 0, output: 0 },
          options: {},
        },
      },
      options: {
        apiKey: "test-key",
        baseURL: "http://localhost:1/v1",
      },
    },
  },
}

function providerCfg(url: string) {
  return {
    ...cfg,
    provider: {
      ...cfg.provider,
      test: {
        ...cfg.provider.test,
        options: {
          ...cfg.provider.test.options,
          baseURL: url,
        },
      },
    },
  }
}

function agent(): Agent.Info {
  return {
    name: "build",
    mode: "primary",
    options: {},
    permission: [{ permission: "*", pattern: "*", action: "allow" }],
  }
}

function defer<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void
  const promise = new Promise<T>((done) => {
    resolve = done
  })
  return { promise, resolve }
}

const waitFor = <A>(check: Effect.Effect<A | undefined>, message: string) =>
  Effect.gen(function* () {
    const stop = Date.now() + 500
    while (Date.now() < stop) {
      const value = yield* check
      if (value !== undefined) return value
      yield* Effect.sleep("10 millis")
    }
    return yield* Effect.fail(new Error(message))
  })

const user = Effect.fn("TestSession.user")(function* (sessionID: SessionID, text: string) {
  const session = yield* Session.Service
  const msg = yield* session.updateMessage({
    id: MessageID.ascending(),
    role: "user",
    sessionID,
    agent: "build",
    model: ref,
    time: { created: Date.now() },
  })
  yield* session.updatePart({
    id: PartID.ascending(),
    messageID: msg.id,
    sessionID,
    type: "text",
    text,
  })
  return msg
})

const assistant = Effect.fn("TestSession.assistant")(function* (
  sessionID: SessionID,
  parentID: MessageID,
  root: string,
) {
  const session = yield* Session.Service
  const msg: SessionV1.Assistant = {
    id: MessageID.ascending(),
    role: "assistant",
    sessionID,
    mode: "build",
    agent: "build",
    path: { cwd: root, root },
    cost: 0,
    tokens: {
      total: 0,
      input: 0,
      output: 0,
      reasoning: 0,
      cache: { read: 0, write: 0 },
    },
    modelID: ref.modelID,
    providerID: ref.providerID,
    parentID,
    time: { created: Date.now() },
    finish: "end_turn",
  }
  yield* session.updateMessage(msg)
  return msg
})

const root = LayerNode.group([
  SessionProcessor.node,
  Session.node,
  SessionProjector.node,
  Provider.node,
  Database.node,
  EventV2Bridge.node,
  SessionStatus.node,
  CrossSpawnSpawner.node,
])
const replacements = [
  [SessionSummary.node, summary],
  [RuntimeFlags.node, RuntimeFlags.layer({ experimentalEventSystem: true })],
] as const
const env = LayerNode.compile(
  LayerNode.group([root, LayerNode.make({ service: TestLLMServer, layer: TestLLMServer.layer, deps: [] })]),
  replacements,
)

const it = testEffect(env)
const nativeEnv = LayerNode.compile(
  LayerNode.group([root, LayerNode.make({ service: TestLLMServer, layer: TestLLMServer.layer, deps: [] })]),
  [
    [SessionSummary.node, summary],
    [RuntimeFlags.node, RuntimeFlags.layer({ experimentalEventSystem: true, experimentalNativeLlm: true })],
  ],
)
const itNative = testEffect(nativeEnv)

interface ProcessorSessionHarnessInterface {
  readonly parts: Map<string, SessionV1.ToolPart>
  readonly creationEntered: Deferred.Deferred<void>
  readonly releaseCreation: Deferred.Deferred<void>
  creationAttempts: number
  creationMode: "normal" | "fail-before-commit" | "block-after-commit"
}

class ProcessorSessionHarness extends Context.Service<ProcessorSessionHarness, ProcessorSessionHarnessInterface>()(
  "@opencode/test/ProcessorSessionHarness",
) {}

const isToolPart = (part: SessionV1.Part): part is SessionV1.ToolPart => part.type === "tool"

const processorSessionHarnessLayer = Layer.effect(
  ProcessorSessionHarness,
  Effect.gen(function* () {
    return ProcessorSessionHarness.of({
      parts: new Map(),
      creationEntered: yield* Deferred.make<void>(),
      releaseCreation: yield* Deferred.make<void>(),
      creationAttempts: 0,
      creationMode: "normal",
    })
  }),
)

const processorSessionLayer = Layer.unwrap(
  Effect.gen(function* () {
    const harness = yield* ProcessorSessionHarness
    return Layer.mock(Session.Service, {
      updateMessage: <T extends SessionV1.Info>(message: T) => Effect.succeed(message),
      updatePart: <T extends SessionV1.Part>(part: T) =>
        Effect.gen(function* () {
          if (!isToolPart(part)) return part
          const creation = part.state.status === "pending" && !harness.parts.has(part.id)
          if (creation) {
            harness.creationAttempts++
            if (harness.creationMode === "fail-before-commit") {
              harness.creationMode = "normal"
              yield* Deferred.succeed(harness.creationEntered, undefined)
              yield* Deferred.await(harness.releaseCreation)
              return yield* Effect.die(new Error("simulated creation failure"))
            }
            harness.parts.set(part.id, structuredClone(part))
            if (harness.creationMode === "block-after-commit") {
              harness.creationMode = "normal"
              yield* Deferred.succeed(harness.creationEntered, undefined)
              yield* Deferred.await(harness.releaseCreation)
            }
            return part
          }
          harness.parts.set(part.id, structuredClone(part))
          return part
        }),
      getPart: (input: { partID: PartID }) => Effect.succeed(harness.parts.get(input.partID)),
    })
  }),
)

const processorSessionHarnessNode = LayerNode.make({
  service: ProcessorSessionHarness,
  layer: processorSessionHarnessLayer,
  deps: [],
})
const processorSessionNode = LayerNode.make({
  service: Session.Service,
  layer: processorSessionLayer,
  deps: [processorSessionHarnessNode],
})
const processorHarnessEnv = LayerNode.compile(
  LayerNode.group([SessionProcessor.node, Provider.node, processorSessionHarnessNode]),
  [...replacements, [Session.node, processorSessionNode]],
)
const itProcessorHarness = testEffect(processorHarnessEnv)

const startsProviderBeforeBusyObserversFinish = (dir: string) =>
  Effect.gen(function* () {
    const llm = yield* TestLLMServer
    const { processors, session, provider } = yield* boot()
    const events = yield* EventV2Bridge.Service
    const status = yield* SessionStatus.Service
    const admitted = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    const finished = yield* Deferred.make<void>()
    yield* events.listen((event) => {
      if (event.type !== SessionStatus.Event.Status.type) return Effect.void
      const data = event.data as typeof SessionStatus.Event.Status.data.Type
      if (data.status.type !== "busy") return Effect.void
      return Deferred.succeed(admitted, undefined).pipe(
        Effect.andThen(Deferred.await(release)),
        Effect.ensuring(Deferred.succeed(finished, undefined)),
        Effect.asVoid,
      )
    })
    yield* llm.hang

    const chat = yield* session.create({})
    const parent = yield* user(chat.id, "blocking status observer")
    const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
    const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
    const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model: mdl })
    const run = yield* handle
      .process({
        user: {
          id: parent.id,
          sessionID: chat.id,
          role: "user",
          time: parent.time,
          agent: parent.agent,
          model: { providerID: ref.providerID, modelID: ref.modelID },
        } satisfies SessionV1.User,
        sessionID: chat.id,
        model: mdl,
        agent: agent(),
        system: [],
        messages: [{ role: "user", content: "blocking status observer" }],
        tools: {},
      })
      .pipe(Effect.forkChild)

    yield* Deferred.await(admitted)
    expect((yield* status.get(chat.id)).type).toBe("busy")
    yield* llm.wait(1)
    expect(yield* Deferred.isDone(finished)).toBeFalse()
    yield* Deferred.succeed(release, undefined)
    yield* Deferred.await(finished)
    yield* Fiber.interrupt(run)
  })

const providerErrorLLM = Layer.succeed(
  LLM.Service,
  LLM.Service.of({
    stream: () =>
      Stream.make(
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.toolInputStart({ id: "call-1", name: "lookup" }),
        LLMEvent.toolInputEnd({ id: "call-1", name: "lookup" }),
        LLMEvent.toolCall({ id: "call-1", name: "lookup", input: {}, providerExecuted: true }),
        LLMEvent.toolResult({
          id: "call-1",
          name: "lookup",
          result: { type: "error", value: "provider boom" },
          providerExecuted: true,
        }),
        LLMEvent.stepFinish({ index: 0, reason: "stop" }),
        LLMEvent.finish({ reason: "stop" }),
      ),
  }),
)
const providerErrorEnv = LayerNode.compile(root, [...replacements, [LLM.node, providerErrorLLM]])
const itProviderError = testEffect(providerErrorEnv)

const providerFactsLLM = Layer.succeed(
  LLM.Service,
  LLM.Service.of({
    stream: () =>
      Stream.make(
        LLMEvent.toolInputDelta({ id: "facts-call", name: "unknown", text: '{"query":' }),
        LLMEvent.toolInputEnd({ id: "facts-call", name: "unknown" }),
        LLMEvent.toolCall({
          id: "facts-call",
          name: "lookup",
          input: { query: "weather" },
          providerExecuted: true,
        }),
        LLMEvent.toolResult({
          id: "facts-call",
          name: "lookup",
          result: {
            type: "json",
            value: { title: "Weather", metadata: { terminal: true }, output: "sunny" },
          },
          providerExecuted: true,
        }),
        LLMEvent.finish({ reason: "stop" }),
      ),
  }),
)
const providerFactsEnv = LayerNode.compile(root, [...replacements, [LLM.node, providerFactsLLM]])
const itProviderFacts = testEffect(providerFactsEnv)

const failedToolLLM = (error: Error, overflow = false) =>
  Layer.succeed(
    LLM.Service,
    LLM.Service.of({
      stream: () =>
        Stream.make(
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.toolCall({ id: "rejected-call", name: "read", input: { filePath: "/tmp/rejected" } }),
          LLMEvent.toolError({
            id: "rejected-call",
            name: "read",
            message: error.message,
            error,
          }),
          LLMEvent.stepFinish({
            index: 0,
            reason: "tool-calls",
            usage: overflow ? { inputTokens: 100, outputTokens: 0, totalTokens: 100 } : undefined,
          }),
          LLMEvent.finish({ reason: "tool-calls" }),
        ),
    }),
  )

const permissionRejectedEnv = LayerNode.compile(root, [
  ...replacements,
  [LLM.node, failedToolLLM(new PermissionV1.RejectedError())],
])
const questionRejectedEnv = LayerNode.compile(root, [
  ...replacements,
  [LLM.node, failedToolLLM(new Question.RejectedError())],
])
const permissionRejectedOverflowEnv = LayerNode.compile(root, [
  ...replacements,
  [LLM.node, failedToolLLM(new PermissionV1.RejectedError(), true)],
])
const ordinaryToolErrorEnv = LayerNode.compile(root, [
  ...replacements,
  [LLM.node, failedToolLLM(new Error("ordinary tool failure"))],
])
const itPermissionRejected = testEffect(permissionRejectedEnv)
const itQuestionRejected = testEffect(questionRejectedEnv)
const itPermissionRejectedOverflow = testEffect(permissionRejectedOverflowEnv)
const itOrdinaryToolError = testEffect(ordinaryToolErrorEnv)

const fragmentFailureLLM = Layer.succeed(
  LLM.Service,
  LLM.Service.of({
    stream: () =>
      Stream.make(
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.reasoningStart({ id: "reasoning-1" }),
        LLMEvent.reasoningDelta({ id: "reasoning-1", text: "thinking" }),
        LLMEvent.textStart({ id: "text-1" }),
        LLMEvent.textDelta({ id: "text-1", text: "partial" }),
        LLMEvent.providerError({ message: "provider boom" }),
      ),
  }),
)
const fragmentFailureEnv = LayerNode.compile(root, [...replacements, [LLM.node, fragmentFailureLLM]])
const itFragmentFailure = testEffect(fragmentFailureEnv)

const boot = Effect.fn("test.boot")(function* () {
  const processors = yield* SessionProcessor.Service
  const session = yield* Session.Service
  const provider = yield* Provider.Service
  return { processors, session, provider }
})

const runRejectedTool = Effect.fn("test.runRejectedTool")(function* (dir: string, overflow = false) {
  const { processors, session, provider } = yield* boot()
  const chat = yield* session.create({})
  const parent = yield* user(chat.id, "reject tool")
  const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
  const base = yield* provider.getModel(ref.providerID, ref.modelID)
  const model = overflow ? { ...base, limit: { context: 20, output: 10 } } : base
  const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model })
  const result = yield* handle.process({
    user: {
      id: parent.id,
      sessionID: chat.id,
      role: "user",
      time: parent.time,
      agent: parent.agent,
      model: { providerID: ref.providerID, modelID: ref.modelID },
    } satisfies SessionV1.User,
    sessionID: chat.id,
    model,
    agent: agent(),
    system: [],
    messages: [{ role: "user", content: "reject tool" }],
    tools: {},
  })
  const stored = yield* MessageV2.get({ sessionID: chat.id, messageID: msg.id })
  const call = stored.parts.find((part): part is SessionV1.ToolPart => part.type === "tool")
  return { result, handle, stored, call }
})

const processorHarness = Effect.fn("test.processorHarness")(function* (dir: string) {
  const processors = yield* SessionProcessor.Service
  const provider = yield* Provider.Service
  const sessionID = SessionID.descending()
  const message: SessionV1.Assistant = {
    id: MessageID.ascending(),
    role: "assistant",
    sessionID,
    mode: "build",
    agent: "build",
    path: { cwd: path.resolve(dir), root: path.resolve(dir) },
    cost: 0,
    tokens: {
      total: 0,
      input: 0,
      output: 0,
      reasoning: 0,
      cache: { read: 0, write: 0 },
    },
    modelID: ref.modelID,
    providerID: ref.providerID,
    parentID: MessageID.ascending(),
    time: { created: Date.now() },
    finish: "end_turn",
  }
  const model = yield* provider.getModel(ref.providerID, ref.modelID)
  const handle = yield* processors.create({ assistantMessage: message, sessionID, model })
  const harness = yield* ProcessorSessionHarness
  return { handle, harness }
})

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

it.live("session.processor effect tests capture llm input cleanly", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const database = yield* Database.Service
        const { processors, session, provider } = yield* boot()

        yield* llm.text("hello")

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "hi")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const input = {
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "hi" }],
          tools: {},
        } satisfies LLM.StreamInput

        const value = yield* handle.process(input)
        const parts = yield* MessageV2.parts({ sessionID: msg.sessionID, messageID: msg.id })
        const calls = yield* llm.calls

        expect(value).toBe("continue")
        expect(calls).toBe(1)
        expect(parts.some((part) => part.type === "text" && part.text === "hello")).toBe(true)
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests preserve text start time", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const database = yield* Database.Service
        const gate = defer<void>()
        const { processors, session, provider } = yield* boot()

        yield* llm.push(
          raw({
            head: [
              {
                id: "chatcmpl-test",
                object: "chat.completion.chunk",
                choices: [{ delta: { role: "assistant" } }],
              },
              {
                id: "chatcmpl-test",
                object: "chat.completion.chunk",
                choices: [{ delta: { content: "hello" } }],
              },
            ],
            wait: gate.promise,
            tail: [
              {
                id: "chatcmpl-test",
                object: "chat.completion.chunk",
                choices: [{ delta: {}, finish_reason: "stop" }],
              },
            ],
          }),
        )

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "hi")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const run = yield* handle
          .process({
            user: {
              id: parent.id,
              sessionID: chat.id,
              role: "user",
              time: parent.time,
              agent: parent.agent,
              model: { providerID: ref.providerID, modelID: ref.modelID },
            } satisfies SessionV1.User,
            sessionID: chat.id,
            model: mdl,
            agent: agent(),
            system: [],
            messages: [{ role: "user", content: "hi" }],
            tools: {},
          })
          .pipe(Effect.forkChild)

        yield* waitFor(
          MessageV2.parts({ sessionID: msg.sessionID, messageID: msg.id }).pipe(
            Effect.map((parts) => parts.find((part): part is SessionV1.TextPart => part.type === "text")),
            Effect.provideService(Database.Service, database),
          ),
          "timed out waiting for text part",
        )
        yield* Effect.sleep("20 millis")
        gate.resolve()

        const exit = yield* Fiber.await(run)
        const text = (yield* MessageV2.parts({ sessionID: msg.sessionID, messageID: msg.id })).find(
          (part): part is SessionV1.TextPart => part.type === "text",
        )

        expect(Exit.isSuccess(exit)).toBe(true)
        expect(text?.text).toBe("hello")
        expect(text?.time?.start).toBeDefined()
        expect(text?.time?.end).toBeDefined()
        if (!text?.time?.start || !text.time.end) return
        expect(text.time.start).toBeLessThan(text.time.end)
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests stop after token overflow requests compaction", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const database = yield* Database.Service
        const { processors, session, provider } = yield* boot()

        yield* llm.text("after", { usage: { input: 100, output: 0 } })

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "compact")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const base = yield* provider.getModel(ref.providerID, ref.modelID)
        const mdl = { ...base, limit: { context: 20, output: 10 } }
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "compact" }],
          tools: {},
        })

        const parts = yield* MessageV2.parts({ sessionID: msg.sessionID, messageID: msg.id })

        expect(value).toBe("compact")
        expect(parts.some((part) => part.type === "text" && part.text === "after")).toBe(true)
        expect(parts.some((part) => part.type === "step-finish")).toBe(true)
      }),
    { config: (url) => providerCfg(url) },
  ),
)

itPermissionRejected.live("session.processor terminalizes permission rejection before persisting the assistant", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        const { result, handle, stored, call } = yield* runRejectedTool(dir)

        expect(result).toBe("stop")
        expect(call?.state.status).toBe("error")
        if (call?.state.status === "error") expect(call.state.error).toBe(new PermissionV1.RejectedError().message)
        expect(handle.message.finish).toBe("stop")
        expect(handle.message.time.completed).toBeDefined()
        expect(stored.info.role).toBe("assistant")
        if (stored.info.role === "assistant") {
          expect(stored.info.finish).toBe("stop")
          expect(stored.info.time.completed).toBeDefined()
        }
      }),
    { config: cfg },
  ),
)

itQuestionRejected.live("session.processor terminalizes question rejection before persisting the assistant", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        const { result, handle, stored, call } = yield* runRejectedTool(dir)

        expect(result).toBe("stop")
        expect(call?.state.status).toBe("error")
        if (call?.state.status === "error") expect(call.state.error).toBe(new Question.RejectedError().message)
        expect(handle.message.finish).toBe("stop")
        expect(handle.message.time.completed).toBeDefined()
        expect(stored.info.role).toBe("assistant")
        if (stored.info.role === "assistant") {
          expect(stored.info.finish).toBe("stop")
          expect(stored.info.time.completed).toBeDefined()
        }
      }),
    { config: cfg },
  ),
)

itPermissionRejectedOverflow.live("session.processor prioritizes blocked rejection over compaction", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        const { result, handle, stored, call } = yield* runRejectedTool(dir, true)

        expect(result).toBe("stop")
        expect(call?.state.status).toBe("error")
        expect(handle.message.finish).toBe("stop")
        expect(stored.info.role).toBe("assistant")
        if (stored.info.role === "assistant") expect(stored.info.finish).toBe("stop")
      }),
    { config: cfg },
  ),
)

itPermissionRejected.live("session.processor preserves tool continuation when continue-on-deny is enabled", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        const { result, handle, stored, call } = yield* runRejectedTool(dir)

        expect(result).toBe("continue")
        expect(call?.state.status).toBe("error")
        expect(handle.message.finish).toBe("tool-calls")
        expect(stored.info.role).toBe("assistant")
        if (stored.info.role === "assistant") expect(stored.info.finish).toBe("tool-calls")
      }),
    {
      config: {
        ...cfg,
        experimental: { continue_loop_on_deny: true },
      },
    },
  ),
)

itOrdinaryToolError.live("session.processor preserves ordinary failed-tool continuation", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        const { result, handle, stored, call } = yield* runRejectedTool(dir)

        expect(result).toBe("continue")
        expect(call?.state.status).toBe("error")
        if (call?.state.status === "error") expect(call.state.error).toBe("ordinary tool failure")
        expect(handle.message.finish).toBe("tool-calls")
        expect(stored.info.role).toBe("assistant")
        if (stored.info.role === "assistant") expect(stored.info.finish).toBe("tool-calls")
      }),
    { config: cfg },
  ),
)

it.live("session.processor effect tests capture reasoning from http mock", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const database = yield* Database.Service
        const { processors, session, provider } = yield* boot()

        yield* llm.push(reply().reason("think").text("done").stop())

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "reason")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "reason" }],
          tools: {},
        })

        const parts = yield* MessageV2.parts({ sessionID: msg.sessionID, messageID: msg.id })
        const reasoning = parts.find((part): part is SessionV1.ReasoningPart => part.type === "reasoning")
        const text = parts.find((part): part is SessionV1.TextPart => part.type === "text")

        expect(value).toBe("continue")
        expect(yield* llm.calls).toBe(1)
        expect(reasoning?.text).toBe("think")
        expect(text?.text).toBe("done")
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests reset reasoning state across retries", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()

        yield* llm.push(reply().reason("one").reset(), reply().reason("two").stop())

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "reason")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "reason" }],
          tools: {},
        })

        const parts = yield* MessageV2.parts({ sessionID: msg.sessionID, messageID: msg.id })
        const reasoning = parts.filter((part): part is SessionV1.ReasoningPart => part.type === "reasoning")

        expect(value).toBe("continue")
        expect(yield* llm.calls).toBe(2)
        expect(reasoning.some((part) => part.text === "two")).toBe(true)
        expect(reasoning.some((part) => part.text === "onetwo")).toBe(false)
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests do not retry unknown json errors", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()

        yield* llm.error(400, { error: { message: "no_kv_space" } })

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "json")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "json" }],
          tools: {},
        })

        expect(value).toBe("stop")
        expect(yield* llm.calls).toBe(1)
        expect(handle.message.error?.name).toBe("APIError")
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests retry recognized structured json errors", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()

        yield* llm.error(429, { type: "error", error: { type: "too_many_requests" } })
        yield* llm.text("after")

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "retry json")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "retry json" }],
          tools: {},
        })

        const parts = yield* MessageV2.parts({ sessionID: msg.sessionID, messageID: msg.id })

        expect(value).toBe("continue")
        expect(yield* llm.calls).toBe(2)
        expect(parts.some((part) => part.type === "text" && part.text === "after")).toBe(true)
        expect(handle.message.error).toBeUndefined()
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests retry OpenAI-compatible midstream server errors", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()

        yield* llm.push(
          raw({ chunks: [{ error: { type: "server_error", code: "server_error", message: "xxx" } }] }),
        )
        yield* llm.text("after")

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "retry midstream server error")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "retry midstream server error" }],
          tools: {},
        })

        const parts = yield* MessageV2.parts({ sessionID: msg.sessionID, messageID: msg.id })

        expect(value).toBe("continue")
        expect(yield* llm.calls).toBe(2)
        expect(parts.some((part) => part.type === "text" && part.text === "after")).toBe(true)
        expect(handle.message.error).toBeUndefined()
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests retry network_error finish reasons", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()

        yield* llm.push(
          raw({
            chunks: [
              {
                id: "chatcmpl-network-error",
                object: "chat.completion.chunk",
                choices: [{ index: 0, delta: { role: "assistant", content: "" }, finish_reason: "network_error" }],
              },
            ],
          }),
        )
        yield* llm.text("after retry")

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "retry network error")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "retry network error" }],
          tools: {},
        })

        const parts = yield* MessageV2.parts(msg.id)

        expect(value).toBe("continue")
        expect(yield* llm.calls).toBe(2)
        expect(parts.some((part) => part.type === "text" && part.text === "after retry")).toBe(true)
        expect(handle.message.error).toBeUndefined()
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests publish retry status updates", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()
        const events = yield* EventV2Bridge.Service

        yield* llm.error(503, { error: "boom" })
        yield* llm.text("")

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "retry")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const states: number[] = []
        const off = yield* events.listen((evt) => {
          if (evt.type !== SessionStatus.Event.Status.type) return Effect.void
          const data = evt.data as typeof SessionStatus.Event.Status.data.Type
          if (data.sessionID === chat.id && data.status.type === "retry") states.push(data.status.attempt)
          return Effect.void
        })
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "retry" }],
          tools: {},
        })

        yield* off

        expect(value).toBe("continue")
        expect(yield* llm.calls).toBe(2)
        expect(states).toStrictEqual([1])
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor starts AI SDK transport before a busy status observer completes", () =>
  provideTmpdirServer(({ dir }) => startsProviderBeforeBusyObserversFinish(dir), {
    config: (url) => providerCfg(url),
  }),
)

itNative.live("session.processor starts native transport before a busy status observer completes", () =>
  provideTmpdirServer(({ dir }) => startsProviderBeforeBusyObserversFinish(dir), {
    config: (url) => providerCfg(url),
  }),
)

it.live("session.processor effect tests compact on structured context overflow", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()

        yield* llm.error(400, { type: "error", error: { code: "context_length_exceeded" } })

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "compact json")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "compact json" }],
          tools: {},
        })

        expect(value).toBe("compact")
        expect(yield* llm.calls).toBe(1)
        expect(handle.message.error).toBeUndefined()
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests complete AI SDK tool calls when native flag is off", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()

        yield* llm.tool("lookup", { query: "weather" })

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "tool")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "tool" }],
          tools: {
            lookup: tool({
              description: "Look up information",
              inputSchema: z.object({ query: z.string() }),
              execute: async (input) => ({
                title: "Weather lookup",
                output: `result:${input.query}`,
                metadata: { source: "test" },
              }),
            }),
          },
        })

        const parts = yield* MessageV2.parts({ sessionID: msg.sessionID, messageID: msg.id })
        const call = parts.find((part): part is SessionV1.ToolPart => part.type === "tool")

        expect(value).toBe("continue")
        expect(yield* llm.calls).toBe(1)
        expect(call?.callID).toBe("call_1")
        expect(call?.tool).toBe("lookup")
        expect(call?.state.status).toBe("completed")
        if (call?.state.status !== "completed") return
        expect(call.state.input).toEqual({ query: "weather" })
        expect(call.state.output).toBe("result:weather")
        expect(call.state.title).toBe("Weather lookup")
        expect(call.state.metadata).toEqual({ source: "test" })
        expect(call.state.time.start).toBeDefined()
        expect(call.state.time.end).toBeDefined()
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor coalesces concurrent registration for the same tool call", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        const database = yield* Database.Service
        const { processors, session, provider } = yield* boot()
        const events = yield* EventV2Bridge.Service
        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "same tool call")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model: mdl })
        const scope = yield* Scope.Scope
        const secondExit = yield* Deferred.make<Exit.Exit<SessionV1.ToolPart>>()
        let creations = 0
        const off = yield* events.listen((event) => {
          if (event.type !== SessionV1.Event.PartUpdated.type) return Effect.void
          const data = event.data as typeof SessionV1.Event.PartUpdated.data.Type
          if (
            data.part.type !== "tool" ||
            data.part.messageID !== msg.id ||
            data.part.callID !== "shared-call" ||
            data.part.state.status !== "pending"
          ) {
            return Effect.void
          }
          creations++
          if (creations !== 1) return Effect.void
          return handle.registerToolCall({ toolCallID: "shared-call", toolName: "lookup" }).pipe(
            Effect.exit,
            Effect.flatMap((exit) => Deferred.succeed(secondExit, exit)),
            Effect.forkIn(scope, { startImmediately: true }),
            Effect.asVoid,
          )
        })

        const first = yield* handle.registerToolCall({ toolCallID: "shared-call", toolName: "lookup" })
        const second = yield* Deferred.await(secondExit)
        const calls = (yield* MessageV2.parts({ sessionID: msg.sessionID, messageID: msg.id }).pipe(
          Effect.provideService(Database.Service, database),
        )).filter((part): part is SessionV1.ToolPart => part.type === "tool" && part.callID === "shared-call")
        expect(calls).toHaveLength(1)
        expect(Exit.isSuccess(second)).toBe(true)
        if (Exit.isSuccess(second)) expect(second.value.id).toBe(first.id)
        expect(creations).toBe(1)
        yield* off
      }),
    { config: cfg },
  ),
)

it.live("session.processor keeps concurrent registration for different tool calls independent", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        const database = yield* Database.Service
        const { processors, session, provider } = yield* boot()
        const events = yield* EventV2Bridge.Service
        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "different tool calls")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model: mdl })
        const scope = yield* Scope.Scope
        const secondExit = yield* Deferred.make<Exit.Exit<SessionV1.ToolPart>>()
        const creations: string[] = []
        const off = yield* events.listen((event) => {
          if (event.type !== SessionV1.Event.PartUpdated.type) return Effect.void
          const data = event.data as typeof SessionV1.Event.PartUpdated.data.Type
          if (
            data.part.type !== "tool" ||
            data.part.messageID !== msg.id ||
            !["call-a", "call-b"].includes(data.part.callID) ||
            data.part.state.status !== "pending"
          ) {
            return Effect.void
          }
          creations.push(data.part.callID)
          if (data.part.callID !== "call-a") return Effect.void
          return handle.registerToolCall({ toolCallID: "call-b", toolName: "lookup" }).pipe(
            Effect.exit,
            Effect.flatMap((exit) => Deferred.succeed(secondExit, exit)),
            Effect.forkIn(scope, { startImmediately: true }),
            Effect.asVoid,
          )
        })

        const first = yield* handle.registerToolCall({ toolCallID: "call-a", toolName: "lookup" })
        const second = yield* Deferred.await(secondExit)
        const calls = (yield* MessageV2.parts({ sessionID: msg.sessionID, messageID: msg.id }).pipe(
          Effect.provideService(Database.Service, database),
        )).filter(
          (part): part is SessionV1.ToolPart => part.type === "tool" && ["call-a", "call-b"].includes(part.callID),
        )
        expect(calls).toHaveLength(2)
        expect(Exit.isSuccess(second)).toBe(true)
        if (Exit.isSuccess(second)) expect(second.value.id).not.toBe(first.id)
        expect(creations).toEqual(["call-a", "call-b"])
        yield* off
      }),
    { config: cfg },
  ),
)

itProcessorHarness.instance(
  "session.processor shares creation failure and permits a later registration attempt",
  () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const { handle, harness } = yield* processorHarness(test.directory)
      const scope = yield* Scope.Scope
      const secondExit = yield* Deferred.make<Exit.Exit<SessionV1.ToolPart>>()
      harness.creationMode = "fail-before-commit"

      const first = yield* handle
        .registerToolCall({ toolCallID: "retry-call", toolName: "lookup" })
        .pipe(Effect.exit, Effect.forkIn(scope, { startImmediately: true }))
      yield* Deferred.await(harness.creationEntered)
      yield* handle.registerToolCall({ toolCallID: "retry-call", toolName: "lookup" }).pipe(
        Effect.exit,
        Effect.flatMap((exit) => Deferred.succeed(secondExit, exit)),
        Effect.forkIn(scope, { startImmediately: true }),
      )
      yield* Deferred.succeed(harness.releaseCreation, undefined)

      const firstResult = yield* Fiber.join(first)
      const secondResult = yield* Deferred.await(secondExit)
      expect(Exit.isFailure(firstResult)).toBe(true)
      expect(Exit.isFailure(secondResult)).toBe(true)
      expect(harness.creationAttempts).toBe(1)
      expect(harness.parts.size).toBe(0)

      const retry = yield* handle.registerToolCall({ toolCallID: "retry-call", toolName: "lookup" })
      expect(retry.callID).toBe("retry-call")
      expect(harness.creationAttempts).toBe(2)
      expect(harness.parts.size).toBe(1)
    }),
  { config: cfg },
)

itProcessorHarness.instance(
  "session.processor retains post-commit ownership when the creator is interrupted",
  () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const { handle, harness } = yield* processorHarness(test.directory)
      const scope = yield* Scope.Scope
      const joined = yield* Deferred.make<Exit.Exit<SessionV1.ToolPart>>()
      harness.creationMode = "block-after-commit"

      const creator = yield* handle
        .registerToolCall({ toolCallID: "committed-call", toolName: "lookup" })
        .pipe(Effect.forkIn(scope, { startImmediately: true }))
      yield* Deferred.await(harness.creationEntered)
      yield* handle.registerToolCall({ toolCallID: "committed-call", toolName: "lookup" }).pipe(
        Effect.exit,
        Effect.flatMap((exit) => Deferred.succeed(joined, exit)),
        Effect.forkIn(scope, { startImmediately: true }),
      )
      const interrupted = yield* Fiber.interrupt(creator).pipe(Effect.forkIn(scope, { startImmediately: true }))
      yield* Deferred.succeed(harness.releaseCreation, undefined)

      yield* Fiber.join(interrupted)
      const joinedResult = yield* Deferred.await(joined)
      expect(Exit.isSuccess(joinedResult)).toBe(true)
      expect(harness.creationAttempts).toBe(1)
      expect(harness.parts.size).toBe(1)
      const later = yield* handle.registerToolCall({ toolCallID: "committed-call", toolName: "lookup" })
      if (Exit.isSuccess(joinedResult)) expect(later.id).toBe(joinedResult.value.id)
    }),
  { config: cfg },
)

itProcessorHarness.instance(
  "session.processor lets a waiting registrant cancel without poisoning the tool call",
  () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const { handle, harness } = yield* processorHarness(test.directory)
      const scope = yield* Scope.Scope
      const waiterStarted = yield* Deferred.make<void>()
      harness.creationMode = "block-after-commit"

      const creator = yield* handle
        .registerToolCall({ toolCallID: "cancelled-waiter", toolName: "lookup" })
        .pipe(Effect.forkIn(scope, { startImmediately: true }))
      yield* Deferred.await(harness.creationEntered)
      const waiter = yield* Deferred.succeed(waiterStarted, undefined).pipe(
        Effect.andThen(handle.registerToolCall({ toolCallID: "cancelled-waiter", toolName: "lookup" })),
        Effect.forkIn(scope, { startImmediately: true }),
      )
      yield* Deferred.await(waiterStarted)
      const cancelled = yield* Fiber.interrupt(waiter).pipe(Effect.forkIn(scope, { startImmediately: true }))
      yield* Deferred.succeed(harness.releaseCreation, undefined)
      yield* Fiber.join(cancelled)
      const waiterExit = yield* Fiber.await(waiter)
      expect(Exit.isFailure(waiterExit)).toBe(true)
      if (Exit.isFailure(waiterExit)) expect(Cause.hasInterruptsOnly(waiterExit.cause)).toBe(true)

      const created = yield* Fiber.join(creator)
      const later = yield* handle.registerToolCall({ toolCallID: "cancelled-waiter", toolName: "lookup" })
      expect(later.id).toBe(created.id)
      expect(harness.creationAttempts).toBe(1)
      expect(harness.parts.size).toBe(1)
    }),
  { config: cfg },
)

itProcessorHarness.instance(
  "session.processor releases the per-call gate after callback defects and rejects conflicting names",
  () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const { handle, harness } = yield* processorHarness(test.directory)
      const created = yield* handle.registerToolCall({ toolCallID: "facts-call", toolName: "lookup" })

      const defect = yield* handle
        .updateToolCall("facts-call", () => {
          throw new Error("simulated callback defect")
        })
        .pipe(Effect.exit)
      expect(Exit.isFailure(defect)).toBe(true)

      const running = yield* handle.updateToolCall("facts-call", (part) => ({
        ...part,
        state: {
          status: "running",
          input: { query: "weather" },
          title: "Working",
          metadata: { progress: 1 },
          time: { start: Date.now() },
        },
      }))
      expect(running?.id).toBe(created.id)

      const conflict = yield* handle
        .registerToolCall({ toolCallID: "facts-call", toolName: "other-tool" })
        .pipe(Effect.exit)
      expect(Exit.isFailure(conflict)).toBe(true)
      expect(harness.creationAttempts).toBe(1)
      expect(harness.parts.size).toBe(1)

      yield* handle.completeToolCall("facts-call", {
        title: "Done",
        metadata: { terminal: true },
        output: "sunny",
      })
      const terminal = harness.parts.get(created.id)
      expect(terminal?.state.status).toBe("completed")
      if (terminal?.state.status === "completed") {
        expect(terminal.state.input).toEqual({ query: "weather" })
        expect(terminal.state.metadata).toEqual({ progress: 1, terminal: true })
        expect(terminal.state.title).toBe("Done")
      }
      expect(yield* handle.updateToolCall("facts-call", (part) => part)).toBeUndefined()
    }),
  { config: cfg },
)

itProcessorHarness.instance(
  "session.processor keeps Task admission under the per-call gate and adopts its persisted part",
  () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const { handle, harness } = yield* processorHarness(test.directory)
      const created = yield* handle.registerToolCall({ toolCallID: "admission-call", toolName: "task" })

      const defect = yield* handle
        .admitToolCall("admission-call", () => Effect.die(new Error("simulated admission failure")))
        .pipe(Effect.exit)
      expect(Exit.isFailure(defect)).toBe(true)

      const replacement = yield* handle.admitToolCall("admission-call", (part) =>
        Effect.sync(() => {
          const admitted = {
            ...part,
            id: PartID.ascending(),
            state: {
              status: "running" as const,
              input: { prompt: "inspect" },
              title: "Inspect",
              metadata: { childTurnMessageId: "msg_child" },
              time: { start: Date.now() },
            },
          }
          harness.parts.delete(part.id)
          harness.parts.set(admitted.id, structuredClone(admitted))
          return admitted
        }),
      )
      expect(replacement?.id).not.toBe(created.id)

      const updated = yield* handle.updateToolCall("admission-call", (part) => ({
        ...part,
        state:
          part.state.status === "running"
            ? { ...part.state, metadata: { ...part.state.metadata, progress: 1 } }
            : part.state,
      }))
      expect(updated?.id).toBe(replacement?.id)
      expect(harness.parts.has(created.id)).toBe(false)
      expect(updated?.state).toMatchObject({
        status: "running",
        title: "Inspect",
        metadata: { childTurnMessageId: "msg_child", progress: 1 },
      })
    }),
  { config: cfg },
)

itProcessorHarness.instance(
  "session.processor fails closed when the durable tool part disappears",
  () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const { handle, harness } = yield* processorHarness(test.directory)
      const created = yield* handle.registerToolCall({ toolCallID: "missing-call", toolName: "lookup" })
      harness.parts.delete(created.id)

      expect(yield* handle.updateToolCall("missing-call", (part) => part)).toBeUndefined()
      const registration = yield* handle
        .registerToolCall({ toolCallID: "missing-call", toolName: "lookup" })
        .pipe(Effect.exit)
      expect(Exit.isFailure(registration)).toBe(true)
      expect(harness.creationAttempts).toBe(1)
      expect(harness.parts.size).toBe(0)
    }),
  { config: cfg },
)

it.live("session.processor effect tests mark pending tools as aborted on cleanup", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const database = yield* Database.Service
        const { processors, session, provider } = yield* boot()

        yield* llm.toolHang("bash", { cmd: "pwd" })

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "tool abort")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const run = yield* handle
          .process({
            user: {
              id: parent.id,
              sessionID: chat.id,
              role: "user",
              time: parent.time,
              agent: parent.agent,
              model: { providerID: ref.providerID, modelID: ref.modelID },
            } satisfies SessionV1.User,
            sessionID: chat.id,
            model: mdl,
            agent: agent(),
            system: [],
            messages: [{ role: "user", content: "tool abort" }],
            tools: {},
          })
          .pipe(Effect.forkChild)

        yield* llm.wait(1)
        yield* waitFor(
          MessageV2.parts({ sessionID: msg.sessionID, messageID: msg.id }).pipe(
            Effect.map((parts) => parts.find((part): part is SessionV1.ToolPart => part.type === "tool")),
            Effect.provideService(Database.Service, database),
          ),
          "timed out waiting for tool part",
        )
        yield* Fiber.interrupt(run)

        const exit = yield* Fiber.await(run)
        const parts = yield* MessageV2.parts({ sessionID: msg.sessionID, messageID: msg.id })
        const call = parts.find((part): part is SessionV1.ToolPart => part.type === "tool")

        expect(Exit.isFailure(exit)).toBe(true)
        if (Exit.isFailure(exit)) {
          expect(Cause.hasInterruptsOnly(exit.cause)).toBe(true)
        }
        expect(yield* llm.calls).toBe(1)
        expect(call?.state.status).toBe("error")
        if (call?.state.status === "error") {
          expect(call.state.error).toBe("Tool execution aborted")
          expect(call.state.metadata?.interrupted).toBe(true)
          expect(call.state.time.end).toBeDefined()
        }
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests record aborted errors", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const seen = defer<void>()
        const { processors, session, provider } = yield* boot()
        const events = yield* EventV2Bridge.Service

        yield* llm.hang

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "abort")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const errs: string[] = []
        const off = yield* events.listen((evt) => {
          if (evt.type !== Session.Event.Error.type) return Effect.void
          const data = evt.data as typeof Session.Event.Error.data.Type
          if (data.sessionID !== chat.id || !data.error) return Effect.void
          errs.push(data.error.name)
          seen.resolve()
          return Effect.void
        })
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const run = yield* handle
          .process({
            user: {
              id: parent.id,
              sessionID: chat.id,
              role: "user",
              time: parent.time,
              agent: parent.agent,
              model: { providerID: ref.providerID, modelID: ref.modelID },
            } satisfies SessionV1.User,
            sessionID: chat.id,
            model: mdl,
            agent: agent(),
            system: [],
            messages: [{ role: "user", content: "abort" }],
            tools: {},
          })
          .pipe(Effect.forkChild)

        yield* llm.wait(1)
        yield* Fiber.interrupt(run)

        const exit = yield* Fiber.await(run)
        yield* Effect.promise(() => seen.promise)
        const stored = yield* MessageV2.get({ sessionID: chat.id, messageID: msg.id })
        yield* off

        expect(Exit.isFailure(exit)).toBe(true)
        if (Exit.isFailure(exit)) {
          expect(Cause.hasInterruptsOnly(exit.cause)).toBe(true)
        }
        expect(handle.message.error?.name).toBe("MessageAbortedError")
        expect(stored.info.role).toBe("assistant")
        if (stored.info.role === "assistant") {
          expect(stored.info.error?.name).toBe("MessageAbortedError")
        }
        expect(errs).toContain("MessageAbortedError")
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests mark interruptions aborted without manual abort", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()

        yield* llm.hang

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "interrupt")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const run = yield* handle
          .process({
            user: {
              id: parent.id,
              sessionID: chat.id,
              role: "user",
              time: parent.time,
              agent: parent.agent,
              model: { providerID: ref.providerID, modelID: ref.modelID },
            } satisfies SessionV1.User,
            sessionID: chat.id,
            model: mdl,
            agent: agent(),
            system: [],
            messages: [{ role: "user", content: "interrupt" }],
            tools: {},
          })
          .pipe(Effect.forkChild)

        yield* llm.wait(1)
        yield* Fiber.interrupt(run)

        const exit = yield* Fiber.await(run)
        const stored = yield* MessageV2.get({ sessionID: chat.id, messageID: msg.id })

        expect(Exit.isFailure(exit)).toBe(true)
        expect(handle.message.error?.name).toBe("MessageAbortedError")
        expect(stored.info.role).toBe("assistant")
        if (stored.info.role === "assistant") {
          expect(stored.info.error?.name).toBe("MessageAbortedError")
        }
      }),
    { config: (url) => providerCfg(url) },
  ),
)

itProviderError.live("session.processor effect tests fail provider-executed error results", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()
        const events = yield* EventV2Bridge.Service

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "provider tool error")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const seen: string[] = []
        const off = yield* events.listen((event) => {
          seen.push(event.type)
          return Effect.void
        })
        const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model: mdl })

        yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "provider tool error" }],
          tools: {},
        })
        yield* off

        const parts = yield* MessageV2.parts({ sessionID: msg.sessionID, messageID: msg.id })
        const call = parts.find((part): part is SessionV1.ToolPart => part.type === "tool")
        expect(call?.state.status).toBe("error")
        expect(call?.metadata?.providerExecuted).toBe(true)
        if (call?.state.status === "error") expect(call.state.error).toBe("provider boom")
        expect(seen).toContain(MessageV2.Event.PartUpdated.type)
        expect(seen).toContain(MessageV2.Event.Updated.type)
        expect(seen.filter((type) => type.startsWith("session.next."))).toEqual([])
      }),
    { config: cfg },
  ),
)

itProviderFacts.live("session.processor reconciles provisional names and provider-executed terminal facts", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()
        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "provider facts")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model: mdl })

        expect(
          yield* handle.process({
            user: {
              id: parent.id,
              sessionID: chat.id,
              role: "user",
              time: parent.time,
              agent: parent.agent,
              model: { providerID: ref.providerID, modelID: ref.modelID },
            } satisfies SessionV1.User,
            sessionID: chat.id,
            model: mdl,
            agent: agent(),
            system: [],
            messages: [{ role: "user", content: "provider facts" }],
            tools: {},
          }),
        ).toBe("continue")

        const calls = (yield* MessageV2.parts({ sessionID: msg.sessionID, messageID: msg.id })).filter(
          (part): part is SessionV1.ToolPart => part.type === "tool" && part.callID === "facts-call",
        )
        expect(calls).toHaveLength(1)
        expect(calls[0]?.tool).toBe("lookup")
        expect(calls[0]?.metadata?.providerExecuted).toBe(true)
        expect(calls[0]?.state.status).toBe("completed")
        if (calls[0]?.state.status === "completed") {
          expect(calls[0].state.input).toEqual({ query: "weather" })
          expect(calls[0].state.output).toBe("sunny")
          expect(calls[0].state.metadata).toEqual({ terminal: true })
        }
      }),
    { config: cfg },
  ),
)

itFragmentFailure.live("session.processor effect tests retain partial legacy parts without v2 events", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()
        const events = yield* EventV2Bridge.Service

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "provider failure")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const seen: string[] = []
        const off = yield* events.listen((event) => {
          seen.push(event.type)
          return Effect.void
        })
        const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model: mdl })

        expect(
          yield* handle.process({
            user: {
              id: parent.id,
              sessionID: chat.id,
              role: "user",
              time: parent.time,
              agent: parent.agent,
              model: { providerID: ref.providerID, modelID: ref.modelID },
            } satisfies SessionV1.User,
            sessionID: chat.id,
            model: mdl,
            agent: agent(),
            system: [],
            messages: [{ role: "user", content: "provider failure" }],
            tools: {},
          }),
        ).toBe("stop")
        yield* off

        const parts = yield* MessageV2.parts({ sessionID: msg.sessionID, messageID: msg.id })
        expect(parts).toEqual(
          expect.arrayContaining([
            expect.objectContaining({ type: "text", text: "partial" }),
            expect.objectContaining({ type: "reasoning", text: "thinking" }),
          ]),
        )
        expect(seen).toContain(MessageV2.Event.PartUpdated.type)
        expect(seen).toContain(Session.Event.Error.type)
        expect(seen.filter((type) => type.startsWith("session.next."))).toEqual([])
      }),
    { config: cfg },
  ),
)
