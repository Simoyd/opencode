import { expect } from "bun:test"
import { PermissionV1 } from "@opencode-ai/core/v1/permission"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Agent } from "@/agent/agent"
import { MCP } from "@/mcp"
import { Permission } from "@/permission"
import { Plugin } from "@/plugin"
import { Provider } from "@/provider/provider"
import { Session } from "@/session/session"
import { SessionTools } from "@/session/tools"
import { MessageID, SessionID } from "@/session/schema"
import { Tool } from "@/tool/tool"
import { ToolRegistry } from "@/tool/registry"
import { Truncate } from "@/tool/truncate"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Effect, Layer, Schema } from "effect"
import type { ToolExecutionOptions } from "ai"
import { testEffect } from "../lib/effect"

const permission = LayerNode.compile(LayerNode.group([Permission.node, CrossSpawnSpawner.node]))
const plugin = Layer.succeed(
  Plugin.Service,
  Plugin.Service.of({ trigger: () => Effect.void } as unknown as Plugin.Interface),
)
const truncate = Layer.succeed(
  Truncate.Service,
  Truncate.Service.of({
    output: (content: string) => Effect.succeed({ content, truncated: false }),
  } as unknown as Truncate.Interface),
)

const it = testEffect(Layer.mergeAll(permission, plugin, truncate, RuntimeFlags.layer()))

const waitForPending = (count: number) =>
  Permission.Service.use((service) =>
    Effect.gen(function* () {
      while ((yield* service.list()).length !== count) yield* Effect.sleep("10 millis")
      return yield* service.list()
    }).pipe(Effect.timeout("1 second")),
  )

const createInput = (registerToolCall: () => Effect.Effect<void> = () => Effect.void) =>
  ({
    agent: {
      name: "build",
      permission: [] as PermissionV1.Ruleset,
    } as unknown as Agent.Info,
    model: {
      api: { id: "test-model" },
      providerID: "test",
    } as unknown as Provider.Model,
    session: {
      id: SessionID.make("ses_tool_abort"),
      permission: [] as PermissionV1.Ruleset,
    } as unknown as Session.Info,
    processor: {
      message: { id: MessageID.make("msg_tool_abort") },
      registerToolCall,
      updateToolCall: () => Effect.void,
    },
    bypassAgentCheck: false,
    messages: [],
    promptOps: {},
  }) as unknown as Parameters<typeof SessionTools.resolve>[0]

const options = (controller: AbortController, callID: string) =>
  ({
    toolCallId: callID,
    abortSignal: controller.signal,
    messages: [],
  }) as unknown as ToolExecutionOptions

const rejects = <T>(promise: Promise<T>) =>
  Effect.promise(() =>
    promise.then(
      () => false,
      () => true,
    ),
  )

it.instance(
  "ordinary and MCP callback abort owns the waiting permission Effect",
  () =>
    Effect.gen(function* () {
      let ordinaryExecuted = 0
      let mcpExecuted = 0
      const ordinary: Tool.Def = {
        id: "ordinary_abort_probe",
        description: "ordinary abort probe",
        parameters: Schema.Struct({}),
        execute: (_args, ctx) =>
          Effect.gen(function* () {
            yield* ctx.ask({
              permission: "ordinary_abort_probe",
              patterns: ["*"],
              always: ["*"],
              metadata: {},
            })
            ordinaryExecuted++
            return { title: "", metadata: {}, output: "executed" }
          }),
      }
      const registry = Layer.succeed(
        ToolRegistry.Service,
        ToolRegistry.Service.of({ tools: () => Effect.succeed([ordinary]) } as unknown as ToolRegistry.Interface),
      )
      const mcp = Layer.succeed(
        MCP.Service,
        MCP.Service.of({
          clients: () => Effect.succeed({}),
          tools: () =>
            Effect.succeed({
              mcp_abort_probe: {
                def: {
                  name: "abort_probe",
                  description: "MCP abort probe",
                  inputSchema: { type: "object", properties: {} },
                },
                client: {
                  callTool: async () => {
                    mcpExecuted++
                    return { content: [{ type: "text" as const, text: "executed" }] }
                  },
                },
              },
            }),
        } as unknown as MCP.Interface),
      )

      const tools = yield* SessionTools.resolve(createInput()).pipe(Effect.provide(Layer.mergeAll(registry, mcp)))

      for (const name of ["ordinary_abort_probe", "mcp_abort_probe"] as const) {
        const controller = new AbortController()
        const callback = tools[name].execute
        if (!callback) return yield* Effect.die(new Error(`missing ${name} callback`))
        const promise = callback({}, options(controller, `call_${name}`))
        expect(yield* waitForPending(1)).toHaveLength(1)

        controller.abort()
        controller.abort()
        expect(yield* rejects(promise)).toBe(true)
        expect(yield* waitForPending(0)).toHaveLength(0)
      }

      expect(ordinaryExecuted).toBe(0)
      expect(mcpExecuted).toBe(0)
    }),
  { git: true },
)

it.instance(
  "pre-aborted ordinary and MCP callbacks publish no permission request",
  () =>
    Effect.gen(function* () {
      let registered = 0
      let executed = 0
      const ordinary: Tool.Def = {
        id: "ordinary_preabort_probe",
        description: "ordinary pre-abort probe",
        parameters: Schema.Struct({}),
        execute: () =>
          Effect.sync(() => {
            executed++
            return { title: "", metadata: {}, output: "executed" }
          }),
      }
      const registry = Layer.succeed(
        ToolRegistry.Service,
        ToolRegistry.Service.of({ tools: () => Effect.succeed([ordinary]) } as unknown as ToolRegistry.Interface),
      )
      const mcp = Layer.succeed(
        MCP.Service,
        MCP.Service.of({
          clients: () => Effect.succeed({}),
          tools: () =>
            Effect.succeed({
              mcp_preabort_probe: {
                def: {
                  name: "preabort_probe",
                  description: "MCP pre-abort probe",
                  inputSchema: { type: "object", properties: {} },
                },
                client: {
                  callTool: async () => {
                    executed++
                    return { content: [{ type: "text" as const, text: "executed" }] }
                  },
                },
              },
            }),
        } as unknown as MCP.Interface),
      )
      const input = createInput(() => Effect.sync(() => void registered++))
      const tools = yield* SessionTools.resolve(input).pipe(Effect.provide(Layer.mergeAll(registry, mcp)))

      for (const name of ["ordinary_preabort_probe", "mcp_preabort_probe"] as const) {
        const controller = new AbortController()
        controller.abort()
        const callback = tools[name].execute
        if (!callback) return yield* Effect.die(new Error(`missing ${name} callback`))
        expect(yield* rejects(callback({}, options(controller, `call_${name}`)))).toBe(true)
      }

      expect(yield* Permission.Service.use((service) => service.list())).toHaveLength(0)
      expect(registered).toBe(0)
      expect(executed).toBe(0)
    }),
  { git: true },
)

it.instance(
  "MCP resource callbacks abort their exact permission wait",
  () =>
    Effect.gen(function* () {
      let resourceExecuted = 0
      const resourceClient = {
        getServerCapabilities: () => ({ resources: {} }),
      }
      const registry = Layer.succeed(
        ToolRegistry.Service,
        ToolRegistry.Service.of({ tools: () => Effect.succeed([]) } as unknown as ToolRegistry.Interface),
      )
      const mcp = Layer.succeed(
        MCP.Service,
        MCP.Service.of({
          clients: () => Effect.succeed({ probe: resourceClient }),
          tools: () => Effect.succeed({}),
          resources: () =>
            Effect.sync(() => {
              resourceExecuted++
              return {}
            }),
          resourceTemplates: () =>
            Effect.sync(() => {
              resourceExecuted++
              return {}
            }),
          readResource: () =>
            Effect.sync(() => {
              resourceExecuted++
              return { contents: [] }
            }),
        } as unknown as MCP.Interface),
      )
      let registered = 0
      const tools = yield* SessionTools.resolve(createInput(() => Effect.sync(() => void registered++))).pipe(
        Effect.provide(Layer.mergeAll(registry, mcp)),
      )
      const cases = [
        ["list_mcp_resources", {}],
        ["list_mcp_resource_templates", {}],
        ["read_mcp_resource", { server: "probe", uri: "resource://probe" }],
      ] as const

      for (const [name, args] of cases) {
        const callback = tools[name]?.execute
        if (!callback) return yield* Effect.die(new Error(`missing ${name} callback`))
        const controller = new AbortController()
        const promise = callback(args, options(controller, `call_${name}`))
        expect(yield* waitForPending(1)).toHaveLength(1)
        controller.abort()
        controller.abort()
        expect(yield* rejects(promise)).toBe(true)
        expect(yield* waitForPending(0)).toHaveLength(0)

        const preAborted = new AbortController()
        preAborted.abort()
        const registeredBefore = registered
        expect(yield* rejects(callback(args, options(preAborted, `call_${name}_preabort`)))).toBe(true)
        expect(registered).toBe(registeredBefore)
        expect(yield* Permission.Service.use((service) => service.list())).toHaveLength(0)
      }

      expect(registered).toBe(3)
      expect(resourceExecuted).toBe(0)
    }),
  { git: true },
)
