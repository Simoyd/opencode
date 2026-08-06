import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { ConfigPermissionV1 } from "@opencode-ai/core/v1/config/permission"
import { InstanceState } from "@/effect/instance-state"
import { Wildcard } from "@opencode-ai/core/util/wildcard"
import { Deferred, Effect, Layer, Context } from "effect"
import os from "os"
import { PermissionV1 } from "@opencode-ai/core/v1/permission"
import { EventV2Bridge } from "@/event-v2-bridge"

export const Event = PermissionV1.Event

export interface Interface {
  readonly ask: (input: PermissionV1.AskInput) => Effect.Effect<void, PermissionV1.Error>
  readonly reply: (input: PermissionV1.ReplyInput) => Effect.Effect<void, PermissionV1.NotFoundError>
  readonly list: () => Effect.Effect<ReadonlyArray<PermissionV1.Request>>
}

interface PendingEntry {
  info: PermissionV1.Request
  deferred: Deferred.Deferred<void, PermissionV1.RejectedError | PermissionV1.CorrectedError>
}

interface State {
  pending: Map<PermissionV1.ID, PendingEntry>
  approved: PermissionV1.Rule[]
  closed: boolean
}

export function evaluate(permission: string, pattern: string, ...rulesets: PermissionV1.Ruleset[]): PermissionV1.Rule {
  return (
    rulesets
      .flat()
      .findLast((rule) => Wildcard.match(permission, rule.permission) && Wildcard.match(pattern, rule.pattern)) ?? {
      action: "ask",
      permission,
      pattern: "*",
    }
  )
}

export class Service extends Context.Service<Service, Interface>()("@opencode/Permission") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const events = yield* EventV2Bridge.Service
    const state = yield* InstanceState.make<State>(
      Effect.fn("Permission.state")(function* (ctx) {
        void ctx
        const state = {
          pending: new Map<PermissionV1.ID, PendingEntry>(),
          approved: [],
          closed: false,
        }

        yield* Effect.addFinalizer(() =>
          Effect.uninterruptible(
            Effect.gen(function* () {
              const cancelled = yield* Effect.sync(() => {
                state.closed = true
                const entries = [...state.pending.values()]
                state.pending.clear()
                return entries
              })
              for (const item of cancelled) {
                yield* Deferred.fail(item.deferred, new PermissionV1.RejectedError())
              }
            }),
          ),
        )

        return state
      }),
    )

    const ask = Effect.fn("Permission.ask")(function* (input: PermissionV1.AskInput) {
      const current = yield* InstanceState.get(state)
      if (current.closed) return yield* new PermissionV1.RejectedError()
      const { approved, pending } = current
      const { ruleset, ...request } = input
      let needsAsk = false

      for (const pattern of request.patterns) {
        const rule = evaluate(request.permission, pattern, ruleset, approved)
        yield* Effect.logInfo("evaluated", { permission: request.permission, pattern, action: rule })
        if (rule.action === "deny") {
          return yield* new PermissionV1.DeniedError({
            ruleset: ruleset.filter((rule) => Wildcard.match(request.permission, rule.permission)),
          })
        }
        if (rule.action === "allow") continue
        needsAsk = true
      }

      if (!needsAsk) return

      const id = request.id ?? PermissionV1.ID.ascending()
      const info: PermissionV1.Request = {
        id,
        sessionID: request.sessionID,
        permission: request.permission,
        patterns: request.patterns,
        metadata: request.metadata,
        always: request.always,
        tool: request.tool,
      }
      const deferred = yield* Deferred.make<void, PermissionV1.RejectedError | PermissionV1.CorrectedError>()
      const entry: PendingEntry = { info, deferred }
      return yield* Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const admitted = yield* Effect.sync(() => {
            if (current.closed) return false
            pending.set(id, entry)
            return true
          })
          if (!admitted) return yield* new PermissionV1.RejectedError()
          return yield* restore(
            Effect.gen(function* () {
              if (yield* Effect.sync(() => current.closed)) return yield* new PermissionV1.RejectedError()
              yield* Effect.logInfo("asking", { id, permission: info.permission, patterns: info.patterns })
              yield* Effect.raceFirst(events.publish(Event.Asked, info), Deferred.await(deferred))
              return yield* Deferred.await(deferred)
            }),
          ).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                if (pending.get(id) === entry) pending.delete(id)
              }),
            ),
          )
        }),
      )
    })

    const reply = Effect.fn("Permission.reply")(function* (input: PermissionV1.ReplyInput) {
      const { approved, pending } = yield* InstanceState.get(state)
      const settled = yield* Effect.uninterruptible(
        Effect.gen(function* () {
          const decided = yield* Effect.sync(() => {
            const existing = pending.get(input.requestID)
            if (!existing) return

            const entries: Array<{ entry: PendingEntry; reply: PermissionV1.Reply }> = []
            pending.delete(input.requestID)
            entries.push({ entry: existing, reply: input.reply })

            if (input.reply === "reject") {
              for (const [id, item] of pending.entries()) {
                if (item.info.sessionID !== existing.info.sessionID) continue
                pending.delete(id)
                entries.push({ entry: item, reply: "reject" })
              }
              return entries
            }

            if (input.reply === "always") {
              for (const pattern of existing.info.always) {
                approved.push({
                  permission: existing.info.permission,
                  pattern,
                  action: "allow",
                })
              }

              for (const [id, item] of pending.entries()) {
                if (item.info.sessionID !== existing.info.sessionID) continue
                const ok = item.info.patterns.every(
                  (pattern) => evaluate(item.info.permission, pattern, approved).action === "allow",
                )
                if (!ok) continue
                pending.delete(id)
                entries.push({ entry: item, reply: "always" })
              }
            }

            return entries
          })
          if (!decided) return

          if (input.reply === "reject") {
            yield* Deferred.fail(
              decided[0].entry.deferred,
              input.message
                ? new PermissionV1.CorrectedError({ feedback: input.message })
                : new PermissionV1.RejectedError(),
            )
            for (const item of decided.slice(1)) {
              yield* Deferred.fail(item.entry.deferred, new PermissionV1.RejectedError())
            }
            return decided
          }

          for (const item of decided) {
            yield* Deferred.succeed(item.entry.deferred, undefined)
          }
          return decided
        }),
      )
      if (!settled) return yield* new PermissionV1.NotFoundError({ requestID: input.requestID })

      for (const item of settled) {
        yield* events.publish(Event.Replied, {
          sessionID: item.entry.info.sessionID,
          requestID: item.entry.info.id,
          reply: item.reply,
        })
      }
    })

    const list = Effect.fn("Permission.list")(function* () {
      const pending = (yield* InstanceState.get(state)).pending
      return Array.from(pending.values(), (item) => item.info)
    })

    return Service.of({ ask, reply, list })
  }),
)

function expand(pattern: string): string {
  if (pattern.startsWith("~/")) return os.homedir() + pattern.slice(1)
  if (pattern === "~") return os.homedir()
  if (pattern.startsWith("$HOME/")) return os.homedir() + pattern.slice(5)
  if (pattern.startsWith("$HOME")) return os.homedir() + pattern.slice(5)
  return pattern
}

export function fromConfig(permission: ConfigPermissionV1.Info) {
  const ruleset: PermissionV1.Rule[] = []
  for (const [key, value] of Object.entries(permission)) {
    if (typeof value === "string") {
      ruleset.push({ permission: key, action: value, pattern: "*" })
      continue
    }
    ruleset.push(
      ...Object.entries(value).map(([pattern, action]) => ({ permission: key, pattern: expand(pattern), action })),
    )
  }
  return ruleset
}

export function merge(...rulesets: PermissionV1.Ruleset[]): PermissionV1.Rule[] {
  return rulesets.flat()
}

export function disabled(tools: string[], ruleset: PermissionV1.Ruleset): Set<string> {
  const edits = ["edit", "write", "apply_patch"]
  const reads = ["list_mcp_resources", "list_mcp_resource_templates", "read_mcp_resource"]
  return new Set(
    tools.filter((tool) => {
      const permission = edits.includes(tool) ? "edit" : reads.includes(tool) ? "read" : tool
      const rule = ruleset.findLast((rule) => Wildcard.match(permission, rule.permission))
      return rule?.pattern === "*" && rule.action === "deny"
    }),
  )
}

export function visibleTools<T>(tools: Record<string, T>, ruleset: PermissionV1.Ruleset): Record<string, T> {
  const hidden = disabled(Object.keys(tools), ruleset)
  return Object.fromEntries(Object.entries(tools).filter(([name]) => !hidden.has(name)))
}

export const node = LayerNode.make({ service: Service, layer: layer, deps: [EventV2Bridge.node] })

export * as Permission from "."
