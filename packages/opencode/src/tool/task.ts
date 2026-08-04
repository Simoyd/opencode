import * as Tool from "./tool"
import DESCRIPTION from "./task.txt"
import { ToolJsonSchema } from "./json-schema"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { BackgroundJob } from "@/background/job"
import { Session } from "@/session/session"
import { SessionID, MessageID } from "../session/schema"
import { MessageV2 } from "../session/message-v2"
import { Agent } from "../agent/agent"
import { deriveSubagentSessionPermission } from "../agent/subagent-permissions"
import type { SessionPrompt } from "../session/prompt"
import { Config } from "@/config/config"
import { Deferred, Effect, Exit, Fiber, Schema } from "effect"
import { EffectBridge } from "@/effect/bridge"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2Bridge } from "@/event-v2-bridge"
import { SessionLifecycle } from "@/session/lifecycle"

export interface TaskPromptOps {
  cancel(sessionID: SessionID): Effect.Effect<void, Session.BusyError>
  resolvePromptParts(template: string): Effect.Effect<SessionPrompt.PromptInput["parts"]>
  prompt(
    input: SessionPrompt.PromptInput,
    preparedSession: Session.Info | undefined,
    admission?: (message: SessionV1.WithParts) => Effect.Effect<boolean>,
  ): Effect.Effect<SessionV1.WithParts>
  promptAdmitted(input: SessionPrompt.PromptInput): Effect.Effect<SessionV1.WithParts>
}

const id = "task"
const BACKGROUND_DESCRIPTION = [
  "Background mode: background=true launches the subagent asynchronously and returns immediately.",
  "Foreground is the default; use it when you need the result before continuing.",
  "Use background only for independent work that can run while you continue elsewhere.",
  "You will be notified automatically when it finishes.",
].join(" ")
const BACKGROUND_STARTED = [
  "The task is working in the background. You will be notified automatically when it finishes.",
  "DO NOT sleep, poll for progress, ask the task for status, or duplicate this task's work — avoid working with the same files or topics it is using.",
  "Work on non-overlapping tasks, or briefly tell the user what you launched and end your response.",
].join("\n")
const BACKGROUND_UPDATED = [
  "Additional context sent to the running background task.",
  "The task is still working in the background. You will be notified automatically when it finishes.",
  "DO NOT sleep, poll for progress, ask the task for status, or duplicate this task's work — avoid working with the same files or topics it is using.",
  "Work on non-overlapping tasks, or briefly tell the user what you sent and end your response.",
].join("\n")

const BaseParameterFields = {
  description: Schema.String.annotate({ description: "A short (3-5 words) description of the task" }),
  prompt: Schema.String.annotate({ description: "The task for the agent to perform" }),
  subagent_type: Schema.String.annotate({ description: "The type of specialized agent to use for this task" }),
  task_id: Schema.optional(Schema.String).annotate({
    description:
      "This should only be set if you mean to resume a previous task (you can pass a prior task_id and the task will continue the same subagent session as before instead of creating a fresh one)",
  }),
  command: Schema.optional(Schema.String).annotate({ description: "The command that triggered this task" }),
}

const BaseParameters = Schema.Struct(BaseParameterFields)

export const Parameters = Schema.Struct({
  ...BaseParameterFields,
  background: Schema.optional(Schema.Boolean).annotate({
    description:
      "Run the agent in the background. You will be notified when it completes. DO NOT sleep, poll, or proactively check on its progress",
  }),
})

function renderOutput(input: {
  sessionID: SessionID
  state: "running" | "completed" | "error"
  summary?: string
  text: string
}) {
  const tag = input.state === "error" ? "task_error" : "task_result"
  return [
    `<task id="${input.sessionID}" state="${input.state}">`,
    ...(input.summary ? [`<summary>${input.summary}</summary>`] : []),
    `<${tag}>`,
    input.text,
    `</${tag}>`,
    "</task>",
  ].join("\n")
}

function childSessionMetadataWithTaskOrigin(input: {
  existing: Record<string, unknown> | undefined
  parentSessionID: SessionID
  sourceMessageID: MessageID
  toolCallID: string
  childSessionID: SessionID
  childTurnMessageID: MessageID
  agent: string
  model: { modelID: string; providerID: string }
  background: boolean
}) {
  return {
    ...(input.existing ?? {}),
    taskOrigin: {
      parentSessionId: input.parentSessionID,
      sourceMessageId: input.sourceMessageID,
      toolCallId: input.toolCallID,
      childSessionId: input.childSessionID,
      childTurnMessageId: input.childTurnMessageID,
      agent: input.agent,
      model: input.model,
      ...(input.background ? { background: true } : {}),
    },
  }
}

export const TaskTool = Tool.define(
  id,
  Effect.gen(function* () {
    const agent = yield* Agent.Service
    const background = yield* BackgroundJob.Service
    const config = yield* Config.Service
    const sessions = yield* Session.Service
    const flags = yield* RuntimeFlags.Service
    const database = yield* Database.Service
    const events = yield* EventV2Bridge.Service
    const lifecycle = yield* SessionLifecycle.Service

    const run = Effect.fn("TaskTool.execute")(function* (
      params: Schema.Schema.Type<typeof Parameters>,
      ctx: Tool.Context,
    ) {
      const cfg = yield* config.get()
      const runInBackground = params.background === true
      if (runInBackground && !flags.experimentalBackgroundSubagents) {
        return yield* Effect.fail(
          new Error("Background subagents require OPENCODE_EXPERIMENTAL_BACKGROUND_SUBAGENTS=true"),
        )
      }

      const parent = yield* sessions.get(ctx.sessionID)
      if (!ctx.messageID) return yield* Effect.die(new Error("TaskTool requires an owning message ID"))
      const sourceMessageID = ctx.messageID
      if (!ctx.callID) return yield* Effect.die(new Error("TaskTool requires an owning tool call ID"))
      const toolCallID = ctx.callID
      let current = parent
      let depth = 0
      while (current.parentID) {
        depth++
        current = yield* sessions.get(current.parentID)
      }
      if (depth >= (cfg.subagent_depth ?? 1)) {
        return yield* Effect.fail(
          new Error(
            `Subagent depth limit reached (${cfg.subagent_depth ?? 1}). Increase "subagent_depth" to allow nested subagents.`,
          ),
        )
      }

      const session = params.task_id
        ? yield* sessions.get(SessionID.make(params.task_id)).pipe(Effect.catchCause(() => Effect.succeed(undefined)))
        : undefined
      if (session && session.parentID !== ctx.sessionID) {
        return yield* Effect.fail(
          new Error(`Task session ${session.id} is not a child of the invoking session`),
        )
      }

      if (!ctx.extra?.bypassAgentCheck) {
        yield* ctx.ask({
          permission: id,
          patterns: [params.subagent_type],
          always: ["*"],
          metadata: {
            description: params.description,
            subagent_type: params.subagent_type,
          },
        })
      }

      const next = yield* agent.get(params.subagent_type)
      if (!next) {
        return yield* Effect.fail(new Error(`Unknown agent type: ${params.subagent_type} is not a valid agent type`))
      }

      const childPermission = deriveSubagentSessionPermission({
        parentSessionPermission: parent.permission ?? [],
        subagent: next,
      })
      const childToolDenies = [
        ...(next.permission.some((rule) => rule.permission === "todowrite")
          ? []
          : [{ permission: "todowrite" as const, pattern: "*" as const, action: "deny" as const }]),
        ...(next.permission.some((rule) => rule.permission === id)
          ? []
          : [{ permission: id, pattern: "*" as const, action: "deny" as const }]),
        ...(cfg.experimental?.primary_tools?.map((permission) => ({
          permission,
          pattern: "*" as const,
          action: "deny" as const,
        })) ?? []),
      ]
      const freshSession = session === undefined
      const nextSession =
        session ??
        (yield* sessions.prepare({
          parentID: ctx.sessionID,
          title: params.description + ` (@${next.name} subagent)`,
          agent: next.name,
          permission: [
            ...childPermission,
            ...childToolDenies.filter(
              (deny) =>
                !childPermission.some(
                  (rule) =>
                    rule.permission === deny.permission && rule.pattern === deny.pattern && rule.action === deny.action,
                ),
            ),
          ],
        }))

      const msg = yield* MessageV2.get({ sessionID: ctx.sessionID, messageID: sourceMessageID }).pipe(
        Effect.provideService(Database.Service, database),
        Effect.orDie,
      )
      if (msg.info.role !== "assistant") return yield* Effect.fail(new Error("Not an assistant message"))
      const variant = msg.info.variant

      const model = next.model ?? {
        modelID: msg.info.modelID,
        providerID: msg.info.providerID,
      }
      const childTurnMessageID = MessageID.ascending()
      const metadata = {
        parentSessionId: ctx.sessionID,
        sessionId: nextSession.id,
        model,
        ...(runInBackground ? { background: true } : {}),
      }

      const ops = ctx.extra?.promptOps as TaskPromptOps
      if (!ops) return yield* Effect.fail(new Error("TaskTool requires promptOps in ctx.extra"))

      const runTask = Effect.fn("TaskTool.runTask")(function* () {
        const parts = yield* ops.resolvePromptParts(params.prompt)
        const publishTaskOrigin = Effect.fnUntraced(function* (childPrompt: SessionV1.WithParts) {
          const admittedMetadata = { ...metadata, childTurnMessageId: childTurnMessageID }
          const admittedParentMessage = yield* MessageV2.get({
            sessionID: ctx.sessionID,
            messageID: sourceMessageID,
          }).pipe(Effect.provideService(Database.Service, database), Effect.orDie)
          const admittedSession = freshSession ? nextSession : yield* sessions.get(nextSession.id).pipe(Effect.orDie)
          const parentTaskParts = admittedParentMessage.parts.filter(
            (part): part is SessionV1.ToolPart =>
              part.type === "tool" && part.tool === id && part.callID === toolCallID,
          )
          if (parentTaskParts.length !== 1) {
            return yield* Effect.die(
              new Error(`Task provenance requires one exact parent tool part for ${toolCallID}`),
            )
          }
          const parentTaskPart = parentTaskParts[0]!
          const time = Date.now()
          const childSession = {
            ...admittedSession,
            metadata: childSessionMetadataWithTaskOrigin({
              existing: admittedSession.metadata,
              parentSessionID: ctx.sessionID,
              sourceMessageID,
              toolCallID,
              childSessionID: nextSession.id,
              childTurnMessageID,
              agent: next.name,
              model,
              background: runInBackground,
            }),
            time: { ...admittedSession.time, updated: time },
          }
          const parentPart = {
            ...parentTaskPart,
            state: parentTaskPart.state.status === "pending"
              ? {
                  status: "running" as const,
                  input: parentTaskPart.state.input,
                  title: params.description,
                  metadata: admittedMetadata,
                  time: { start: time },
                }
              : {
                  ...parentTaskPart.state,
                  ...(parentTaskPart.state.status === "error" ? {} : { title: params.description }),
                  metadata: { ...parentTaskPart.state.metadata, ...admittedMetadata },
                },
          } satisfies SessionV1.ToolPart
          yield* events.publishTaskAdmission({
            session: freshSession
              ? { kind: "created", info: childSession }
              : { kind: "updated", info: childSession },
            parentPart,
            childPrompt,
            time,
          })
          Object.assign(metadata, admittedMetadata)
          yield* ctx.metadata({ title: params.description, metadata: admittedMetadata })
          return true
        })
        const result = yield* ops.prompt(
          {
            messageID: childTurnMessageID,
            sessionID: nextSession.id,
            model: {
              modelID: model.modelID,
              providerID: model.providerID,
            },
            variant: next.model ? undefined : variant,
            agent: next.name,
            parts,
          },
          freshSession ? nextSession : undefined,
          publishTaskOrigin,
        )
        return result.parts.findLast((item) => item.type === "text")?.text ?? ""
      })

      const deliver = Effect.fn("TaskTool.deliverBackgroundResult")(function* (info: BackgroundJob.Info) {
        if (info.status !== "completed" && info.status !== "error") return
        const currentParent = yield* sessions.get(ctx.sessionID)
        yield* ops.promptAdmitted({
          sessionID: ctx.sessionID,
          agent: currentParent.agent ?? ctx.agent,
          variant,
          parts: [
            {
              type: "text",
              synthetic: true,
              text: renderOutput({
                sessionID: nextSession.id,
                state: info.status,
                summary:
                  info.status === "completed"
                    ? `Background task completed: ${params.description}`
                    : `Background task failed: ${params.description}`,
                text: info.status === "completed" ? info.output ?? "" : info.error ?? "",
              }),
            },
          ],
        })
      })

      const registerDelivery = Effect.fn("TaskTool.registerBackgroundDelivery")(function* (info: BackgroundJob.Info) {
        const admitted = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const fiber = yield* lifecycle
          .admit(
            ctx.sessionID,
            Deferred.succeed(admitted, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.andThen(deliver(info)),
            ),
          )
          .pipe(Effect.forkChild)
        yield* Effect.raceFirst(
          Deferred.await(admitted),
          Fiber.join(fiber).pipe(Effect.andThen(Effect.die("Background delivery ended before admission"))),
        )
        return {
          run: Deferred.succeed(release, undefined).pipe(Effect.andThen(Fiber.join(fiber))),
        }
      })

      if (yield* lifecycle.admit(ctx.sessionID, background.extend({ id: nextSession.id, run: runTask() }))) {
        return {
          title: params.description,
          metadata: {
            ...metadata,
            background: true,
            jobId: nextSession.id,
          },
          output: renderOutput({
            sessionID: nextSession.id,
            state: "running",
            summary: "Background task updated",
            text: BACKGROUND_UPDATED,
          }),
        }
      }

      const info = yield* lifecycle.admit(
        ctx.sessionID,
        background.start({
          id: nextSession.id,
          type: id,
          title: params.description,
          metadata,
          onPromote: ctx.metadata({
            title: params.description,
            metadata: { ...metadata, background: true, jobId: nextSession.id },
          }),
          terminalDelivery: registerDelivery,
          run: runTask(),
        }),
      )

      function backgroundResult() {
        return {
          title: params.description,
          metadata: {
            ...metadata,
            background: true,
            jobId: info.id,
          },
          output: renderOutput({
            sessionID: nextSession.id,
            state: "running",
            summary: "Background task started",
            text: BACKGROUND_STARTED,
          }),
        }
      }

      if (runInBackground) {
        return backgroundResult()
      }

      const runCancel = yield* EffectBridge.make()
      const cancel = ops.cancel(nextSession.id)
      let cancelFiber: Fiber.Fiber<void, Session.BusyError> | undefined

      function onAbort() {
        cancelFiber ??= runCancel.fork(cancel)
      }

      return yield* Effect.acquireUseRelease(
        Effect.sync(() => {
          ctx.abort.addEventListener("abort", onAbort)
        }),
        () =>
          Effect.gen(function* () {
            const result = yield* Effect.raceFirst(
              background.wait({ id: nextSession.id }).pipe(Effect.map((waited) => waited.info)),
              background.waitForPromotion(nextSession.id),
            )
            if (result?.metadata?.background === true) return backgroundResult()
            if (result?.status === "error") return yield* Effect.fail(new Error(result.error ?? "Task failed"))
            if (result?.status === "cancelled") return yield* Effect.fail(new Error("Task cancelled"))
            return {
              title: params.description,
              metadata,
              output: renderOutput({ sessionID: nextSession.id, state: "completed", text: result?.output ?? "" }),
            }
          }),
        (_, exit) =>
          Effect.gen(function* () {
            if (Exit.hasInterrupts(exit)) onAbort()
            if (cancelFiber) yield* Fiber.await(cancelFiber)
          }).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                ctx.abort.removeEventListener("abort", onAbort)
              }),
            ),
          ),
      )
    })

    return {
      description: flags.experimentalBackgroundSubagents
        ? [DESCRIPTION, BACKGROUND_DESCRIPTION].join("\n\n")
        : DESCRIPTION,
      parameters: Parameters,
      jsonSchema: flags.experimentalBackgroundSubagents ? undefined : ToolJsonSchema.fromSchema(BaseParameters),
      execute: (params: Schema.Schema.Type<typeof Parameters>, ctx: Tool.Context) =>
        run(params, ctx).pipe(Effect.orDie),
    }
  }),
)
