import { describe, expect } from "bun:test"
import { DateTime, Effect, Layer, Schema } from "effect"
import { asc, eq } from "drizzle-orm"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { EventSequenceTable } from "@opencode-ai/core/event/sql"
import { ModelV2 } from "@opencode-ai/core/model"
import { Project } from "@opencode-ai/core/project"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { SessionV2 } from "@opencode-ai/core/session"
import { SessionEvent } from "@opencode-ai/core/session/event"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { Prompt } from "@opencode-ai/core/session/prompt"
import { SessionMessageUpdater } from "@opencode-ai/core/session/message-updater"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import { SessionInput } from "@opencode-ai/core/session/input"
import { SessionStore } from "@opencode-ai/core/session/store"
import {
  CompactionRegionTable,
  MessageTable,
  PartTable,
  SessionInputTable,
  SessionMessageTable,
  SessionTable,
} from "@opencode-ai/core/session/sql"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { testEffect } from "./lib/effect"

const database = Database.layerFromPath(":memory:")
const events = EventV2.layer.pipe(Layer.provide(database))
const projector = SessionProjector.layer.pipe(Layer.provide(events), Layer.provide(database))
const it = testEffect(Layer.mergeAll(database, events, projector))
const sessionID = SessionV2.ID.make("ses_projector_test")
const created = DateTime.makeUnsafe(0)
const model = { id: ModelV2.ID.make("model"), providerID: ProviderV2.ID.make("provider") }
const encodeMessage = Schema.encodeSync(SessionMessage.Message)

const assistantRow = (
  id: SessionMessage.ID,
  seq: number,
  time: { created: DateTime.Utc; completed?: DateTime.Utc } = { created },
) => {
  const {
    id: _,
    type,
    ...data
  } = encodeMessage(new SessionMessage.Assistant({ id, type: "assistant", agent: "build", model, content: [], time }))
  return { id, session_id: sessionID, type, seq, time_created: DateTime.toEpochMillis(time.created), data }
}

describe("SessionProjector", () => {
  it.effect("orders projected messages and context by durable aggregate sequence", () =>
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      yield* db
        .insert(ProjectTable)
        .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
        .run()
        .pipe(Effect.orDie)
      yield* db
        .insert(SessionTable)
        .values({
          id: sessionID,
          project_id: Project.ID.global,
          slug: "test",
          directory: "/project",
          title: "test",
          version: "test",
        })
        .run()
        .pipe(Effect.orDie)
      const events = yield* EventV2.Service

      yield* events.publish(
        SessionEvent.Prompted,
        {
          sessionID,
          messageID: SessionMessage.ID.make("msg_first"),
          timestamp: created,
          prompt: new Prompt({ text: "first" }),
          delivery: "steer",
        },
        { id: EventV2.ID.make("evt_z") },
      )
      yield* events.publish(
        SessionEvent.Prompted,
        {
          sessionID,
          messageID: SessionMessage.ID.make("msg_second"),
          timestamp: created,
          prompt: new Prompt({ text: "second" }),
          delivery: "steer",
        },
        { id: EventV2.ID.make("evt_a") },
      )

      const sessions = yield* SessionV2.Service
      const firstPage = yield* sessions.messages({ sessionID, limit: 1, order: "asc" })
      expect(firstPage.map((message) => (message.type === "user" ? message.text : message.type))).toEqual(["first"])
      const secondPage = yield* sessions.messages({
        sessionID,
        limit: 1,
        order: "asc",
        cursor: { id: firstPage[0]!.id, direction: "next" },
      })
      expect(secondPage.map((message) => (message.type === "user" ? message.text : message.type))).toEqual(["second"])
      expect(
        (yield* sessions.messages({
          sessionID,
          limit: 1,
          order: "asc",
          cursor: { id: secondPage[0]!.id, direction: "previous" },
        })).map((message) => (message.type === "user" ? message.text : message.type)),
      ).toEqual(["first"])
      expect(
        (yield* sessions.context(sessionID)).map((message) => (message.type === "user" ? message.text : message.type)),
      ).toEqual(["first", "second"])
    }).pipe(
      Effect.provide(
        SessionV2.layer.pipe(
          Layer.provide(events),
          Layer.provide(database),
          Layer.provide(Project.defaultLayer),
          Layer.provide(SessionStore.layer.pipe(Layer.provide(database))),
          Layer.provide(SessionExecution.noopLayer),
        ),
      ),
    ),
  )

  it.effect("marks an admitted lifecycle row promoted with the PromptPromoted event sequence", () =>
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      yield* db
        .insert(ProjectTable)
        .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
        .run()
        .pipe(Effect.orDie)
      yield* db
        .insert(SessionTable)
        .values({
          id: sessionID,
          project_id: Project.ID.global,
          slug: "test",
          directory: "/project",
          title: "test",
          version: "test",
        })
        .run()
        .pipe(Effect.orDie)
      const events = yield* EventV2.Service
      const id = SessionMessage.ID.make("msg_admitted")
      yield* SessionInput.admit(db, events, {
        id,
        sessionID,
        prompt: new Prompt({ text: "promote me" }),
        delivery: "steer",
      })

      const event = yield* events.publish(SessionEvent.PromptLifecycle.Promoted, {
        sessionID,
        timestamp: created,
        messageID: id,
        prompt: new Prompt({ text: "promote me" }),
        timeCreated: created,
      })

      expect(
        yield* db.select().from(SessionInputTable).where(eq(SessionInputTable.id, id)).get().pipe(Effect.orDie),
      ).toMatchObject({ promoted_seq: event.seq })
    }),
  )

  it.effect("projects durable context messages supported by the updater", () =>
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      yield* db
        .insert(ProjectTable)
        .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
        .run()
        .pipe(Effect.orDie)
      yield* db
        .insert(SessionTable)
        .values({
          id: sessionID,
          project_id: Project.ID.global,
          slug: "test",
          directory: "/project",
          title: "test",
          version: "test",
        })
        .run()
        .pipe(Effect.orDie)
      const events = yield* EventV2.Service

      yield* events.publish(SessionEvent.AgentSwitched, {
        sessionID,
        messageID: SessionMessage.ID.create(),
        timestamp: created,
        agent: "build",
      })
      yield* events.publish(SessionEvent.ModelSwitched, {
        sessionID,
        messageID: SessionMessage.ID.create(),
        timestamp: created,
        model,
      })
      yield* events.publish(SessionEvent.Synthetic, {
        sessionID,
        messageID: SessionMessage.ID.create(),
        timestamp: created,
        text: "synthetic context",
      })
      yield* events.publish(SessionEvent.Shell.Started, {
        sessionID,
        messageID: SessionMessage.ID.create(),
        timestamp: created,
        callID: "shell-1",
        command: "pwd",
      })
      yield* events.publish(SessionEvent.Shell.Ended, {
        sessionID,
        timestamp: DateTime.makeUnsafe(1),
        callID: "shell-1",
        output: "/project",
      })
      yield* events.publish(SessionEvent.Compaction.Started, {
        sessionID,
        messageID: SessionMessage.ID.create(),
        timestamp: created,
        reason: "manual",
      })
      yield* events.publish(SessionEvent.Compaction.Delta, { sessionID, timestamp: created, text: "partial" })
      yield* events.publish(SessionEvent.Compaction.Ended, {
        sessionID,
        timestamp: DateTime.makeUnsafe(1),
        text: "summary",
        include: "msg-1",
      })

      const rows = yield* db
        .select()
        .from(SessionMessageTable)
        .where(eq(SessionMessageTable.session_id, sessionID))
        .orderBy(asc(SessionMessageTable.seq))
        .all()
        .pipe(Effect.orDie)
      const messages = rows.map((row) =>
        Schema.decodeUnknownSync(SessionMessage.Message)({ ...row.data, id: row.id, type: row.type }),
      )

      expect(messages.map((message) => message.type)).toEqual([
        "agent-switched",
        "model-switched",
        "synthetic",
        "shell",
        "compaction",
      ])
      expect(messages.find((message) => message.type === "shell")).toMatchObject({
        output: "/project",
        time: { completed: DateTime.makeUnsafe(1) },
      })
      expect(messages.find((message) => message.type === "compaction")).toMatchObject({
        summary: "summary",
        include: "msg-1",
      })
      expect(
        yield* db.select().from(SessionTable).where(eq(SessionTable.id, sessionID)).get().pipe(Effect.orDie),
      ).toMatchObject({
        agent: "build",
        model,
        time_updated: DateTime.toEpochMillis(created),
      })
    }),
  )

  it.effect("rejects distinct creator events that reuse one projected message ID", () =>
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      yield* db
        .insert(ProjectTable)
        .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
        .run()
        .pipe(Effect.orDie)
      yield* db
        .insert(SessionTable)
        .values({
          id: sessionID,
          project_id: Project.ID.global,
          slug: "test",
          directory: "/project",
          title: "test",
          version: "test",
        })
        .run()
        .pipe(Effect.orDie)
      const events = yield* EventV2.Service
      const id = SessionMessage.ID.make("msg_creator_collision")

      yield* events.publish(SessionEvent.Synthetic, { sessionID, messageID: id, timestamp: created, text: "keep me" })
      const exit = yield* events
        .publish(SessionEvent.Step.Started, {
          sessionID,
          assistantMessageID: id,
          timestamp: created,
          agent: "build",
          model,
        })
        .pipe(Effect.exit)

      expect(exit._tag).toBe("Failure")
      expect(
        yield* db.select().from(SessionMessageTable).where(eq(SessionMessageTable.id, id)).get().pipe(Effect.orDie),
      ).toMatchObject({ type: "synthetic" })
    }),
  )

  it.effect("rejects a Prompted event that conflicts with an admitted inbox row", () =>
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      yield* db
        .insert(ProjectTable)
        .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
        .run()
        .pipe(Effect.orDie)
      yield* db
        .insert(SessionTable)
        .values({
          id: sessionID,
          project_id: Project.ID.global,
          slug: "test",
          directory: "/project",
          title: "test",
          version: "test",
        })
        .run()
        .pipe(Effect.orDie)
      const events = yield* EventV2.Service
      const id = SessionMessage.ID.make("msg_conflict")
      yield* SessionInput.admit(db, events, {
        id,
        sessionID,
        prompt: new Prompt({ text: "admitted" }),
        delivery: "steer",
      })

      const exit = yield* events
        .publish(SessionEvent.Prompted, {
          sessionID,
          messageID: id,
          timestamp: created,
          prompt: new Prompt({ text: "different" }),
          delivery: "steer",
        })
        .pipe(Effect.exit)

      expect(String(exit)).toContain("SessionInput.LifecycleConflict")
      expect(
        yield* db.select().from(SessionInputTable).where(eq(SessionInputTable.id, id)).get().pipe(Effect.orDie),
      ).toMatchObject({ promoted_seq: null })
    }),
  )

  it.effect("rejects an assistant message ID that conflicts with an admitted inbox row", () =>
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      yield* db
        .insert(ProjectTable)
        .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
        .run()
        .pipe(Effect.orDie)
      yield* db
        .insert(SessionTable)
        .values({
          id: sessionID,
          project_id: Project.ID.global,
          slug: "test",
          directory: "/project",
          title: "test",
          version: "test",
        })
        .run()
        .pipe(Effect.orDie)
      const events = yield* EventV2.Service
      const id = SessionMessage.ID.make("msg_conflict")
      yield* SessionInput.admit(db, events, {
        id,
        sessionID,
        prompt: new Prompt({ text: "admitted" }),
        delivery: "steer",
      })

      const exit = yield* events
        .publish(SessionEvent.Step.Started, {
          sessionID,
          timestamp: created,
          assistantMessageID: id,
          agent: "build",
          model,
        })
        .pipe(Effect.exit)

      expect(String(exit)).toContain("SessionInput.LifecycleConflict")
      expect(
        yield* db.select().from(SessionMessageTable).where(eq(SessionMessageTable.id, id)).get().pipe(Effect.orDie),
      ).toBeUndefined()
    }),
  )

  it.effect("rejects a Prompted delivery mode that conflicts with an admitted inbox row", () =>
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      yield* db
        .insert(ProjectTable)
        .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
        .run()
        .pipe(Effect.orDie)
      yield* db
        .insert(SessionTable)
        .values({
          id: sessionID,
          project_id: Project.ID.global,
          slug: "test",
          directory: "/project",
          title: "test",
          version: "test",
        })
        .run()
        .pipe(Effect.orDie)
      const events = yield* EventV2.Service
      const id = SessionMessage.ID.make("msg_delivery_conflict")
      const prompt = new Prompt({ text: "admitted" })
      yield* SessionInput.admit(db, events, { id, sessionID, prompt, delivery: "queue" })

      const exit = yield* events
        .publish(SessionEvent.Prompted, { sessionID, messageID: id, timestamp: created, prompt, delivery: "steer" })
        .pipe(Effect.exit)

      expect(String(exit)).toContain("SessionInput.LifecycleConflict")
      expect(
        yield* db.select().from(SessionInputTable).where(eq(SessionInputTable.id, id)).get().pipe(Effect.orDie),
      ).toMatchObject({ delivery: "queue", promoted_seq: null })
    }),
  )

  it.effect("does not revive a stale incomplete in-memory assistant projection", () =>
    Effect.gen(function* () {
      const stale = new SessionMessage.Assistant({
        id: SessionMessage.ID.make("msg_assistant_stale"),
        type: "assistant",
        agent: "build",
        model,
        content: [],
        time: { created },
      })
      const completed = new SessionMessage.Assistant({
        id: SessionMessage.ID.make("msg_assistant_completed"),
        type: "assistant",
        agent: "build",
        model,
        content: [],
        time: { created: DateTime.makeUnsafe(1), completed: DateTime.makeUnsafe(2) },
      })

      expect(
        yield* SessionMessageUpdater.memory({ messages: [stale, completed] }).getCurrentAssistant(),
      ).toBeUndefined()
    }),
  )

  it.effect("updates only the newest incomplete assistant projection", () =>
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      yield* db
        .insert(ProjectTable)
        .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
        .run()
        .pipe(Effect.orDie)
      yield* db
        .insert(SessionTable)
        .values({
          id: sessionID,
          project_id: Project.ID.global,
          slug: "test",
          directory: "/project",
          title: "test",
          version: "test",
        })
        .run()
        .pipe(Effect.orDie)
      yield* db
        .insert(SessionMessageTable)
        .values([
          assistantRow(SessionMessage.ID.make("msg_assistant_1"), 0),
          assistantRow(SessionMessage.ID.make("msg_assistant_2"), 1),
        ])
        .run()
        .pipe(Effect.orDie)

      const service = yield* EventV2.Service
      yield* service.publish(SessionEvent.Step.Ended, {
        sessionID,
        timestamp: DateTime.makeUnsafe(1),
        assistantMessageID: SessionMessage.ID.make("msg_assistant_2"),
        finish: "stop",
        cost: 0,
        tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
      })

      const rows = yield* db
        .select()
        .from(SessionMessageTable)
        .where(eq(SessionMessageTable.session_id, sessionID))
        .orderBy(asc(SessionMessageTable.id))
        .all()
        .pipe(Effect.orDie)
      const messages = rows.map((row) =>
        Schema.decodeUnknownSync(SessionMessage.Message)({ ...row.data, id: row.id, type: row.type }),
      )
      expect(messages[0]).not.toHaveProperty("time.completed")
      expect(messages[1]).toMatchObject({
        type: "assistant",
        finish: "stop",
        time: { completed: DateTime.makeUnsafe(1) },
      })
    }),
  )

  it.effect("does not revive a stale incomplete assistant projection", () =>
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      yield* db
        .insert(ProjectTable)
        .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
        .run()
        .pipe(Effect.orDie)
      yield* db
        .insert(SessionTable)
        .values({
          id: sessionID,
          project_id: Project.ID.global,
          slug: "test",
          directory: "/project",
          title: "test",
          version: "test",
        })
        .run()
        .pipe(Effect.orDie)
      yield* db
        .insert(SessionMessageTable)
        .values([
          assistantRow(SessionMessage.ID.make("msg_assistant_stale"), 0),
          assistantRow(SessionMessage.ID.make("msg_assistant_completed"), 1, {
            created: DateTime.makeUnsafe(1),
            completed: DateTime.makeUnsafe(2),
          }),
        ])
        .run()
        .pipe(Effect.orDie)

      const service = yield* EventV2.Service
      yield* service.publish(SessionEvent.Text.Started, {
        sessionID,
        assistantMessageID: SessionMessage.ID.make("msg_assistant_completed"),
        timestamp: DateTime.makeUnsafe(3),
        textID: "text-stale",
      })

      const rows = yield* db
        .select()
        .from(SessionMessageTable)
        .where(eq(SessionMessageTable.session_id, sessionID))
        .orderBy(asc(SessionMessageTable.id))
        .all()
        .pipe(Effect.orDie)
      const messages = rows.map((row) =>
        Schema.decodeUnknownSync(SessionMessage.Message)({ ...row.data, id: row.id, type: row.type }),
      )
      expect(messages).toEqual([
        new SessionMessage.Assistant({
          id: SessionMessage.ID.make("msg_assistant_completed"),
          type: "assistant",
          agent: "build",
          model,
          content: [new SessionMessage.AssistantText({ type: "text", id: "text-stale", text: "" })],
          time: { created: DateTime.makeUnsafe(1), completed: DateTime.makeUnsafe(2) },
        }),
        new SessionMessage.Assistant({
          id: SessionMessage.ID.make("msg_assistant_stale"),
          type: "assistant",
          agent: "build",
          model,
          content: [],
          time: { created },
        }),
      ])
    }),
  )

  it.effect("rejects an agreed outer and nested part owner when the canonical parent belongs to another session", () =>
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      const sessionA = SessionV2.ID.make("ses_projector_owner_a")
      const sessionB = SessionV2.ID.make("ses_projector_owner_b")
      const messageID = SessionV1.MessageID.ascending("msg_projector_owner_a")
      const partID = SessionV1.PartID.ascending("prt_projector_wrong_parent")
      yield* insertOwnerFixture(db, sessionA, sessionB)
      const service = yield* EventV2.Service
      yield* service.publish(SessionV1.Event.MessageUpdated, {
        sessionID: sessionA,
        info: userMessage(sessionA, messageID),
      })
      const sequenceBefore = yield* db.select().from(EventSequenceTable).all().pipe(Effect.orDie)
      const sessionsBefore = yield* db.select().from(SessionTable).orderBy(asc(SessionTable.id)).all().pipe(Effect.orDie)
      const catalogBefore = yield* db.select().from(CompactionRegionTable).all().pipe(Effect.orDie)

      const exit = yield* service
        .publish(SessionV1.Event.PartUpdated, {
          sessionID: sessionB,
          part: { id: partID, sessionID: sessionB, messageID, type: "text", text: "must not persist" },
          time: 2,
        })
        .pipe(Effect.exit)

      expect(exit._tag).toBe("Failure")
      expect(String(exit)).toContain("no canonical parent")
      expect(yield* db.select().from(PartTable).where(eq(PartTable.id, partID)).get().pipe(Effect.orDie)).toBeUndefined()
      expect(yield* db.select().from(MessageTable).where(eq(MessageTable.id, messageID)).get().pipe(Effect.orDie)).toMatchObject({
        session_id: sessionA,
      })
      expect(yield* db.select().from(SessionTable).orderBy(asc(SessionTable.id)).all().pipe(Effect.orDie)).toEqual(
        sessionsBefore,
      )
      expect(yield* db.select().from(CompactionRegionTable).all().pipe(Effect.orDie)).toEqual(catalogBefore)
      expect(yield* db.select().from(EventSequenceTable).all().pipe(Effect.orDie)).toEqual(sequenceBefore)
    }),
  )

  it.effect("wrong-owner PartRemoved preserves the part usage catalog and event sequence", () =>
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      const sessionA = SessionV2.ID.make("ses_projector_remove_a")
      const sessionB = SessionV2.ID.make("ses_projector_remove_b")
      const messageID = SessionV1.MessageID.ascending("msg_projector_remove_a")
      const partID = SessionV1.PartID.ascending("prt_projector_remove_a")
      yield* insertOwnerFixture(db, sessionA, sessionB)
      const service = yield* EventV2.Service
      yield* service.publish(SessionV1.Event.MessageUpdated, {
        sessionID: sessionA,
        info: userMessage(sessionA, messageID),
      })
      yield* service.publish(SessionV1.Event.PartUpdated, {
        sessionID: sessionA,
        part: {
          id: partID,
          sessionID: sessionA,
          messageID,
          type: "step-finish",
          reason: "stop",
          cost: 5,
          tokens: { total: 21, input: 11, output: 7, reasoning: 3, cache: { read: 2, write: 1 } },
        },
        time: 2,
      })
      const partBefore = yield* db.select().from(PartTable).where(eq(PartTable.id, partID)).get().pipe(Effect.orDie)
      const sessionBefore = yield* db.select().from(SessionTable).where(eq(SessionTable.id, sessionA)).get().pipe(Effect.orDie)
      const catalogBefore = yield* db.select().from(CompactionRegionTable).all().pipe(Effect.orDie)
      const sequenceBefore = yield* db.select().from(EventSequenceTable).all().pipe(Effect.orDie)

      const exit = yield* service
        .publish(SessionV1.Event.PartRemoved, { sessionID: sessionB, messageID, partID })
        .pipe(Effect.exit)

      expect(exit._tag).toBe("Failure")
      expect(String(exit)).toContain("different persisted owner")
      expect(yield* db.select().from(PartTable).where(eq(PartTable.id, partID)).get().pipe(Effect.orDie)).toEqual(
        partBefore,
      )
      expect(yield* db.select().from(SessionTable).where(eq(SessionTable.id, sessionA)).get().pipe(Effect.orDie)).toEqual(
        sessionBefore,
      )
      expect(yield* db.select().from(CompactionRegionTable).all().pipe(Effect.orDie)).toEqual(catalogBefore)
      expect(yield* db.select().from(EventSequenceTable).all().pipe(Effect.orDie)).toEqual(sequenceBefore)
    }),
  )
})

const insertOwnerFixture = (
  db: Database.Interface["db"],
  sessionA: SessionV2.ID,
  sessionB: SessionV2.ID,
) =>
  Effect.gen(function* () {
    yield* db
      .insert(ProjectTable)
      .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
      .run()
      .pipe(Effect.orDie)
    yield* db
      .insert(SessionTable)
      .values(
        [sessionA, sessionB].map((id) => ({
          id,
          project_id: Project.ID.global,
          slug: String(id),
          directory: "/project",
          title: String(id),
          version: "test",
        })),
      )
      .run()
      .pipe(Effect.orDie)
  })

const userMessage = (sessionID: SessionV2.ID, id: SessionV1.MessageID) => ({
  id,
  sessionID,
  role: "user" as const,
  time: { created: 1 },
  agent: "test",
  model: { providerID: model.providerID, modelID: model.id },
  tools: {},
  mode: "",
})
