import { sqliteTable, text, integer, index, primaryKey, real, uniqueIndex } from "drizzle-orm/sqlite-core"
import * as DatabasePath from "../database/path"
import { ProjectTable } from "../project/sql"
import type { SessionMessage } from "./message"
import type { Prompt } from "./prompt"
import type { SessionInput } from "./input"
import type { Snapshot } from "../snapshot"
import { PermissionV1 } from "../v1/permission"
import { ProjectV2 } from "../project"
import type { SessionSchema } from "./schema"
import type { MessageID, PartID, SessionV1 } from "../v1/session"
import { WorkspaceV2 } from "../workspace"
import { Timestamps } from "../database/schema.sql"
import type { SystemContext } from "../system-context/index"
import { AgentV2 } from "../agent"

type SessionMessageData = Omit<(typeof SessionMessage.Message)["Encoded"], "type" | "id">
type V1MessageData = Omit<SessionV1.Info, "id" | "sessionID">
type V1PartData = Omit<SessionV1.Part, "id" | "sessionID" | "messageID">

export const SessionTable = sqliteTable(
  "session",
  {
    id: text().$type<SessionSchema.ID>().primaryKey(),
    project_id: text()
      .$type<ProjectV2.ID>()
      .notNull()
      .references(() => ProjectTable.id, { onDelete: "cascade" }),
    workspace_id: text().$type<WorkspaceV2.ID>(),
    parent_id: text().$type<SessionSchema.ID>(),
    slug: text().notNull(),
    directory: DatabasePath.directoryColumn().notNull(),
    path: DatabasePath.pathColumn(),
    title: text().notNull(),
    version: text().notNull(),
    share_url: text(),
    summary_additions: integer(),
    summary_deletions: integer(),
    summary_files: integer(),
    summary_diffs: text({ mode: "json" }).$type<Snapshot.FileDiff[]>(),
    metadata: text({ mode: "json" }).$type<Record<string, unknown>>(),
    cost: real().notNull().default(0),
    tokens_input: integer().notNull().default(0),
    tokens_output: integer().notNull().default(0),
    tokens_reasoning: integer().notNull().default(0),
    tokens_cache_read: integer().notNull().default(0),
    tokens_cache_write: integer().notNull().default(0),
    revert: text({ mode: "json" }).$type<{ messageID: MessageID; partID?: PartID; snapshot?: string; diff?: string }>(),
    permission: text({ mode: "json" }).$type<PermissionV1.Ruleset>(),
    agent: text(),
    model: text({ mode: "json" }).$type<{
      id: string
      providerID: string
      variant?: string
    }>(),
    ...Timestamps,
    time_compacting: integer(),
    time_archived: integer(),
  },
  (table) => [
    index("session_project_idx").on(table.project_id),
    index("session_workspace_idx").on(table.workspace_id),
    index("session_parent_idx").on(table.parent_id),
  ],
)

export const MessageTable = sqliteTable(
  "message",
  {
    id: text().$type<MessageID>().primaryKey(),
    session_id: text()
      .$type<SessionSchema.ID>()
      .notNull()
      .references(() => SessionTable.id, { onDelete: "cascade" }),
    ...Timestamps,
    data: text({ mode: "json" }).notNull().$type<V1MessageData>(),
  },
  (table) => [index("message_session_time_created_id_idx").on(table.session_id, table.time_created, table.id)],
)

export const PartTable = sqliteTable(
  "part",
  {
    id: text().$type<PartID>().primaryKey(),
    message_id: text()
      .$type<MessageID>()
      .notNull()
      .references(() => MessageTable.id, { onDelete: "cascade" }),
    session_id: text().$type<SessionSchema.ID>().notNull(),
    ...Timestamps,
    data: text({ mode: "json" }).notNull().$type<V1PartData>(),
  },
  (table) => [
    index("part_message_id_id_idx").on(table.message_id, table.id),
    index("part_session_idx").on(table.session_id),
  ],
)

export const TodoTable = sqliteTable(
  "todo",
  {
    session_id: text()
      .$type<SessionSchema.ID>()
      .notNull()
      .references(() => SessionTable.id, { onDelete: "cascade" }),
    content: text().notNull(),
    status: text().notNull(),
    priority: text().notNull(),
    position: integer().notNull(),
    ...Timestamps,
  },
  (table) => [
    primaryKey({ columns: [table.session_id, table.position] }),
    index("todo_session_idx").on(table.session_id),
  ],
)

export const SessionMessageTable = sqliteTable(
  "session_message",
  {
    id: text().$type<SessionMessage.ID>().primaryKey(),
    session_id: text()
      .$type<SessionSchema.ID>()
      .notNull()
      .references(() => SessionTable.id, { onDelete: "cascade" }),
    type: text().$type<SessionMessage.Type>().notNull(),
    seq: integer().notNull(),
    ...Timestamps,
    data: text({ mode: "json" }).notNull().$type<SessionMessageData>(),
  },
  (table) => [
    uniqueIndex("session_message_session_seq_idx").on(table.session_id, table.seq),
    index("session_message_session_type_seq_idx").on(table.session_id, table.type, table.seq),
    index("session_message_session_time_created_id_idx").on(table.session_id, table.time_created, table.id),
    index("session_message_time_created_idx").on(table.time_created),
  ],
)

export const SessionInputTable = sqliteTable(
  "session_input",
  {
    id: text().$type<SessionMessage.ID>().primaryKey(),
    session_id: text()
      .$type<SessionSchema.ID>()
      .notNull()
      .references(() => SessionTable.id, { onDelete: "cascade" }),
    prompt: text({ mode: "json" }).notNull().$type<Prompt>(),
    delivery: text().$type<SessionInput.Delivery>().notNull(),
    admitted_seq: integer().notNull(),
    promoted_seq: integer(),
    time_created: integer()
      .notNull()
      .$default(() => Date.now()),
  },
  (table) => [
    index("session_input_session_pending_delivery_seq_idx").on(
      table.session_id,
      table.promoted_seq,
      table.delivery,
      table.admitted_seq,
    ),
    uniqueIndex("session_input_session_admitted_seq_idx").on(table.session_id, table.admitted_seq),
    uniqueIndex("session_input_session_promoted_seq_idx").on(table.session_id, table.promoted_seq),
  ],
)

export const SessionContextEpochTable = sqliteTable("session_context_epoch", {
  session_id: text()
    .$type<SessionSchema.ID>()
    .primaryKey()
    .references(() => SessionTable.id, { onDelete: "cascade" }),
  baseline: text().notNull(),
  agent: text().$type<AgentV2.ID>().notNull().default(AgentV2.defaultID),
  snapshot: text({ mode: "json" }).notNull().$type<SystemContext.Snapshot>(),
  baseline_seq: integer().notNull(),
  replacement_seq: integer(),
  revision: integer().notNull().default(0),
})

export const SessionMaintenanceTable = sqliteTable("session_maintenance", {
  session_id: text()
    .$type<SessionSchema.ID>()
    .primaryKey()
    .references(() => SessionTable.id, { onDelete: "cascade" }),
  owner_id: text().notNull(),
  epoch: integer().notNull(),
  kind: text().$type<"transcript_index">().notNull(),
  time_started: integer().notNull(),
  time_updated: integer().notNull(),
})

export const SessionAdmissionTable = sqliteTable(
  "session_admission",
  {
    session_id: text()
      .$type<SessionSchema.ID>()
      .notNull()
      .references(() => SessionTable.id, { onDelete: "cascade" }),
    operation_id: text().notNull(),
    kind: text().notNull(),
    time_started: integer().notNull(),
  },
  (table) => [
    primaryKey({ columns: [table.session_id, table.operation_id] }),
    index("session_admission_session_idx").on(table.session_id),
  ],
)

export const TranscriptWindowStateTable = sqliteTable("transcript_window_state", {
  session_id: text()
    .$type<SessionSchema.ID>()
    .primaryKey()
    .references(() => SessionTable.id, { onDelete: "cascade" }),
  source_generation: text().notNull(),
  window_revision: integer().notNull(),
  index_status: text()
    .$type<"complete" | "index_required" | "indexing" | "index_failed" | "stale" | "revert_unrepresentable" | "too_large">()
    .notNull(),
  tail_start_id: text().$type<MessageID>(),
  descriptor_count: integer().notNull().default(0),
  message_count: integer().notNull().default(0),
  part_count: integer().notNull().default(0),
  text_units: integer().notNull().default(2),
  decoded_bytes: integer().notNull().default(2),
  index_owner_id: text(),
  index_cursor_time: integer(),
  index_cursor_id: text().$type<MessageID>(),
  index_state: text({ mode: "json" }).$type<Record<string, unknown>>(),
})

export const CompactionArchiveManifestTable = sqliteTable(
  "compaction_archive_manifest",
  {
    session_id: text()
      .$type<SessionSchema.ID>()
      .notNull()
      .references(() => SessionTable.id, { onDelete: "cascade" }),
    archive_id: text().notNull(),
    archive_revision: integer().notNull(),
    ordinal: integer().notNull(),
    marker_id: text().$type<MessageID>().notNull(),
    tail_start_id: text().$type<MessageID>(),
    source_message_id: text().$type<MessageID>().notNull(),
    summary_message_id: text().$type<MessageID>().notNull(),
    range_start_id: text().$type<MessageID>().notNull(),
    range_end_id: text().$type<MessageID>().notNull(),
    summary_preview: text().notNull(),
    continuity_message_ids: text({ mode: "json" }).$type<MessageID[]>().notNull(),
    replay_message_ids: text({ mode: "json" }).$type<MessageID[]>().notNull(),
    message_count: integer().notNull(),
    part_count: integer().notNull(),
    text_units: integer().notNull(),
    decoded_bytes: integer().notNull(),
  },
  (table) => [
    primaryKey({ columns: [table.session_id, table.archive_id] }),
    uniqueIndex("compaction_archive_session_ordinal_idx").on(table.session_id, table.ordinal),
    index("compaction_archive_session_marker_idx").on(table.session_id, table.marker_id),
  ],
)

export const CompactionArchiveStagingTable = sqliteTable(
  "compaction_archive_staging",
  {
    session_id: text()
      .$type<SessionSchema.ID>()
      .notNull()
      .references(() => SessionTable.id, { onDelete: "cascade" }),
    owner_id: text().notNull(),
    archive_id: text().notNull(),
    archive_revision: integer().notNull(),
    ordinal: integer().notNull(),
    marker_id: text().$type<MessageID>().notNull(),
    tail_start_id: text().$type<MessageID>(),
    source_message_id: text().$type<MessageID>().notNull(),
    summary_message_id: text().$type<MessageID>().notNull(),
    range_start_id: text().$type<MessageID>().notNull(),
    range_end_id: text().$type<MessageID>().notNull(),
    summary_preview: text().notNull(),
    continuity_message_ids: text({ mode: "json" }).$type<MessageID[]>().notNull(),
    replay_message_ids: text({ mode: "json" }).$type<MessageID[]>().notNull(),
    message_count: integer().notNull(),
    part_count: integer().notNull(),
    text_units: integer().notNull(),
    decoded_bytes: integer().notNull(),
  },
  (table) => [primaryKey({ columns: [table.session_id, table.owner_id, table.archive_id] })],
)
