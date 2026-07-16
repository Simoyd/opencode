import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20260715000000_transcript_windowing",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`CREATE TABLE \`session_maintenance\` (
        \`session_id\` text PRIMARY KEY NOT NULL REFERENCES \`session\`(\`id\`) ON DELETE CASCADE,
        \`owner_id\` text NOT NULL,
        \`epoch\` integer NOT NULL,
        \`kind\` text NOT NULL,
        \`time_started\` integer NOT NULL,
        \`time_updated\` integer NOT NULL
      );`)
      yield* tx.run(`CREATE TABLE \`session_admission\` (
        \`session_id\` text NOT NULL REFERENCES \`session\`(\`id\`) ON DELETE CASCADE,
        \`operation_id\` text NOT NULL,
        \`kind\` text NOT NULL,
        \`time_started\` integer NOT NULL,
        PRIMARY KEY(\`session_id\`, \`operation_id\`)
      );`)
      yield* tx.run(`CREATE INDEX \`session_admission_session_idx\` ON \`session_admission\` (\`session_id\`);`)
      yield* tx.run(`CREATE TABLE \`transcript_window_state\` (
        \`session_id\` text PRIMARY KEY NOT NULL REFERENCES \`session\`(\`id\`) ON DELETE CASCADE,
        \`source_generation\` text NOT NULL,
        \`window_revision\` integer NOT NULL,
        \`index_status\` text NOT NULL,
        \`tail_start_id\` text,
        \`descriptor_count\` integer DEFAULT 0 NOT NULL,
        \`message_count\` integer DEFAULT 0 NOT NULL,
        \`part_count\` integer DEFAULT 0 NOT NULL,
        \`text_units\` integer DEFAULT 2 NOT NULL,
        \`decoded_bytes\` integer DEFAULT 2 NOT NULL,
        \`index_owner_id\` text,
        \`index_cursor_time\` integer,
        \`index_cursor_id\` text,
        \`index_state\` text
      );`)
      yield* tx.run(`CREATE TABLE \`compaction_archive_manifest\` (
        \`session_id\` text NOT NULL REFERENCES \`session\`(\`id\`) ON DELETE CASCADE,
        \`archive_id\` text NOT NULL,
        \`archive_revision\` integer NOT NULL,
        \`ordinal\` integer NOT NULL,
        \`marker_id\` text NOT NULL,
        \`tail_start_id\` text,
        \`source_message_id\` text NOT NULL,
        \`summary_message_id\` text NOT NULL,
        \`range_start_id\` text NOT NULL,
        \`range_end_id\` text NOT NULL,
        \`summary_preview\` text NOT NULL,
        \`continuity_message_ids\` text NOT NULL,
        \`replay_message_ids\` text NOT NULL,
        \`message_count\` integer NOT NULL,
        \`part_count\` integer NOT NULL,
        \`text_units\` integer NOT NULL,
        \`decoded_bytes\` integer NOT NULL,
        PRIMARY KEY(\`session_id\`, \`archive_id\`)
      );`)
      yield* tx.run(`CREATE UNIQUE INDEX \`compaction_archive_session_ordinal_idx\` ON \`compaction_archive_manifest\` (\`session_id\`, \`ordinal\`);`)
      yield* tx.run(`CREATE INDEX \`compaction_archive_session_marker_idx\` ON \`compaction_archive_manifest\` (\`session_id\`, \`marker_id\`);`)
      yield* tx.run(`CREATE TABLE \`compaction_archive_staging\` (
        \`session_id\` text NOT NULL REFERENCES \`session\`(\`id\`) ON DELETE CASCADE,
        \`owner_id\` text NOT NULL,
        \`archive_id\` text NOT NULL,
        \`archive_revision\` integer NOT NULL,
        \`ordinal\` integer NOT NULL,
        \`marker_id\` text NOT NULL,
        \`tail_start_id\` text,
        \`source_message_id\` text NOT NULL,
        \`summary_message_id\` text NOT NULL,
        \`range_start_id\` text NOT NULL,
        \`range_end_id\` text NOT NULL,
        \`summary_preview\` text NOT NULL,
        \`continuity_message_ids\` text NOT NULL,
        \`replay_message_ids\` text NOT NULL,
        \`message_count\` integer NOT NULL,
        \`part_count\` integer NOT NULL,
        \`text_units\` integer NOT NULL,
        \`decoded_bytes\` integer NOT NULL,
        PRIMARY KEY(\`session_id\`, \`owner_id\`, \`archive_id\`)
      );`)
      yield* tx.run(`CREATE TRIGGER \`session_maintenance_block_session_update\`
        BEFORE UPDATE ON \`session\`
        WHEN EXISTS (SELECT 1 FROM \`session_maintenance\` WHERE \`session_id\` = OLD.\`id\`)
        BEGIN SELECT RAISE(ABORT, 'session locked for transcript indexing'); END;`)
      yield* tx.run(`CREATE TRIGGER \`session_maintenance_block_session_delete\`
        BEFORE DELETE ON \`session\`
        WHEN EXISTS (SELECT 1 FROM \`session_maintenance\` WHERE \`session_id\` = OLD.\`id\`)
        BEGIN SELECT RAISE(ABORT, 'session locked for transcript indexing'); END;`)
      yield* tx.run(`CREATE TRIGGER \`session_maintenance_block_message_insert\`
        BEFORE INSERT ON \`message\`
        WHEN EXISTS (SELECT 1 FROM \`session_maintenance\` WHERE \`session_id\` = NEW.\`session_id\`)
        BEGIN SELECT RAISE(ABORT, 'session locked for transcript indexing'); END;`)
      yield* tx.run(`CREATE TRIGGER \`session_maintenance_block_message_update\`
        BEFORE UPDATE ON \`message\`
        WHEN EXISTS (SELECT 1 FROM \`session_maintenance\` WHERE \`session_id\` IN (OLD.\`session_id\`, NEW.\`session_id\`))
        BEGIN SELECT RAISE(ABORT, 'session locked for transcript indexing'); END;`)
      yield* tx.run(`CREATE TRIGGER \`session_maintenance_block_message_delete\`
        BEFORE DELETE ON \`message\`
        WHEN EXISTS (SELECT 1 FROM \`session_maintenance\` WHERE \`session_id\` = OLD.\`session_id\`)
        BEGIN SELECT RAISE(ABORT, 'session locked for transcript indexing'); END;`)
      yield* tx.run(`CREATE TRIGGER \`session_maintenance_block_part_insert\`
        BEFORE INSERT ON \`part\`
        WHEN EXISTS (SELECT 1 FROM \`session_maintenance\` WHERE \`session_id\` = NEW.\`session_id\`)
        BEGIN SELECT RAISE(ABORT, 'session locked for transcript indexing'); END;`)
      yield* tx.run(`CREATE TRIGGER \`session_maintenance_block_part_update\`
        BEFORE UPDATE ON \`part\`
        WHEN EXISTS (SELECT 1 FROM \`session_maintenance\` WHERE \`session_id\` IN (OLD.\`session_id\`, NEW.\`session_id\`))
        BEGIN SELECT RAISE(ABORT, 'session locked for transcript indexing'); END;`)
      yield* tx.run(`CREATE TRIGGER \`session_maintenance_block_part_delete\`
        BEFORE DELETE ON \`part\`
        WHEN EXISTS (SELECT 1 FROM \`session_maintenance\` WHERE \`session_id\` = OLD.\`session_id\`)
        BEGIN SELECT RAISE(ABORT, 'session locked for transcript indexing'); END;`)
    })
  },
} satisfies DatabaseMigration.Migration
