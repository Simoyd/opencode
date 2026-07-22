import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20260715000000_compaction_region",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`CREATE TABLE \`compaction_region\` (
        \`session_id\` text NOT NULL REFERENCES \`session\`(\`id\`) ON DELETE CASCADE,
        \`marker_id\` text NOT NULL,
        \`start_message_id\` text NOT NULL,
        \`summary_message_id\` text NOT NULL,
        \`summary_preview\` text NOT NULL,
        \`physical_message_count\` integer NOT NULL,
        \`semantic_message_count\` integer NOT NULL,
        \`part_count\` integer NOT NULL,
        PRIMARY KEY(\`session_id\`, \`marker_id\`)
      );`)
      yield* tx.run(`CREATE INDEX \`compaction_region_session_marker_idx\` ON \`compaction_region\` (\`session_id\`, \`marker_id\`);`)
      yield* tx.run(`CREATE INDEX \`compaction_region_session_start_idx\` ON \`compaction_region\` (\`session_id\`, \`start_message_id\`);`)
    })
  },
} satisfies DatabaseMigration.Migration
