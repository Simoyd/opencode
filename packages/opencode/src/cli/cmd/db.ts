import type { Argv } from "yargs"
import { spawn } from "child_process"
import { Database } from "@opencode-ai/core/database/database"
import { Effect } from "effect"
import { sql } from "drizzle-orm"
import { CliError, effectCmd } from "../effect-cmd"
import { SessionTranscriptIndex } from "@/session/transcript-index"
import { SessionID } from "@/session/schema"

const QueryCommand = effectCmd({
  command: "$0 [query]",
  describe: "open an interactive sqlite3 shell or run a query",
  instance: false,
  builder: (yargs: Argv) => {
    return yargs
      .positional("query", {
        type: "string",
        describe: "SQL query to execute",
      })
      .option("format", {
        type: "string",
        choices: ["json", "tsv"],
        default: "tsv",
        describe: "Output format",
      })
  },
  handler: Effect.fn("Cli.db.query")(function* (args: { query?: string; format: string }) {
    const query = args.query as string | undefined
    if (query) {
      const { db } = yield* Database.Service
      const result = yield* db.all<Record<string, unknown>>(sql.raw(query)).pipe(Effect.orDie)
      if (args.format === "json") console.log(JSON.stringify(result, null, 2))
      else if (result.length > 0) {
        const keys = Object.keys(result[0])
        console.log(keys.join("\t"))
        for (const row of result) console.log(keys.map((key) => row[key]).join("\t"))
      }
      return
    }
    const child = spawn("sqlite3", [Database.path()], {
      stdio: "inherit",
    })
    yield* Effect.promise(() => new Promise((resolve) => child.on("close", resolve)))
  }),
})

const PathCommand = effectCmd({
  command: "path",
  describe: "print the database path",
  instance: false,
  handler: Effect.fn("Cli.db.path")(function* () {
    console.log(Database.path())
  }),
})

const TranscriptIndexCommand = effectCmd({
  command: "transcript-index <session>",
  describe: "build or resume one bounded transcript window index",
  instance: false,
  builder: (yargs: Argv) =>
    yargs
      .positional("session", {
        type: "string",
        demandOption: true,
        describe: "session ID to index",
      })
      .option("owner", {
        type: "string",
        describe: "explicit owner token printed by a prior interrupted run",
      }),
  handler: Effect.fn("Cli.db.transcriptIndex")(function* (args: { session: string; owner?: string }) {
    const ownerID = args.owner ?? crypto.randomUUID()
    console.log(`Transcript index owner: ${ownerID}`)
    const result = yield* SessionTranscriptIndex.run({
      sessionID: SessionID.make(args.session),
      ownerID,
    }).pipe(
      Effect.mapError(
        (error) => new CliError({ message: error instanceof Error ? error.message : "Transcript indexing failed" }),
      ),
    )
    console.log(`Indexed ${result.descriptors} compacted transcript ranges${result.resumed ? " (resumed)" : ""}.`)
  }),
})

export const DbCommand = effectCmd({
  command: "db",
  describe: "database tools",
  instance: false,
  builder: (yargs: Argv) => {
    return yargs.command(QueryCommand).command(PathCommand).command(TranscriptIndexCommand).demandCommand()
  },
  handler: Effect.fn("Cli.db")(function* () {}),
})
