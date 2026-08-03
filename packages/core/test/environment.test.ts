import { describe, expect, test } from "bun:test"
import fs from "fs/promises"
import os from "os"
import path from "path"
import { Environment } from "@opencode-ai/core/environment"

function cleanEnv(root: string) {
  const env: NodeJS.ProcessEnv = { ...process.env, [Environment.ISOLATED_ROOT_ENV]: root }
  for (const key of Environment.ISOLATED_ROOT_CONFLICT_KEYS) delete env[key]
  return env
}

async function run(script: string, env: NodeJS.ProcessEnv) {
  const proc = Bun.spawn([process.execPath, "-e", script], {
    env,
    stdout: "pipe",
    stderr: "pipe",
  })
  const [stdout, stderr, code] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ])
  return { stdout, stderr, code }
}

describe("Environment", () => {
  test("derives concise isolated OpenCode-owned paths from a single root", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-isolated-root-"))
    const result = await run(
      `
        const { Global } = await import("@opencode-ai/core/global")
        const { Database } = await import("@opencode-ai/core/database/database")
        process.stdout.write(JSON.stringify({ path: Global.Path, make: Global.make(), db: Database.path() }))
      `,
      cleanEnv(root),
    )

    expect(result.stderr).toBe("")
    expect(result.code).toBe(0)
    const output = JSON.parse(result.stdout) as {
      path: Record<string, string>
      make: Record<string, string>
      db: string
    }
    expect(output.path.config).toBe(path.join(root, "config"))
    expect(output.path.data).toBe(path.join(root, "data"))
    expect(output.path.cache).toBe(path.join(root, "cache"))
    expect(output.path.state).toBe(path.join(root, "state"))
    expect(output.path.locks).toBe(path.join(root, "locks"))
    expect(output.path.home).toBe(path.join(root, "config"))
    expect(output.path.bin).toBe(path.join(root, "cache", "bin"))
    expect(output.path.log).toBe(path.join(root, "data", "log"))
    expect(output.path.repos).toBe(path.join(root, "data", "repos"))
    expect(output.make.config).toBe(output.path.config)
    expect(path.dirname(output.db)).toBe(path.join(root, "data"))
    expect(path.basename(output.db)).toMatch(/^opencode.*\.db$/)
  })

  test("fails closed when legacy path or content overrides conflict with isolated-root mode", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-isolated-root-"))
    const result = await run(`await import("@opencode-ai/core/global")`, {
      ...cleanEnv(root),
      OPENCODE_DB: "legacy.db",
      OPENCODE_CONFIG_CONTENT: "{}",
    })

    expect(result.code).not.toBe(0)
    expect(result.stderr).toContain(Environment.ISOLATED_ROOT_ENV)
    expect(result.stderr).toContain("OPENCODE_DB")
    expect(result.stderr).toContain("OPENCODE_CONFIG_CONTENT")
  })

  test("scrubs sidecar-only variables from default user tool environments", () => {
    const env: NodeJS.ProcessEnv = {
      PATH: "/usr/bin",
      OPENCODE_API_KEY: "provider-key",
      [Environment.ISOLATED_ROOT_ENV]: "/isolated",
      OPENCODE_AVALONIA_MANAGED_WSL_STATE_ENVIRONMENT_LABEL: "source-dev",
      OPENCODE_AVALONIA_RUNTIME_ASSET_ROOT: "/sidecar/assets",
      OPENCODE_SERVER_PASSWORD: "secret",
      opencode_server_username: "case-insensitive-secret",
      OPENCODE_AVALONIA_STREAM_DIAGNOSTICS: "1",
      OPENCODE_WORKSPACE_ID: "workspace",
      OPENCODE_CONFIG_DIR: "/legacy/config",
      OPENCODE_AUTH_CONTENT: "{}",
    }

    const scrubbed = Environment.scrubUserToolEnv(env)
    expect(scrubbed.PATH).toBe("/usr/bin")
    expect(scrubbed.OPENCODE_API_KEY).toBe("provider-key")
    expect(scrubbed[Environment.ISOLATED_ROOT_ENV]).toBeUndefined()
    expect(scrubbed.OPENCODE_AVALONIA_MANAGED_WSL_STATE_ENVIRONMENT_LABEL).toBeUndefined()
    expect(scrubbed.OPENCODE_AVALONIA_RUNTIME_ASSET_ROOT).toBeUndefined()
    expect(scrubbed.OPENCODE_SERVER_PASSWORD).toBeUndefined()
    expect(scrubbed.opencode_server_username).toBeUndefined()
    expect(scrubbed.OPENCODE_AVALONIA_STREAM_DIAGNOSTICS).toBeUndefined()
    expect(scrubbed.OPENCODE_WORKSPACE_ID).toBeUndefined()
    expect(scrubbed.OPENCODE_CONFIG_DIR).toBeUndefined()
    expect(scrubbed.OPENCODE_AUTH_CONTENT).toBeUndefined()
  })

  test("scrubs sidecar-only overrides while preserving normal user-tool overrides", () => {
    const env = Environment.userToolEnv(
      {
        [Environment.ISOLATED_ROOT_ENV]: "/isolated",
        PATH: "/usr/bin",
      },
      { EXTRA: "1", OPENCODE_SERVER_PASSWORD: "explicit" },
    )
    expect(env[Environment.ISOLATED_ROOT_ENV]).toBeUndefined()
    expect(env.PATH).toBe("/usr/bin")
    expect(env.EXTRA).toBe("1")
    expect(env.OPENCODE_SERVER_PASSWORD).toBeUndefined()
  })
})
