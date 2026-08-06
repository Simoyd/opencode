export * as Environment from "./environment"

import path from "path"

export const ISOLATED_ROOT_ENV = "OPENCODE_AVALONIA_ENVIRONMENT_ROOT"

export const ISOLATED_ROOT_CONFLICT_KEYS = [
  "OPENCODE_CONFIG_DIR",
  "OPENCODE_DB",
  "OPENCODE_PLUGIN_META_FILE",
  "OPENCODE_CONFIG",
  "OPENCODE_CONFIG_CONTENT",
  "OPENCODE_AUTH_CONTENT",
] as const

export const USER_TOOL_SCRUB_KEYS = [
  ISOLATED_ROOT_ENV,
  "OPENCODE_AUTO_SHARE",
  "OPENCODE_CLIENT",
  "OPENCODE_DISABLE_CHANNEL_DB",
  "OPENCODE_EXPERIMENTAL",
  "OPENCODE_PERMISSION",
  "OPENCODE_PURE",
  "OPENCODE_SERVER_PASSWORD",
  "OPENCODE_SERVER_USERNAME",
  "OPENCODE_TUI_CONFIG",
  "OPENCODE_AVALONIA_BUN_PATH",
  "OPENCODE_AVALONIA_DISABLE_WORKSPACE_ROUTING",
  "OPENCODE_AVALONIA_ENABLE_DEV_SOURCE_LAUNCH",
  "OPENCODE_AVALONIA_MANAGED_WSL_ENVIRONMENT_ROOT",
  "OPENCODE_AVALONIA_MANAGED_WSL_PARENT_GRACE_MS",
  "OPENCODE_AVALONIA_MANAGED_WSL_STATE_ENVIRONMENT_LABEL",
  "OPENCODE_AVALONIA_MANAGED_WSL_STATE_ENVIRONMENT_ROOT_HASH",
  "OPENCODE_AVALONIA_MANAGED_WSL_STATE_LAYOUT_VERSION",
  "OPENCODE_AVALONIA_RUNTIME_ASSET_ROOT",
  "OPENCODE_AVALONIA_UPSTREAM_ROOT",
  "OPENCODE_AVALONIA_WSL_LINUX_SIDECAR_PATH",
  "OPENCODE_AVALONIA_WSL_PACKAGE_ASSET_ROOT",
  "OPENCODE_WORKSPACE_ID",
  ...ISOLATED_ROOT_CONFLICT_KEYS,
] as const

export const USER_TOOL_SCRUB_PREFIXES = ["OPENCODE_EXPERIMENTAL_"] as const

export interface IsolatedPaths {
  readonly root: string
  readonly config: string
  readonly data: string
  readonly cache: string
  readonly state: string
  readonly locks: string
}

function envKey(env: NodeJS.ProcessEnv, key: string) {
  if (process.platform !== "win32") return Object.prototype.hasOwnProperty.call(env, key) ? key : undefined
  const lower = key.toLowerCase()
  return Object.keys(env).find((item) => item.toLowerCase() === lower)
}

function envValue(env: NodeJS.ProcessEnv, key: string) {
  const hit = envKey(env, key)
  if (!hit) return
  return env[hit]
}

export function isolatedRoot(env: NodeJS.ProcessEnv = process.env) {
  const value = envValue(env, ISOLATED_ROOT_ENV)
  return value || undefined
}

export function isolatedPaths(env: NodeJS.ProcessEnv = process.env): IsolatedPaths | undefined {
  const root = isolatedRoot(env)
  if (!root) return
  return {
    root,
    config: path.join(root, "config"),
    data: path.join(root, "data"),
    cache: path.join(root, "cache"),
    state: path.join(root, "state"),
    locks: path.join(root, "locks"),
  }
}

export function assertNoIsolatedRootConflicts(env: NodeJS.ProcessEnv = process.env) {
  if (!isolatedRoot(env)) return
  const conflicts = ISOLATED_ROOT_CONFLICT_KEYS.filter((key) => envValue(env, key) !== undefined)
  if (!conflicts.length) return
  throw new Error(`${ISOLATED_ROOT_ENV} cannot be combined with ${conflicts.join(", ")}`)
}

export function scrubUserToolEnv(env: NodeJS.ProcessEnv = process.env): Record<string, string> {
  const scrub = new Set(USER_TOOL_SCRUB_KEYS.map((key) => key.toLowerCase()))
  const prefixes = USER_TOOL_SCRUB_PREFIXES.map((prefix) => prefix.toLowerCase())
  const result: Record<string, string> = {}
  for (const [key, value] of Object.entries(env)) {
    const lower = key.toLowerCase()
    if (scrub.has(lower) || prefixes.some((prefix) => lower.startsWith(prefix))) continue
    if (value !== undefined) result[key] = value
  }
  return result
}

export function userToolEnv(
  env: NodeJS.ProcessEnv = process.env,
  ...overrides: Array<NodeJS.ProcessEnv | Record<string, string | undefined> | undefined>
): Record<string, string> {
  const result = scrubUserToolEnv(env)
  for (const override of overrides) {
    if (override) Object.assign(result, override)
  }
  return scrubUserToolEnv(result)
}
