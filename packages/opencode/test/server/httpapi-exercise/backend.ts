import { ConfigProvider, Effect, Layer } from "effect"
import { HttpRouter } from "effect/unstable/http"
import { parse } from "./assertions"
import { exerciseAuthProbeDirectory } from "./environment"
import { runtime, type Runtime } from "./runtime"
import type { ActiveScenario, BackendApp, CallResult, CaptureMode, RequestSpec, SeededContext } from "./types"

type CallOptions = {
  auth?: {
    password?: string
    username?: string
  }
}

export function call(scenario: ActiveScenario, ctx: SeededContext<unknown>, options: CallOptions = {}) {
  return Effect.promise(async () =>
    capture(await app(await runtime(), options).request(toRequest(scenario, ctx)), scenario.capture),
  )
}

export function callAuthProbe(
  scenario: ActiveScenario,
  credentials: "missing" | "valid" = "missing",
  request?: (input: Request) => Response | Promise<Response>,
) {
  return Effect.promise(async () => {
    if (credentials === "valid" && scenario.method === "PATCH") {
      const fs = await import("fs/promises")
      await fs.mkdir(exerciseAuthProbeDirectory, { recursive: true })
    }
    const controller = new AbortController()
    const send =
      request ?? (async (input: Request) => app(await runtime(), { auth: { password: "secret" } }).request(input))
    const pending = Promise.resolve(send(toAuthProbeRequest(scenario, credentials, controller.signal))).then(
      (response) => capture(response, scenario.capture),
    )
    let timer: ReturnType<typeof setTimeout> | undefined
    const timeout = new Promise<"timeout">((resolve) => {
      timer = setTimeout(() => {
        controller.abort("auth probe timed out")
        resolve("timeout")
      }, 1_000)
    })
    try {
      const result = await Promise.race([pending, timeout])
      if (result !== "timeout") return result
      await pending.catch((error: unknown) => {
        if (!controller.signal.aborted) throw error
      })
      return {
        status: 0,
        contentType: "",
        text: "auth probe timed out",
        body: undefined,
        timedOut: true,
      }
    } finally {
      if (timer) clearTimeout(timer)
    }
  })
}

type CachedApp = BackendApp & { readonly dispose: () => Promise<void> }

const appCache: Partial<Record<string, CachedApp>> = {}

export async function disposeApps() {
  const apps = Object.values(appCache)
  for (const key of Object.keys(appCache)) delete appCache[key]
  await Promise.all(apps.flatMap((app) => (app === undefined ? [] : [app.dispose()])))
}

function app(modules: Runtime, options: CallOptions) {
  const username = options.auth?.username
  const password = options.auth?.password
  const cacheKey = `${username ?? ""}:${password ?? ""}`
  if (appCache[cacheKey]) return appCache[cacheKey]

  const web = HttpRouter.toWebHandler(
    modules.HttpApiApp.routes.pipe(
      Layer.provide(
        ConfigProvider.layer(
          ConfigProvider.fromUnknown({ OPENCODE_SERVER_PASSWORD: password, OPENCODE_SERVER_USERNAME: username }),
        ),
      ),
    ),
    { disableLogger: true, memoMap: modules.memoMap },
  )
  return (appCache[cacheKey] = {
    dispose: web.dispose,
    request(input: string | URL | Request, init?: RequestInit) {
      return web.handler(
        input instanceof Request ? input : new Request(new URL(input, "http://localhost"), init),
        modules.HttpApiApp.context,
      )
    },
  })
}

function toRequest(scenario: ActiveScenario, ctx: SeededContext<unknown>) {
  const spec = scenario.request(ctx, ctx.state)
  return new Request(new URL(spec.path, "http://localhost"), {
    method: scenario.method,
    headers: spec.body === undefined ? spec.headers : { "content-type": "application/json", ...spec.headers },
    body: spec.body === undefined ? undefined : JSON.stringify(spec.body),
  })
}

function toAuthProbeRequest(scenario: ActiveScenario, credentials: "missing" | "valid", signal: AbortSignal) {
  const spec: RequestSpec =
    credentials === "valid"
      ? (scenario.authProbe ?? validAuthProbe(scenario.method))
      : {
          path: authProbePath(scenario.path),
          body: scenario.method === "GET" ? undefined : {},
        }
  const headers = {
    ...(spec.body === undefined ? {} : { "content-type": "application/json" }),
    ...(credentials === "valid" && scenario.method === "PATCH"
      ? { "x-opencode-directory": exerciseAuthProbeDirectory }
      : {}),
    ...spec.headers,
    ...(credentials === "valid" ? { authorization: basic("opencode", "secret") } : {}),
  }
  return new Request(new URL(spec.path, "http://localhost"), {
    method: scenario.method,
    headers,
    body: spec.body === undefined ? undefined : JSON.stringify(spec.body),
    signal,
  })
}

function validAuthProbe(method: ActiveScenario["method"]): { path: string; body?: unknown } {
  if (method === "GET") return { path: "/path" }
  if (method === "POST") return { path: "/log", body: {} }
  if (method === "PATCH") return { path: "/config", body: {} }
  if (method === "PUT") return { path: "/auth/auth_probe", body: {} }
  return { path: "/auth/auth_probe" }
}

function basic(username: string, password: string) {
  return `Basic ${Buffer.from(`${username}:${password}`).toString("base64")}`
}

function authProbePath(path: string) {
  return path
    .replace(/\{([^}]+)\}/g, (_match, key: string) => `auth_${key}`)
    .replace(/:([^/]+)/g, (_match, key: string) => `auth_${key}`)
}

async function capture(response: Response, mode: CaptureMode): Promise<CallResult> {
  const text = mode === "stream" ? await captureStream(response) : await response.text()
  return {
    status: response.status,
    contentType: response.headers.get("content-type") ?? "",
    text,
    body: parse(text),
    timedOut: false,
  }
}

async function captureStream(response: Response) {
  if (!response.body) return ""
  const reader = response.body.getReader()
  const read = reader.read().then(
    (result) => ({ result }),
    (error: unknown) => ({ error }),
  )
  const winner = await Promise.race([read, Bun.sleep(1_000).then(() => ({ timeout: true }))])
  if ("timeout" in winner) {
    await reader.cancel("timed out waiting for stream chunk").catch(() => undefined)
    throw new Error("timed out waiting for stream chunk")
  }
  if ("error" in winner) throw winner.error
  await reader.cancel().catch(() => undefined)
  if (winner.result.done) return ""
  return new TextDecoder().decode(winner.result.value)
}
