import { afterAll, expect, test } from "bun:test"
import { Effect } from "effect"
import path from "path"
import { callAuthProbe } from "./httpapi-exercise/backend"
import { http } from "./httpapi-exercise/dsl"
import { cleanupExercisePaths, exerciseAuthProbeDirectory } from "./httpapi-exercise/environment"
import { runtime } from "./httpapi-exercise/runtime"

afterAll(async () => {
  await Effect.runPromise(cleanupExercisePaths)
})

test("auth probe aborts and settles a pending valid request", async () => {
  const scenario = http.protected
    .get("/auth-probe", "auth probe cancellation")
    .global()
    .probe({ path: "/pending-valid" })
    .ok()
  let active = 0
  let cancelled = false
  const paths = new Array<string>()
  const request = (input: Request) => {
    paths.push(new URL(input.url).pathname)
    if (!input.headers.has("authorization")) return Promise.resolve(new Response(undefined, { status: 401 }))
    active++
    return new Promise<Response>((_resolve, reject) => {
      input.signal.addEventListener(
        "abort",
        () => {
          cancelled = true
          active--
          reject(input.signal.reason)
        },
        { once: true },
      )
    })
  }

  const missing = await Effect.runPromise(callAuthProbe(scenario, "missing", request))
  const valid = await Effect.runPromise(callAuthProbe(scenario, "valid", request))

  expect(missing).toMatchObject({ status: 401, timedOut: false })
  expect(valid).toMatchObject({ status: 0, timedOut: true })
  expect(paths).toEqual(["/auth-probe", "/pending-valid"])
  expect(cancelled).toBeTrue()
  expect(active).toBe(0)
})

test("valid PATCH auth probes write config only under the exercise root", async () => {
  const scenario = http.protected.patch("/auth-probe", "auth PATCH isolation").ok()
  const packageConfig = path.resolve(import.meta.dir, "../../config.json")
  await runtime()

  const result = await Effect.runPromise(callAuthProbe(scenario, "valid"))

  expect(result).toMatchObject({ status: 200, timedOut: false })
  expect(await Bun.file(path.join(exerciseAuthProbeDirectory, "config.json")).exists()).toBeTrue()
  expect(await Bun.file(packageConfig).exists()).toBeFalse()
})
