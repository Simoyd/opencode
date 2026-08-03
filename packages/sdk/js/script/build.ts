#!/usr/bin/env bun
import { fileURLToPath } from "url"

const dir = fileURLToPath(new URL("..", import.meta.url))
process.chdir(dir)

import { $ } from "bun"
import path from "path"

import { createClient } from "@hey-api/openapi-ts"

const opencode = path.resolve(dir, "../../opencode")

await $`bun dev generate > ${dir}/openapi.json`.cwd(opencode)

await createClient({
  input: "./openapi.json",
  output: {
    path: "./src/v2/gen",
    tsConfigPath: path.join(dir, "tsconfig.json"),
    clean: true,
  },
  plugins: [
    {
      name: "@hey-api/typescript",
      exportFromIndex: false,
    },
    {
      name: "@hey-api/sdk",
      instance: "OpencodeClient",
      exportFromIndex: false,
      auth: false,
      paramsStructure: "flat",
    },
    {
      name: "@hey-api/client-fetch",
      exportFromIndex: false,
      baseUrl: "http://localhost:4096",
    },
  ],
})

// Patch a @hey-api/openapi-ts codegen bug: SseFn incorrectly passes the
// endpoint's TError into the second generic of ServerSentEventsResult, which
// is the AsyncGenerator's TReturn slot. Iterator return values have nothing
// to do with HTTP errors, and any consumer that calls `.return()` or returns
// from a mock generator gets type-checked against the wrong shape. Drop the
// arg so TReturn defaults to void.
const sseTypesPath = "./src/v2/gen/client/types.gen.ts"
const sseTypesFile = Bun.file(sseTypesPath)
const sseTypesSource = await sseTypesFile.text()
const sseTypesPatched = sseTypesSource.replace(
  "=> Promise<ServerSentEventsResult<TData, TError>>",
  "=> Promise<ServerSentEventsResult<TData>>",
)
if (sseTypesPatched === sseTypesSource) {
  throw new Error(`SseFn patch did not apply; @hey-api/openapi-ts output may have changed (${sseTypesPath})`)
}
await Bun.write(sseTypesPath, sseTypesPatched)

const generatedTypes = await Bun.file("./src/v2/gen/types.gen.ts").text()
const provenanceStart = generatedTypes.indexOf("export type ContinuityProvenance =")
const provenanceEnd = generatedTypes.indexOf("export type TextPart =", provenanceStart)
if (provenanceStart < 0 || provenanceEnd < 0) {
  throw new Error("Generated ContinuityProvenance read contract is missing")
}

const legacyTypesPath = "./src/gen/types.gen.ts"
let legacyTypes = await Bun.file(legacyTypesPath).text()
const legacyProvenanceStart = legacyTypes.indexOf("export type ContinuityProvenance =")
if (legacyProvenanceStart < 0) {
  const insertion = legacyTypes.indexOf("export type TextPart =")
  if (insertion < 0) throw new Error("Legacy generated TextPart read contract is missing")
  legacyTypes =
    legacyTypes.slice(0, insertion) +
    generatedTypes.slice(provenanceStart, provenanceEnd) +
    legacyTypes.slice(insertion)
} else {
  const legacyProvenanceEnd = legacyTypes.indexOf("export type TextPart =", legacyProvenanceStart)
  if (legacyProvenanceEnd < 0) throw new Error("Legacy generated ContinuityProvenance block is unbounded")
  legacyTypes =
    legacyTypes.slice(0, legacyProvenanceStart) +
    generatedTypes.slice(provenanceStart, provenanceEnd) +
    legacyTypes.slice(legacyProvenanceEnd)
}

for (const [name, next] of [
  ["TextPart", "ReasoningPart"],
  ["ToolPart", "StepStartPart"],
] as const) {
  const start = legacyTypes.indexOf(`export type ${name} =`)
  const end = legacyTypes.indexOf(`export type ${next} =`, start)
  if (start < 0 || end < 0) throw new Error(`Legacy generated ${name} read contract is missing`)
  const block = legacyTypes.slice(start, end).replaceAll("\n  serverProvenance?: ContinuityProvenance", "")
  const close = block.lastIndexOf("\n}")
  if (close < 0) throw new Error(`Legacy generated ${name} read contract has an unexpected shape`)
  legacyTypes =
    legacyTypes.slice(0, start) +
    block.slice(0, close) +
    "\n  serverProvenance?: ContinuityProvenance" +
    block.slice(close) +
    legacyTypes.slice(end)
}
if (
  legacyTypes.indexOf("export type ContinuityProvenance =") !==
  legacyTypes.lastIndexOf("export type ContinuityProvenance =")
) {
  throw new Error("Legacy generated ContinuityProvenance block is duplicated")
}
await Bun.write(legacyTypesPath, legacyTypes)

await $`bun prettier --write src/gen`
await $`bun prettier --write src/v2`
await $`rm -rf dist`
await $`bun tsc`
await $`rm openapi.json`
