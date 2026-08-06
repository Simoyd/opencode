export * as ServerEvent from "./server-event"

import { Event } from "./event"

export const Connected = Event.defineEmpty({ type: "server.connected" })
export const Heartbeat = Event.defineEmpty({ type: "server.heartbeat" })
export const Disposed = Event.defineEmpty({ type: "global.disposed" })

export const Definitions = Event.inventory(Connected, Heartbeat, Disposed)
