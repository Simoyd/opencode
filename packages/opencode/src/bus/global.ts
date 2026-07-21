import { EventEmitter } from "events"
import { Identifier } from "@/id/id"
import { StreamDiagnostics } from "@/diagnostic/stream"
import { CompactionDiagnostics } from "@/diagnostic/compaction"
import { Log } from "@opencode-ai/core/util/log"

const log = Log.create({ service: "global-bus" })

export type GlobalEvent = {
  directory?: string
  project?: string
  workspace?: string
  payload: any
}

class GlobalBusEmitter extends EventEmitter<{
  event: [GlobalEvent]
}> {
  override emit(eventName: "event", event: GlobalEvent): boolean {
    if (event.payload && typeof event.payload === "object" && !("id" in event.payload)) {
      event.payload.id = event.payload.syncEvent?.id ?? Identifier.create("evt", "ascending")
    }
    StreamDiagnostics.record({
      stage: "global.bus",
      action: "emit",
      eventType: StreamDiagnostics.eventType(event),
      routeMode: "global",
      shape: StreamDiagnostics.shape(event),
      correlation: StreamDiagnostics.correlationForPayload(event),
    })
    const listeners = this.rawListeners(eventName)
    CompactionDiagnostics.recordPayload(event, "global.bus", "emit", { listeners: listeners.length })
    for (const listener of listeners) {
      try {
        listener.call(this, event)
      } catch (error) {
        log.error("global event observer failed", {
          eventType: StreamDiagnostics.eventType(event),
          error,
        })
      }
    }
    CompactionDiagnostics.recordPayload(event, "global.bus", "emitted", { listeners: listeners.length })
    return listeners.length > 0
  }
}

export const GlobalBus = new GlobalBusEmitter()
