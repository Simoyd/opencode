import { EventEmitter } from "events"
import { Identifier } from "@/id/id"
import { StreamDiagnostics } from "@/diagnostic/stream"

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
    return super.emit(eventName, event)
  }
}

export const GlobalBus = new GlobalBusEmitter()
