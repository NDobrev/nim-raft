## SimClock - Deterministic virtual time for simulation
##
## Provides a virtual clock that is manually advanced, ensuring deterministic
## execution. Manages unified event queue for timers, network events, and lifecycle events.

import std/heapqueue
import std/options
import std/hashes

import types
import ../../src/raft/types

# TimerId and TimerCallback are defined in types.nim

type
  SimClock* = ref object
    nowMs*: int64
    nextTimerId*: TimerId
    events*: HeapQueue[SimEvent]  # unified event queue
    firedCount*: uint64  # for debugging/metrics

proc newSimClock*(): SimClock =
  ## Create a new SimClock starting at time 0
  SimClock(
    nowMs: 0,
    nextTimerId: TimerId(1),
    events: initHeapQueue[SimEvent](),
    firedCount: 0
  )

proc scheduleTimer*(clock: SimClock, delayMs: int64, callback: TimerCallback,
                   kind: TimerKind = CustomTimer, nodeId: RaftNodeId = newRaftNodeId(""),
                   periodic: bool = false, interval: int64 = 0): TimerId =
  ## Schedule a timer to fire after delayMs milliseconds
  let deadline = clock.nowMs + delayMs
  let id = clock.nextTimerId
  clock.nextTimerId = TimerId(clock.nextTimerId.uint64 + 1)

  let timerData = TimerEventData(
    id: id,
    callback: callback,
    kind: kind,
    generation: 0,  # Not used for now
    cancelled: false,
    periodic: periodic,
    interval: interval,
    nodeId: nodeId
  )

  let event = SimEvent(
    deliverAt: deadline,
    kind: TimerEvent,
    timer: timerData
  )

  clock.events.push(event)
  return id

proc `==`*(a, b: TimerId): bool = a.uint64 == b.uint64
proc `!=`*(a, b: TimerId): bool = a.uint64 != b.uint64
proc hash*(x: TimerId): Hash = hash(x.uint64)

proc cancelTimer*(clock: SimClock, id: TimerId): bool =
  ## Cancel a timer by ID. Returns true if timer was found and cancelled.
  # Find and mark the timer as cancelled in the event queue
  var found = false
  var tempEvents = initHeapQueue[SimEvent]()

  while clock.events.len > 0:
    let event = clock.events.pop()
    if event.kind == TimerEvent and event.timer.id == id:
      # Mark as cancelled instead of removing
      var cancelledEvent = event
      cancelledEvent.timer.cancelled = true
      tempEvents.push(cancelledEvent)
      found = true
    else:
      tempEvents.push(event)

  clock.events = tempEvents
  return found

proc scheduleEvent*(clock: SimClock, event: SimEvent) =
  ## Schedule a general simulation event
  clock.events.push(event)

proc tick*(clock: SimClock, dtMs: int64 = 1, deliverCallback: proc(event: SimEvent) = nil) =
  ## Advance time by dtMs milliseconds and process any due events
  let targetTime = clock.nowMs + dtMs

  while clock.events.len > 0 and clock.events[0].deliverAt <= targetTime:
    let event = clock.events.pop()
    clock.nowMs = event.deliverAt

    # Process the event based on its type
    case event.kind:
    of TimerEvent:
      if not event.timer.cancelled:
        # Fire the timer (basic cancellation support)
        event.timer.callback(event.timer.id)
        clock.firedCount += 1

        # Reschedule periodic timers
        if event.timer.periodic:
          let newDeadline = clock.nowMs + event.timer.interval
          let newTimerData = TimerEventData(
            id: event.timer.id,
            callback: event.timer.callback,
            kind: event.timer.kind,
            generation: event.timer.generation,
            cancelled: false,
            periodic: true,
            interval: event.timer.interval,
            nodeId: event.timer.nodeId
          )
          let newEvent = SimEvent(
            deliverAt: newDeadline,
            kind: TimerEvent,
            timer: newTimerData
          )
          clock.events.push(newEvent)
    of NetworkEvent, LifecycleEvt:
      # Deliver to callback for processing (if provided)
      if deliverCallback != nil:
        deliverCallback(event)

  # Periodic cleanup of cancelled events to maintain performance
  const cleanupThreshold = 1000  # Clean up when queue has 1000+ events
  const cleanupRatio = 0.5       # Clean up if 50% of events are cancelled

  if clock.events.len > cleanupThreshold:
    var cancelledCount = 0
    for event in clock.events:
      if event.kind == TimerEvent and event.timer.cancelled:
        cancelledCount += 1

    if cancelledCount.float / clock.events.len.float > cleanupRatio:
      # Rebuild heap without cancelled events
      var activeEvents = newSeq[SimEvent]()
      for event in clock.events:
        if not (event.kind == TimerEvent and event.timer.cancelled):
          activeEvents.add(event)

      # Rebuild the heap
      clock.events = initHeapQueue[SimEvent]()
      for event in activeEvents:
        clock.events.push(event)

  # Advance to target time if no events fired or after processing events
  clock.nowMs = targetTime

proc nextEventDeadline*(clock: SimClock): Option[int64] =
  ## Return the deadline of the next event, or none if no events scheduled
  if clock.events.len == 0:
    return none(int64)
  return some(clock.events[0].deliverAt)

proc timeUntilNextEvent*(clock: SimClock): Option[int64] =
  ## Return milliseconds until next event fires, or none if no events
  let nextDeadline = clock.nextEventDeadline()
  if nextDeadline.isNone:
    return none(int64)
  return some(nextDeadline.get() - clock.nowMs)

# Backward compatibility aliases
proc nextTimerDeadline*(clock: SimClock): Option[int64] = clock.nextEventDeadline()
proc timeUntilNextTimer*(clock: SimClock): Option[int64] = clock.timeUntilNextEvent()
