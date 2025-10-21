## SimClock - Deterministic virtual time for simulation
##
## Provides a virtual clock that is manually advanced, ensuring deterministic
## execution. Manages timers with stable ordering for reproducible runs.

import std/heapqueue
import std/options

type
  TimerId* = distinct uint64
  TimerCallback* = proc(id: TimerId) {.gcsafe, closure.}

  Timer* = object
    id*: TimerId
    deadline*: int64  # milliseconds since sim start
    callback*: TimerCallback
    periodic*: bool   # if true, reschedule after firing
    interval*: int64  # for periodic timers

  SimClock* = ref object
    nowMs*: int64
    nextTimerId*: TimerId
    timers*: HeapQueue[Timer]
    firedCount*: uint64  # for debugging/metrics

proc `<`*(a, b: Timer): bool =
  ## Timer ordering: earlier deadline first, stable sort by ID on ties
  if a.deadline != b.deadline:
    return a.deadline < b.deadline
  return a.id.uint64 < b.id.uint64

proc newSimClock*(): SimClock =
  ## Create a new SimClock starting at time 0
  SimClock(
    nowMs: 0,
    nextTimerId: TimerId(1),
    timers: initHeapQueue[Timer](),
    firedCount: 0
  )

proc scheduleTimer*(clock: SimClock, delayMs: int64, callback: TimerCallback,
                   periodic: bool = false, interval: int64 = 0): TimerId =
  ## Schedule a timer to fire after delayMs milliseconds
  let deadline = clock.nowMs + delayMs
  let id = clock.nextTimerId
  clock.nextTimerId = TimerId(clock.nextTimerId.uint64 + 1)

  let timer = Timer(
    id: id,
    deadline: deadline,
    callback: callback,
    periodic: periodic,
    interval: interval
  )

  clock.timers.push(timer)
  return id

proc `==`*(a, b: TimerId): bool = a.uint64 == b.uint64
proc `!=`*(a, b: TimerId): bool = a.uint64 != b.uint64

proc cancelTimer*(clock: SimClock, id: TimerId): bool =
  ## Cancel a timer by ID. Returns true if timer was found and cancelled.
  var found = false
  var newTimers = initHeapQueue[Timer]()

  while clock.timers.len > 0:
    let timer = clock.timers.pop()
    if timer.id != id:
      newTimers.push(timer)
    else:
      found = true

  clock.timers = newTimers
  return found

proc tick*(clock: SimClock, dtMs: int64 = 1) =
  ## Advance time by dtMs milliseconds and fire any due timers
  let targetTime = clock.nowMs + dtMs

  while clock.timers.len > 0 and clock.timers[0].deadline <= targetTime:
    let timer = clock.timers.pop()
    clock.nowMs = timer.deadline

    # Fire the timer
    timer.callback(timer.id)
    clock.firedCount += 1

    # Reschedule if periodic
    if timer.periodic:
      let newDeadline = timer.deadline + timer.interval
      let newTimer = Timer(
        id: timer.id,
        deadline: newDeadline,
        callback: timer.callback,
        periodic: true,
        interval: timer.interval
      )
      clock.timers.push(newTimer)

  # Advance to target time if no timers fired or after processing timers
  clock.nowMs = targetTime

proc nextTimerDeadline*(clock: SimClock): Option[int64] =
  ## Return the deadline of the next timer, or none if no timers scheduled
  if clock.timers.len == 0:
    return none(int64)
  return some(clock.timers[0].deadline)

proc timeUntilNextTimer*(clock: SimClock): Option[int64] =
  ## Return milliseconds until next timer fires, or none if no timers
  let nextDeadline = clock.nextTimerDeadline()
  if nextDeadline.isNone:
    return none(int64)
  return some(nextDeadline.get() - clock.nowMs)
