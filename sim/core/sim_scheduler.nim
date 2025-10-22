## SimScheduler - High-level scheduling utilities
##
## Provides convenient abstractions for periodic tasks, hooks, and
## event scheduling on top of the low-level SimClock timer system.

import std/options
import std/tables

import sim_clock
import types
import std/strformat

type
  SchedulerCallback* = proc() {.gcsafe, closure.}

  PeriodicTask* = object
    id*: TimerId
    callback*: SchedulerCallback
    interval*: int64
    active*: bool

  SimScheduler* = ref object
    clock*: SimClock
    periodicTasks*: Table[string, PeriodicTask]
    nextTaskId*: int

proc newSimScheduler*(clock: SimClock): SimScheduler =
  SimScheduler(
    clock: clock,
    periodicTasks: initTable[string, PeriodicTask](),
    nextTaskId: 0
  )

proc scheduleOnce*(scheduler: SimScheduler, delayMs: int64,
                  callback: SchedulerCallback): TimerId =
  ## Schedule a one-time callback after delayMs
  scheduler.clock.scheduleTimer(delayMs, proc(id: TimerId) = callback())

proc schedulePeriodic*(scheduler: SimScheduler, name: string, intervalMs: int64,
                      callback: SchedulerCallback): TimerId =
  ## Schedule a periodic task with the given name and interval
  ## If a task with this name already exists, it will be replaced
  if scheduler.periodicTasks.contains(name):
    # Cancel existing task
    let oldTask = scheduler.periodicTasks[name]
    discard scheduler.clock.cancelTimer(oldTask.id)

  # Create the periodic callback that executes the user callback and reschedules itself
  proc periodicCallback(id: TimerId) {.gcsafe.} =
    if scheduler.periodicTasks.contains(name):
      var task = scheduler.periodicTasks[name]
      if task.active:
        task.callback()

  let taskId = scheduler.clock.scheduleTimer(intervalMs, periodicCallback, periodic = true, interval = intervalMs)

  let task = PeriodicTask(
    id: taskId,
    callback: callback,
    interval: intervalMs,
    active: true
  )
  scheduler.periodicTasks[name] = task
  return taskId

proc cancelPeriodic*(scheduler: SimScheduler, name: string): bool =
  ## Cancel a periodic task by name. Returns true if task existed and was cancelled.
  if not scheduler.periodicTasks.contains(name):
    return false

  let task = scheduler.periodicTasks[name]
  let cancelled = scheduler.clock.cancelTimer(task.id)
  scheduler.periodicTasks.del(name)
  return cancelled

proc pausePeriodic*(scheduler: SimScheduler, name: string): bool =
  ## Pause a periodic task (stops firing but keeps it registered)
  if not scheduler.periodicTasks.contains(name):
    return false

  var task = scheduler.periodicTasks[name]
  if task.active:
    discard scheduler.clock.cancelTimer(task.id)
    task.active = false
    scheduler.periodicTasks[name] = task
  return true

proc resumePeriodic*(scheduler: SimScheduler, name: string): bool =
  ## Resume a paused periodic task
  if not scheduler.periodicTasks.contains(name):
    return false

  var task = scheduler.periodicTasks[name]
  if not task.active:
    task.id = scheduler.clock.scheduleTimer(task.interval, proc(id: TimerId) =
      if scheduler.periodicTasks.contains(name):
        var t = scheduler.periodicTasks[name]
        if t.active:
          t.callback()
          # Reschedule
          t.id = scheduler.clock.scheduleTimer(t.interval, proc(newId: TimerId) =
            if scheduler.periodicTasks.contains(name):
              var nt = scheduler.periodicTasks[name]
              nt.id = newId
              scheduler.periodicTasks[name] = nt
          , periodic = true, interval = t.interval)
          scheduler.periodicTasks[name] = t
    , periodic = true, interval = task.interval)
    task.active = true
    scheduler.periodicTasks[name] = task
  return true

proc hasPeriodic*(scheduler: SimScheduler, name: string): bool =
  ## Check if a periodic task with the given name exists
  scheduler.periodicTasks.contains(name)

proc isPeriodicActive*(scheduler: SimScheduler, name: string): bool =
  ## Check if a periodic task is currently active (not paused)
  if not scheduler.periodicTasks.contains(name):
    return false
  return scheduler.periodicTasks[name].active

proc nextEventTime*(scheduler: SimScheduler): Option[int64] =
  ## Get the time of the next scheduled event (timer or periodic task)
  scheduler.clock.nextTimerDeadline()

proc advanceToNext*(scheduler: SimScheduler): int64 =
  ## Advance clock to the next event time and return the delta
  let nextTime = scheduler.nextEventTime()
  if nextTime.isSome:
    let delta = nextTime.get() - scheduler.clock.nowMs
    scheduler.clock.tick(delta)
    return delta
  return 0

proc advanceBy*(scheduler: SimScheduler, deltaMs: int64) =
  ## Advance the scheduler by a specific amount of time
  scheduler.clock.tick(deltaMs)
