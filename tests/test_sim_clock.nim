## Unit tests for SimClock

import std/unittest
import std/options
import ../sim/core/sim_clock
import ../sim/core/types

suite "SimClock":
  test "new clock starts at time 0":
    let clock = newSimClock()
    check clock.nowMs == 0

  test "tick advances time":
    let clock = newSimClock()
    clock.tick(100)
    check clock.nowMs == 100

    clock.tick(50)
    check clock.nowMs == 150

  test "scheduleTimer fires at correct time":
    let clock = newSimClock()
    var fired = false

    let timerId = clock.scheduleTimer(50, proc(id: TimerId) {.gcsafe.} =
      fired = true
    )

    # Timer shouldn't fire before deadline
    clock.tick(49)
    check not fired
    check clock.nowMs == 49

    # Timer should fire at deadline
    clock.tick(1)
    check fired
    check clock.nowMs == 50

  test "cancelTimer prevents firing":
    let clock = newSimClock()
    var fired = false

    let timerId = clock.scheduleTimer(50, proc(id: TimerId) {.gcsafe.} =
      fired = true
    )

    check clock.cancelTimer(timerId)
    clock.tick(100)
    check not fired

  test "cancelTimer returns false for non-existent timer":
    let clock = newSimClock()
    check not clock.cancelTimer(TimerId(999))

  test "multiple timers fire in order":
    let clock = newSimClock()
    var order: seq[int] = @[]

    # Use a shared counter approach
    var counter = 0
    discard clock.scheduleTimer(30, proc(id: TimerId) {.gcsafe.} =
      counter += 30)
    discard clock.scheduleTimer(10, proc(id: TimerId) {.gcsafe.} =
      counter += 10)
    discard clock.scheduleTimer(20, proc(id: TimerId) {.gcsafe.} =
      counter += 20)

    clock.tick(50)
    # Should be 10 + 20 + 30 = 60
    check counter == 60

  test "periodic timers reschedule":
    let clock = newSimClock()
    var fireCount = 0

    discard clock.scheduleTimer(10, proc(id: TimerId) {.gcsafe.} =
      fireCount.inc()
    , periodic = true, interval = 10)

    clock.tick(5)
    check fireCount == 0

    clock.tick(5)  # t=10, first fire
    check fireCount == 1

    clock.tick(10) # t=20, second fire
    check fireCount == 2

    clock.tick(10) # t=30, third fire
    check fireCount == 3

  test "nextTimerDeadline returns earliest timer":
    let clock = newSimClock()

    discard clock.scheduleTimer(50, proc(id: TimerId) {.gcsafe.} = discard)
    discard clock.scheduleTimer(30, proc(id: TimerId) {.gcsafe.} = discard)
    discard clock.scheduleTimer(70, proc(id: TimerId) {.gcsafe.} = discard)

    let next = clock.nextTimerDeadline()
    check next != none(int64)
    check next.get() == 30

  test "timeUntilNextTimer calculates correctly":
    let clock = newSimClock()
    clock.tick(10)

    discard clock.scheduleTimer(40, proc(id: TimerId) {.gcsafe.} = discard)  # fires at t=50

    let until = clock.timeUntilNextTimer()
    check until != none(int64)
    check until.get() == 40  # 50 - 10 = 40
