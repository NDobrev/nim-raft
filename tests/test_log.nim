import unittest

import ../src/raft/log
import ../src/raft
import std/options

suite "RaftLog Tests":
  test "RaftLog.init with non-empty entries should initialize RaftLog correctly":
    var snapshot =
      RaftSnapshot(index: RaftLogIndex(5), term: RaftNodeTerm(1), config: RaftConfig())

    var entries =
      @[
        LogEntry(
          term: RaftNodeTerm(1),
          index: RaftLogIndex(6),
          kind: RaftLogEntryType.rletCommand,
          command: Command(data: @[]),
        ),
        LogEntry(
          term: RaftNodeTerm(1),
          index: RaftLogIndex(7),
          kind: RaftLogEntryType.rletConfig,
          config: RaftConfig(),
        ),
        LogEntry(
          term: RaftNodeTerm(1),
          index: RaftLogIndex(8),
          kind: RaftLogEntryType.rletCommand,
          command: Command(data: @[1, 2, 3]),
        ),
      ]

    # Call the function under test
    var log = RaftLog.init(snapshot, entries)

    check log.lastConfigIndex == RaftLogIndex(7)
    check log.prevConfigIndex == RaftLogIndex(0)
    check log.entriesCount == 3

  test "RaftLog.init with non-empty entries and multiple config entries should initialize RaftLog correctly":
    var snapshot =
      RaftSnapshot(index: RaftLogIndex(5), term: RaftNodeTerm(1), config: RaftConfig())

    var entries =
      @[
        LogEntry(
          term: RaftNodeTerm(1),
          index: RaftLogIndex(6),
          kind: RaftLogEntryType.rletCommand,
          command: Command(data: @[]),
        ),
        LogEntry(
          term: RaftNodeTerm(1),
          index: RaftLogIndex(7),
          kind: RaftLogEntryType.rletConfig,
          config: RaftConfig(),
        ),
        LogEntry(
          term: RaftNodeTerm(1),
          index: RaftLogIndex(8),
          kind: RaftLogEntryType.rletCommand,
          command: Command(data: @[1, 2, 3]),
        ),
        LogEntry(
          term: RaftNodeTerm(1),
          index: RaftLogIndex(9),
          kind: RaftLogEntryType.rletConfig,
          config: RaftConfig(),
        ),
        LogEntry(
          term: RaftNodeTerm(1),
          index: RaftLogIndex(10),
          kind: RaftLogEntryType.rletCommand,
          command: Command(data: @[4, 5, 6]),
        ),
      ]

    var log = RaftLog.init(snapshot, entries)

    check log.lastConfigIndex == RaftLogIndex(9)
    check log.prevConfigIndex == RaftLogIndex(7)
    check log.entriesCount == 5


  test "appendAsLeader rejects out-of-order indices":
    var snapshot =
      RaftSnapshot(index: RaftLogIndex(0), term: RaftNodeTerm(1), config: RaftConfig())
    var log = RaftLog.init(snapshot)
    log.appendAsLeader(
      term = RaftNodeTerm(1),
      index = RaftLogIndex(1),
      data = Command(data: @[]),
    )
    expect AssertionError:
      log.appendAsLeader(
        term = RaftNodeTerm(1),
        index = RaftLogIndex(3),
        data = Command(data: @[]),
      )
    expect AssertionError:
      log.appendAsLeader(
        term = RaftNodeTerm(1),
        index = RaftLogIndex(1),
        data = Command(data: @[]),
      )

  test "appendAsLeader accepts sequential indices":
    var snapshot =
      RaftSnapshot(index: RaftLogIndex(0), term: RaftNodeTerm(1), config: RaftConfig())
    var log = RaftLog.init(snapshot)
    log.appendAsLeader(
      term = RaftNodeTerm(1),
      index = RaftLogIndex(1),
      data = Command(data: @[]),
    )
    log.appendAsLeader(
      term = RaftNodeTerm(1),
      index = RaftLogIndex(2),
      data = Command(data: @[]),
    )
    check log.lastIndex == RaftLogIndex(2)
    check log.entriesCount == 2

  test "checkInvariant validates sequential log and config indices":
    var snapshot =
      RaftSnapshot(index: RaftLogIndex(0), term: RaftNodeTerm(1), config: RaftConfig())
    var log = RaftLog.init(snapshot)
    log.appendAsLeader(
      term = RaftNodeTerm(1),
      index = RaftLogIndex(1),
      data = Command(data: @[]),
    )
    log.appendAsLeader(
      term = RaftNodeTerm(1),
      index = RaftLogIndex(2),
      data = Command(data: @[]),
    )
    log.checkInvariant()

  test "checkInvariant detects gaps":
    var snapshot =
      RaftSnapshot(index: RaftLogIndex(0), term: RaftNodeTerm(1), config: RaftConfig())
    var entries = @[
      LogEntry(
        term: RaftNodeTerm(1),
        index: RaftLogIndex(1),
        kind: RaftLogEntryType.rletCommand,
        command: Command(data: @[]),
      ),
      LogEntry(
        term: RaftNodeTerm(1),
        index: RaftLogIndex(3),
        kind: RaftLogEntryType.rletCommand,
        command: Command(data: @[]),
      ),
    ]
    var log = RaftLog.init(snapshot, entries)
    expect AssertionError:
      log.checkInvariant()

  test "lastIndexOfTerm finds indices and considers snapshot":
    var log = RaftLog.init(RaftSnapshot(index: 0, term: 0, config: RaftConfig()))
    log.appendAsLeader(term = 1, index = 1, data = Command(data: @[]))
    log.appendAsLeader(term = 1, index = 2, data = Command(data: @[]))
    log.appendAsLeader(term = 2, index = 3, data = Command(data: @[]))
    log.appendAsLeader(term = 2, index = 4, data = Command(data: @[]))
    check log.lastIndexOfTerm(2).isSome and log.lastIndexOfTerm(2).get() == 4
    check log.lastIndexOfTerm(1).isSome and log.lastIndexOfTerm(1).get() == 2
    check not log.lastIndexOfTerm(3).isSome

    var log2 = RaftLog.init(RaftSnapshot(index: 5, term: 7, config: RaftConfig()))
    check log2.lastIndexOfTerm(7).isSome and log2.lastIndexOfTerm(7).get() == 5
    check not log2.lastIndexOfTerm(1).isSome

  test "appendAsFollower ignores index < firstIndex and > nextIndex":
    var log = RaftLog.init(RaftSnapshot(index: 2, term: 1, config: RaftConfig()))
    # nextIndex is 3
    log.appendAsFollower(LogEntry(term: 1, index: 1, kind: rletEmpty))
    check log.lastIndex == 2 and log.entriesCount == 0
    log.appendAsFollower(LogEntry(term: 1, index: 4, kind: rletEmpty))
    check log.lastIndex == 2 and log.entriesCount == 0

  test "matchTerm returns match for index 0":
    var log = RaftLog.init(RaftSnapshot(index: 0, term: 0, config: RaftConfig()))
    let (reason, t) = log.matchTerm(0, 0)
    check reason == mtrMatch and t == 0

  test "matchTerm reports mismatched term":
    var log = RaftLog.init(RaftSnapshot(index: 0, term: 0, config: RaftConfig()))
    log.appendAsLeader(term = 1, index = 1, data = Command(data: @[]))
    let (reason, t) = log.matchTerm(1, 2)
    check reason == mtrTermMismatch and t == RaftNodeTerm(1)

  test "matchTerm reports missing index":
    var log = RaftLog.init(RaftSnapshot(index: 0, term: 0, config: RaftConfig()))
    let (reason, t) = log.matchTerm(5, 0)
    check reason == mtrIndexNotFound and t == 0
