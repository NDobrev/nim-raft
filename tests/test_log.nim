import unittest

import ../src/raft/log
import ../src/raft

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
