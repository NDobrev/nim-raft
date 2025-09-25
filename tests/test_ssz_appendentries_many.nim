# nim-raft
# Copyright (c) 2023 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

import std/sequtils
import unittest

import ssz_serialization
import ../src/raft/ssz
import ../src/raft/log
import ../src/raft/types
import ../src/raft/consensus_state_machine

const
  sampleCounts = [0, 8, 64, maxAppendEntries]

proc buildCommandEntry(term: int, index: int, payloadLen: int): LogEntry =
  if payloadLen == 0:
    return LogEntry(
      term: RaftNodeTerm(term),
      index: RaftLogIndex(index),
      kind: RaftLogEntryType.rletEmpty,
    )

  var data = newSeq[byte](payloadLen)
  for i in 0..<payloadLen:
    data[i] = byte((i + term + index) mod 256)

  LogEntry(
    term: RaftNodeTerm(term),
    index: RaftLogIndex(index),
    kind: RaftLogEntryType.rletCommand,
    command: Command(data: data),
  )

proc checkSameEntry(actual, expected: LogEntry) =
  check actual.term == expected.term
  check actual.index == expected.index
  check actual.kind == expected.kind
  case expected.kind
  of RaftLogEntryType.rletCommand:
    check actual.command.data == expected.command.data
  of RaftLogEntryType.rletEmpty:
    discard
  of RaftLogEntryType.rletConfig:
    doAssert false, "Config entries are not supported in SSZ tests yet"

suite "SSZ AppendEntries many":
  for count in sampleCounts:
    test "AppendEntries round-trip with " & $count & " entries":
      var entries: seq[LogEntry] = @[]
      var totalBytes = 0
      for i in 0..<count:
        let payloadLen =
          if count == maxAppendEntries:
            0
          else:
            case i mod 4
            of 0: 0
            of 1: 1
            of 2: 32
            else: maxLogEntryPayloadBytes - 1
        let effectiveLen =
          if totalBytes + payloadLen > maxAppendEntriesBytes:
            0
          else:
            payloadLen
        let entry = buildCommandEntry(100 + i, 200 + i, effectiveLen)
        totalBytes += effectiveLen
        entries.add(entry)

      let request = RaftRpcAppendRequest(
        previousTerm: RaftNodeTerm(5),
        previousLogIndex: RaftLogIndex(10),
        commitIndex: RaftLogIndex(9),
        entries: entries,
      )

      let encoded = request.toSsz()
      let decoded = RaftRpcAppendRequest.fromSsz(encoded)

      check decoded.previousTerm == request.previousTerm
      check decoded.previousLogIndex == request.previousLogIndex
      check decoded.commitIndex == request.commitIndex
      check decoded.entries.len == entries.len
      for i in 0..<entries.len:
        decoded.entries[i].checkSameEntry(entries[i])

  test "AppendEntries encoding rejects too many entries":
    var entries = newSeqWith(maxAppendEntries + 1, buildCommandEntry(1, 1, 0))
    let request = RaftRpcAppendRequest(
      previousTerm: RaftNodeTerm(1),
      previousLogIndex: RaftLogIndex(1),
      commitIndex: RaftLogIndex(1),
      entries: entries,
    )

    expect SszError:
      discard request.toSsz()

  test "AppendEntries encoding rejects oversized payload":
    var entries: seq[LogEntry] = @[]
    for i in 0..<5:
      entries.add(buildCommandEntry(10 + i, 20 + i, maxLogEntryPayloadBytes))
    let request = RaftRpcAppendRequest(
      previousTerm: RaftNodeTerm(2),
      previousLogIndex: RaftLogIndex(3),
      commitIndex: RaftLogIndex(4),
      entries: entries,
    )

    expect SszError:
      discard request.toSsz()

  test "AppendEntries decode rejects oversized payload":
    var container = AppendRequestSsz(
      previousTerm: 1,
      previousLogIndex: 1,
      commitIndex: 1,
      entries: @[],
    )
    let oversizedEntry = LogEntrySsz(
      term: 1,
      index: 1,
      kind: uint8(RaftLogEntryType.rletCommand),
      commandData: newSeqWith(maxLogEntryPayloadBytes, byte(1)),
    )
    for _ in 0..<5:
      container.entries.add(oversizedEntry)

    let encoded = encode(SSZ, container)

    expect SszError:
      discard RaftRpcAppendRequest.fromSsz(encoded)
