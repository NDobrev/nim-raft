# nim-raft
# Copyright (c) 2023 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

import std/options
import unittest

import ../src/raft/ssz
import ../src/raft/log
import ../src/raft/types
import ../src/raft/consensus_state_machine

suite "SSZ AppendEntries":
  test "AppendEntries request round-trip without entries":
    let original = RaftRpcAppendRequest(
      previousTerm: RaftNodeTerm(4),
      previousLogIndex: RaftLogIndex(7),
      commitIndex: RaftLogIndex(5),
      entries: @[],
    )

    let encoded = original.toSsz()
    let decoded = RaftRpcAppendRequest.fromSsz(encoded)

    check decoded.previousTerm == original.previousTerm
    check decoded.previousLogIndex == original.previousLogIndex
    check decoded.commitIndex == original.commitIndex
    check decoded.entries.len == 0

  test "AppendEntries request round-trip with single command entry":
    let entry = LogEntry(
      term: RaftNodeTerm(9),
      index: RaftLogIndex(11),
      kind: RaftLogEntryType.rletCommand,
      command: Command(data: @[1'u8, 2'u8, 3'u8]),
    )
    let original = RaftRpcAppendRequest(
      previousTerm: RaftNodeTerm(8),
      previousLogIndex: RaftLogIndex(10),
      commitIndex: RaftLogIndex(9),
      entries: @[entry],
    )

    let encoded = original.toSsz()
    let decoded = RaftRpcAppendRequest.fromSsz(encoded)

    check decoded.previousTerm == original.previousTerm
    check decoded.previousLogIndex == original.previousLogIndex
    check decoded.commitIndex == original.commitIndex
    check decoded.entries.len == 1
    let decodedEntry = decoded.entries[0]
    check decodedEntry.term == entry.term
    check decodedEntry.index == entry.index
    check decodedEntry.kind == entry.kind
    check decodedEntry.command.data == entry.command.data

  test "AppendEntries request round-trip with two empty entries":
    let entries = @[
      LogEntry(term: 1, index: 1, kind: RaftLogEntryType.rletEmpty),
      LogEntry(term: 1, index: 2, kind: RaftLogEntryType.rletEmpty),
    ]
    let request = RaftRpcAppendRequest(
      previousTerm: RaftNodeTerm(1),
      previousLogIndex: RaftLogIndex(1),
      commitIndex: RaftLogIndex(1),
      entries: entries,
    )

    let encoded = request.toSsz()
    let decoded = RaftRpcAppendRequest.fromSsz(encoded)

    check decoded.entries.len == 2
    for i in 0..<entries.len:
      check decoded.entries[i].term == entries[i].term
      check decoded.entries[i].index == entries[i].index
      check decoded.entries[i].kind == entries[i].kind

  test "AppendEntries request encoding rejects config entry":
    let request = RaftRpcAppendRequest(
      previousTerm: RaftNodeTerm(1),
      previousLogIndex: RaftLogIndex(1),
      commitIndex: RaftLogIndex(1),
      entries: @[
        LogEntry(
          term: RaftNodeTerm(1),
          index: RaftLogIndex(2),
          kind: RaftLogEntryType.rletConfig,
          config: RaftConfig(),
        ),
      ],
    )

    expect SszError:
      discard request.toSsz()

  test "AppendEntries reply round-trip accepted":
    let original = RaftRpcAppendReply(
      commitIndex: RaftLogIndex(12),
      term: RaftNodeTerm(6),
      result: RaftRpcCode.Accepted,
      accepted: RaftRpcAppendReplyAccepted(lastNewIndex: RaftLogIndex(15)),
    )

    let encoded = original.toSsz()
    let decoded = RaftRpcAppendReply.fromSsz(encoded)

    check decoded.commitIndex == original.commitIndex
    check decoded.term == original.term
    check decoded.result == original.result
    check decoded.accepted.lastNewIndex == original.accepted.lastNewIndex

  test "AppendEntries reply round-trip rejected with conflict":
    let original = RaftRpcAppendReply(
      commitIndex: RaftLogIndex(3),
      term: RaftNodeTerm(4),
      result: RaftRpcCode.Rejected,
      rejected: RaftRpcAppendReplyRejected(
        nonMatchingIndex: RaftLogIndex(2),
        lastIdx: RaftLogIndex(9),
        conflictTerm: some(RaftNodeTerm(5)),
        conflictIndex: RaftLogIndex(8),
      ),
    )

    let encoded = original.toSsz()
    let decoded = RaftRpcAppendReply.fromSsz(encoded)

    check decoded.commitIndex == original.commitIndex
    check decoded.term == original.term
    check decoded.result == original.result
    check decoded.rejected.nonMatchingIndex == original.rejected.nonMatchingIndex
    check decoded.rejected.lastIdx == original.rejected.lastIdx
    check decoded.rejected.conflictTerm == original.rejected.conflictTerm
    check decoded.rejected.conflictIndex == original.rejected.conflictIndex

  test "AppendEntries reply decoding rejects invalid result":
    let original = RaftRpcAppendReply(
      commitIndex: RaftLogIndex(1),
      term: RaftNodeTerm(1),
      result: RaftRpcCode.Accepted,
      accepted: RaftRpcAppendReplyAccepted(lastNewIndex: RaftLogIndex(1)),
    )

    var encoded = original.toSsz()
    doAssert encoded.len > 0
    encoded.setLen(encoded.len - 1)

    expect SszError:
      discard RaftRpcAppendReply.fromSsz(encoded)
