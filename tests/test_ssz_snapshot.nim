# nim-raft
# Copyright (c) 2023 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

import unittest

import ../src/raft/ssz
import ../src/raft/log
import ../src/raft/types
import ../src/raft/consensus_state_machine

suite "SSZ InstallSnapshot":
  test "InstallSnapshot round-trip":
    let snapshot = RaftSnapshot(
      index: RaftLogIndex(123),
      term: RaftNodeTerm(7),
      config: RaftConfig(),
    )
    let original = RaftInstallSnapshot(term: RaftNodeTerm(9), snapshot: snapshot)

    let encoded = encodeInstallSnapshot(original)
    let decoded = decodeInstallSnapshot(encoded)

    check decoded.term == original.term
    check decoded.snapshot.index == snapshot.index
    check decoded.snapshot.term == snapshot.term

  test "SnapshotReply round-trip":
    let original = RaftSnapshotReply(term: RaftNodeTerm(5), success: true)

    let encoded = encodeSnapshotReply(original)
    let decoded = decodeSnapshotReply(encoded)

    check decoded == original

  test "InstallSnapshot through message envelope":
    let snapshot = RaftSnapshot(
      index: RaftLogIndex(42),
      term: RaftNodeTerm(8),
      config: RaftConfig(),
    )
    let original = RaftRpcMessage(
      currentTerm: RaftNodeTerm(11),
      sender: RaftNodeId(id: "nodeA"),
      receiver: RaftNodeId(id: "nodeB"),
      kind: RaftRpcMessageType.InstallSnapshot,
      installSnapshot: RaftInstallSnapshot(term: RaftNodeTerm(9), snapshot: snapshot),
    )

    let encoded = encodeRpcMessage(original)
    let decoded = decodeRpcMessage(encoded)

    check decoded.kind == original.kind
    check decoded.installSnapshot.term == original.installSnapshot.term
    check decoded.installSnapshot.snapshot.index == snapshot.index

  test "SnapshotReply through message envelope":
    let original = RaftRpcMessage(
      currentTerm: RaftNodeTerm(3),
      sender: RaftNodeId(id: "nodeC"),
      receiver: RaftNodeId(id: "nodeD"),
      kind: RaftRpcMessageType.SnapshotReply,
      snapshotReply: RaftSnapshotReply(term: RaftNodeTerm(4), success: false),
    )

    let encoded = encodeRpcMessage(original)
    let decoded = decodeRpcMessage(encoded)

    check decoded.kind == original.kind
    check decoded.snapshotReply == original.snapshotReply
