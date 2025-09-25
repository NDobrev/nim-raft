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
import ../src/raft/types
import ../src/raft/consensus_state_machine

suite "SSZ RequestVote":
  test "Request serialization round-trip preserves fields":
    let original = RaftRpcVoteRequest(
      currentTerm: RaftNodeTerm(42),
      lastLogIndex: RaftLogIndex(128),
      lastLogTerm: RaftNodeTerm(41),
      force: true,
    )

    let encoded = original.toSsz()
    let decoded = RaftRpcVoteRequest.fromSsz(encoded)

    check decoded == original

  test "Reply serialization round-trip preserves fields":
    let original = RaftRpcVoteReply(
      currentTerm: RaftNodeTerm(99),
      voteGranted: false,
    )

    let encoded = original.toSsz()
    let decoded = RaftRpcVoteReply.fromSsz(encoded)

    check decoded == original

  test "Request decoding fails when bytes truncated":
    let original = RaftRpcVoteRequest(
      currentTerm: RaftNodeTerm.high,
      lastLogIndex: RaftLogIndex.high,
      lastLogTerm: RaftNodeTerm.high,
      force: false,
    )

    var encoded = original.toSsz()
    doAssert encoded.len > 0
    encoded.setLen(encoded.len - 1)

    expect SszError:
      discard RaftRpcVoteRequest.fromSsz(encoded)

  test "Reply decoding fails when trailing data present":
    let original = RaftRpcVoteReply(
      currentTerm: RaftNodeTerm(7),
      voteGranted: true,
    )

    var encoded = original.toSsz()
    encoded.add 0'u8

    expect SszError:
      discard RaftRpcVoteReply.fromSsz(encoded)
