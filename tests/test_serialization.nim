import unittest2
import ssz_serialization

#import ../src/raft/serialize
#import ../src/raft/consensus_state_machine
#import ../src/raft/log
import ../src/raft/types

suite "SSZ serialization":
#   test "AppendRequest roundtrip":
#     let msg = RaftRpcMessage(
#       currentTerm: 1,
#       receiver: RaftNodeId(id: "123"),
#       sender: RaftNodeId(id: "456"),
#       kind: RaftRpcMessageType.AppendRequest,
#       appendRequest: RaftRpcAppendRequest(
#         previousTerm: 1,
#         previousLogIndex: 0,
#         commitIndex: 0,
#         entries: @[LogEntry(term: 1, index: 1, kind: rletEmpty)],
#       ),
#     )
#     let encoded = msg.toSsz()
#     let decoded = SSZ.decode(encoded, RaftRpcMessage)
#     check decoded == msg

#   test "AppendReply roundtrip":
#     let msg = RaftRpcMessage(
#       currentTerm: 2,
#       receiver: RaftNodeId(id: "123"),
#       sender: RaftNodeId(id: "456"),
#       kind: RaftRpcMessageType.AppendReply,
#       appendReply: RaftRpcAppendReply(
#         commitIndex: 1,
#         term: 2,
#         result: Accepted,
#         accepted: RaftRpcAppendReplyAccepted(lastNewIndex: 2),
#       ),
#     )
#     let encoded = msg.toSsz()
#     let decoded = SSZ.decode(encoded, RaftRpcMessage)
#     check decoded == msg

#   test "VoteRequest roundtrip":
#     let msg = RaftRpcMessage(
#       currentTerm: 3,
#       receiver: RaftNodeId(id: "123"),
#       sender: RaftNodeId(id: "456"),
#       kind: RaftRpcMessageType.VoteRequest,
#       voteRequest: RaftRpcVoteRequest(
#         currentTerm: 3,
#         lastLogIndex: 5,
#         lastLogTerm: 2,
#         force: false,
#       ),
#     )
#     let encoded = msg.toSsz()
#     let decoded = SSZ.decode(encoded, RaftRpcMessage)
#     check decoded == msg

#   test "VoteReply roundtrip":
#     let msg = RaftRpcMessage(
#       currentTerm: 4,
#       receiver: RaftNodeId(id: "123"),
#       sender: RaftNodeId(id: "456"),
#       kind: RaftRpcMessageType.VoteReply,
#       voteReply: RaftRpcVoteReply(
#         currentTerm: 4,
#         voteGranted: true,
#       ),
#     )
#     let encoded = msg.toSsz()
#     let decoded = SSZ.decode(encoded, RaftRpcMessage)
#     check decoded == msg

#   test "InstallSnapshot roundtrip":
#     let msg = RaftRpcMessage(
#       currentTerm: 5,
#       receiver: RaftNodeId(id: "123"),
#       sender: RaftNodeId(id: "456"),
#       kind: RaftRpcMessageType.InstallSnapshot,
#       installSnapshot: RaftInstallSnapshot(
#         term: 5,
#         snapshot: RaftSnapshot(
#           index: 1,
#           term: 1,
#           config: RaftConfig(),
#         ),
#       ),
#     )
#     let encoded = msg.toSsz()
#     let decoded = SSZ.decode(encoded, RaftRpcMessage)
#     check decoded == msg

#   test "SnapshotReply roundtrip":
#     let msg = RaftRpcMessage(
#       currentTerm: 6,
#       receiver: RaftNodeId(id: "123"),
#       sender: RaftNodeId(id: "456"),
#       kind: RaftRpcMessageType.SnapshotReply,
#       snapshotReply: RaftSnapshotReply(
#         term: 6,
#         success: true,
#       ),
#     )
#     let encoded = msg.toSsz()
#     let decoded = SSZ.decode(encoded, RaftRpcMessage)
#     check decoded == msg

  test "Raft log index":
    let index = RaftLogIndex(1)
    let encoded = SSZ.encode(index)
    let decoded = SSZ.decode(encoded, RaftLogIndex)
    check decoded == index
  test "Raft log term":
    let term = RaftNodeTerm(1)
    let encoded = SSZ.encode(term)
    let decoded = SSZ.decode(encoded, RaftNodeTerm)
    check decoded == term
  # test "Raft log entry":
  #   let entry = LogEntry(term: 1, index: 1, kind: rletEmpty)
  #   let encoded = SSZ.encode(entry)#entry.toSsz()
  #   let decoded = SSZ.decode(encoded, LogEntry)
  #   check decoded == entry

