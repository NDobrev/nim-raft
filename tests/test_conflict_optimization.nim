# nim-raft
# Tests for AppendEntries conflict optimization

import unittest2
import ../src/raft/types
import ../src/raft/consensus_state_machine
import ../src/raft/log
import ../src/raft/state
import ../src/raft/config
import std/times

proc createConfig(ids: seq[RaftNodeId]): RaftConfig =
  RaftConfig(currentSet: ids)

suite "AppendEntries conflict optimization":
  test "leader uses conflictTerm when present":
    let id1 = newRaftNodeId("n1")
    let id2 = newRaftNodeId("n2")
    let cfg = createConfig(@[id1, id2])
    var log = RaftLog.init(RaftSnapshot(index: 0, term: 0, config: cfg))
    var now = dateTime(2020, mJan, 01, 00, 00, 00, 00, utc())
    let electionTime = initDuration(milliseconds = 150)
    let heartbeatTime = initDuration(milliseconds = 50)
    var sm = RaftStateMachineRef.new(id1, 0, log, 0, now, electionTime, heartbeatTime)
    sm.becomeLeader()
    discard sm.poll()
    # Add two more entries in same term to make lastIndex larger
    discard sm.addEntry(Empty())
    discard sm.addEntry(Empty())
    discard sm.poll()
    var follower = sm.findFollowerProgressById(id2)
    check follower.isSome()
    # Set a divergent nextIndex to confirm it changes
    follower.get().nextIndex = 100

    let rej = RaftRpcAppendReplyRejected(
      nonMatchingIndex: 5,
      lastIdx: sm.log.lastIndex,
      conflictTerm: some(sm.term),
      conflictIndex: 1,
    )
    let reply = RaftRpcAppendReply(
      term: sm.term,
      commitIndex: sm.commitIndex,
      result: RaftRpcCode.Rejected,
      rejected: rej,
    )
    sm.appendEntryReply(id2, reply)
    follower = sm.findFollowerProgressById(id2)
    check follower.isSome()
    # Leader should jump to last index of current term + 1
    check follower.get().nextIndex == sm.log.lastIndex + 1

  test "leader falls back to conflictIndex when term absent":
    let id1 = newRaftNodeId("n1")
    let id2 = newRaftNodeId("n2")
    let cfg = createConfig(@[id1, id2])
    var log = RaftLog.init(RaftSnapshot(index: 0, term: 0, config: cfg))
    var now = dateTime(2020, mJan, 01, 00, 00, 00, 00, utc())
    let electionTime = initDuration(milliseconds = 150)
    let heartbeatTime = initDuration(milliseconds = 50)
    var sm = RaftStateMachineRef.new(id1, 0, log, 0, now, electionTime, heartbeatTime)
    sm.becomeLeader()
    discard sm.poll()
    var follower = sm.findFollowerProgressById(id2)
    check follower.isSome()
    follower.get().nextIndex = 100
    let rej = RaftRpcAppendReplyRejected(
      nonMatchingIndex: 10,
      lastIdx: sm.log.lastIndex,
      conflictTerm: none(RaftNodeTerm),
      conflictIndex: 2,
    )
    let reply = RaftRpcAppendReply(
      term: sm.term,
      commitIndex: sm.commitIndex,
      result: RaftRpcCode.Rejected,
      rejected: rej,
    )
    sm.appendEntryReply(id2, reply)
    follower = sm.findFollowerProgressById(id2)
    check follower.isSome()
    check follower.get().nextIndex == 2

  test "follower provides conflictTerm and conflictIndex":
    let leaderId = newRaftNodeId("L")
    let followerId = newRaftNodeId("F")
    let cfg = createConfig(@[leaderId, followerId])
    # follower log: [1@1, 1@2, 3@3, 3@4]
    var flog = RaftLog.init(RaftSnapshot(index: 0, term: 0, config: cfg))
    flog.appendAsLeader(1, 1)
    flog.appendAsLeader(1, 2)
    flog.appendAsLeader(3, 3)
    flog.appendAsLeader(3, 4)
    var now = dateTime(2020, mJan, 01, 00, 00, 00, 00, utc())
    let electionTime = initDuration(milliseconds = 150)
    let heartbeatTime = initDuration(milliseconds = 50)
    var fsm = RaftStateMachineRef.new(
      followerId, 3, flog, 0, now, electionTime, heartbeatTime
    )
    fsm.becomeFollower(leaderId)
    discard fsm.poll()
    # Leader sends prevLogIndex=3 with prevTerm=2 (mismatch)
    let req = RaftRpcAppendRequest(
      previousTerm: 2,
      previousLogIndex: 3,
      commitIndex: 0,
      entries: @[],
    )
    fsm.appendEntry(leaderId, req)
    let output1 = fsm.poll()
    check output1.messages.len == 1
    check output1.messages[0].kind == RaftRpcMessageType.AppendReply
    check output1.messages[0].appendReply.result == RaftRpcCode.Rejected
    let rej = output1.messages[0].appendReply.rejected
    check rej.conflictTerm.isSome
    check rej.conflictTerm.get() == 3
    check rej.conflictIndex == 3

  test "follower hints when log too short and below snapshot":
    let leaderId = newRaftNodeId("L")
    let followerId = newRaftNodeId("F")
    let cfg = createConfig(@[leaderId, followerId])
    # follower has snapshot at index 2
    var flog = RaftLog.init(RaftSnapshot(index: 2, term: 1, config: cfg))
    var now = dateTime(2020, mJan, 01, 00, 00, 00, 00, utc())
    let electionTime = initDuration(milliseconds = 150)
    let heartbeatTime = initDuration(milliseconds = 50)
    var fsm = RaftStateMachineRef.new(
      followerId, 1, flog, 2, now, electionTime, heartbeatTime
    )
    fsm.becomeFollower(leaderId)
    discard fsm.poll()
    # Case A: prevLogIndex > lastIndex
    block:
      let req = RaftRpcAppendRequest(
        previousTerm: 5,
        previousLogIndex: 10,
        commitIndex: 0,
        entries: @[],
      )
      fsm.appendEntry(leaderId, req)
      let outputA = fsm.poll()
      check outputA.messages.len == 1
      let rej = outputA.messages[0].appendReply.rejected
      check not rej.conflictTerm.isSome
      check rej.conflictIndex == fsm.log.lastIndex + 1
    # Case B: prevLogIndex < firstIndex (below snapshot)
    block:
      let req = RaftRpcAppendRequest(
        previousTerm: 0,
        previousLogIndex: 1,
        commitIndex: 0,
        entries: @[],
      )
      fsm.appendEntry(leaderId, req)
      let outputB = fsm.poll()
      check outputB.messages.len == 1
      let rej2 = outputB.messages[0].appendReply.rejected
      check not rej2.conflictTerm.isSome
      check rej2.conflictIndex == fsm.log.firstIndex

  test "leader uses conflictIndex when conflictTerm not in leader log":
    let id1 = newRaftNodeId("n1")
    let id2 = newRaftNodeId("n2")
    let cfg = createConfig(@[id1, id2])
    var log = RaftLog.init(RaftSnapshot(index: 0, term: 0, config: cfg))
    var now = dateTime(2020, mJan, 01, 00, 00, 00, 00, utc())
    let electionTime = initDuration(milliseconds = 100)
    let heartbeatTime = initDuration(milliseconds = 50)
    var sm = RaftStateMachineRef.new(id1, 0, log, 0, now, electionTime, heartbeatTime)
    sm.becomeLeader()
    discard sm.poll()
    var follower = sm.findFollowerProgressById(id2)
    check follower.isSome()
    follower.get().nextIndex = 100
    let rej = RaftRpcAppendReplyRejected(
      nonMatchingIndex: 5,
      lastIdx: sm.log.lastIndex,
      conflictTerm: some(RaftNodeTerm(42)),
      conflictIndex: 3,
    )
    let reply = RaftRpcAppendReply(
      term: sm.term,
      commitIndex: sm.commitIndex,
      result: RaftRpcCode.Rejected,
      rejected: rej,
    )
    sm.appendEntryReply(id2, reply)
    follower = sm.findFollowerProgressById(id2)
    check follower.isSome()
    check follower.get().nextIndex == 3
