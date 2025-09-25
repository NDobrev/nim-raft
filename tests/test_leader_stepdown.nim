# nim-raft
# Tests for leader step-down on lost quorum

import unittest2
import ../src/raft/types
import ../src/raft/consensus_state_machine
import ../src/raft/log
import ../src/raft/state
import std/[times, options]

proc cfg(ids: seq[RaftNodeId]): RaftConfig =
  RaftConfig(currentSet: ids)

suite "Leader step-down":
  test "leader steps down when no quorum replies":
    let id1 = newRaftNodeId("n1")
    let id2 = newRaftNodeId("n2")
    let id3 = newRaftNodeId("n3")
    let config = cfg(@[id1, id2, id3])
    var log = RaftLog.init(RaftSnapshot(index: 0, term: 0, config: config))
    var now = dateTime(2020, mJan, 01, 00, 00, 00, 00, utc())
    let electionTime = initDuration(milliseconds = 100)
    let heartbeatTime = initDuration(milliseconds = 50)
    var sm = RaftStateMachineRef.new(id1, 0, log, 0, now, electionTime, heartbeatTime)
    sm.becomeLeader()
    discard sm.poll()
    check sm.state.isLeader
    # Advance beyond electionTime without any replies
    now = now + 101.milliseconds
    sm.tick(now)
    discard sm.poll()
    check sm.state.isFollower

  test "single-node leader does not step down":
    let id1 = newRaftNodeId("n1")
    let config = cfg(@[id1])
    var log = RaftLog.init(RaftSnapshot(index: 0, term: 0, config: config))
    var now = dateTime(2020, mJan, 01, 00, 00, 00, 00, utc())
    let electionTime = initDuration(milliseconds = 100)
    let heartbeatTime = initDuration(milliseconds = 50)
    var sm = RaftStateMachineRef.new(id1, 0, log, 0, now, electionTime, heartbeatTime)
    sm.becomeLeader()
    discard sm.poll()
    check sm.state.isLeader
    # Advance much further than electionTime
    now = now + 1000.milliseconds
    sm.tick(now)
    discard sm.poll()
    check sm.state.isLeader

  test "leader retains leadership with recent quorum reply":
    let id1 = newRaftNodeId("n1")
    let id2 = newRaftNodeId("n2")
    let id3 = newRaftNodeId("n3")
    let config = cfg(@[id1, id2, id3])
    var log = RaftLog.init(RaftSnapshot(index: 0, term: 0, config: config))
    var now = dateTime(2020, mJan, 01, 00, 00, 00, 00, utc())
    let electionTime = initDuration(milliseconds = 100)
    let heartbeatTime = initDuration(milliseconds = 50)
    var sm = RaftStateMachineRef.new(id1, 0, log, 0, now, electionTime, heartbeatTime)
    sm.becomeLeader()
    discard sm.poll()
    check sm.state.isLeader
    # Simulate a recent reply from one follower (enough for quorum with self)
    let rej = RaftRpcAppendReplyRejected(
      nonMatchingIndex: 0,
      lastIdx: sm.log.lastIndex,
      conflictTerm: none(RaftNodeTerm),
      conflictIndex: sm.log.lastIndex + 1,
    )
    let reply = RaftRpcAppendReply(
      term: sm.term,
      commitIndex: sm.commitIndex,
      result: RaftRpcCode.Rejected,
      rejected: rej,
    )
    sm.appendEntryReply(id2, reply)
    discard sm.poll()
    # Advance slightly less than electionTime
    now = now + 80.milliseconds
    sm.tick(now)
    discard sm.poll()
    check sm.state.isLeader
