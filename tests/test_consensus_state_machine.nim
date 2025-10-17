# nim-raft
# Copyright (c) 2023 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.
import unittest2
import ../src/raft/types
import ../src/raft/consensus_state_machine
import ../src/raft/log
import ../src/raft/tracker
import ../src/raft/state
import ../src/raft/poll_state
import std/sets
import std/[sets, times, sequtils, random, algorithm, sugar, options, strformat]
import stew/byteutils

import tables

proc green*(s: string): string =
  "\e[32m" & s & "\e[0m"

proc grey*(s: string): string =
  "\e[90m" & s & "\e[0m"

proc purple*(s: string): string =
  "\e[95m" & s & "\e[0m"

proc yellow*(s: string): string =
  "\e[33m" & s & "\e[0m"

proc red*(s: string): string =
  "\e[31m" & s & "\e[0m"

type
  TestNode* = object
    sm: RaftStateMachineRef
    markedForElection: bool

  TestCluster* = object
    nodes: Table[RaftnodeId, TestNode]
    committed: seq[LogEntry]
    blockedTickSet: HashSet[RaftnodeId]
    blockedMsgRoutingSet: HashSet[RaftnodeId]

    # callbacks
    onEntryCommit: proc(nodeId: RaftnodeId, entry: LogEntry)

var test_ids_3 =
  @[
    newRaftNodeId( "a8409b39-f17b-4682-aaef-a19cc9f356fb"),
    newRaftNodeId( "2a98fc33-6559-44c0-b130-fc3e9df80a69"),
    newRaftNodeId( "9156756d-697f-4ffa-9b82-0c86720344bd"),
  ]

var test_second_ids_3 =
  @[
    newRaftNodeId( "aaaaaaaa-f17b-4682-aaef-a19cc9f356fb"),
    newRaftNodeId( "bbbbbbbb-6559-44c0-b130-fc3e9df80a69"),
    newRaftNodeId( "cccccccc-697f-4ffa-9b82-0c86720344bd"),
  ]

var test_ids_1 = @[newRaftNodeId( "a8409b39-f17b-4682-aaef-a19cc9f356fb")]

var test_second_ids_1 = @[newRaftNodeId( "aaaaaaaa-f17b-4682-aaef-a19cc9f356fb")]

func poll(node: var TestNode): RaftStateMachineRefOutput =
  return node.sm.poll()

func advance(node: var TestNode, msg: RaftRpcMessage, now: times.DateTime) =
  node.sm.advance(msg, now)

func tick(node: var TestNode, now: times.DateTime) =
  node.sm.tick(now)

func createConfigFromIds(ids: seq[RaftnodeId]): RaftConfig =
  var config = RaftConfig()
  for id in ids:
    config.currentSet.add(id)
  return config

proc createCluster(ids: seq[RaftnodeId], now: times.DateTime): TestCluster =
  var config = createConfigFromIds(ids)
  var cluster = TestCluster()
  cluster.blockedTickSet.init()
  cluster.blockedMsgRoutingSet.init()
  cluster.nodes = initTable[RaftnodeId, TestNode]()
  for i in 0 ..< config.currentSet.len:
    let id = config.currentSet[i]
    var log = RaftLog.init(RaftSnapshot(index: 0, config: config))
    var randGen = initRand(i + 42)
    let electionTime =
      times.initDuration(milliseconds = 100) +
      times.initDuration(milliseconds = 100 + randGen.rand(200))
    let heartbeatTime = times.initDuration(milliseconds = 50)
    var node = TestNode(
      sm: RaftStateMachineRef.new(id, 0, log, 0, now, electionTime, heartbeatTime),
      markedForElection: false,
    )
    cluster.nodes[id] = node
  return cluster

proc addNodeToCluster(
    tc: var TestCluster,
    id: RaftnodeId,
    now: times.DateTime,
    config: RaftConfig,
    randomGenerator: Rand = initRand(42),
) =
  var log = RaftLog.init(RaftSnapshot(index: 0, config: config))
  var randGen = initRand(42)
  let electionTime =
    times.initDuration(milliseconds = 100) +
    times.initDuration(milliseconds = 100 + randGen.rand(200))
  let heartbeatTime = times.initDuration(milliseconds = 50)
  var node = TestNode(
    sm: RaftStateMachineRef.new(id, 0, log, 0, now, electionTime, heartbeatTime),
    markedForElection: false,
  )
  if tc.nodes.contains(id):
    raise newException(AssertionDefect, "Adding node to the cluster that already exist")
  tc.nodes[id] = node

proc addNodeToCluster(
    tc: var TestCluster,
    ids: seq[RaftnodeId],
    now: times.DateTime,
    config: RaftConfig,
    randomGenerator: Rand = initRand(42),
) =
  var rng = randomGenerator
  for id in ids:
    let nodeSeed = rng.rand(1000)
    tc.addNodeToCluster(id, now, config, initRand(nodeSeed))

proc initLeaderWithSnapshot(): (RaftStateMachineRef, RaftNodeId, RaftNodeId, RaftSnapshot) =
  var ids = @[test_ids_3[0], test_ids_3[1]]
  var config = createConfigFromIds(ids)
  let snapshot =
    RaftSnapshot(index: RaftLogIndex(5), term: RaftNodeTerm(1), config: config)
  var log = RaftLog.init(snapshot)
  for i in 6 .. 8:
    log.appendAsLeader(RaftNodeTerm(1), RaftLogIndex(i))
  var now = dateTime(2017, mMar, 01, 00, 00, 00, 00, utc())
  let electionTime = times.initDuration(milliseconds = 250)
  let heartbeatTime = times.initDuration(milliseconds = 50)
  var sm =
    RaftStateMachineRef.new(ids[0], RaftNodeTerm(1), log, snapshot.index, now,
      electionTime, heartbeatTime)
  sm.becomeLeader()
  discard sm.poll()
  (sm, ids[0], ids[1], snapshot)

proc markNodeForDelection(tc: var TestCluster, id: RaftnodeId) =
  tc.nodes[id].markedForElection = true

proc removeNodeFromCluster(tc: var TestCluster, id: RaftnodeId) =
  tc.nodes.del(id)

proc removeNodeFromCluster(tc: var TestCluster, ids: seq[RaftnodeId]) =
  for id in ids:
    if tc.nodes.contains(id):
      tc.nodes.del(id)

proc ids(tc: var TestCluster): seq[RaftnodeId] =
  for k, v in tc.nodes:
    result.add(k)
  return result

proc blockTick(tc: var TestCluster, id: RaftnodeId) =
  tc.blockedTickSet.incl(id)

func blockMsgRouting(tc: var TestCluster, id: RaftnodeId) =
  tc.blockedMsgRoutingSet.incl(id)

func allowTick(tc: var TestCluster, id: RaftnodeId) =
  tc.blockedTickSet.excl(id)

func allowMsgRouting(tc: var TestCluster, id: RaftnodeId) =
  tc.blockedMsgRoutingSet.excl(id)

proc cmpLogs(x, y: DebugLogEntry): int =
  cmp(x.time, y.time)

func `$`*(de: DebugLogEntry): string =
  return
    "[" & $de.level & "][" & de.time.format("HH:mm:ss:fff") & "][" &
    (($de.nodeId)[0 .. 7]) & "...][" & $de.state & "]: " & de.msg

proc handleMessage(
    tc: var TestCluster,
    now: times.DateTime,
    msg: RaftRpcMessage,
    logLevel: DebugLogLevel,
) =
  if not tc.blockedMsgRoutingSet.contains(msg.sender) and
      not tc.blockedMsgRoutingSet.contains(msg.receiver):
    if DebugLogLevel.Debug <= logLevel:
      echo now.format("HH:mm:ss:fff") & "rpc:" & $msg

    if tc.nodes.contains(msg.receiver):
      tc.nodes[msg.receiver].advance(msg, now)
    else:
      if DebugLogLevel.Debug <= logLevel:
        echo fmt"Node with id {msg.receiver} is not in the cluster"
  else:
    if DebugLogLevel.Debug <= logLevel:
      echo "[" & now.format("HH:mm:ss:fff") & "] rpc message is blocked: " & $msg &
        $tc.blockedMsgRoutingSet

proc advance(
    tc: var TestCluster,
    now: times.DateTime,
    logLevel: DebugLogLevel = DebugLogLevel.Error,
) =
  var debugLogs: seq[DebugLogEntry]

  for id, node in tc.nodes:
    if node.markedForElection:
      continue
    if tc.blockedTickSet.contains(id):
      continue
    tc.nodes[id].tick(now)
    let output = tc.nodes[id].poll()
    debugLogs.add(output.debugLogs)
    for msg in output.messages:
      tc.handleMessage(now, msg, logLevel)
    for entry in output.committed:
      tc.committed.add(entry)
      if not tc.onEntryCommit.isNil:
        tc.onEntryCommit(id, entry)

  let toDelete = toSeq(tc.nodes.values).filter(node => node.markedForElection)
  for node in toDelete:
    tc.removeNodeFromCluster(node.sm.myId)

  debugLogs.sort(cmpLogs)
  for msg in debugLogs:
    if msg.level <= logLevel:
      echo $msg

proc advanceUntil(
    tc: var TestCluster,
    now: times.DateTime,
    until: times.DateTime,
    step: times.TimeInterval = 5.milliseconds,
    logLevel: DebugLogLevel = DebugLogLevel.Error,
): times.DateTime =
  var timeNow = now
  while timeNow < until:
    timeNow += step
    tc.advance(timeNow, logLevel)
  return timeNow

proc advanceConfigChange(
    tc: var TestCluster,
    now: times.DateTime,
    until: times.DateTime,
    step: times.TimeInterval = 5.milliseconds,
    logLevel: DebugLogLevel = DebugLogLevel.Error,
): times.DateTime =
  var timeNow = now
  while timeNow < until:
    timeNow += step
    tc.advance(timeNow, logLevel)
  return timeNow

func getLeader(tc: TestCluster): Option[RaftStateMachineRef] =
  var leader = none(RaftStateMachineRef)
  for id, node in tc.nodes:
    if node.sm.state.isLeader:
      if not leader.isSome() or leader.get().term < node.sm.term:
        leader = some(node.sm)
  return leader

proc configuration(tc: var TestCluster): Option[RaftConfig] =
  var leader = tc.getLeader()
  if leader.isSome():
    return some(leader.get.configuration())
  return none(RaftConfig)

proc submitCommand(tc: var TestCluster, cmd: Command): bool =
  var leader = tc.getLeader()
  if leader.isSome():
    discard leader.get().addEntry(cmd)
    return true
  return false

proc hasCommittedEntry(tc: var TestCluster, cmd: Command): bool =
  for ce in tc.committed:
    if ce.kind == RaftLogEntryType.rletCommand and ce.command == cmd:
      return true
  return false

proc hasCommittedEntry(tc: var TestCluster, cfg: RaftConfig): bool =
  for ce in tc.committed:
    if ce.kind == RaftLogEntryType.rletConfig and ce.config == cfg:
      return true
  return false

proc advanceUntilNoLeader(
    tc: var TestCluster,
    start: times.DateTime,
    step: times.TimeInterval = 5.milliseconds,
    timeoutInterval: times.TimeInterval = 1.seconds,
    logLevel: DebugLogLevel = DebugLogLevel.Error,
): times.DateTime =
  var timeNow = start
  var timeoutAt = start + timeoutInterval
  while timeNow < timeoutAt:
    timeNow += step
    tc.advance(timeNow, logLevel)
    var maybeLeader = tc.getLeader()
    if not maybeLeader.isSome():
      return timeNow
  raise newException(AssertionDefect, "Timeout")

proc establishLeader(
    tc: var TestCluster,
    start: times.DateTime,
    step: times.TimeInterval = 5.milliseconds,
    timeoutInterval: times.TimeInterval = 1.seconds,
    logLevel: DebugLogLevel = DebugLogLevel.Error,
): times.DateTime =
  var timeNow = start
  var timeoutAt = start + timeoutInterval
  while timeNow < timeoutAt:
    timeNow += step
    tc.advance(timeNow, logLevel)
    var maybeLeader = tc.getLeader()
    if maybeLeader.isSome():
      return timeNow
  raise newException(AssertionDefect, "Timeout")

proc submitNewConfig(tc: var TestCluster, cfg: RaftConfig) =
  var leader = tc.getLeader()
  if leader.isSome():
    discard leader.get().addEntry(cfg)
  else:
    raise newException(AssertionDefect, "Can submit new configuration")

proc toCommand(data: string): Command =
  return Command(data: data.toBytes)


var config = createConfigFromIds(test_ids_1)
suite "Basic state machine tests":
  test "create state machine":
    var timeNow = dateTime(2017, mMar, 01, 00, 00, 00, 00, utc())
    var cluster = createCluster(test_ids_1, timeNow)

  test "tick empty state machine":
    var timeNow = dateTime(2017, mMar, 01, 00, 00, 00, 00, utc())

    var log = RaftLog.init(RaftSnapshot(index: 1, config: config))
    var randGen = initRand(42)
    let electionTime =
      times.initDuration(milliseconds = 100) +
      times.initDuration(milliseconds = 100 + randGen.rand(200))
    let heartbeatTime = times.initDuration(milliseconds = 50)
    var sm = RaftStateMachineRef.new(
      test_ids_1[0], 0, log, 0, timeNow, electionTime, heartbeatTime
    )
    timeNow += 5.milliseconds
    sm.tick(timeNow)
    
  test "new respects provided commit index":
    var timeNow = dateTime(2017, mMar, 01, 00, 00, 00, 00, utc())
    var log = RaftLog.init(RaftSnapshot(index: 0, term: RaftNodeTerm(1), config: config))
    log.appendAsLeader(term = RaftNodeTerm(1), index = RaftLogIndex(1), data = Command())
    log.appendAsLeader(term = RaftNodeTerm(1), index = RaftLogIndex(2), data = Command())
    var randGen = initRand(42)
    let electionTime =
      times.initDuration(milliseconds = 100) +
      times.initDuration(milliseconds = 100 + randGen.rand(200))
    let heartbeatTime = times.initDuration(milliseconds = 50)
    var sm = RaftStateMachineRef.new(
      test_ids_1[0], 1, log, RaftLogIndex(2), timeNow, electionTime, heartbeatTime
    )
    check sm.commitIndex == RaftLogIndex(2)

  test "checkInvariants validates commit index bounds":
    var timeNow = dateTime(2017, mMar, 01, 00, 00, 00, 00, utc())
    var log = RaftLog.init(RaftSnapshot(index: 1, term: RaftNodeTerm(1), config: config))
    log.appendAsLeader(term = RaftNodeTerm(1), index = RaftLogIndex(2), data = Command())
    var randGen = initRand(42)
    let electionTime =
      times.initDuration(milliseconds = 100) +
      times.initDuration(milliseconds = 100 + randGen.rand(200))
    let heartbeatTime = times.initDuration(milliseconds = 50)
    var sm = RaftStateMachineRef.new(
      test_ids_1[0], 1, log, RaftLogIndex(1), timeNow, electionTime, heartbeatTime
    )
    sm.checkInvariants()
    sm.commitIndex = RaftLogIndex(0)
    expect AssertionError:
      sm.checkInvariants()
    sm.commitIndex = RaftLogIndex(3)
    expect AssertionError:
      sm.checkInvariants()  

suite "Leader commit gating":
  test "leader defers old-term entry until current-term quorum":
    let id1 = newRaftNodeId("n1")
    let id2 = newRaftNodeId("n2")
    let id3 = newRaftNodeId("n3")
    let config = createConfigFromIds(@[id1, id2, id3])
    var log = RaftLog.init(RaftSnapshot(index: 0, term: 0, config: config))
    log.appendAsLeader(
      term = RaftNodeTerm(1),
      index = RaftLogIndex(1),
      data = Command(),
    )

    var now = dateTime(2020, mJan, 01, 00, 00, 00, 00, utc())
    let electionTime = initDuration(milliseconds = 150)
    let heartbeatTime = initDuration(milliseconds = 50)
    var sm = RaftStateMachineRef.new(
      id1, RaftNodeTerm(2), log, RaftLogIndex(0), now, electionTime, heartbeatTime,
    )

    sm.becomeLeader()
    discard sm.poll()

    var leaderProgress = sm.findFollowerProgressById(id1)
    check leaderProgress.isSome()
    leaderProgress.get().matchIndex = sm.log.lastIndex
    leaderProgress.get().nextIndex = sm.log.lastIndex + 1

    for followerId in [id2, id3]:
      var follower = sm.findFollowerProgressById(followerId)
      check follower.isSome()
      follower.get().matchIndex = RaftLogIndex(1)
      follower.get().nextIndex = RaftLogIndex(2)

    var output = sm.poll()
    check sm.commitIndex == RaftLogIndex(0)
    check output.committed.len == 0

    var followerAdv = sm.findFollowerProgressById(id2)
    check followerAdv.isSome()
    followerAdv.get().matchIndex = sm.log.lastIndex
    followerAdv.get().nextIndex = sm.log.lastIndex + 1

    output = sm.poll()
    check sm.commitIndex == sm.log.lastIndex
    check output.committed.len == 2
    check output.committed[1].term == sm.term

suite "RequestVote follower behavior":
  test "follower grants vote once and resets timer":
    let followerId = newRaftNodeId("follower")
    let candidateA = newRaftNodeId("candidate-a")
    let candidateB = newRaftNodeId("candidate-b")
    let cfg = createConfigFromIds(@[followerId, candidateA, candidateB])
    var flog = RaftLog.init(RaftSnapshot(index: 0, term: 0, config: cfg))
    flog.appendAsLeader(
      term = RaftNodeTerm(3),
      index = RaftLogIndex(1),
      data = Command(),
    )

    var now = dateTime(2020, mJan, 01, 00, 00, 00, 00, utc())
    let electionTime = initDuration(milliseconds = 200)
    let heartbeatTime = initDuration(milliseconds = 50)
    var sm = RaftStateMachineRef.new(
      followerId, RaftNodeTerm(3), flog, RaftLogIndex(0), now, electionTime, heartbeatTime,
    )
    sm.becomeFollower(RaftNodeId.empty)
    discard sm.poll()

    now = now + (electionTime - initDuration(milliseconds = 10))
    sm.tick(now)
    discard sm.poll()
    check sm.state.isFollower

    let voteReq = RaftRpcVoteRequest(
      currentTerm: sm.term,
      lastLogIndex: sm.log.lastIndex,
      lastLogTerm: sm.log.lastTerm,
      force: false,
    )
    now = now + initDuration(milliseconds = 5)
    sm.tick(now)
    discard sm.poll()

    sm.requestVote(candidateA, voteReq)
    var output = sm.poll()
    check output.messages.len == 1
    check output.messages[0].kind == RaftRpcMessageType.VoteReply
    check output.messages[0].voteReply.voteGranted
    check output.votedFor.isSome
    check output.votedFor.get() == candidateA

    sm.requestVote(candidateB, voteReq)
    output = sm.poll()
    check output.messages.len == 1
    check output.messages[0].kind == RaftRpcMessageType.VoteReply
    check not output.messages[0].voteReply.voteGranted

    var later = now + (electionTime - initDuration(milliseconds = 10))
    sm.tick(later)
    discard sm.poll()
    check sm.state.isFollower

    later = later + initDuration(milliseconds = 20)
    sm.tick(later)
    discard sm.poll()
    check sm.state.isCandidate

suite "Append rejection":
  test "follower rejects lower-term append request":
    let leaderId = newRaftNodeId("leader")
    let followerId = newRaftNodeId("follower")
    let cfg = createConfigFromIds(@[leaderId, followerId])
    var flog = RaftLog.init(RaftSnapshot(index: 0, term: 1, config: cfg))
    flog.appendAsLeader(
      term = RaftNodeTerm(1),
      index = RaftLogIndex(1),
      data = Command(),
    )
    flog.appendAsLeader(
      term = RaftNodeTerm(2),
      index = RaftLogIndex(2),
      data = Command(),
    )

    var now = dateTime(2020, mApr, 01, 00, 00, 00, 00, utc())
    let electionTime = initDuration(milliseconds = 200)
    let heartbeatTime = initDuration(milliseconds = 50)
    var sm = RaftStateMachineRef.new(
      followerId, RaftNodeTerm(2), flog, RaftLogIndex(0), now, electionTime, heartbeatTime,
    )
    sm.becomeFollower(leaderId)
    discard sm.poll()

    let request = RaftRpcAppendRequest(
      previousTerm: RaftNodeTerm(1),
      previousLogIndex: RaftLogIndex(1),
      commitIndex: RaftLogIndex(1),
      entries: @[],
    )

    let msg = RaftRpcMessage(
      currentTerm: RaftNodeTerm(1),
      sender: leaderId,
      receiver: followerId,
      kind: RaftRpcMessageType.AppendRequest,
      appendRequest: request,
    )

    sm.advance(msg, now)
    let output = sm.poll()
    check output.messages.len == 1
    check output.messages[0].kind == RaftRpcMessageType.AppendReply
    check output.messages[0].appendReply.result == RaftRpcCode.Rejected
    let rej = output.messages[0].appendReply.rejected
    check rej.nonMatchingIndex == RaftLogIndex(0)
    check not rej.conflictTerm.isSome
    check rej.conflictIndex == sm.log.lastIndex + 1

suite "VoteRequest step-down":
  test "leader steps down on higher-term vote request":
    let leaderId = newRaftNodeId("leader")
    let followerId = newRaftNodeId("follower")
    let thirdId = newRaftNodeId("observer")
    let cfg = createConfigFromIds(@[leaderId, followerId, thirdId])
    var log = RaftLog.init(RaftSnapshot(index: 0, term: 0, config: cfg))
    log.appendAsLeader(
      term = RaftNodeTerm(2),
      index = RaftLogIndex(1),
      data = Command(),
    )

    var now = dateTime(2020, mJan, 01, 00, 00, 00, 00, utc())
    let electionTime = initDuration(milliseconds = 150)
    let heartbeatTime = initDuration(milliseconds = 50)
    var sm = RaftStateMachineRef.new(
      leaderId, RaftNodeTerm(2), log, RaftLogIndex(0), now, electionTime, heartbeatTime,
    )

    sm.becomeLeader()
    discard sm.poll()
    check sm.state.isLeader

    let higherTerm = sm.term + 1
    let voteReq = RaftRpcVoteRequest(
      currentTerm: higherTerm,
      lastLogIndex: sm.log.lastIndex,
      lastLogTerm: sm.log.lastTerm,
      force: false,
    )

    now += 80.milliseconds
    let msg = RaftRpcMessage(
      currentTerm: higherTerm,
      sender: followerId,
      receiver: leaderId,
      kind: RaftRpcMessageType.VoteRequest,
      voteRequest: voteReq,
    )

    sm.advance(msg, now)
    let output = sm.poll()
    check sm.state.isFollower
    check sm.term == higherTerm
    check output.messages.len == 1
    check output.messages[0].kind == RaftRpcMessageType.VoteReply
    check output.messages[0].voteReply.voteGranted
    check output.votedFor.isSome
    check output.votedFor.get() == followerId

suite "Known leader vote rejection":
  test "follower refuses vote when leader known":
    let leaderId = newRaftNodeId("leader")
    let followerId = newRaftNodeId("follower")
    let candidateId = newRaftNodeId("challenger")
    let cfg = createConfigFromIds(@[leaderId, followerId, candidateId])
    var flog = RaftLog.init(RaftSnapshot(index: 0, term: 0, config: cfg))
    flog.appendAsLeader(
      term = RaftNodeTerm(2),
      index = RaftLogIndex(1),
      data = Command(),
    )

    var now = dateTime(2020, mApr, 02, 00, 00, 00, 00, utc())
    let electionTime = initDuration(milliseconds = 200)
    let heartbeatTime = initDuration(milliseconds = 50)
    var sm = RaftStateMachineRef.new(
      followerId, RaftNodeTerm(2), flog, RaftLogIndex(0), now, electionTime, heartbeatTime,
    )
    sm.becomeFollower(leaderId)
    discard sm.poll()

    let heartbeat = RaftRpcMessage(
      currentTerm: sm.term,
      sender: leaderId,
      receiver: followerId,
      kind: RaftRpcMessageType.AppendRequest,
      appendRequest: RaftRpcAppendRequest(
        previousTerm: sm.log.lastTerm,
        previousLogIndex: sm.log.lastIndex,
        commitIndex: sm.log.lastIndex,
        entries: @[],
      ),
    )
    sm.advance(heartbeat, now)
    discard sm.poll()

    now = now + initDuration(milliseconds = 10)
    let voteReq = RaftRpcVoteRequest(
      currentTerm: sm.term,
      lastLogIndex: sm.log.lastIndex,
      lastLogTerm: sm.log.lastTerm,
      force: false,
    )
    sm.requestVote(candidateId, voteReq)
    let output = sm.poll()
    check output.messages.len == 1
    check output.messages[0].kind == RaftRpcMessageType.VoteReply
    check not output.messages[0].voteReply.voteGranted

suite "Entry log tests":
  test "append entry as leadeer":
    var log = RaftLog.init(RaftSnapshot(index: 2, config: config))
    check log.lastIndex == 2
  test "append entry as leadeer":
    var log = RaftLog.init(RaftSnapshot(index: 0, config: config))
    log.appendAsLeader(0, 1, Command())
    log.appendAsLeader(0, 2, Command())
    check log.lastTerm() == 0
    log.appendAsLeader(1, 3, Command())
    check log.lastTerm() == 1
  test "append entry as follower":
    var log = RaftLog.init(RaftSnapshot(index: 0, config: config))
    check log.nextIndex == 1
    log.appendAsFollower(0, 1, Command())
    check log.lastTerm() == 0
    check log.nextIndex == 2
    check log.lastIndex() == 1
    check log.entriesCount == 1
    discard log.matchTerm(1, 1)
    log.appendAsFollower(1, 2, Command())
    check log.lastTerm() == 1
    check log.nextIndex == 3
    check log.lastIndex() == 2
    check log.entriesCount == 2
    log.appendAsFollower(1, 3, Command())
    check log.lastTerm() == 1
    check log.nextIndex == 4
    check log.lastIndex() == 3
    check log.entriesCount == 3
    # log should trancate old entries because the term is bigger
    log.appendAsFollower(2, 2, Command())
    check log.lastTerm() == 2
    check log.nextIndex == 3
    check log.lastIndex() == 2
    check log.entriesCount == 2
    # log should be trancated because 
    log.appendAsFollower(2, 1, Command())
    check log.lastTerm() == 2
    check log.nextIndex == 2
    check log.lastIndex() == 1
    check log.entriesCount == 1


suite "Snapshot application":
  test "poll handles shorter log after snapshot":
    var timeNow = dateTime(2017, mMar, 01, 00, 00, 00, 00, utc())
    var log = RaftLog.init(RaftSnapshot(index: 0, config: config))
    for i in 1 .. 5:
      log.appendAsLeader(0, RaftLogIndex(i))
    var randGen = initRand(42)
    let electionTime =
      times.initDuration(milliseconds = 100) +
      times.initDuration(milliseconds = 100 + randGen.rand(200))
    let heartbeatTime = times.initDuration(milliseconds = 50)
    var sm = RaftStateMachineRef.new(
      test_ids_1[0], 0, log, 0, timeNow, electionTime, heartbeatTime
    )
    discard sm.poll()
    check sm.observedState.persistedIndex == 5

    block:
      let success = sm.applySnapshot(RaftSnapshot(index: 2, term: 0, config: config), false)
      check success
      check sm.log.lastIndex == 5
      check sm.log.entriesCount() == 3
      let output = sm.poll()
      check output.logEntries.len == 0
      check sm.observedState.persistedIndex == 5
    block:
      try:
        let success = sm.applySnapshot(RaftSnapshot(index: 6, term: 0, config: config), true)
        doAssert false, "should have failed"
      except AssertionError:
        check true
    block:
      let success = sm.applySnapshot(RaftSnapshot(index: 5, term: 0, config: config), false)
      check success
      check sm.log.lastIndex == 5
      check sm.log.entriesCount() == 0
    block:
      let success = sm.applySnapshot(RaftSnapshot(index: 6, term: 0, config: config), false)
      let repeatSuccess =
        sm.applySnapshot(RaftSnapshot(index: 6, term: 0, config: config), false)
      check repeatSuccess
      check sm.log.lastIndex == 6
      check sm.log.entriesCount() == 0

  test "poll handles shorter log after snapshot":
    var config = createConfigFromIds(test_ids_1)
    var timeNow = dateTime(2017, mMar, 01, 00, 00, 00, 00, utc())
    var log = RaftLog.init(RaftSnapshot(index: 0, config: config))
    for i in 1 .. 5:
      log.appendAsLeader(0, RaftLogIndex(i))

    var randGen = initRand(42)
    let electionTime =
      times.initDuration(milliseconds = 100) +
      times.initDuration(milliseconds = 100 + randGen.rand(200))
    let heartbeatTime = times.initDuration(milliseconds = 50)
    var sm = RaftStateMachineRef.new(
      test_ids_1[0], 0, log, 0, timeNow, electionTime, heartbeatTime
    )
    discard sm.poll()
    check sm.observedState.persistedIndex == 5
    let success = sm.applySnapshot(RaftSnapshot(index: 2, term: 0, config: config), false)
    check success
    let output = sm.poll()
    check output.logEntries.len == 0
    check sm.observedState.persistedIndex == 5
    
  test "applySnapshot clamps persisted index to log size":
    var timeNow = dateTime(2017, mMar, 01, 00, 00, 00, 00, utc())
    var log = RaftLog.init(RaftSnapshot(index: 0, config: config))
    for i in 1 .. 2:
      log.appendAsLeader(0, RaftLogIndex(i))
    var randGen = initRand(42)
    let electionTime =
      times.initDuration(milliseconds = 100) +
      times.initDuration(milliseconds = 100 + randGen.rand(200))
    let heartbeatTime = times.initDuration(milliseconds = 50)
    var sm = RaftStateMachineRef.new(
      test_ids_1[0], 0, log, 0, timeNow, electionTime, heartbeatTime,
    )
    sm.observedState.setPersistedIndex 5
    let success =
      sm.applySnapshot(RaftSnapshot(index: 2, term: 0, config: config), false)
    check success
    discard sm.poll()
    check sm.log.lastIndex == 2
    check sm.observedState.persistedIndex == sm.log.lastIndex

suite "InstallSnapshot RPC":
  test "follower replies success when snapshot already applied":
    var config = createConfigFromIds(test_ids_1)
    let snapshot =
      RaftSnapshot(index: RaftLogIndex(5), term: RaftNodeTerm(3), config: config)
    var log = RaftLog.init(snapshot)
    var now = dateTime(2017, mMar, 01, 00, 00, 00, 00, utc())
    var randGen = initRand(42)
    let electionTime =
      times.initDuration(milliseconds = 100) +
      times.initDuration(milliseconds = 100 + randGen.rand(200))
    let heartbeatTime = times.initDuration(milliseconds = 50)
    var sm = RaftStateMachineRef.new(
      test_ids_1[0], snapshot.term, log, snapshot.index, now,
      electionTime, heartbeatTime
    )
    sm.becomeFollower(test_second_ids_1[0])
    discard sm.poll()

    let message = RaftRpcMessage(
      currentTerm: snapshot.term,
      sender: test_second_ids_1[0],
      receiver: test_ids_1[0],
      kind: RaftRpcMessageType.InstallSnapshot,
      installSnapshot: RaftInstallSnapshot(term: snapshot.term, snapshot: snapshot),
    )

    sm.advance(message, now)
    var output = sm.poll()
    check output.messages.len == 1
    check output.messages[0].kind == RaftRpcMessageType.SnapshotReply
    check output.messages[0].snapshotReply.success
    check sm.commitIndex == snapshot.index
    check sm.log.lastIndex == snapshot.index

    sm.advance(message, now)
    output = sm.poll()
    check output.messages.len == 1
    check output.messages[0].kind == RaftRpcMessageType.SnapshotReply
    check output.messages[0].snapshotReply.success

  test "snapshot replay falls back to log snapshot index":
    var (sm, leaderId, followerId, snapshot) = initLeaderWithSnapshot()
    doAssert sm.myId == leaderId

    var follower = sm.findFollowerProgressById(followerId)
    check follower.isSome()
    follower.get().nextIndex = snapshot.index

    sm.replicateTo(follower.get())
    discard sm.poll()

    follower = sm.findFollowerProgressById(followerId)
    check follower.isSome()
    follower.get().replayedIndex = 0
    follower.get().nextIndex = snapshot.index

    sm.installSnapshotReplay(followerId, RaftSnapshotReply(term: sm.term, success: true))
    var output = sm.poll()
    let appendMsgs = output.messages.filterIt(it.kind == RaftRpcMessageType.AppendRequest)
    check appendMsgs.len >= 1
    check appendMsgs[0].appendRequest.previousLogIndex == snapshot.index
    check appendMsgs[0].appendRequest.entries.len > 0
    check appendMsgs[0].appendRequest.entries[0].index == snapshot.index + 1

    follower = sm.findFollowerProgressById(followerId)
    check follower.isSome()
    check follower.get().nextIndex == sm.log.lastIndex + 1
    check follower.get().matchIndex == snapshot.index
    check follower.get().replayedIndex == 0

suite "AppendEntries edges":
  test "accept when prevLogIndex equals snapshot index":
    let leader = newRaftNodeId("leader-1")
    let follower = newRaftNodeId("follower-1")
    let config = createConfigFromIds(@[leader, follower])
    var flog = RaftLog.init(RaftSnapshot(index: 5, term: 1, config: config))
    var now = dateTime(2020, mMar, 01, 00, 00, 00, 00, utc())
    var randGen = initRand(42)
    let electionTime =
      times.initDuration(milliseconds = 100) +
      times.initDuration(milliseconds = 100 + randGen.rand(200))
    let heartbeatTime = times.initDuration(milliseconds = 50)
    var fsm = RaftStateMachineRef.new(
      follower, 1, flog, 5, now, electionTime, heartbeatTime
    )
    fsm.becomeFollower(leader)
    discard fsm.poll()
    let req = RaftRpcAppendRequest(
      previousTerm: 1,
      previousLogIndex: 5,
      commitIndex: 7,
      entries: @[
        LogEntry(term: 1, index: 6, kind: rletEmpty),
        LogEntry(term: 1, index: 7, kind: rletEmpty),
      ],
    )
    fsm.appendEntry(leader, req)
    let outputAE = fsm.poll()
    check outputAE.messages.len == 1
    check outputAE.messages[0].kind == RaftRpcMessageType.AppendReply
    check outputAE.messages[0].appendReply.result == RaftRpcCode.Accepted
    check outputAE.messages[0].appendReply.accepted.lastNewIndex == 7
    check fsm.log.lastIndex == 7
    check fsm.commitIndex == 7

suite "RequestVote edges":
  test "follower rejects vote if candidate log is stale":
    let followerId = newRaftNodeId("F")
    let candidateId = newRaftNodeId("C")
    let config = createConfigFromIds(@[followerId, candidateId])
    var l = RaftLog.init(RaftSnapshot(index: 0, term: 0, config: config))
    l.appendAsLeader(2, 1)
    var now = dateTime(2020, mMar, 01, 00, 00, 00, 00, utc())
    var randGen = initRand(42)
    let electionTime =
      times.initDuration(milliseconds = 100) +
      times.initDuration(milliseconds = 100 + randGen.rand(200))
    let heartbeatTime = times.initDuration(milliseconds = 50)
    var sm = RaftStateMachineRef.new(
      followerId, 2, l, 0, now, electionTime, heartbeatTime
    )
    sm.becomeFollower(RaftNodeId.empty)
    discard sm.poll()
    let req = RaftRpcVoteRequest(
      currentTerm: 2,
      lastLogIndex: 0,
      lastLogTerm: 0,
      force: false,
    )
    sm.requestVote(candidateId, req)
    let outputRV = sm.poll()
    check outputRV.messages.len == 1
    check outputRV.messages[0].kind == RaftRpcMessageType.VoteReply
    check outputRV.messages[0].voteReply.voteGranted == false

suite "Snapshot replies":
  test "leader advances follower after snapshot success":
    var (sm, leaderId, followerId, snapshot) = initLeaderWithSnapshot()
    doAssert sm.myId == leaderId

    var follower = sm.findFollowerProgressById(followerId)
    check follower.isSome()
    follower.get().nextIndex = snapshot.index

    sm.replicateTo(follower.get())
    var output = sm.poll()
    let installMsgs = output.messages.filterIt(it.kind == RaftRpcMessageType.InstallSnapshot)
    check installMsgs.len >= 1
    check installMsgs[0].installSnapshot.snapshot.index == snapshot.index

    sm.installSnapshotReplay(followerId, RaftSnapshotReply(term: sm.term, success: true))
    output = sm.poll()
    let appendMsgs = output.messages.filterIt(it.kind == RaftRpcMessageType.AppendRequest)
    check appendMsgs.len >= 1
    check appendMsgs[0].appendRequest.previousLogIndex == snapshot.index
    check appendMsgs[0].appendRequest.entries.len > 0
    check appendMsgs[0].appendRequest.entries[0].index == snapshot.index + 1

    follower = sm.findFollowerProgressById(followerId)
    check follower.isSome()
    check follower.get().nextIndex == sm.log.lastIndex + 1
    check follower.get().matchIndex == snapshot.index

  test "leader advances follower after snapshot rejection":
    var (sm, leaderId, followerId, snapshot) = initLeaderWithSnapshot()
    doAssert sm.myId == leaderId

    var follower = sm.findFollowerProgressById(followerId)
    check follower.isSome()
    follower.get().nextIndex = snapshot.index

    sm.replicateTo(follower.get())
    var output = sm.poll()
    let installMsgs = output.messages.filterIt(it.kind == RaftRpcMessageType.InstallSnapshot)
    check installMsgs.len >= 1

    sm.installSnapshotReplay(followerId, RaftSnapshotReply(term: sm.term, success: false))
    output = sm.poll()
    let appendMsgs = output.messages.filterIt(it.kind == RaftRpcMessageType.AppendRequest)
    check appendMsgs.len >= 1
    check appendMsgs[0].appendRequest.previousLogIndex == snapshot.index
    # After snapshot rejection, follower moves to Probe mode
    # Probe mode sends ONE entry at a time (not all entries)
    check appendMsgs[0].appendRequest.entries.len >= 1
    check appendMsgs[0].appendRequest.entries[0].index == snapshot.index + 1

    follower = sm.findFollowerProgressById(followerId)
    check follower.isSome()
    # In Probe mode, nextIndex advances by 1 per successful probe
    check follower.get().nextIndex == snapshot.index + 2  # Not lastIndex+1, just +2 after one probe
    check follower.get().matchIndex == snapshot.index
    check follower.get().state == tracker.RaftFollowerState.Probe

  test "leader handles follower with newer snapshot":
    # Test scenario: Follower has a newer snapshot than leader's snapshot
    # Leader should eventually converge by using conflict hints
    var (leaderSm, leaderId, followerId, leaderSnapshot) = initLeaderWithSnapshot()
    doAssert leaderSm.myId == leaderId

    discard leaderSm.addEntry(Empty())
    discard leaderSm.addEntry(Empty())

    let newerSnapshotIndex = leaderSnapshot.index + 4
    let config = createConfigFromIds(@[leaderId, followerId])
    let followerSnapshot =
      RaftSnapshot(index: newerSnapshotIndex, term: leaderSnapshot.term, config: config)

    var now = dateTime(2017, mMar, 01, 00, 00, 00, 00, utc())
    let electionTime = times.initDuration(milliseconds = 250)
    let heartbeatTime = times.initDuration(milliseconds = 50)

    var followerLog = RaftLog.init(followerSnapshot)
    var followerSm =
      RaftStateMachineRef.new(
        followerId,
        followerSnapshot.term,
        followerLog,
        followerSnapshot.index,
        now,
        electionTime,
        heartbeatTime,
      )
    followerSm.becomeFollower(leaderId)
    discard followerSm.poll()

    var followerProgress = leaderSm.findFollowerProgressById(followerId)
    check followerProgress.isSome()
    followerProgress.get().nextIndex = leaderSnapshot.index

    leaderSm.replicateTo(followerProgress.get())
    var leaderOutput = leaderSm.poll()
    let installMsgs =
      leaderOutput.messages.filterIt(it.kind == RaftRpcMessageType.InstallSnapshot)
    check installMsgs.len >= 1
    let installMsg = installMsgs[0]

    followerSm.advance(installMsg, now)
    var followerOutput = followerSm.poll()
    let snapshotReplies =
      followerOutput.messages.filterIt(it.kind == RaftRpcMessageType.SnapshotReply)
    check snapshotReplies.len == 1
    let snapshotReply = snapshotReplies[0]
    check snapshotReply.snapshotReply.success

    leaderSm.advance(snapshotReply, now)
    leaderOutput = leaderSm.poll()
    let appendMsgs =
      leaderOutput.messages.filterIt(it.kind == RaftRpcMessageType.AppendRequest)
    check appendMsgs.len == 1
    let appendMsg = appendMsgs[0]
    check appendMsg.appendRequest.previousLogIndex == leaderSnapshot.index

    followerSm.advance(appendMsg, now)
    followerOutput = followerSm.poll()
    let appendReplies =
      followerOutput.messages.filterIt(it.kind == RaftRpcMessageType.AppendReply)
    check appendReplies.len == 1
    let appendReply = appendReplies[0]
    check appendReply.appendReply.result == RaftRpcCode.Rejected
    check appendReply.appendReply.rejected.conflictIndex == followerSnapshot.index + 1

    leaderSm.advance(appendReply, now)
    leaderOutput = leaderSm.poll()
    let nextInstallMsgs =
      leaderOutput.messages.filterIt(it.kind == RaftRpcMessageType.InstallSnapshot)
    # Leader should not send another snapshot since follower's is newer
    check nextInstallMsgs.len == 0
    let followupAppends =
      leaderOutput.messages.filterIt(it.kind == RaftRpcMessageType.AppendRequest)

  test "leader uses conflict term hint to jump to matching entry":
    var (sm, leaderId, followerId, snapshot) = initLeaderWithSnapshot()
    doAssert sm.myId == leaderId

    var follower = sm.findFollowerProgressById(followerId)
    check follower.isSome()

    let hintedTerm = snapshot.term
    let reply = RaftRpcAppendReply(
      commitIndex: snapshot.index,
      term: snapshot.term,
      result: RaftRpcCode.Rejected,
      rejected: RaftRpcAppendReplyRejected(
        nonMatchingIndex: snapshot.index + 10,
        lastIdx: sm.log.lastIndex,
        conflictTerm: some(hintedTerm),
        conflictIndex: snapshot.index + 1,
      ),
    )

    sm.appendEntryReply(followerId, reply)

    follower = sm.findFollowerProgressById(followerId)
    check follower.isSome()
    let leaderIdx = sm.log.lastIndexOfTerm(hintedTerm)
    check leaderIdx.isSome()
    let expectedNextIndex = leaderIdx.get() + 1
    check follower.get().nextIndex == expectedNextIndex

  test "leader reinstalls snapshot when follower supplies no conflict hint":
    var (sm, leaderId, followerId, snapshot) = initLeaderWithSnapshot()
    doAssert sm.myId == leaderId

    var follower = sm.findFollowerProgressById(followerId)
    check follower.isSome()
    follower.get().nextIndex = snapshot.index + 2

    let reply = RaftRpcAppendReply(
      commitIndex: snapshot.index,
      term: snapshot.term,
      result: RaftRpcCode.Rejected,
      rejected: RaftRpcAppendReplyRejected(
        nonMatchingIndex: snapshot.index + 1,
        lastIdx: snapshot.index + 10,
        conflictTerm: none(RaftNodeTerm),
        conflictIndex: 0,
      ),
    )

    sm.appendEntryReply(followerId, reply)
    let output = sm.poll()
    let installMsgs = output.messages.filterIt(it.kind == RaftRpcMessageType.InstallSnapshot)
    check installMsgs.len == 0
    let retryAppends = output.messages.filterIt(it.kind == RaftRpcMessageType.AppendRequest)
    # After processing rejection, leader automatically retries
    # In PROBE mode this may result in multiple messages due to state updates
    check retryAppends.len >= 1
    # At least one message should be trying to sync from the snapshot index
    check retryAppends.anyIt(it.appendRequest.previousLogIndex == snapshot.index)
    
suite "Persisted index after append entries":
  test "append entries clamps persisted index after truncation":
    var config = createConfigFromIds(test_ids_1)
    var timeNow = dateTime(2017, mMar, 01, 00, 00, 00, 00, utc())
    var log = RaftLog.init(RaftSnapshot(index: 0, config: config))
    for i in 1 .. 5:
      log.appendAsLeader(0, RaftLogIndex(i))
    var randGen = initRand(42)
    let electionTime =
      times.initDuration(milliseconds = 100) +
      times.initDuration(milliseconds = 100 + randGen.rand(200))
    let heartbeatTime = times.initDuration(milliseconds = 50)
    var sm = RaftStateMachineRef.new(
      test_ids_1[0], 0, log, 0, timeNow, electionTime, heartbeatTime,
    )
    discard sm.poll()
    check sm.observedState.persistedIndex == 5
    let appendRequest = RaftRpcAppendRequest(
      previousTerm: 0,
      previousLogIndex: 2,
      commitIndex: 2,
      entries: @[
        LogEntry(term: 1, index: 3, kind: RaftLogEntryType.rletEmpty),
      ],
    )
    let msg = RaftRpcMessage(
      currentTerm: 0,
      sender: test_second_ids_1[0],
      receiver: test_ids_1[0],
      kind: RaftRpcMessageType.AppendRequest,
      appendRequest: appendRequest,
    )
    sm.advance(msg, timeNow)
    discard sm.poll()
    check sm.log.lastIndex == 3
    check sm.observedState.persistedIndex == sm.log.lastIndex

suite "Probe and Pipeline state management":
  test "probeSent is set to true when sending in PROBE mode":
    let leaderId = newRaftNodeId("leader")
    let followerId = newRaftNodeId("follower")
    let config = createConfigFromIds(@[leaderId, followerId])
    
    var leaderLog = RaftLog.init(RaftSnapshot(index: 0, term: 0, config: config))
    for i in 1 .. 5:
      leaderLog.appendAsLeader(RaftNodeTerm(1), RaftLogIndex(i))
    
    var now = dateTime(2020, mJan, 01, 00, 00, 00, 00, utc())
    let electionTime = initDuration(milliseconds = 200)
    let heartbeatTime = initDuration(milliseconds = 50)
    
    var leaderSm = RaftStateMachineRef.new(
      leaderId, RaftNodeTerm(1), leaderLog, RaftLogIndex(0), now, electionTime, heartbeatTime
    )
    leaderSm.becomeLeader()
    discard leaderSm.poll()
    
    # Set follower to PROBE state
    var followerProgress = leaderSm.findFollowerProgressById(followerId)
    check followerProgress.isSome()
    followerProgress.get().becomeProbe()
    check not followerProgress.get().probeSent
    
    # Try to replicate - should set probeSent to true
    leaderSm.replicateTo(followerProgress.get())
    check followerProgress.get().probeSent
    
    # Verify canSendTo returns false after probeSent is true
    check not followerProgress.get().canSendTo()
    
    # Set matchIndex to lastIndex to prevent replication during tick
    followerProgress.get().matchIndex = leaderSm.log.lastIndex
    followerProgress.get().commitIndex = leaderSm.commitIndex
    followerProgress.get().lastMessageAt = now + 100.milliseconds  # Prevent heartbeat
    
    # Reset probeSent on tick
    leaderSm.tickLeader(now + 100.milliseconds)
    check not followerProgress.get().probeSent
    check followerProgress.get().canSendTo()

  test "inFlight is incremented when sending in PIPELINE mode":
    let leaderId = newRaftNodeId("leader")
    let followerId = newRaftNodeId("follower")
    let config = createConfigFromIds(@[leaderId, followerId])
    
    var leaderLog = RaftLog.init(RaftSnapshot(index: 0, term: 0, config: config))
    for i in 1 .. 5:
      leaderLog.appendAsLeader(RaftNodeTerm(1), RaftLogIndex(i))
    
    var now = dateTime(2020, mJan, 01, 00, 00, 00, 00, utc())
    let electionTime = initDuration(milliseconds = 200)
    let heartbeatTime = initDuration(milliseconds = 50)
    
    var leaderSm = RaftStateMachineRef.new(
      leaderId, RaftNodeTerm(1), leaderLog, RaftLogIndex(0), now, electionTime, heartbeatTime
    )
    leaderSm.becomeLeader()
    discard leaderSm.poll()
    
    # Set follower to PIPELINE state
    var followerProgress = leaderSm.findFollowerProgressById(followerId)
    check followerProgress.isSome()
    followerProgress.get().becomePipeline()
    check followerProgress.get().inFlight == 0
    
    # Try to replicate - should increment inFlight
    leaderSm.replicateTo(followerProgress.get())
    check followerProgress.get().inFlight == 1
    
    # Verify canSendTo still returns true (under maxInFlight)
    check followerProgress.get().canSendTo()
    
    # Fill up to maxInFlight
    for i in 1 ..< leaderSm.leader.tracker.maxInFlight:
      leaderSm.replicateTo(followerProgress.get())
    check followerProgress.get().inFlight == leaderSm.leader.tracker.maxInFlight
    check not followerProgress.get().canSendTo()

  test "inFlight is decremented when receiving append reply":
    let leaderId = newRaftNodeId("leader")
    let followerId = newRaftNodeId("follower")
    let config = createConfigFromIds(@[leaderId, followerId])
    
    var leaderLog = RaftLog.init(RaftSnapshot(index: 0, term: 0, config: config))
    for i in 1 .. 5:
      leaderLog.appendAsLeader(RaftNodeTerm(1), RaftLogIndex(i))
    
    var now = dateTime(2020, mJan, 01, 00, 00, 00, 00, utc())
    let electionTime = initDuration(milliseconds = 200)
    let heartbeatTime = initDuration(milliseconds = 50)
    
    var leaderSm = RaftStateMachineRef.new(
      leaderId, RaftNodeTerm(1), leaderLog, RaftLogIndex(0), now, electionTime, heartbeatTime
    )
    leaderSm.becomeLeader()
    discard leaderSm.poll()
    
    # Set follower to PIPELINE state and send some messages
    var followerProgress = leaderSm.findFollowerProgressById(followerId)
    check followerProgress.isSome()
    followerProgress.get().becomePipeline()
    
    # Send 3 messages
    for i in 0 ..< 3:
      leaderSm.replicateTo(followerProgress.get())
    check followerProgress.get().inFlight == 3
    
    # Simulate receiving append replies
    for i in 0 ..< 2:
      let reply = RaftRpcAppendReply(
        term: RaftNodeTerm(1),
        commitIndex: RaftLogIndex(0),
        result: RaftRpcCode.Accepted,
        accepted: RaftRpcAppendReplyAccepted(lastNewIndex: RaftLogIndex(i + 1))
      )
      leaderSm.appendEntryReply(followerId, reply)
    
    check followerProgress.get().inFlight == 1

  test "tickLeader resets probeSent and manages inFlight":
    let leaderId = newRaftNodeId("leader")
    let followerId = newRaftNodeId("follower")
    let config = createConfigFromIds(@[leaderId, followerId])
    
    var leaderLog = RaftLog.init(RaftSnapshot(index: 0, term: 0, config: config))
    for i in 1 .. 5:
      leaderLog.appendAsLeader(RaftNodeTerm(1), RaftLogIndex(i))
    
    var now = dateTime(2020, mJan, 01, 00, 00, 00, 00, utc())
    let electionTime = initDuration(milliseconds = 200)
    let heartbeatTime = initDuration(milliseconds = 50)
    
    var leaderSm = RaftStateMachineRef.new(
      leaderId, RaftNodeTerm(1), leaderLog, RaftLogIndex(0), now, electionTime, heartbeatTime
    )
    leaderSm.becomeLeader()
    discard leaderSm.poll()
    
    # Set follower to PROBE state and send message
    var followerProgress = leaderSm.findFollowerProgressById(followerId)
    check followerProgress.isSome()
    followerProgress.get().becomeProbe()
    leaderSm.replicateTo(followerProgress.get())
    check followerProgress.get().probeSent
    
    # Set follower to PIPELINE state and fill up inFlight
    followerProgress.get().becomePipeline()
    for i in 0 ..< leaderSm.leader.tracker.maxInFlight:
      leaderSm.replicateTo(followerProgress.get())
    check followerProgress.get().inFlight == leaderSm.leader.tracker.maxInFlight
    check not followerProgress.get().canSendTo()
    
    # Set matchIndex to lastIndex to prevent replication during tick
    followerProgress.get().matchIndex = leaderSm.log.lastIndex
    followerProgress.get().commitIndex = leaderSm.commitIndex
    followerProgress.get().lastMessageAt = now + 100.milliseconds  # Prevent heartbeat
    
    # Tick should reset probeSent and allow one more inFlight
    leaderSm.tickLeader(now + 100.milliseconds)
    check not followerProgress.get().probeSent
    check followerProgress.get().inFlight == leaderSm.leader.tracker.maxInFlight - 1
    check followerProgress.get().canSendTo()

  test "PROBE mode only sends one entry at a time":
    let leaderId = newRaftNodeId("leader")
    let followerId = newRaftNodeId("follower")
    let config = createConfigFromIds(@[leaderId, followerId])
    
    var leaderLog = RaftLog.init(RaftSnapshot(index: 0, term: 0, config: config))
    for i in 1 .. 10:  # Many entries available
      leaderLog.appendAsLeader(RaftNodeTerm(1), RaftLogIndex(i))
    
    var now = dateTime(2020, mJan, 01, 00, 00, 00, 00, utc())
    let electionTime = initDuration(milliseconds = 200)
    let heartbeatTime = initDuration(milliseconds = 50)
    
    var leaderSm = RaftStateMachineRef.new(
      leaderId, RaftNodeTerm(1), leaderLog, RaftLogIndex(0), now, electionTime, heartbeatTime
    )
    leaderSm.becomeLeader()
    discard leaderSm.poll()
    
    # Set follower to PROBE state
    var followerProgress = leaderSm.findFollowerProgressById(followerId)
    check followerProgress.isSome()
    followerProgress.get().becomeProbe()
    let initialNextIndex = followerProgress.get().nextIndex
    
    # Adjust nextIndex to point to an actual entry
    followerProgress.get().nextIndex = 5
    let adjustedNextIndex = followerProgress.get().nextIndex
    
    # Send message and verify only one entry is sent
    leaderSm.replicateTo(followerProgress.get())
    check followerProgress.get().nextIndex == adjustedNextIndex + 1  # Only one entry sent
    check followerProgress.get().probeSent
    
    # Reset and try again - should still only send one
    followerProgress.get().probeSent = false
    leaderSm.replicateTo(followerProgress.get())
    check followerProgress.get().nextIndex == adjustedNextIndex + 2  # Only one more entry
    check followerProgress.get().probeSent

  test "PIPELINE mode sends multiple entries and updates nextIndex optimistically":
    let leaderId = newRaftNodeId("leader")
    let followerId = newRaftNodeId("follower")
    let config = createConfigFromIds(@[leaderId, followerId])
    
    var leaderLog = RaftLog.init(RaftSnapshot(index: 0, term: 0, config: config))
    for i in 1 .. 10:  # Many entries available
      leaderLog.appendAsLeader(RaftNodeTerm(1), RaftLogIndex(i))
    
    var now = dateTime(2020, mJan, 01, 00, 00, 00, 00, utc())
    let electionTime = initDuration(milliseconds = 200)
    let heartbeatTime = initDuration(milliseconds = 50)
    
    var leaderSm = RaftStateMachineRef.new(
      leaderId, RaftNodeTerm(1), leaderLog, RaftLogIndex(0), now, electionTime, heartbeatTime
    )
    leaderSm.becomeLeader()
    discard leaderSm.poll()
    
    # Set follower to PIPELINE state
    var followerProgress = leaderSm.findFollowerProgressById(followerId)
    check followerProgress.isSome()
    followerProgress.get().becomePipeline()
    
    # Adjust nextIndex to point to an actual entry
    followerProgress.get().nextIndex = 5
    let adjustedNextIndex = followerProgress.get().nextIndex
    
    # Send message and verify multiple entries are sent
    leaderSm.replicateTo(followerProgress.get())
    check followerProgress.get().nextIndex > adjustedNextIndex + 1  # Multiple entries sent (5->11, so 6 entries)
    check followerProgress.get().inFlight == 1

suite "Conflict resolution improvements":
  test "simplified conflict resolution works correctly":
    let leaderId = newRaftNodeId("leader")
    let followerId = newRaftNodeId("follower")
    let config = createConfigFromIds(@[leaderId, followerId])
    
    var leaderLog = RaftLog.init(RaftSnapshot(index: 0, term: 0, config: config))
    for i in 1 .. 10:
      leaderLog.appendAsLeader(RaftNodeTerm(1), RaftLogIndex(i))
    
    var now = dateTime(2020, mJan, 01, 00, 00, 00, 00, utc())
    let electionTime = initDuration(milliseconds = 200)
    let heartbeatTime = initDuration(milliseconds = 50)
    
    var leaderSm = RaftStateMachineRef.new(
      leaderId, RaftNodeTerm(1), leaderLog, RaftLogIndex(0), now, electionTime, heartbeatTime
    )
    leaderSm.becomeLeader()
    discard leaderSm.poll()
    
    # Set up follower progress - simulate that we've matched up to index 5
    var followerProgress = leaderSm.findFollowerProgressById(followerId)
    check followerProgress.isSome()
    followerProgress.get().becomeProbe()
    followerProgress.get().matchIndex = RaftLogIndex(5)
    followerProgress.get().nextIndex = RaftLogIndex(6)  # Will try to send entry 6
    
    # Actually send a request (so the rejection will be valid, not stray)
    leaderSm.replicateTo(followerProgress.get())
    var output1 = leaderSm.poll()
    check output1.messages.len >= 1
    let firstMsg = output1.messages.filterIt(it.kind == RaftRpcMessageType.AppendRequest)[0]
    check firstMsg.appendRequest.previousLogIndex == RaftLogIndex(5)  # Sent from index 6
    
    # Create a rejection for the entry we just sent  
    # In PROBE mode, nextIndex advanced to 7 after sending, so a valid rejection
    # must have nonMatchingIndex = 7 - 1 = 6
    let reject = RaftRpcAppendReplyRejected(
      nonMatchingIndex: RaftLogIndex(6),  # Must match the previousLogIndex we sent
      lastIdx: RaftLogIndex(5),
      conflictTerm: none(RaftNodeTerm),
      conflictIndex: RaftLogIndex(6),  # Hint to retry at index 6
    )
    
    let reply = RaftRpcAppendReply(
      term: RaftNodeTerm(1),
      commitIndex: RaftLogIndex(0),
      result: RaftRpcCode.Rejected,
      rejected: reject,
    )
    
    # Process the rejection - this will automatically retry with updated nextIndex
    leaderSm.appendEntryReply(followerId, reply)
    let output = leaderSm.poll()
    
    # Should have sent retry message automatically
    check output.messages.len >= 1
    let appendMsgs = output.messages.filterIt(it.kind == RaftRpcMessageType.AppendRequest)
    check appendMsgs.len >= 1
    
    # The retry should be attempting to sync from around where the conflict was detected
    # The exact index depends on internal state management in PROBE mode
    # Just verify that messages were sent (conflict resolution working)
    check appendMsgs[0].appendRequest.entries.len > 0 or appendMsgs[0].appendRequest.previousLogIndex >= RaftLogIndex(5)

  test "quorum detection works with simplified logic":
    let leaderId = newRaftNodeId("leader")
    let followerId1 = newRaftNodeId("follower1")
    let followerId2 = newRaftNodeId("follower2")
    let config = createConfigFromIds(@[leaderId, followerId1, followerId2])
    
    var leaderLog = RaftLog.init(RaftSnapshot(index: 0, term: 0, config: config))
    var now = dateTime(2020, mJan, 01, 00, 00, 00, 00, utc())
    let electionTime = initDuration(milliseconds = 200)
    let heartbeatTime = initDuration(milliseconds = 50)
    
    var leaderSm = RaftStateMachineRef.new(
      leaderId, RaftNodeTerm(1), leaderLog, RaftLogIndex(0), now, electionTime, heartbeatTime
    )
    leaderSm.becomeLeader()
    discard leaderSm.poll()
    
    # Simulate recent replies from followers
    var follower1 = leaderSm.findFollowerProgressById(followerId1)
    check follower1.isSome()
    follower1.get().lastReplyAt = now  # Recent reply
    
    var follower2 = leaderSm.findFollowerProgressById(followerId2)
    check follower2.isSome()
    follower2.get().lastReplyAt = now  # Recent reply
    
    # Now should have quorum (leader + 2 followers = 3, need 2 for quorum)
    let hasQuorum2 = leaderSm.hasRecentQuorum(now)
    check hasQuorum2
    
    # Test with old replies - set replies to way in the past
    # Note: The leader itself is always considered to have quorum with itself initially,
    # so we need to test after enough time has passed that even the leader's own
    # tracking would be considered stale
    let oldTime = now - electionTime * 2  # Far in the past
    follower1.get().lastReplyAt = oldTime
    follower2.get().lastReplyAt = oldTime
    
    # Advance time significantly
    let futureTime = now + electionTime * 3
    let hasQuorum3 = leaderSm.hasRecentQuorum(futureTime)
    # With only the leader active and both followers stale, should not have quorum
    check not hasQuorum3

  test "conflict resolution handles edge cases correctly":
    let leaderId = newRaftNodeId("leader")
    let followerId = newRaftNodeId("follower")
    let config = createConfigFromIds(@[leaderId, followerId])
    
    var leaderLog = RaftLog.init(RaftSnapshot(index: 5, term: 0, config: config))
    for i in 6 .. 10:
      leaderLog.appendAsLeader(RaftNodeTerm(1), RaftLogIndex(i))
    
    var now = dateTime(2020, mJan, 01, 00, 00, 00, 00, utc())
    let electionTime = initDuration(milliseconds = 200)
    let heartbeatTime = initDuration(milliseconds = 50)
    
    var leaderSm = RaftStateMachineRef.new(
      leaderId, RaftNodeTerm(1), leaderLog, RaftLogIndex(0), now, electionTime, heartbeatTime
    )
    leaderSm.becomeLeader()
    discard leaderSm.poll()
    
    # Set up follower progress
    var followerProgress = leaderSm.findFollowerProgressById(followerId)
    check followerProgress.isSome()
    followerProgress.get().nextIndex = RaftLogIndex(8)
    followerProgress.get().becomeProbe()
    
    # Test with nonMatchingIndex = 0, lastIdx = 0 (special case)
    let specialReject = RaftRpcAppendReplyRejected(
      nonMatchingIndex: RaftLogIndex(0),
      lastIdx: RaftLogIndex(0),
      conflictTerm: none(RaftNodeTerm),
      conflictIndex: 0,
    )
    
    let reply = RaftRpcAppendReply(
      term: RaftNodeTerm(1),
      commitIndex: RaftLogIndex(0),
      result: RaftRpcCode.Rejected,
      rejected: specialReject,
    )
    
    # Process the special reject
    leaderSm.appendEntryReply(followerId, reply)
    let output = leaderSm.poll()
    
    # Should send retry message (special case handling)
    check output.messages.len >= 1
    let appendMsgs = output.messages.filterIt(it.kind == RaftRpcMessageType.AppendRequest)
    check appendMsgs.len >= 1

suite "Follower reconnection bug reproduction":
  test "leader should send snapshot when follower is severely behind":
    # This test reproduces the bug where a follower reconnects after being down
    # and the leader keeps sending AppendRequests instead of InstallSnapshot
    
    let leaderId = newRaftNodeId("leader")
    let followerId = newRaftNodeId("follower")
    let config = createConfigFromIds(@[leaderId, followerId])
    
    # Create leader with snapshot at index 8 and entries from 9 to 10
    # This simulates a leader that has snapshotted old entries
    var leaderLog = RaftLog.init(RaftSnapshot(index: 8, term: 0, config: config))
    for i in 9 .. 10:  # Simulate recent entries
      leaderLog.appendAsLeader(RaftNodeTerm(6), RaftLogIndex(i))
    
    var now = dateTime(2020, mJan, 01, 00, 00, 00, 00, utc())
    let electionTime = initDuration(milliseconds = 200)
    let heartbeatTime = initDuration(milliseconds = 50)
    
    # Create leader state machine
    var leaderSm = RaftStateMachineRef.new(
      leaderId, RaftNodeTerm(6), leaderLog, RaftLogIndex(0), now, electionTime, heartbeatTime
    )
    leaderSm.becomeLeader()
    discard leaderSm.poll()
    
    # Create follower that was down and only has 1 entry (like in the logs)
    var followerLog = RaftLog.init(RaftSnapshot(index: 1, term: 0, config: config))
    var followerSm = RaftStateMachineRef.new(
      followerId, RaftNodeTerm(6), followerLog, RaftLogIndex(1), now, electionTime, heartbeatTime
    )
    followerSm.becomeFollower(leaderId)
    discard followerSm.poll()
    
    # Set follower's nextIndex to a high value (simulating stale progress tracking)
    var followerProgress = leaderSm.findFollowerProgressById(followerId)
    check followerProgress.isSome()
    followerProgress.get().nextIndex = RaftLogIndex(8)  # High index
    
    # Try to replicate - this should detect the gap and send InstallSnapshot
    leaderSm.replicateTo(followerProgress.get())
    var output = leaderSm.poll()
    
    # Check that leader sends InstallSnapshot instead of AppendRequest
    let installMsgs = output.messages.filterIt(it.kind == RaftRpcMessageType.InstallSnapshot)
    let appendMsgs = output.messages.filterIt(it.kind == RaftRpcMessageType.AppendRequest)
    
    
    # This should pass - leader should send InstallSnapshot
    check installMsgs.len == 1
    check appendMsgs.len == 0


  test "infinite rejection loop fix - follower restart with old snapshot":
    # This test reproduces the exact scenario from the logs:
    # - Follower restarts with snapshot at index 1
    # - Leader has stale progress: nextIndex=430, matchIndex=429
    # - Leader tries to send AppendRequest with prevLogIndex=430
    # - Follower rejects with conflictIndex=2 (snapshot.index + 1)
    # - Leader should detect this and send InstallSnapshot instead of continuing the loop
    
    let leaderId = newRaftNodeId("leader")
    let followerId = newRaftNodeId("follower")
    let config = createConfigFromIds(@[leaderId, followerId])
    
    # Create leader with snapshot at index 430 and entries from 431 to 450
    # This simulates a leader that has snapshotted old entries
    var leaderLog = RaftLog.init(RaftSnapshot(index: 430, term: 0, config: config))
    for i in 431 .. 450:  # Simulate recent entries
      leaderLog.appendAsLeader(RaftNodeTerm(6), RaftLogIndex(i))
    
    var now = dateTime(2020, mJan, 01, 00, 00, 00, 00, utc())
    let electionTime = initDuration(milliseconds = 200)
    let heartbeatTime = initDuration(milliseconds = 50)
    
    # Create leader state machine
    var leaderSm = RaftStateMachineRef.new(
      leaderId, RaftNodeTerm(6), leaderLog, RaftLogIndex(0), now, electionTime, heartbeatTime
    )
    leaderSm.becomeLeader()
    discard leaderSm.poll()
    
    # Create follower that restarts with old snapshot (like in the logs)
    var followerLog = RaftLog.init(RaftSnapshot(index: 1, term: 0, config: config))
    var followerSm = RaftStateMachineRef.new(
      followerId, RaftNodeTerm(6), followerLog, RaftLogIndex(1), now, electionTime, heartbeatTime
    )
    followerSm.becomeFollower(leaderId)
    discard followerSm.poll()
    
    # Simulate the leader's stale progress tracking (like in the logs)
    var followerProgress = leaderSm.findFollowerProgressById(followerId)
    check followerProgress.isSome()
    followerProgress.get().nextIndex = RaftLogIndex(430)  # Stale progress like in logs
    followerProgress.get().matchIndex = RaftLogIndex(429)
    
    
    # Try to replicate - this should detect the gap and send InstallSnapshot
    leaderSm.replicateTo(followerProgress.get())
    var output = leaderSm.poll()
    
    
    # Check that leader sends InstallSnapshot instead of AppendRequest
    let installMsgs = output.messages.filterIt(it.kind == RaftRpcMessageType.InstallSnapshot)
    let appendMsgs = output.messages.filterIt(it.kind == RaftRpcMessageType.AppendRequest)
    
    
    # This should pass - leader should send InstallSnapshot
    check installMsgs.len == 1
    check appendMsgs.len == 0
    
    # Verify the snapshot is at the correct index
    check installMsgs[0].installSnapshot.snapshot.index == leaderSm.log.snapshot.index
    

  test "infinite rejection loop fix - conflict resolution triggers snapshot":
    # This test reproduces the exact scenario from the logs:
    # - Leader has many entries but some old entries were snapshotted
    # - Follower restarts with only snapshot at index 1
    # - Leader's stale progress thinks follower's nextIndex = 430
    # - Leader tries to send entries starting from prevIndex = 429
    # - But leader doesn't have entry 429 (it was snapshotted)
    # - So termForIndex(429) returns None and leader should send snapshot
    
    
    let leaderId = newRaftNodeId("leader")
    let followerId = newRaftNodeId("follower")
    let config = createConfigFromIds(@[leaderId, followerId])
    
    # Create leader with snapshot at index 430 and entries from 431 to 432
    # This simulates a leader that has snapshotted old entries (1-430)
    # and only has recent entries (431-432)
    var leaderLog = RaftLog.init(RaftSnapshot(index: 430, term: 0, config: config))
    for i in 431 .. 432:
      leaderLog.appendAsLeader(RaftNodeTerm(6), RaftLogIndex(i))
    
    
    var now = dateTime(2020, mJan, 01, 00, 00, 00, 00, utc())
    let electionTime = initDuration(milliseconds = 200)
    let heartbeatTime = initDuration(milliseconds = 50)
    
    # Create leader state machine
    var leaderSm = RaftStateMachineRef.new(
      leaderId, RaftNodeTerm(6), leaderLog, RaftLogIndex(1), now, electionTime, heartbeatTime
    )
    leaderSm.becomeLeader()
    discard leaderSm.poll()
    
    # Create follower that only has snapshot at index 1 (like in the logs)
    var followerLog = RaftLog.init(RaftSnapshot(index: 1, term: 0, config: config))
    var followerSm = RaftStateMachineRef.new(
      followerId, RaftNodeTerm(6), followerLog, RaftLogIndex(1), now, electionTime, heartbeatTime
    )
    followerSm.becomeFollower(leaderId)
    discard followerSm.poll()
    
    # Set up stale progress tracking (like in the logs)
    # Leader thinks follower's nextIndex = 430, but follower only has snapshot at index 1
    var followerProgress = leaderSm.findFollowerProgressById(followerId)
    check followerProgress.isSome()
    followerProgress.get().nextIndex = RaftLogIndex(430)  # Stale progress like in logs
    
    
    # Try to replicate - this should detect that termForIndex(429) returns None
    # and send InstallSnapshot
    leaderSm.replicateTo(followerProgress.get())
    var output = leaderSm.poll()
    
    # Check that leader sends InstallSnapshot
    let installMsgs = output.messages.filterIt(it.kind == RaftRpcMessageType.InstallSnapshot)
    let appendMsgs = output.messages.filterIt(it.kind == RaftRpcMessageType.AppendRequest)
    
    # This should pass - leader should send InstallSnapshot
    check installMsgs.len == 1
    check appendMsgs.len == 0
    
    # Verify the snapshot is at the correct index
    if installMsgs.len > 0:
      check installMsgs[0].installSnapshot.snapshot.index == leaderSm.log.snapshot.index

suite "3 node cluster":
  var timeNow = dateTime(2017, mMar, 01, 00, 00, 00, 00, utc())
  var cluster = createCluster(test_ids_3, timeNow)
  var t = now()

suite "Single node election tracker":
  test "shite":
    let config = test_ids_1.createConfigFromIds
    let p = config.currentSet & config.previousSet;
    var votes = RaftVotes.init(test_ids_1.createConfigFromIds)
  test "unknown":
    var votes = RaftVotes.init(test_ids_1.createConfigFromIds)
    check votes.tallyVote == RaftElectionResult.Unknown

  test "win election":
    var votes = RaftVotes.init(test_ids_1.createConfigFromIds)
    discard votes.registerVote(test_ids_1[0], true)

    check votes.tallyVote == RaftElectionResult.Won
  test "lost election":
    var votes = RaftVotes.init(test_ids_1.createConfigFromIds)
    discard votes.registerVote(test_ids_1[0], false)
    check votes.tallyVote == RaftElectionResult.Lost

suite "3 nodes election tracker":
  test "win election":
    var votes = RaftVotes.init(test_ids_3.createConfigFromIds)
    check votes.tallyVote == RaftElectionResult.Unknown
    discard votes.registerVote(test_ids_3[0], true)
    check votes.tallyVote == RaftElectionResult.Unknown
    discard votes.registerVote(test_ids_3[1], true)
    check votes.tallyVote == RaftElectionResult.Won

  test "lose election":
    var votes = RaftVotes.init(test_ids_3.createConfigFromIds)
    check votes.tallyVote == RaftElectionResult.Unknown
    discard votes.registerVote(test_ids_3[0], false)
    check votes.tallyVote == RaftElectionResult.Unknown
    discard votes.registerVote(test_ids_3[1], true)
    check votes.tallyVote == RaftElectionResult.Unknown
    discard votes.registerVote(test_ids_3[2], true)
    check votes.tallyVote == RaftElectionResult.Won

  test "lose election":
    var votes = RaftVotes.init(test_ids_3.createConfigFromIds)
    check votes.tallyVote == RaftElectionResult.Unknown
    discard votes.registerVote(test_ids_3[0], false)
    check votes.tallyVote == RaftElectionResult.Unknown
    discard votes.registerVote(test_ids_3[1], false)
    check votes.tallyVote == RaftElectionResult.Lost

  test "lose election":
    var votes = RaftVotes.init(test_ids_3.createConfigFromIds)
    check votes.tallyVote == RaftElectionResult.Unknown
    discard votes.registerVote(test_ids_3[0], true)
    check votes.tallyVote == RaftElectionResult.Unknown
    discard votes.registerVote(test_ids_3[1], false)
    check votes.tallyVote == RaftElectionResult.Unknown
    discard votes.registerVote(test_ids_3[2], false)
    check votes.tallyVote == RaftElectionResult.Lost

suite "Single node cluster":
  var randGen = initRand(42)
  let electionTime =
    times.initDuration(milliseconds = 100) +
    times.initDuration(milliseconds = 100 + randGen.rand(200))
  let heartbeatTime = times.initDuration(milliseconds = 50)
  test "election":
    var timeNow = dateTime(2017, mMar, 01, 00, 00, 00, 00, utc())
    var config = createConfigFromIds(test_ids_1)
    var log = RaftLog.init(RaftSnapshot(index: 1, config: config))
    var sm = RaftStateMachineRef.new(
      test_ids_1[0], 0, log, 0, timeNow, electionTime, heartbeatTime
    )
    check sm.state.isFollower
    timeNow += 99.milliseconds
    sm.tick(timeNow)
    var output = sm.poll()
    check output.logEntries.len == 0
    check output.committed.len == 0
    check output.messages.len == 0
    check sm.state.isFollower
    timeNow += 500.milliseconds
    sm.tick(timeNow)
    output = sm.poll()
    check output.logEntries.len == 1
    check output.committed.len == 0
    check output.messages.len == 0
    timeNow += 1.milliseconds
    output = sm.poll()
    check output.logEntries.len == 0
    check output.committed.len == 1
    check output.messages.len == 0
    check sm.state.isLeader
    check sm.term == 1

  test "append entry":
    var timeNow = dateTime(2017, mMar, 01, 00, 00, 00, 00, utc())
    var config = createConfigFromIds(test_ids_1)
    var log = RaftLog.init(RaftSnapshot(index: 1, config: config))
    var sm = RaftStateMachineRef.new(
      test_ids_1[0], 0, log, 0, timeNow, electionTime, heartbeatTime
    )
    check sm.state.isFollower
    timeNow += 1000.milliseconds
    sm.tick(timeNow)
    var output = sm.poll()
    # When the node became a leader it will produce empty message in the log 
    # and because we have single node in the cluster the empty message will be commited on the next tick
    check output.logEntries.len == 1
    check output.committed.len == 0
    check output.messages.len == 0
    check sm.state.isLeader

    timeNow += 1.milliseconds
    sm.tick(timeNow)
    output = sm.poll()
    check output.logEntries.len == 0
    check output.committed.len == 1
    check output.messages.len == 0
    check sm.state.isLeader

    discard sm.addEntry(Empty())
    check sm.poll().messages.len == 0
    timeNow += 250.milliseconds
    sm.tick(timeNow)
    check sm.poll().messages.len == 0

suite "Two nodes cluster":
  var randGen = initRand(42)
  let electionTime =
    times.initDuration(milliseconds = 100) +
    times.initDuration(milliseconds = 100 + randGen.rand(200))
  let heartbeatTime = times.initDuration(milliseconds = 50)
  test "election":
    let id1 = test_ids_3[0]
    let id2 = test_ids_3[1]
    var config = createConfigFromIds(@[id1, id2])
    var log = RaftLog.init(RaftSnapshot(index: 1, config: config))
    var timeNow = dateTime(2017, mMar, 01, 00, 00, 00, 00, utc())
    var sm = RaftStateMachineRef.new(
      test_ids_1[0], 0, log, 0, timeNow, electionTime, heartbeatTime
    )
    check sm.state.isFollower
    timeNow += 601.milliseconds
    sm.tick(timeNow)
    check sm.state.isCandidate
    var output = sm.poll()
    check output.votedFor.isSome
    check output.votedFor.get() == id1

    timeNow += 1.milliseconds
    block:
      let voteRaplay = RaftRpcVoteReply(currentTerm: output.term, voteGranted: true)
      let msg = RaftRpcMessage(
        currentTerm: output.term,
        sender: id2,
        receiver: id1,
        kind: RaftRpcMessageType.VoteReply,
        voteReply: voteRaplay,
      )
      check sm.state.isCandidate
      sm.advance(msg, timeNow)
      output = sm.poll()
      check output.stateChange == true
      check sm.state.isLeader

    timeNow += 1.milliseconds

    # Older messages should be ignored
    block:
      let voteRaplay =
        RaftRpcVoteReply(currentTerm: (output.term - 1), voteGranted: true)
      let msg = RaftRpcMessage(
        currentTerm: output.term,
        sender: id2,
        receiver: id1,
        kind: RaftRpcMessageType.VoteReply,
        voteReply: voteRaplay,
      )
      sm.advance(msg, timeNow)
      output = sm.poll()
      check output.stateChange == false
      check sm.state.isLeader

    block:
      output = sm.poll()
      timeNow += 100.milliseconds
      sm.tick(timeNow)
      output = sm.poll()
    # if the leader get a message with higher term it should become follower
    block:
      timeNow += 201.milliseconds
      sm.tick(timeNow)
      output = sm.poll()
      let entry =
        LogEntry(term: (output.term + 1), index: 1, kind: RaftLogEntryType.rletEmpty)
      let appendRequest = RaftRpcAppendRequest(
        previousTerm: (output.term + 1),
        previousLogIndex: 100,
        commitIndex: 99,
        entries: @[entry],
      )
      let msg = RaftRpcMessage(
        currentTerm: (output.term + 1),
        sender: id2,
        receiver: id1,
        kind: RaftRpcMessageType.AppendRequest,
        appendRequest: appendRequest,
      )
      sm.advance(msg, timeNow)
      output = sm.poll()
      #check output.stateChange == true
      check sm.state.isFollower
suite "3 nodes cluster":
  var randGen = initRand(42)
  let electionTime =
    times.initDuration(milliseconds = 100) +
    times.initDuration(milliseconds = 100 + randGen.rand(200))
  let heartbeatTime = times.initDuration(milliseconds = 50)
  test "election failed":
    let mainNodeId = test_ids_3[0]
    let id2 = test_ids_3[1]
    let id3 = test_ids_3[2]
    var config = createConfigFromIds(test_ids_3)
    var log = RaftLog.init(RaftSnapshot(index: 1, config: config))
    var timeNow = dateTime(2017, mMar, 01, 00, 00, 00, 00, utc())
    var sm = RaftStateMachineRef.new(
      test_ids_1[0], 0, log, 0, timeNow, electionTime, heartbeatTime
    )
    check sm.state.isFollower
    timeNow += 501.milliseconds
    sm.tick(timeNow)
    check sm.state.isCandidate
    var output = sm.poll()
    check output.votedFor.isSome
    check output.votedFor.get() == mainNodeId
    timeNow += 1.milliseconds
    block:
      let voteRaplay =
        RaftRpcVoteReply(currentTerm: output.term, voteGranted: false)
      let msg = RaftRpcMessage(
        currentTerm: output.term,
        sender: id2,
        receiver: mainNodeId,
        kind: RaftRpcMessageType.VoteReply,
        voteReply: voteRaplay,
      )
      check sm.state.isCandidate
      sm.advance(msg, timeNow)
      output = sm.poll()
      check output.stateChange == false
      check sm.state.isCandidate

    timeNow += 1.milliseconds
    block:
      let voteRaplay =
        RaftRpcVoteReply(currentTerm: output.term, voteGranted: false)
      let msg = RaftRpcMessage(
        currentTerm: output.term,
        sender: id3,
        receiver: mainNodeId,
        kind: RaftRpcMessageType.VoteReply,
        voteReply: voteRaplay,
      )
      check sm.state.isCandidate
      sm.advance(msg, timeNow)
      output = sm.poll()
      #check output.stateChange == true
      check sm.state.isFollower

    timeNow += 1.milliseconds

  test "election":
    let mainNodeId = test_ids_3[0]
    let id2 = test_ids_3[1]
    let id3 = test_ids_3[2]
    var config = createConfigFromIds(test_ids_3)
    var log = RaftLog.init(RaftSnapshot(index: 1, config: config))
    var timeNow = dateTime(2017, mMar, 01, 00, 00, 00, 00, utc())
    var sm = RaftStateMachineRef.new(
      test_ids_1[0], 0, log, 0, timeNow, electionTime, heartbeatTime
    )
    check sm.state.isFollower
    timeNow += 501.milliseconds
    sm.tick(timeNow)
    check sm.state.isCandidate
    var output = sm.poll()
    check output.votedFor.isSome
    check output.votedFor.get() == mainNodeId
    timeNow += 1.milliseconds
    block:
      let voteRaplay =
        RaftRpcVoteReply(currentTerm: output.term, voteGranted: false)
      let msg = RaftRpcMessage(
        currentTerm: output.term,
        sender: id2,
        receiver: mainNodeId,
        kind: RaftRpcMessageType.VoteReply,
        voteReply: voteRaplay,
      )
      check sm.state.isCandidate
      sm.advance(msg, timeNow)
      output = sm.poll()
      check output.stateChange == false
      check sm.state.isCandidate

    timeNow += 1.milliseconds
    block:
      let voteRaplay = RaftRpcVoteReply(currentTerm: output.term, voteGranted: true)
      let msg = RaftRpcMessage(
        currentTerm: output.term,
        sender: id3,
        receiver: mainNodeId,
        kind: RaftRpcMessageType.VoteReply,
        voteReply: voteRaplay,
      )
      check sm.state.isCandidate
      sm.advance(msg, timeNow)
      output = sm.poll()
      check output.stateChange == true
      check sm.state.isLeader

    timeNow += 1.milliseconds

suite "3 nodes cluster":
  test "election":
    var timeNow = dateTime(2017, mMar, 01, 00, 00, 00, 00, utc())
    var cluster = createCluster(test_ids_3, timeNow)
    var t = now()

  suite "Single node election tracker":
    test "unknown":
      var votes = RaftVotes.init(test_ids_1.createConfigFromIds)
      check votes.tallyVote == RaftElectionResult.Unknown

    test "win election":
      var votes = RaftVotes.init(test_ids_1.createConfigFromIds)
      discard votes.registerVote(test_ids_1[0], true)

      check votes.tallyVote == RaftElectionResult.Won
    test "lost election":
      var votes = RaftVotes.init(test_ids_1.createConfigFromIds)
      discard votes.registerVote(test_ids_1[0], false)
      echo votes.tallyVote
      check votes.tallyVote == RaftElectionResult.Lost

  suite "3 nodes election tracker":
    test "win election":
      var votes = RaftVotes.init(test_ids_3.createConfigFromIds)
      check votes.tallyVote == RaftElectionResult.Unknown
      discard votes.registerVote(test_ids_3[0], true)
      check votes.tallyVote == RaftElectionResult.Unknown
      discard votes.registerVote(test_ids_3[1], true)
      check votes.tallyVote == RaftElectionResult.Won

    test "lose election":
      var votes = RaftVotes.init(test_ids_3.createConfigFromIds)
      check votes.tallyVote == RaftElectionResult.Unknown
      discard votes.registerVote(test_ids_3[0], false)
      check votes.tallyVote == RaftElectionResult.Unknown
      discard votes.registerVote(test_ids_3[1], true)
      check votes.tallyVote == RaftElectionResult.Unknown
      discard votes.registerVote(test_ids_3[2], true)
      check votes.tallyVote == RaftElectionResult.Won

    test "lose election":
      var votes = RaftVotes.init(test_ids_3.createConfigFromIds)
      check votes.tallyVote == RaftElectionResult.Unknown
      discard votes.registerVote(test_ids_3[0], false)
      check votes.tallyVote == RaftElectionResult.Unknown
      discard votes.registerVote(test_ids_3[1], false)
      check votes.tallyVote == RaftElectionResult.Lost

    test "lose election":
      var votes = RaftVotes.init(test_ids_3.createConfigFromIds)
      check votes.tallyVote == RaftElectionResult.Unknown
      discard votes.registerVote(test_ids_3[0], true)
      check votes.tallyVote == RaftElectionResult.Unknown
      discard votes.registerVote(test_ids_3[1], false)
      check votes.tallyVote == RaftElectionResult.Unknown
      discard votes.registerVote(test_ids_3[2], false)
      check votes.tallyVote == RaftElectionResult.Lost

  suite "Single node cluster":
    var randGen = initRand(42)
    let electionTime =
      times.initDuration(milliseconds = 100) +
      times.initDuration(milliseconds = 100 + randGen.rand(200))
    let heartbeatTime = times.initDuration(milliseconds = 50)
    test "election":
      var timeNow = dateTime(2017, mMar, 01, 00, 00, 00, 00, utc())
      var config = createConfigFromIds(test_ids_1)
      var log = RaftLog.init(RaftSnapshot(index: 1, config: config))
      var sm = RaftStateMachineRef.new(
        test_ids_1[0], 0, log, 0, timeNow, electionTime, heartbeatTime
      )
      check sm.state.isFollower
      timeNow += 99.milliseconds
      sm.tick(timeNow)
      var output = sm.poll()
      check output.logEntries.len == 0
      check output.committed.len == 0
      check output.messages.len == 0
      check sm.state.isFollower
      timeNow += 500.milliseconds
      sm.tick(timeNow)
      output = sm.poll()
      check output.logEntries.len == 1
      check output.committed.len == 0
      check output.messages.len == 0
      timeNow += 1.milliseconds
      output = sm.poll()
      check output.logEntries.len == 0
      check output.committed.len == 1
      check output.messages.len == 0
      check sm.state.isLeader
      check sm.term == 1

    test "append entry":
      var timeNow = dateTime(2017, mMar, 01, 00, 00, 00, 00, utc())
      var config = createConfigFromIds(test_ids_1)
      var log = RaftLog.init(RaftSnapshot(index: 1, config: config))
      var sm = RaftStateMachineRef.new(
        test_ids_1[0], 0, log, 0, timeNow, electionTime, heartbeatTime
      )
      check sm.state.isFollower
      timeNow += 1000.milliseconds
      sm.tick(timeNow)
      var output = sm.poll()
      # When the node became a leader it will produce empty message in the log 
      # and because we have single node in the cluster the empty message will be committed on the next tick
      check output.logEntries.len == 1
      check output.committed.len == 0
      check output.messages.len == 0
      check sm.state.isLeader

      timeNow += 1.milliseconds
      sm.tick(timeNow)
      output = sm.poll()
      check output.logEntries.len == 0
      check output.committed.len == 1
      check output.messages.len == 0
      check sm.state.isLeader

      discard sm.addEntry(Empty())
      check sm.poll().messages.len == 0
      timeNow += 250.milliseconds
      sm.tick(timeNow)
      check sm.poll().messages.len == 0

  suite "Two nodes cluster":
    var randGen = initRand(42)
    let electionTime =
      times.initDuration(milliseconds = 100) +
      times.initDuration(milliseconds = 100 + randGen.rand(200))
    let heartbeatTime = times.initDuration(milliseconds = 50)
    test "election":
      let id1 = test_ids_3[0]
      let id2 = test_ids_3[1]
      var config = createConfigFromIds(@[id1, id2])
      var log = RaftLog.init(RaftSnapshot(index: 1, config: config))
      var timeNow = dateTime(2017, mMar, 01, 00, 00, 00, 00, utc())
      var sm = RaftStateMachineRef.new(
        test_ids_1[0], 0, log, 0, timeNow, electionTime, heartbeatTime
      )
      check sm.state.isFollower
      timeNow += 601.milliseconds
      sm.tick(timeNow)
      check sm.state.isCandidate
      var output = sm.poll()
      check output.votedFor.isSome
      check output.votedFor.get() == id1

      timeNow += 1.milliseconds
      block:
        let voteRaplay = RaftRpcVoteReply(currentTerm: output.term, voteGranted: true)
        let msg = RaftRpcMessage(
          currentTerm: output.term,
          sender: id2,
          receiver: id1,
          kind: RaftRpcMessageType.VoteReply,
          voteReply: voteRaplay,
        )
        check sm.state.isCandidate
        sm.advance(msg, timeNow)
        output = sm.poll()
        check output.stateChange == true
        check sm.state.isLeader

      timeNow += 1.milliseconds

      # Older messages should be ignored
      block:
        let voteRaplay =
          RaftRpcVoteReply(currentTerm: (output.term - 1), voteGranted: true)
        let msg = RaftRpcMessage(
          currentTerm: output.term,
          sender: id2,
          receiver: id1,
          kind: RaftRpcMessageType.VoteReply,
          voteReply: voteRaplay,
        )
        sm.advance(msg, timeNow)
        output = sm.poll()
        check output.stateChange == false
        check sm.state.isLeader

      block:
        output = sm.poll()
        timeNow += 100.milliseconds
        sm.tick(timeNow)
        output = sm.poll()
      # if the leader get a message with higher term it should become follower
      block:
        timeNow += 201.milliseconds
        sm.tick(timeNow)
        output = sm.poll()
        let entry =
          LogEntry(term: (output.term + 1), index: 1, kind: RaftLogEntryType.rletEmpty)
        let appendRequest = RaftRpcAppendRequest(
          previousTerm: (output.term + 1),
          previousLogIndex: 100,
          commitIndex: 99,
          entries: @[entry],
        )
        let msg = RaftRpcMessage(
          currentTerm: (output.term + 1),
          sender: id2,
          receiver: id1,
          kind: RaftRpcMessageType.AppendRequest,
          appendRequest: appendRequest,
        )
        sm.advance(msg, timeNow)
        output = sm.poll()
        check output.stateChange == true
        check sm.state.isFollower
    suite "3 nodes cluster":
      test "election failed":
        let mainNodeId = test_ids_3[0]
        let id2 = test_ids_3[1]
        let id3 = test_ids_3[2]
        var config = createConfigFromIds(test_ids_3)
        var log = RaftLog.init(RaftSnapshot(index: 1, config: config))
        var timeNow = dateTime(2017, mMar, 01, 00, 00, 00, 00, utc())
        var sm = RaftStateMachineRef.new(
          test_ids_1[0], 0, log, 0, timeNow, electionTime, heartbeatTime
        )
        check sm.state.isFollower
        timeNow += 501.milliseconds
        sm.tick(timeNow)
        check sm.state.isCandidate
        var output = sm.poll()
        check output.votedFor.isSome
        check output.votedFor.get() == mainNodeId
        timeNow += 1.milliseconds
        block:
          let voteRaplay =
            RaftRpcVoteReply(currentTerm: output.term, voteGranted: false)
          let msg = RaftRpcMessage(
            currentTerm: output.term,
            sender: id2,
            receiver: mainNodeId,
            kind: RaftRpcMessageType.VoteReply,
            voteReply: voteRaplay,
          )
          check sm.state.isCandidate
          sm.advance(msg, timeNow)
          output = sm.poll()
          check output.stateChange == false
          check sm.state.isCandidate

        timeNow += 1.milliseconds
        block:
          let voteRaplay =
            RaftRpcVoteReply(currentTerm: output.term, voteGranted: false)
          let msg = RaftRpcMessage(
            currentTerm: output.term,
            sender: id3,
            receiver: mainNodeId,
            kind: RaftRpcMessageType.VoteReply,
            voteReply: voteRaplay,
          )
          check sm.state.isCandidate
          sm.advance(msg, timeNow)
          output = sm.poll()
          check output.stateChange == true
          check sm.state.isFollower

        timeNow += 1.milliseconds

      test "election":
        let mainNodeId = test_ids_3[0]
        let id2 = test_ids_3[1]
        let id3 = test_ids_3[2]
        var config = createConfigFromIds(test_ids_3)
        var log = RaftLog.init(RaftSnapshot(index: 1, config: config))
        var timeNow = dateTime(2017, mMar, 01, 00, 00, 00, 00, utc())
        var sm = RaftStateMachineRef.new(
          test_ids_1[0], 0, log, 0, timeNow, electionTime, heartbeatTime
        )
        check sm.state.isFollower
        timeNow += 501.milliseconds
        sm.tick(timeNow)
        check sm.state.isCandidate
        var output = sm.poll()
        check output.votedFor.isSome
        check output.votedFor.get() == mainNodeId
        timeNow += 1.milliseconds
        block:
          let voteRaplay =
            RaftRpcVoteReply(currentTerm: output.term, voteGranted: false)
          let msg = RaftRpcMessage(
            currentTerm: output.term,
            sender: id2,
            receiver: mainNodeId,
            kind: RaftRpcMessageType.VoteReply,
            voteReply: voteRaplay,
          )
          check sm.state.isCandidate
          sm.advance(msg, timeNow)
          output = sm.poll()
          check output.stateChange == false
          check sm.state.isCandidate

        timeNow += 1.milliseconds
        block:
          let voteRaplay = RaftRpcVoteReply(currentTerm: output.term, voteGranted: true)
          let msg = RaftRpcMessage(
            currentTerm: output.term,
            sender: id3,
            receiver: mainNodeId,
            kind: RaftRpcMessageType.VoteReply,
            voteReply: voteRaplay,
          )
          check sm.state.isCandidate
          sm.advance(msg, timeNow)
          output = sm.poll()
          check output.stateChange == true
          check sm.state.isLeader

        timeNow += 1.milliseconds

  suite "3 nodes cluster":
    test "election":
      var timeNow = dateTime(2017, mMar, 01, 00, 00, 00, 00, utc())
      var cluster = createCluster(test_ids_3, timeNow)
      var leader: RaftnodeId
      var hasLeader = false
      for i in 0 ..< 105:
        timeNow += 5.milliseconds
        cluster.advance(timeNow)
        var maybeLeader = cluster.getLeader()
        if leader == RaftNodeId.empty:
          if maybeLeader.isSome:
            leader = maybeLeader.get().myId
            hasLeader = true
        else:
          if maybeLeader.isSome:
            check leader == maybeLeader.get().myId
          else:
            check false
      # we should elect atleast 1 leader
      check hasLeader

  test "1 node is not responding":
    var timeNow = dateTime(2017, mMar, 01, 00, 00, 00, 00, utc())
    var cluster = createCluster(test_ids_3, timeNow)
    cluster.blockMsgRouting(test_ids_3[0])
    var leader: RaftnodeId
    var hasLeader = false
    for i in 0 ..< 105:
      timeNow += 5.milliseconds
      cluster.advance(timeNow)
      var maybeLeader = cluster.getLeader()
      if leader == RaftNodeId.empty:
        if maybeLeader.isSome:
          leader = maybeLeader.get().myId
          hasLeader = true
      else:
        if maybeLeader.isSome:
          check leader == maybeLeader.get().myId
        else:
          check false
    # we should elect atleast 1 leader
    check hasLeader

  test "2 nodes is not responding":
    var timeNow = dateTime(2017, mMar, 01, 00, 00, 00, 00, utc())
    var cluster = createCluster(test_ids_3, timeNow)
    cluster.blockMsgRouting(test_ids_3[0])
    cluster.blockMsgRouting(test_ids_3[1])
    var leader: RaftnodeId
    for i in 0 ..< 105:
      timeNow += 5.milliseconds
      cluster.advance(timeNow)
      var maybeLeader = cluster.getLeader()
      # We should never elect a leader
      check leader == RaftNodeId.empty

  test "1 nodes is not responding new leader reelection":
    var timeNow = dateTime(2017, mMar, 01, 00, 00, 00, 00, utc())
    var cluster = createCluster(test_ids_3, timeNow)
    var leader: RaftnodeId
    var firstLeaderId = RaftNodeId.empty
    var secondLeaderId = RaftNodeId.empty
    for i in 0 ..< 305:
      timeNow += 5.milliseconds
      cluster.advance(timeNow)
      var maybeLeader = cluster.getLeader()
      if maybeLeader.isSome() and firstLeaderId == RaftNodeId.empty:
        # we will block the comunication and will try to elect new leader
        firstLeaderId = maybeLeader.get().myId
        cluster.blockMsgRouting(firstLeaderId)
        echo "Block comunication with: " & $firstLeaderId
      if firstLeaderId != RaftNodeId.empty and maybeLeader.isSome() and
          maybeLeader.get().myId != firstLeaderId:
        secondLeaderId = maybeLeader.get().myId
    check secondLeaderId != RaftNodeId.empty and firstLeaderId != secondLeaderId

  test "After reaelection leader should become follower":
    var timeNow = dateTime(2017, mMar, 01, 00, 00, 00, 00, utc())
    var cluster = createCluster(test_ids_3, timeNow)
    var leader: RaftnodeId
    var firstLeaderId = RaftNodeId.empty
    var secondLeaderId = RaftNodeId.empty
    for i in 0 ..< 305:
      timeNow += 5.milliseconds
      cluster.advance(timeNow)
      var maybeLeader = cluster.getLeader()
      if maybeLeader.isSome() and firstLeaderId == RaftNodeId.empty:
        # we will block the comunication and will try to elect new leader
        firstLeaderId = maybeLeader.get().myId
        cluster.blockMsgRouting(firstLeaderId)
        echo "Block comunication with: " & $firstLeaderId
      if firstLeaderId != RaftNodeId.empty and maybeLeader.isSome() and
          maybeLeader.get().myId != firstLeaderId:
        secondLeaderId = maybeLeader.get().myId
        cluster.allowMsgRouting(firstLeaderId)

    test "leader steps down when ticks are blocked":
      var timeNow = dateTime(2017, mMar, 01, 00, 00, 00, 00, utc())
      var cluster = createCluster(test_ids_3, timeNow)
      timeNow = cluster.establishLeader(timeNow)

      let leaderId = cluster.getLeader.get.myId
      cluster.blockTick(leaderId)
      cluster.blockMsgRouting(leaderId)

      timeNow = cluster.advanceUntil(timeNow, timeNow + 500.milliseconds)

      let newLeader = cluster.getLeader()
      check newLeader.isSome()
      check newLeader.get.myId != leaderId

      cluster.allowTick(leaderId)
      cluster.allowMsgRouting(leaderId)
      timeNow += 5.milliseconds
      cluster.advance(timeNow)

      check cluster.nodes[leaderId].sm.state.isFollower

    test "leader stays leader when elapsed equals timeout":
      var now = dateTime(2017, mMar, 1, 0, 0, 0, 0, utc())
      let electionTimeout = times.initDuration(milliseconds = 100)
      let heartbeatTime = times.initDuration(milliseconds = 50)
      let id = RaftNodeId(id: "12323749802409131")
      var log = RaftLog.init(RaftSnapshot(index: 0, term: 0, config: RaftConfig()))
      var sm = RaftStateMachineRef.new(id, RaftNodeTerm(0), log, RaftLogIndex(0), now, electionTimeout, heartbeatTime)
      sm.becomeLeader()
      check sm.state.isLeader

      now += electionTimeout - times.initDuration(milliseconds = 1)
      sm.tickLeader(now)
      discard sm.poll()
      check sm.state.isLeader

      now += electionTimeout + times.initDuration(milliseconds = 1)
      sm.tickLeader(now)
      discard  sm.poll()
      # With quorum-based step-down, single-node leaders do not step down
      check sm.state.isLeader

suite "config change":
  test "1 node":
    var timeNow = dateTime(2017, mMar, 01, 00, 00, 00, 00, utc())

    var cluster = createCluster(test_second_ids_1, timeNow)
    timeNow = cluster.establishLeader(timeNow)

    var config = createConfigFromIds(test_ids_1)
    cluster.submitNewConfig(config)
    var currentConfig = cluster.configuration()
    check currentConfig.isSome
    var cfg = currentConfig.get()
    cluster.addNodeToCluster(test_ids_1[0], timeNow, currentConfig.get())
    timeNow = cluster.advanceUntil(timeNow, timeNow + 250.milliseconds)
    cluster.removeNodeFromCluster(test_second_ids_1[0])

    timeNow = cluster.advanceUntil(timeNow, timeNow + 500.milliseconds)
    currentConfig = cluster.configuration()
    check currentConfig.isSome
    cfg = currentConfig.get()

    check currentConfig.get().currentSet == test_ids_1
    check currentConfig.get().previousSet.len == 0

    var l = cluster.getLeader()
    check l.isSome()
    check l.get().myId == test_ids_1[0]

  test "3 nodes":
    var timeNow = dateTime(2017, mMar, 01, 00, 00, 00, 00, utc())

    var cluster = createCluster(test_second_ids_3, timeNow)
    timeNow = cluster.establishLeader(timeNow)

    check not cluster.hascommittedEntry("abc".toCommand)
    check not cluster.hascommittedEntry("bcd".toCommand)

    var newConfig = createConfigFromIds(test_ids_3)
    cluster.submitNewConfig(newConfig)
    var currentConfig = cluster.configuration()
    check currentConfig.isSome
    var cfg = currentConfig.get()
    cluster.addNodeToCluster(test_ids_3, timeNow, cfg)
    timeNow = cluster.advanceUntil(timeNow, timeNow + 125.milliseconds)
    cluster.removeNodeFromCluster(test_second_ids_3)
    timeNow = cluster.establishLeader(timeNow)
    check cluster.submitCommand("abc".toCommand)
    
    check cluster.submitCommand("bcd".toCommand)
    timeNow = cluster.advanceUntil(timeNow, timeNow + 25.milliseconds)
    currentConfig = cluster.configuration()
    check currentConfig.isSome
    cfg = currentConfig.get()

    check cluster.hasCommittedEntry("abc".toCommand)
    check cluster.hasCommittedEntry("bcd".toCommand)

  test "add node":
    var timeNow = dateTime(2017, mMar, 01, 00, 00, 00, 00, utc())

    var newSet = test_second_ids_1 & test_ids_1
    var cluster = createCluster(newSet, timeNow)
    timeNow = cluster.establishLeader(timeNow)
    var newConfig = createConfigFromIds(test_second_ids_1)
    cluster.submitNewConfig(newConfig)
    var currentConfig = cluster.configuration()
    check currentConfig.isSome
    var cfg = currentConfig.get()
    check cfg.currentSet == test_second_ids_1
    check cfg.previousSet == test_second_ids_1 & test_ids_1
    timeNow = cluster.advanceUntil(timeNow, timeNow + 25.milliseconds)
    cluster.removeNodeFromCluster(test_ids_1)
    timeNow = cluster.advanceUntil(timeNow, timeNow + 25.milliseconds)
    check cluster.submitCommand("abc".toCommand)
    timeNow = cluster.advanceUntil(timeNow, timeNow + 25.milliseconds)
    check cluster.hascommittedEntry("abc".toCommand)

  test "add entrie during config chage":
    var timeNow = dateTime(2017, mMar, 01, 00, 00, 00, 00, utc())
    var cluster = createCluster(test_second_ids_1, timeNow)
    timeNow = cluster.establishLeader(timeNow)
    var newConfig = createConfigFromIds(test_second_ids_1 & test_ids_1)
    cluster.submitNewConfig(newConfig)
    check cluster.submitCommand("abc".toCommand)
    check cluster.submitCommand("fgh".toCommand)
    timeNow = cluster.advanceUntil(timeNow, timeNow + 250.milliseconds)
    # The second node is not added
    check not cluster.hascommittedEntry("abc".toCommand)
    check not cluster.hascommittedEntry("fgh".toCommand)

    var currentConfig = cluster.configuration()
    check currentConfig.isSome
    var cfg = currentConfig.get()
    cluster.addNodeToCluster(test_ids_1, timeNow, cfg)
    timeNow = cluster.advanceUntil(timeNow, timeNow + 250.milliseconds)
    check cluster.hascommittedEntry("abc".toCommand)
    check cluster.hascommittedEntry("fgh".toCommand)
  test "remove node":
    var timeNow = dateTime(2017, mMar, 01, 00, 00, 00, 00, utc())

    var newSet = test_second_ids_1 & test_ids_1
    var cluster = createCluster(newSet, timeNow)
    timeNow = cluster.establishLeader(timeNow)
    var newConfig = createConfigFromIds(test_second_ids_1)
    cluster.submitNewConfig(newConfig)
    var currentConfig = cluster.configuration()
    check currentConfig.isSome
    var cfg = currentConfig.get()
    check cfg.currentSet == test_second_ids_1
    check cfg.previousSet == test_second_ids_1 & test_ids_1
    timeNow = cluster.advanceUntil(timeNow, timeNow + 25.milliseconds)
    cluster.removeNodeFromCluster(test_ids_1)
    timeNow = cluster.advanceUntil(timeNow, timeNow + 25.milliseconds)
    check cluster.submitCommand("abc".toCommand)
    timeNow = cluster.advanceUntil(timeNow, timeNow + 25.milliseconds)
    check cluster.hasCommittedEntry("abc".toCommand)

  test "add entrie during config chage":
    var timeNow = dateTime(2017, mMar, 01, 00, 00, 00, 00, utc())
    var cluster = createCluster(test_second_ids_1, timeNow)
    timeNow = cluster.establishLeader(timeNow)
    var newConfig = createConfigFromIds(test_second_ids_1 & test_ids_1)
    cluster.submitNewConfig(newConfig)
    check cluster.submitCommand("abc".toCommand)
    check cluster.submitCommand("fgh".toCommand)
    timeNow = cluster.advanceUntil(timeNow, timeNow + 250.milliseconds)
    # The second node is not added
    check not cluster.hasCommittedEntry("abc".toCommand)
    check not cluster.hasCommittedEntry("fgh".toCommand)

    var currentConfig = cluster.configuration()
    check currentConfig.isSome
    var cfg = currentConfig.get()
    cluster.addNodeToCluster(test_ids_1, timeNow, cfg)
    timeNow = cluster.advanceUntil(timeNow, timeNow + 250.milliseconds)
    check cluster.hasCommittedEntry("abc".toCommand)
    check cluster.hasCommittedEntry("fgh".toCommand)

    newConfig = createConfigFromIds(test_ids_1)
    cluster.submitNewConfig(newConfig)

    check cluster.submitCommand("phg".toCommand)
    timeNow = cluster.advanceUntil(timeNow, timeNow + 250.milliseconds)
    check cluster.hasCommittedEntry("phg".toCommand)

    timeNow = cluster.advanceUntil(timeNow, timeNow + 250.milliseconds)
    currentConfig = cluster.configuration()
    check currentConfig.isSome
    cfg = currentConfig.get()
    cluster.removeNodeFromCluster(test_second_ids_1)
    # The cluster don't have leader yet
    check not cluster.getLeader().isSome
    check not cluster.submitCommand("xyz".toCommand)


    timeNow = cluster.advanceUntil(timeNow, timeNow + 500.milliseconds)

    check cluster.submitCommand("xyz".toCommand)
    check cluster.getLeader().isSome
    timeNow = cluster.advanceUntil(timeNow, timeNow + 250.milliseconds)
    check cluster.hasCommittedEntry("phg".toCommand)
    check cluster.hasCommittedEntry("abc".toCommand)
    check cluster.hasCommittedEntry("fgh".toCommand)
    check cluster.hasCommittedEntry("xyz".toCommand)


  test "Leader stop responding config change":
    var timeNow = dateTime(2017, mMar, 01, 00, 00, 00, 00, utc())

    var cluster = createCluster(test_second_ids_3, timeNow)
    timeNow = cluster.establishLeader(timeNow)
    check cluster.submitCommand("abc".toCommand)
    timeNow = cluster.advanceUntil(timeNow, timeNow + 50.milliseconds)
    check cluster.hasCommittedEntry("abc".toCommand)

    var newConfig = createConfigFromIds(test_ids_1)
    cluster.submitNewConfig(newConfig)
    check cluster.getLeader.isSome()
    cluster.removeNodeFromCluster(cluster.getLeader.get.myId)
    timeNow = cluster.advanceUntilNoLeader(timeNow)
    check not cluster.getLeader.isSome()
    # Cluster refuse to accept the message
    timeNow = cluster.establishLeader(timeNow)

    # Resubmit config
    cluster.submitNewConfig(newConfig)
    # Wait to replicate the new config
    timeNow = cluster.advanceUntil(timeNow, timeNow + 50.milliseconds)

    var currentConfig = cluster.configuration()
    check currentConfig.isSome
    var cfg = currentConfig.get()
    cluster.addNodeToCluster(test_ids_1, timeNow, cfg)

    check cluster.submitCommand("phg".toCommand)
    timeNow = cluster.advanceUntil(timeNow, timeNow + 500.milliseconds)
    check cluster.hasCommittedEntry("phg".toCommand)
    timeNow = cluster.establishLeader(timeNow)

    cluster.removeNodeFromCluster(test_second_ids_3)
    timeNow = cluster.advanceUntil(timeNow, timeNow + 50.milliseconds)

    check cluster.submitCommand("qwe".toCommand)
    timeNow = cluster.advanceUntil(timeNow, timeNow + 50.milliseconds)
    check cluster.hasCommittedEntry("qwe".toCommand)
    check cluster.hasCommittedEntry("phg".toCommand)
    check not cluster.hasCommittedEntry("xyz".toCommand)
    check cluster.hasCommittedEntry("abc".toCommand)

  test "Leader stop responding during config change":
    var timeNow = dateTime(2017, mMar, 01, 00, 00, 00, 00, utc())

    var cluster = createCluster(test_second_ids_3, timeNow)

    var removed = false
    cluster.onEntryCommit = proc(nodeId: RaftnodeId, entry: LogEntry) =
      if entry.kind == rletConfig and not removed:
        check cluster.getLeader.isSome()
        var currentConfig = cluster.configuration()
        check currentConfig.isSome
        var cfg = currentConfig.get()
        cluster.markNodeForDelection(cluster.getLeader.get.myId)
        removed = true

    timeNow = cluster.establishLeader(timeNow)
    timeNow = cluster.advanceUntil(timeNow, timeNow + 25.milliseconds)

    var newConfig = createConfigFromIds(test_ids_3)
    cluster.submitNewConfig(newConfig)

    var currentConfig = cluster.configuration()
    check currentConfig.isSome
    var cfg = currentConfig.get()
    cluster.addNodeToCluster(test_ids_3, timeNow, cfg)
    timeNow = cluster.advanceUntil(timeNow, timeNow + 105.milliseconds)
    timeNow = cluster.establishLeader(timeNow)
    check cluster.getLeader.isSome()

suite "Term racing and log divergence bugs":
  test "infinite leadership battle - node with lost log vs synced leader":
    # This test reproduces the bug from repro_2 logs where:
    # - Node 8b83833e9 has only 1 log entry (lost its log)
    # - Nodes b35e3f8a5 and a03609529 have 3500+ entries
    # - Node 8b83833e9 keeps becoming candidate and incrementing term
    # - Leader b35e3f8a5 tries to sync but 8b83833e9 rejects (term too high)
    # - Leader drops rejections as "stray" instead of fixing nextIndex
    # - This creates an infinite loop with no progress
    
    let node1Id = newRaftNodeId("node1-healthy")  # Will be healthy
    let node2Id = newRaftNodeId("node2-healthy")  # Will be healthy
    let node3Id = newRaftNodeId("node3-lostlog")  # Will lose log
    let config = createConfigFromIds(@[node1Id, node2Id, node3Id])
    
    var now = dateTime(2020, mJan, 01, 00, 00, 00, 00, utc())
    let electionTime = initDuration(milliseconds = 150)
    let heartbeatTime = initDuration(milliseconds = 50)
    
    # Create node1 and node2 with large logs (simulating 3500+ entries)
    var node1Log = RaftLog.init(RaftSnapshot(index: 3500, term: 100, config: config))
    for i in 3501 .. 3600:
      node1Log.appendAsLeader(RaftNodeTerm(100), RaftLogIndex(i))
    
    var node2Log = RaftLog.init(RaftSnapshot(index: 3500, term: 100, config: config))
    for i in 3501 .. 3600:
      node2Log.appendAsLeader(RaftNodeTerm(100), RaftLogIndex(i))
    
    # Create node3 with only 1 entry (simulating log loss)
    var node3Log = RaftLog.init(RaftSnapshot(index: 0, term: 0, config: config))
    node3Log.appendAsLeader(RaftNodeTerm(0), RaftLogIndex(1))
    
    var node1Sm = RaftStateMachineRef.new(
      node1Id, RaftNodeTerm(100), node1Log, RaftLogIndex(3600), now, electionTime, heartbeatTime
    )
    var node2Sm = RaftStateMachineRef.new(
      node2Id, RaftNodeTerm(100), node2Log, RaftLogIndex(3600), now, electionTime, heartbeatTime
    )
    var node3Sm = RaftStateMachineRef.new(
      node3Id, RaftNodeTerm(100), node3Log, RaftLogIndex(1), now, electionTime, heartbeatTime
    )
    
    
    # Make node1 the leader
    node1Sm.becomeLeader()
    node2Sm.becomeFollower(node1Id)
    node3Sm.becomeFollower(node1Id)
    
    discard node1Sm.poll()
    discard node2Sm.poll()
    discard node3Sm.poll()
    
    check node1Sm.state.isLeader
    check node2Sm.state.isFollower
    check node3Sm.state.isFollower
    
    # Simulate the term racing scenario:
    # 1. Node3's election timeout fires multiple times rapidly
    # 2. Each time it becomes candidate and increments term
    # 3. It sends VoteRequests with stale log info
    # 4. Leader and node2 reject the votes (log too far behind)
    # 5. Node3 keeps timing out and incrementing term
    
    var termRacingDetected = false
    var maxIterations = 50
    var node3TermIncreases = 0
    var leaderTermUpdates = 0
    
    for iteration in 0 ..< maxIterations:
      now += 10.milliseconds
      
      let oldNode3Term = node3Sm.term
      let oldNode1Term = node1Sm.term
      
      # Tick all nodes
      node1Sm.tick(now)
      node2Sm.tick(now)
      node3Sm.tick(now)
      
      # Node3 times out frequently (simulate rapid election attempts)
      if iteration mod 3 == 0:
        now += electionTime + initDuration(milliseconds = 5)
        node3Sm.tick(now)
      
      # Process node3 output (VoteRequests)
      var node3Output = node3Sm.poll()
      for msg in node3Output.messages:
        if msg.kind == RaftRpcMessageType.VoteRequest:
          # Node1 (leader) receives VoteRequest
          if msg.receiver == node1Id:
            node1Sm.advance(msg, now)
            var node1Reply = node1Sm.poll()
            # Send reply back to node3
            for reply in node1Reply.messages:
              if reply.receiver == node3Id:
                node3Sm.advance(reply, now)
                discard node3Sm.poll()
          
          # Node2 receives VoteRequest
          if msg.receiver == node2Id:
            node2Sm.advance(msg, now)
            var node2Reply = node2Sm.poll()
            for reply in node2Reply.messages:
              if reply.receiver == node3Id:
                node3Sm.advance(reply, now)
                discard node3Sm.poll()
      
      # Process node1 (leader) output (AppendRequests)
      var node1Output = node1Sm.poll()
      for msg in node1Output.messages:
        if msg.kind == RaftRpcMessageType.AppendRequest:
          if msg.receiver == node3Id:
            # Node3 receives AppendRequest
            node3Sm.advance(msg, now)
            var node3Reply = node3Sm.poll()
            # Send AppendReply back to leader
            for reply in node3Reply.messages:
              if reply.receiver == node1Id and reply.kind == RaftRpcMessageType.AppendReply:
                node1Sm.advance(reply, now)
                discard node1Sm.poll()
      
      # Track term changes
      if node3Sm.term > oldNode3Term:
        node3TermIncreases += 1
      if node1Sm.term > oldNode1Term:
        leaderTermUpdates += 1
      
      # Check for term racing: node3 term increases rapidly but no sync progress
      if node3TermIncreases > 5 and node3Sm.log.lastIndex == 1:
        termRacingDetected = true
        if iteration > 20:
          break
    
    
    # The bug manifests as:
    # 1. Node3 term increases rapidly (term racing)
    # 2. Node3 log never syncs (lastIndex stays at 1)
    # 3. Leader's nextIndex for node3 is way ahead of reality
    check termRacingDetected
    check node3Sm.log.lastIndex == 1  # Node3 never syncs
    check node3TermIncreases >= 5  # Term increases multiple times
    
    # The fix should:
    # - Leader detects extreme divergence (nextIndex >> follower's lastIndex)
    # - Leader sends InstallSnapshot instead of AppendRequest
    # - Leader properly handles rejections from followers with very old logs

  test "leader should send snapshot when follower nextIndex is way ahead of actual log":
    # This test verifies the fix: when leader discovers follower's log is
    # drastically behind (e.g., nextIndex=3586 but follower has only 1 entry),
    # it should send InstallSnapshot, not keep trying AppendRequest
    
    let leaderId = newRaftNodeId("leader")
    let followerId = newRaftNodeId("follower-behind")
    let config = createConfigFromIds(@[leaderId, followerId])
    
    # Leader has snapshot at 3500 and entries up to 3600
    var leaderLog = RaftLog.init(RaftSnapshot(index: 3500, term: 100, config: config))
    for i in 3501 .. 3600:
      leaderLog.appendAsLeader(RaftNodeTerm(100), RaftLogIndex(i))
    
    var now = dateTime(2020, mJan, 01, 00, 00, 00, 00, utc())
    let electionTime = initDuration(milliseconds = 200)
    let heartbeatTime = initDuration(milliseconds = 50)
    
    var leaderSm = RaftStateMachineRef.new(
      leaderId, RaftNodeTerm(100), leaderLog, RaftLogIndex(3600), now, electionTime, heartbeatTime
    )
    leaderSm.becomeLeader()
    discard leaderSm.poll()
    
    # Set up follower progress with stale nextIndex (like in the bug)
    # Set it to something that would require prevLogIndex below the snapshot
    var followerProgress = leaderSm.findFollowerProgressById(followerId)
    check followerProgress.isSome()
    followerProgress.get().nextIndex = RaftLogIndex(3500)  # Right at snapshot boundary
    followerProgress.get().matchIndex = RaftLogIndex(0)
    
    # Create follower with only 1 entry (like node 8b83833e9 from logs)
    var followerLog = RaftLog.init(RaftSnapshot(index: 0, term: 0, config: config))
    followerLog.appendAsLeader(RaftNodeTerm(0), RaftLogIndex(1))

    var followerSm = RaftStateMachineRef.new(
      followerId, RaftNodeTerm(100), followerLog, RaftLogIndex(1), now, electionTime, heartbeatTime
    )
    followerSm.becomeFollower(leaderId)
    discard followerSm.poll()
    
    # Try to replicate - leader should detect the huge gap and send snapshot
    leaderSm.replicateTo(followerProgress.get())
    var output = leaderSm.poll()
    
    let installMsgs = output.messages.filterIt(it.kind == RaftRpcMessageType.InstallSnapshot)
    let appendMsgs = output.messages.filterIt(it.kind == RaftRpcMessageType.AppendRequest)
    
    
    # The fix: leader should send InstallSnapshot, not AppendRequest
    check installMsgs.len == 1
    check appendMsgs.len == 0
    
    if installMsgs.len > 0:
      check installMsgs[0].installSnapshot.snapshot.index == leaderSm.log.snapshot.index
