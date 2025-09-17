# nim-raft
# Copyright (c) 2023 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

import types
import log
import tracker
import state
import config
import poll_state

import std/[times]
import std/strformat

type
  RaftRpcMessageType* = enum
    VoteRequest = 0
    VoteReply = 1
    AppendRequest = 2
    AppendReply = 3
    InstallSnapshot = 4
    SnapshotReply = 5

  RaftRpcCode* = enum
    Accepted = 0
    Rejected = 1

  DebugLogLevel* = enum
    None = 0
    Critical = 1
    Error = 2
    Warning = 3
    Debug = 4
    Info = 5
    All = 6

  DebugLogEntry* = object
    level*: DebugLogLevel
    time*: times.DateTime
    nodeId*: RaftNodeId
    term*: RaftNodeTerm
    state*: RaftNodeState
    msg*: string

  RaftRpcAppendRequest* = object
    previousTerm*: RaftNodeTerm
    previousLogIndex*: RaftLogIndex
    commitIndex*: RaftLogIndex
    entries*: seq[LogEntry]

  RaftRpcAppendReplyRejected* = object
    nonMatchingIndex*: RaftLogIndex
    lastIdx*: RaftLogIndex
    # Conflict optimization hints (optional)
    # If follower knows the term of the conflicting entry, it sets
    # conflictTerm to that term and conflictIndex to the first index
    # in its log with that term. Otherwise (log too short or below snapshot),
    # conflictTerm is none and conflictIndex is a hint where to resume.
    conflictTerm*: Option[RaftNodeTerm]
    conflictIndex*: RaftLogIndex

  RaftRpcAppendReplyAccepted* = object
    lastNewIndex*: RaftLogIndex

  RaftRpcAppendReply* = object
    commitIndex*: RaftLogIndex
    term*: RaftNodeTerm
    case result*: RaftRpcCode
    of Accepted: accepted*: RaftRpcAppendReplyAccepted
    of Rejected: rejected*: RaftRpcAppendReplyRejected

  RaftRpcVoteRequest* = object
    currentTerm*: RaftNodeTerm
    lastLogIndex*: RaftLogIndex
    lastLogTerm*: RaftNodeTerm
    force*: bool

  RaftRpcVoteReply* = object
    currentTerm*: RaftNodeTerm
    voteGranted*: bool

  RaftInstallSnapshot* = object
    term*: RaftNodeTerm
    snapshot*: RaftSnapshot

  RaftSnapshotReply* = object
    term*: RaftNodeTerm
    success*: bool

  RaftRpcMessage* = object
    currentTerm*: RaftNodeTerm
    sender*: RaftNodeId
    receiver*: RaftNodeId
    case kind*: RaftRpcMessageType
    of VoteRequest: voteRequest*: RaftRpcVoteRequest
    of VoteReply: voteReply*: RaftRpcVoteReply
    of AppendRequest: appendRequest*: RaftRpcAppendRequest
    of AppendReply: appendReply*: RaftRpcAppendReply
    of InstallSnapshot: installSnapshot*: RaftInstallSnapshot
    of SnapshotReply: snapshotReply*: RaftSnapshotReply

  RaftStateMachineRefOutput* = object
    logEntries*: seq[LogEntry]
    # Entries that should be applyed to the "User" State machine
    committed*: seq[LogEntry]
    messages*: seq[RaftRpcMessage]
    debugLogs*: seq[DebugLogEntry]
    term*: RaftNodeTerm
    config*: Option[RaftConfig]
    votedFor*: Option[RaftNodeId]
    stateChange*: bool
    applyedSnapshots: Option[RaftSnapshot]
    snapshotsToDrop: seq[RaftSnapshot]

  RaftStateMachineRef* = ref object
    myId*: RaftNodeId
    term*: RaftNodeTerm
    commitIndex*: RaftLogIndex
    toCommit: RaftLogIndex
    log*: RaftLog
    output: RaftStateMachineRefOutput
    lastUpdate: times.Time
    votedFor: RaftNodeId
    pingLeader: bool

    lastElectionTime: times.DateTime
    randomizedElectionTime: times.Duration
    heartbeatTime: times.Duration
    timeNow: times.DateTime
    startTime: times.DateTime
    # Last time the leader had evidence of contact with a quorum
    lastQuorumTime: times.DateTime

    observedState*: RaftLastPollState
    state*: RaftStateMachineRefState

func eq(ps: RaftLastPollState, sm: RaftStateMachineRef): bool =
  ps.term() == sm.term and ps.votedFor() == sm.votedFor and
    ps.commitIndex() == sm.commitIndex and ps.persistedIndex() == sm.log.lastIndex

func leader*(sm: RaftStateMachineRef): var LeaderState =
  sm.state.leader

func follower*(sm: RaftStateMachineRef): var FollowerState =
  sm.state.follower

func candidate*(sm: RaftStateMachineRef): var CandidateState =
  sm.state.candidate

func isLeader*(sm: RaftStateMachineRef): bool =
  sm.state.isLeader

func isFollower*(sm: RaftStateMachineRef): bool =
  sm.state.isFollower

func isCandidate*(sm: RaftStateMachineRef): bool =
  sm.state.isCandidate

func checkInvariants*(sm: RaftStateMachineRef) =
  doAssert sm.commitIndex >= sm.log.snapshot.index,
    fmt"commit index {sm.commitIndex} behind snapshot {sm.log.snapshot.index}"
  doAssert sm.commitIndex <= sm.log.lastIndex,
    fmt"commit index {sm.commitIndex} beyond log {sm.log.lastIndex}"

const loglevel {.intdefine.}: int = int(DebugLogLevel.Error)
const pollCheckInvariants {.booldefine.} = false

template addDebugLogEntry(
    smArg: RaftStateMachineRef, levelArg: DebugLogLevel, messageArg: string
) =
  let sm = smArg
  let level = levelArg
  let message = messageArg
  if loglevel >= int(level):
    sm.output.debugLogs.add(
      DebugLogEntry(
        time: sm.timeNow,
        state: sm.state.state,
        term: sm.term,
        level: level,
        msg: message,
        nodeId: sm.myId,
      )
    )

template debug*(sm: RaftStateMachineRef, log: string) =
  when loglevel >= int(DebugLogLevel.Debug):
    sm.addDebugLogEntry(DebugLogLevel.Debug, log)

template warning*(sm: RaftStateMachineRef, log: string) =
  when loglevel >= int(DebugLogLevel.Warning):
    sm.addDebugLogEntry(DebugLogLevel.Warning, log)

template error*(sm: RaftStateMachineRef, log: string) =
  when loglevel >= int(DebugLogLevel.Error):
    sm.addDebugLogEntry(DebugLogLevel.Error, log)

template info*(sm: RaftStateMachineRef, log: string) =
  when loglevel >= int(DebugLogLevel.Info):
    sm.addDebugLogEntry(DebugLogLevel.Info, log)

template critical*(sm: RaftStateMachineRef, log: string) =
  when loglevel >= int(DebugLogLevel.Critical):
    sm.addDebugLogEntry(DebugLogLevel.Critical, log)

# Forward declaration for use in hasRecentQuorum
func findFollowerProgressById*(
    sm: RaftStateMachineRef, id: RaftNodeId
): Option[RaftFollowerProgressTrackerRef]

func hasRecentQuorum(sm: RaftStateMachineRef, now: times.DateTime): bool =
  # Count active members in a set: those with recent replies
  proc activeInSet(ids: seq[RaftNodeId]): int =
    var count = 0
    for id in ids:
      if id == sm.myId:
        count += 1
      else:
        let p = sm.findFollowerProgressById(id)
        if p.isSome:
          if now - p.get().lastReplyAt <= sm.randomizedElectionTime:
            count += 1
    count

  let quorumCurrent = int(sm.leader.tracker.current.len div 2) + 1
  if sm.leader.tracker.current.len == 0:
    return true
  let activeCurrent = activeInSet(sm.leader.tracker.current)
  if sm.log.configuration.isJoint:
    let quorumPrev = int(sm.leader.tracker.previous.len div 2) + 1
    let activePrev = activeInSet(sm.leader.tracker.previous)
    return activeCurrent >= quorumCurrent and activePrev >= quorumPrev
  else:
    return activeCurrent >= quorumCurrent

func getLeaderId*(sm: RaftStateMachineRef): Option[RaftNodeId] =
  if sm.state.isLeader:
    return some(sm.myId)
  elif sm.state.isFollower and sm.state.follower.leader != RaftNodeId.empty:
    return some(sm.state.follower.leader)
  none(RaftNodeId)

func observe*(ps: var RaftLastPollState, sm: RaftStateMachineRef) =
  ps.setTerm sm.term
  ps.setVotedFor sm.votedFor
  ps.setCommitIndex sm.commitIndex
  ps.setPersistedIndex sm.log.lastIndex

func replicationStatus*(sm: RaftStateMachineRef): string =
  var progressStrings = newSeqOfCap[string](sm.leader.tracker.progress.len)

  const reportPreamble = "\nReplication report\n"
  const separator = "=============\n"
  var size = reportPreamble.len

  for p in sm.leader.tracker.progress:
    let str = $p
    size += str.len + separator.len
    progressStrings.add(str)

  var report = newStringOfCap(size)
  report.add(reportPreamble)
  for p in progressStrings:
    report.add(separator)
    report.add($p)

  assert report.len == size
  report

func new*(
    T: type RaftStateMachineRef,
    id: RaftNodeId,
    currentTerm: RaftNodeTerm,
    log: RaftLog,
    commitIndex: RaftLogIndex,
    now: times.DateTime,
    randomizedElectionTime: times.Duration,
    heartbeatTime: times.Duration,
): T =
  var sm = T()
  sm.term = currentTerm
  sm.log = log
  sm.commitIndex = max(log.snapshot.index, min(commitIndex, log.lastIndex))
  sm.state = initFollower(RaftNodeId.empty)
  sm.lastElectionTime = now
  sm.timeNow = now
  sm.startTime = now
  sm.myId = id
  sm.heartbeatTime = heartbeatTime
  sm.randomizedElectionTime = randomizedElectionTime
  sm.observedState.observe(sm)
  sm.output = RaftStateMachineRefOutput()
  sm.lastQuorumTime = now
  sm.checkInvariants()
  return sm

func currentLeader(sm: RaftStateMachineRef): RaftNodeId =
  if sm.state.isFollower:
    return sm.state.follower.leader
  RaftNodeId.empty

func findFollowerProgressById*(
    sm: RaftStateMachineRef, id: RaftNodeId
): Option[RaftFollowerProgressTrackerRef] =
  sm.leader.tracker.find(id)

func sendToImpl*(
    sm: RaftStateMachineRef, id: RaftNodeId, request: RaftRpcAppendRequest
) =
  sm.output.messages.add(
    RaftRpcMessage(
      currentTerm: sm.term,
      receiver: id,
      sender: sm.myId,
      kind: RaftRpcMessageType.AppendRequest,
      appendRequest: request,
    )
  )

func sendToImpl*(sm: RaftStateMachineRef, id: RaftNodeId, request: RaftRpcAppendReply) =
  sm.output.messages.add(
    RaftRpcMessage(
      currentTerm: sm.term,
      receiver: id,
      sender: sm.myId,
      kind: RaftRpcMessageType.AppendReply,
      appendReply: request,
    )
  )

func sendToImpl*(sm: RaftStateMachineRef, id: RaftNodeId, request: RaftRpcVoteRequest) =
  sm.output.messages.add(
    RaftRpcMessage(
      currentTerm: sm.term,
      receiver: id,
      sender: sm.myId,
      kind: RaftRpcMessageType.VoteRequest,
      voteRequest: request,
    )
  )

func sendToImpl*(sm: RaftStateMachineRef, id: RaftNodeId, request: RaftRpcVoteReply) =
  sm.output.messages.add(
    RaftRpcMessage(
      currentTerm: sm.term,
      receiver: id,
      sender: sm.myId,
      kind: RaftRpcMessageType.VoteReply,
      voteReply: request,
    )
  )

func sendToImpl*(sm: RaftStateMachineRef, id: RaftNodeId, request: RaftSnapshotReply) =
  sm.output.messages.add(
    RaftRpcMessage(
      currentTerm: sm.term,
      receiver: id,
      sender: sm.myId,
      kind: RaftRpcMessageType.SnapshotReply,
      snapshotReply: request,
    )
  )

func sendToImpl*(
    sm: RaftStateMachineRef, id: RaftNodeId, request: RaftInstallSnapshot
) =
  sm.output.messages.add(
    RaftRpcMessage(
      currentTerm: sm.term,
      receiver: id,
      sender: sm.myId,
      kind: RaftRpcMessageType.InstallSnapshot,
      installSnapshot: request,
    )
  )

func applySnapshot*(sm: RaftStateMachineRef, snapshot: RaftSnapshot, local: bool): bool =
  sm.debug "Apply snapshot" & $snapshot
  doAssert (local and snapshot.index <= sm.observedState.commitIndex) or (not local and sm.isFollower)

  let current = sm.log.snapshot
  if snapshot.index <= current.index or (not local and snapshot.index <= sm.commitIndex):
    sm.debug "Ignore outdated snapshot " & $snapshot & " " & $current
    sm.commitIndex = max(sm.commitIndex, snapshot.index)
    sm.checkInvariants()
    return true

  sm.commitIndex = max(sm.commitIndex, snapshot.index)
  sm.output.applyedSnapshots = some(snapshot)

  sm.log.applySnapshot(snapshot)
  let newPersistedIndex = max(sm.observedState.persistedIndex, snapshot.index)
  sm.observedState.setPersistedIndex min(newPersistedIndex, sm.log.lastIndex)
  sm.checkInvariants()
  true

func sendTo[MsgType](sm: RaftStateMachineRef, id: RaftNodeId, request: MsgType) =
  if sm.state.isLeader:
    var follower = sm.findFollowerProgressById(id)
    if follower.isSome:
      follower.get().lastMessageAt = sm.timeNow
    else:
      sm.warning "Follower not found: " & $id
  #sm.debug "Sent to " & $request
  sm.sendToImpl(id, request)

func createVoteRequest*(sm: RaftStateMachineRef): RaftRpcMessage =
  RaftRpcMessage(
    currentTerm: sm.term,
    sender: sm.myId,
    kind: VoteRequest,
    voteRequest: RaftRpcVoteRequest(),
  )

func replicateTo*(sm: RaftStateMachineRef, follower: RaftFollowerProgressTrackerRef) =
  if follower.nextIndex > sm.log.lastIndex:
    return

  var previousTerm = sm.log.termForIndex(follower.nextIndex - 1)
  #sm.debug "Replicate to " & $follower[]
  if not previousTerm.isSome:
    when loglevel >= int(DebugLogLevel.Debug):
      debugEcho follower.nextIndex - 1, sm.log.lastIndex, sm.log.firstIndex, sm.log.entriesCount
    let snapshot = sm.log.snapshot
    follower.replayedIndex = snapshot.index
    sm.sendTo(follower.id, RaftInstallSnapshot(term: sm.term, snapshot: snapshot))
    return
  var request = RaftRpcAppendRequest(
    previousTerm: previousTerm.get(),
    previousLogIndex: follower.nextIndex - 1,
    commitIndex: sm.commitIndex,
    entries: @[],
  )

  let nextIdx = follower.nextIndex
  let toSendCount = int(sm.log.lastIndex - nextIdx + 1)
  if toSendCount > 0:
    request.entries = newSeqOfCap[LogEntry](toSendCount)
  for i in nextIdx .. sm.log.lastIndex:
    request.entries.add(sm.log.getEntryByIndex(i))
    follower.nextIndex += 1
  sm.sendTo(follower.id, request)

func replicate*(sm: RaftStateMachineRef) =
  if sm.state.isLeader:
    for followerIndex in 0 ..< sm.leader.tracker.progress.len:
      if sm.myId != sm.leader.tracker.progress[followerIndex].id:
        sm.replicateTo(sm.leader.tracker.progress[followerIndex])

func configuration*(sm: RaftStateMachineRef): RaftConfig =
  sm.log.configuration

func addEntry(sm: RaftStateMachineRef, entry: LogEntry): LogEntry =
  var tmpEntry = entry
  if not sm.state.isLeader:
    sm.error "Only the leader can handle new entries"
    return

  if entry.kind == rletConfig:
    if sm.log.lastConfigIndex > sm.commitIndex or sm.log.configuration.isJoint:
      sm.error "The previous configuration is not committed yet"
      return
    sm.debug "Configuration change"
    # 4.3. Arbitrary configuration changes using joint consensus
    var tmpCfg = RaftConfig(currentSet: sm.log.configuration().currentSet)
    tmpCfg.enterJoint(tmpEntry.config.currentSet)
    tmpEntry.config = tmpCfg

  sm.log.appendAsLeader(tmpEntry)
  if entry.kind == rletConfig:
    sm.debug "Update leader config"
    # 4.1. Cluster membership changes/Safety.
    #
    # The new configuration takes effect on each server as
    # soon as it is added to that server’s log
    sm.leader.tracker.setConfig(sm.log.configuration, sm.log.lastIndex, sm.timeNow)
  entry

func addEntry*(sm: RaftStateMachineRef, command: Command): LogEntry =
  sm.addEntry(
    LogEntry(
      term: sm.term, index: sm.log.nextIndex, kind: rletCommand, command: command
    )
  )

func addEntry*(sm: RaftStateMachineRef, config: RaftConfig): LogEntry =
  sm.debug "new config entry" & $config.currentSet
  sm.addEntry(
    LogEntry(term: sm.term, index: sm.log.nextIndex, kind: rletConfig, config: config)
  )

func addEntry*(sm: RaftStateMachineRef, dummy: Empty): LogEntry =
  sm.addEntry(LogEntry(term: sm.term, index: sm.log.nextIndex, kind: rletEmpty))

func becomeFollower*(sm: RaftStateMachineRef, leaderId: RaftNodeId) =
  if sm.myId == leaderId:
    sm.error "Can't be follower of itself"
  sm.output.stateChange = sm.output.stateChange or not sm.state.isFollower
  sm.state = initFollower(leaderId)
  if leaderId != RaftNodeId.empty:
    sm.pingLeader = false
    sm.lastElectionTime = sm.timeNow
  sm.debug "Become follower with leader" & $leaderId

func becomeLeader*(sm: RaftStateMachineRef) =
  if sm.state.isLeader:
    sm.error "The leader can't become leader second time"
    return
  sm.debug "Become leader"
  sm.output.stateChange = true

  # Because we will add new entry on the next line
  sm.state = initLeader(sm.log.configuration, sm.log.lastIndex + 1, sm.timeNow)
  discard sm.addEntry(Empty())
  sm.pingLeader = false
  sm.lastElectionTime = sm.timeNow
  sm.lastQuorumTime = sm.timeNow
  sm.info "Become leader"
  return

func becomeCandidate*(sm: RaftStateMachineRef) =
  if not sm.state.isCandidate:
    sm.output.stateChange = true

  sm.state = initCandidate(sm.log.configuration)
  sm.debug "Become candidate with config" & $sm.log.configuration
  sm.lastElectionTime = sm.timeNow

  if not sm.candidate.votes.contains(sm.myId):
    if sm.log.lastConfigIndex <= sm.commitIndex:
      sm.debug "The node is not part of the current cluster"
      sm.becomeFollower(RaftNodeId.empty)
      return

  sm.term = sm.term + 1
  for nodeId in sm.candidate.votes.voters:
    if nodeId == sm.myId:
      sm.debug "register vote for it self in term" & $sm.term
      discard sm.candidate.votes.registerVote(nodeId, true)
      sm.votedFor = nodeId
      continue

    let request = RaftRpcVoteRequest(
      currentTerm: sm.term,
      lastLogIndex: sm.log.lastIndex,
      lastLogTerm: sm.log.lastTerm,
      force: false,
    )
    sm.sendTo(nodeId, request)
  if sm.candidate.votes.tallyVote == RaftElectionResult.Won:
    sm.debug "Elecation won" & $(sm.candidate.votes) & $sm.myId
    sm.becomeLeader()

func heartbeat(sm: RaftStateMachineRef, follower: var RaftFollowerProgressTrackerRef) =
  #sm.info "heartbeat " & $follower.nextIndex
  var previousTerm = RaftNodeTerm(0)
  # If the log of the followeris already in sync then we should use the last intedex
  let previousLogIndex = RaftLogIndex(min(follower.nextIndex - 1, sm.log.lastIndex))
  if sm.log.lastIndex > 1:
    previousTerm = sm.log.termForIndex(previousLogIndex).get()
  let request = RaftRpcAppendRequest(
    previousTerm: previousTerm,
    previousLogIndex: previousLogIndex,
    commitIndex: sm.commitIndex,
    entries: @[],
  )
  sm.debug "Send heartbeat to " & $follower
  sm.sendTo(follower.id, request)

func tickLeader*(sm: RaftStateMachineRef, now: times.DateTime) =
  assert sm.timeNow <= now
  sm.timeNow = now
  if not sm.state.isLeader:
    sm.error "tickLeader can be called only on the leader"
    return

  # Step-down heuristic: if leader hasn't heard from a quorum recently,
  # it steps down to allow a new election.
  if not sm.hasRecentQuorum(now):
    if sm.timeNow - sm.lastQuorumTime > sm.randomizedElectionTime:
      sm.debug "Leader stepping down due to lost quorum"
      sm.becomeFollower(RaftNodeId.empty)
      return
  else:
    sm.lastQuorumTime = now
  for followerIndex in 0 ..< sm.leader.tracker.progress.len:
    var follower = sm.leader.tracker.progress[followerIndex]
    if sm.myId != follower.id:
      if follower.matchIndex < sm.log.lastIndex or follower.commitIndex < sm.commitIndex:
        sm.replicateTo(follower)

      if now - follower.lastMessageAt > sm.heartbeatTime:
        sm.heartbeat(follower)

func tick*(sm: RaftStateMachineRef, now: times.DateTime) =
  sm.info "Term: " & $sm.term & " commit idx " & $sm.commitIndex &
    " Time since last update: " & $(now - sm.timeNow).inMilliseconds &
    "ms time until election:" &
    $(sm.randomizedElectionTime - (sm.timeNow - sm.lastElectionTime)).inMilliseconds &
    "ms"
  sm.timeNow = now

  sm.debug "Time until election " & $(sm.randomizedElectionTime - (now - sm.lastElectionTime)).inMilliseconds & "ms"
  if sm.state.isLeader:
    sm.tickLeader(now)
  elif sm.timeNow - sm.lastElectionTime > sm.randomizedElectionTime:
    sm.becomeCandidate()
    

func commit(sm: RaftStateMachineRef) =
  if not sm.state.isLeader:
    return

  sm.checkInvariants()
  if sm.commitIndex == sm.log.lastIndex:
    return

  let newCommitIndex = sm.leader.tracker.committed(sm.commitIndex)
  if newCommitIndex <= sm.commitIndex:
    return

  let configurationChange =
    sm.commitIndex < sm.log.lastConfigIndex and newCommitIndex >= sm.log.lastConfigIndex
  let logFirstIndex = sm.log.snapshot.index + 1
  if newCommitIndex < logFirstIndex or newCommitIndex > sm.log.lastIndex:
    sm.error "new commit index " & $newCommitIndex & " out of range [" & $logFirstIndex & "," & $sm.log.lastIndex & "]"
    return
  if sm.log.getEntryByIndex(newCommitIndex).term != sm.term:
    sm.error "connot commit because new entry has different term"
    return

  sm.commitIndex = newCommitIndex
  sm.checkInvariants()
  if configurationChange:
    sm.debug "committed conf change at log index" & $sm.log.lastIndex
    if sm.log.configuration.isJoint:
      var config = sm.log.configuration
      config.leaveJoint()
      sm.log.appendAsLeader(
        LogEntry(
          term: sm.term, index: sm.log.nextIndex, kind: rletConfig, config: config
        )
      )
      sm.leader.tracker.setConfig(config, sm.log.lastIndex, sm.timeNow)

func poll*(sm: RaftStateMachineRef): RaftStateMachineRefOutput =
  if sm.state.isLeader:
    sm.replicate()
    sm.commit()

  when pollCheckInvariants:
    sm.log.checkInvariant()

  if sm.state.isCandidate:
    sm.debug("Current vote status " & $sm.candidate.votes.current)

  sm.output.term = sm.term
  if sm.observedState.persistedIndex > sm.log.lastIndex:
    sm.observedState.setPersistedIndex sm.log.lastIndex

  let lastIndex = sm.log.lastIndex
  let persistedIndex = sm.observedState.persistedIndex
  let diff = lastIndex - persistedIndex
  doAssert lastIndex >= persistedIndex 
  if diff > 0:
    sm.output.logEntries = newSeqOfCap[LogEntry](diff)
    for i in (persistedIndex + 1) .. lastIndex:
      if not sm.log.hasIndex(i):
        sm.error("Entry not found in log")
        sm.error("Log: " & $sm.log)
        sm.error("Observed: " & $sm.observedState)
        sm.error("Index: " & $i)
        sm.error("Last index: " & $lastIndex)
        # Consider returning or error flag here to stop further processing
        break
      sm.output.logEntries.add(sm.log.getEntryByIndex(i))

  let observedCommitIndex = max(sm.observedState.commitIndex, sm.log.snapshot.index)
  if observedCommitIndex < sm.commitIndex:
    doAssert sm.output.committed.len == 0
    for i in (observedCommitIndex + 1) .. sm.commitIndex:
      sm.output.committed.add(sm.log.getEntryByIndex(i))

  if sm.votedFor != RaftNodeId.empty:
    sm.output.votedFor = some(sm.votedFor)

  sm.observedState.observe(sm)

  var output = RaftStateMachineRefOutput()
  swap(output, sm.output)

  if sm.state.isLeader and output.logEntries.len > 0:
    sm.debug "Leader accept index: " &
      $output.logEntries[output.logEntries.len - 1].index
    var leaderProgress = sm.findFollowerProgressById(sm.myId)
    if not leaderProgress.isSome:
      sm.critical "Leader progress not found"
      return
    leaderProgress.get().accepted(output.logEntries[output.logEntries.len - 1].index)
    sm.commit()
  output

func appendEntryReply*(
    sm: RaftStateMachineRef, fromId: RaftNodeId, reply: RaftRpcAppendReply
) =
  if not sm.state.isLeader:
    sm.debug "You can't append append reply to the follower"
    return
  var follower = sm.findFollowerProgressById(fromId)
  if not follower.isSome:
    sm.debug "Can't find the follower"
    return
  follower.get().commitIndex = max(follower.get().commitIndex, reply.commitIndex)
  case reply.result
  of RaftRpcCode.Accepted:
    let lastIndex = reply.accepted.lastNewIndex
    sm.debug "Accpeted message from " & $fromId & " last log index: " & $lastIndex
    follower.get().accepted(lastIndex)
    follower.get().lastReplyAt = sm.timeNow

    sm.commit()
    # TODO: Implement leader voluntary step-down logic.
    # In certain scenarios, the leader may want
    # to proactively step down (e.g., due to degraded state or connectivity).
    if not sm.state.isLeader:
      return
  of RaftRpcCode.Rejected:
    sm.debug "Rejected message from " & $fromId & " last log index: " & $reply.rejected
    follower.get().lastReplyAt = sm.timeNow
    if reply.rejected.nonMatchingIndex == 0 and reply.rejected.lastIdx == 0:
      # Legacy hint-free rejection: just try replicate again
      sm.replicateTo(follower.get())
    else:
      # Conflict optimization per Raft thesis:
      # - if follower provided conflictTerm:
      #     if leader has entries with that term, jump nextIndex to last index of that term + 1
      #     else set nextIndex to follower's conflictIndex
      # - otherwise set nextIndex to follower's conflictIndex
      if reply.rejected.conflictTerm.isSome:
        let t = reply.rejected.conflictTerm.get()
        let leaderIdxOpt = sm.log.lastIndexOfTerm(t)
        if leaderIdxOpt.isSome:
          follower.get().nextIndex = leaderIdxOpt.get() + 1
        else:
          follower.get().nextIndex = reply.rejected.conflictIndex
      else:
        follower.get().nextIndex = reply.rejected.conflictIndex
    sm.debug $follower.get()
  # if commit apply configuration that removes current follower 
  # we should take it again
  var follower2 = sm.findFollowerProgressById(fromId)
  if follower2.isSome:
    sm.replicateTo(follower2.get())

func advanceCommitIdx(sm: RaftStateMachineRef, leaderIdx: RaftLogIndex) =
  let newIdx = min(leaderIdx, sm.log.lastIndex)
  if newIdx > sm.commitIndex:
    sm.commitIndex = newIdx

func appendEntry*(
    sm: RaftStateMachineRef, fromId: RaftNodeId, request: RaftRpcAppendRequest
) =
  if not sm.state.isFollower:
    sm.debug "You can't append append request to the non follower"
    return

  let (match, term) = sm.log.matchTerm(request.previousLogIndex, request.previousTerm)
  if not match:
    # Build conflict hints per Raft paper for faster backtracking
    var conflictTerm: Option[RaftNodeTerm] = none(RaftNodeTerm)
    var conflictIndex: RaftLogIndex = 0
    let fi = sm.log.firstIndex
    let li = sm.log.lastIndex
    if request.previousLogIndex > li:
      # Follower log too short
      conflictIndex = li + 1
    elif request.previousLogIndex < fi:
      # Leader is too far behind follower's snapshot/log; hint where follower's log starts
      conflictIndex = fi
    else:
      # Term mismatch at previousLogIndex: provide follower's term and the
      # first index for that term in follower's log.
      let myTermOpt = sm.log.termForIndex(request.previousLogIndex)
      if myTermOpt.isSome:
        conflictTerm = some(myTermOpt.get())
        var i = request.previousLogIndex
        # Walk back to the first index with this term within follower's log bounds
        while i > fi:
          let prevTermOpt = sm.log.termForIndex(i - 1)
          if prevTermOpt.isSome and prevTermOpt.get() == myTermOpt.get():
            i = i - 1
          else:
            break
        conflictIndex = i
      else:
        # Should not happen, but provide a conservative hint
        conflictIndex = fi
    let rejected = RaftRpcAppendReplyRejected(
      nonMatchingIndex: request.previousLogIndex,
      lastIdx: sm.log.lastIndex,
      conflictTerm: conflictTerm,
      conflictIndex: conflictIndex,
    )
    let responce = RaftRpcAppendReply(
      term: sm.term,
      commitIndex: sm.commitIndex,
      result: RaftRpcCode.Rejected,
      rejected: rejected,
    )
    sm.sendTo(fromId, responce)
    sm.debug "Reject to apply the entry"
    return

  for entry in request.entries:
    sm.log.appendAsFollower(entry)

  if request.entries.len == 0:
    sm.debug "Handle heartbeat from " & $fromId & "  leader:" & $sm.follower.leader
   
  if sm.observedState.persistedIndex > sm.log.lastIndex:
    sm.observedState.setPersistedIndex sm.log.lastIndex

  sm.advanceCommitIdx(request.commitIndex)
  let accepted = RaftRpcAppendReplyAccepted(lastNewIndex: sm.log.lastIndex)
  let responce = RaftRpcAppendReply(
    term: sm.term,
    commitIndex: sm.commitIndex,
    result: RaftRpcCode.Accepted,
    accepted: accepted,
  )
  sm.sendTo(fromId, responce)

func requestVote*(
    sm: RaftStateMachineRef, fromId: RaftNodeId, request: RaftRpcVoteRequest
) =
  # D-Raft-Extended 
  # 6. The third issue is that removed servers (those not in
  # Cnew) can disrupt the cluster. These servers will not re-
  # ceive heartbeats, so they will time out and start new elec-
  # tions. They will then send RequestVote RPCs with new
  # term numbers, and this will cause the current leader to
  # revert to follower state. A new leader will eventually be
  # elected, but the removed servers will time out again and
  # the process will repeat, resulting in poor availability.
  let canVote =
    sm.votedFor == fromId or
    (sm.votedFor == RaftNodeId.empty and sm.currentLeader == RaftNodeId.empty)
  if canVote and sm.log.isUpToDate(request.lastLogIndex, request.lastLogTerm):
    sm.votedFor = fromId
    sm.lastElectionTime = sm.timeNow
    sm.debug "Voted for " & $fromId & " term" & $sm.term
    let responce = RaftRpcVoteReply(currentTerm: sm.term, voteGranted: true)
    sm.sendTo(fromId, responce)
  else:
    let responce: RaftRpcVoteReply =
      RaftRpcVoteReply(currentTerm: sm.term, voteGranted: false)
    sm.sendTo(fromId, responce)

func requestVoteReply*(
    sm: RaftStateMachineRef, fromId: RaftNodeId, request: RaftRpcVoteReply
) =
  if not sm.state.isCandidate:
    sm.debug "Non candidate can't handle votes"
    return
  discard sm.candidate.votes.registerVote(fromId, request.voteGranted)

  case sm.candidate.votes.tallyVote
  of RaftElectionResult.Unknown:
    return
  of RaftElectionResult.Won:
    sm.debug "Elecation won" & $(sm.candidate.votes) & $sm.myId
    sm.becomeLeader()
  of RaftElectionResult.Lost:
    sm.debug "Lost election"
    sm.becomeFollower(RaftNodeId.empty)

func installSnapshotReplay*(
    sm: RaftStateMachineRef, fromId: RaftNodeId, replay: RaftSnapshotReply
) =
  let follower = sm.findFollowerProgressById(fromId)
  if not follower.isSome:
    return

  let followerRef = follower.get()
  followerRef.lastReplyAt = sm.timeNow
  var snapshotIndex = followerRef.replayedIndex
  if snapshotIndex == 0:
    snapshotIndex = sm.log.snapshot.index
  let nextIndex = snapshotIndex + 1
  followerRef.nextIndex = max(followerRef.nextIndex, nextIndex)
  followerRef.matchIndex = max(followerRef.matchIndex, snapshotIndex)
  followerRef.commitIndex = max(followerRef.commitIndex, sm.commitIndex)
  followerRef.replayedIndex = 0

  if not replay.success:
    sm.debug "Snapshot reply rejected for follower " & $fromId & " index " &
      $snapshotIndex

  sm.replicateTo(followerRef)

func advance*(sm: RaftStateMachineRef, msg: RaftRpcMessage, now: times.DateTime) =
  sm.debug "Advance with" & $msg
  if msg.receiver != sm.myId:
    sm.debug "Invalid receiver:" & $msg.receiver & $msg
    return
  if msg.currentTerm > sm.term:
    sm.debug "Current node is behind my term:" & $sm.term & " message term:" &
      $msg.currentTerm

    var leaderId = RaftNodeId.empty
    if msg.kind == RaftRpcMessageType.AppendRequest or
        msg.kind == RaftRpcMessageType.InstallSnapshot:
      leaderId = msg.sender
    sm.becomeFollower(leaderId)
    sm.term = msg.currentTerm
    sm.votedFor = RaftNodeId.empty
  elif msg.currentTerm < sm.term:
    if msg.kind == RaftRpcMessageType.AppendRequest:
      # Instruct leader to step down
      sm.debug "Reject to apply the entry and instruct leader to step down" & $sm.term & " " & $msg.currentTerm
      let rejected = RaftRpcAppendReplyRejected(
        nonMatchingIndex: 0,
        lastIdx: sm.log.lastIndex,
        conflictTerm: none(RaftNodeTerm),
        conflictIndex: sm.log.lastIndex + 1,
      )
      let responce = RaftRpcAppendReply(
        term: sm.term,
        commitIndex: sm.commitIndex,
        result: RaftRpcCode.Rejected,
        rejected: rejected,
      )
      sm.sendTo(msg.sender, responce)
    elif msg.kind == RaftRpcMessageType.InstallSnapshot:
      sm.sendTo(msg.sender, RaftSnapshotReply(term: sm.term, success: false))
    elif msg.kind == RaftRpcMessageType.VoteRequest:
      sm.sendTo(msg.sender, RaftRpcVoteReply(currentTerm: sm.term, voteGranted: false))
    else:
      sm.warning "Ignore message with lower term" & $sm.term & " " & $msg
    return
  else:
    if msg.kind == RaftRpcMessageType.AppendRequest or
        msg.kind == RaftRpcMessageType.InstallSnapshot:
      if sm.state.isLeader:
        sm.error "Got message different leader with same term" & $msg
      elif sm.state.isCandidate:
        sm.becomeFollower(msg.sender)
      elif sm.currentLeader == RaftNodeId.empty:
        sm.state.follower.leader = msg.sender
      sm.lastElectionTime = now
      if msg.sender != sm.currentLeader:
        sm.error "Got message from unexpected leader" & $msg

  if sm.state.isCandidate:
    if msg.kind == RaftRpcMessageType.VoteRequest:
      sm.requestVote(msg.sender, msg.voteRequest)
    elif msg.kind == RaftRpcMessageType.VoteReply:
      sm.debug "Apply vote"
      sm.requestVoteReply(msg.sender, msg.voteReply)
    elif msg.kind == RaftRpcMessageType.InstallSnapshot:
      sm.sendTo(msg.sender, RaftSnapshotReply(term: sm.term, success: false))
    else:
      sm.warning "Candidate ignore message"
  elif sm.state.isFollower:
    if msg.sender == sm.follower.leader:
      sm.lastElectionTime = now
    if msg.kind == RaftRpcMessageType.AppendRequest:
      sm.appendEntry(msg.sender, msg.appendRequest)
    elif msg.kind == RaftRpcMessageType.VoteRequest:
      sm.requestVote(msg.sender, msg.voteRequest)
    elif msg.kind == RaftRpcMessageType.InstallSnapshot:
      let success = sm.applySnapshot(msg.installSnapshot.snapshot, false)
      sm.sendTo(msg.sender, RaftSnapshotReply(term: sm.term, success: success))
    else:
      sm.warning "Follower ignore message" & $msg
  elif sm.state.isLeader:
    if msg.kind == RaftRpcMessageType.AppendRequest:
      sm.warning "Ignore message leader append his entries directly"
    elif msg.kind == RaftRpcMessageType.AppendReply:
      sm.appendEntryReply(msg.sender, msg.appendReply)
    elif msg.kind == RaftRpcMessageType.VoteRequest:
      sm.requestVote(msg.sender, msg.voteRequest)
    elif msg.kind == RaftRpcMessageType.InstallSnapshot:
      sm.sendTo(msg.sender, RaftSnapshotReply(term: sm.term, success: false))
    elif msg.kind == RaftRpcMessageType.SnapshotReply:
      sm.installSnapshotReplay(msg.sender, msg.snapshotReply)
    else:
      sm.warning "Leader ignore message" & $msg
  sm.debug "Advance done next election time: " & $(sm.randomizedElectionTime - (now - sm.lastElectionTime)).inMilliseconds & "ms"
