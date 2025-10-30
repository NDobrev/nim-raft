## JSON Trace Writer - Structured logging of simulation runs
##
## Generates detailed JSON traces containing configuration, per-tick state,
## events, faults, and invariant violations for analysis and debugging.

import std/json
import std/tables
import std/strformat
import std/options
import std/strutils

import ../core/types
import ../run/scenario_config
import ../invariants/checker
import ../raft/node_host
import ../../src/raft/consensus_state_machine
import ../../src/raft/types
import ../../src/raft/log

type
  # JSON trace data structures
  JsonTableEntry* = object
    nodeId*: int
    index*: uint64

  JsonNodeState* = object
    id*: int
    role*: string  # "follower", "candidate", "leader"
    currentTerm*: uint64
    votedFor*: Option[int]
    commitIndex*: uint64
    lastApplied*: uint64
    # Current in-memory log length (may shrink due to compaction/snapshot)
    logLen*: int
    # Absolute log indices for this node (snapshot-aware)
    # firstLogIndex = snapshot.index + 1 when log is empty
    # lastLogIndex  = max(snapshot.index, last entry.index)
    firstLogIndex*: uint64
    lastLogIndex*: uint64
    alive*: bool
    nextIndex*: seq[JsonTableEntry]  # only for leaders
    matchIndex*: seq[JsonTableEntry]  # only for leaders

  JsonClusterState* = object
    timeMs*: int64
    nodes*: seq[JsonNodeState]
    leaderId*: Option[int]
    totalCommits*: int
    invariantsOk*: bool

  JsonRpcEvent* = object
    timeMs*: int64
    fromNode*: int
    toNode*: int
    rpcType*: string  # "RequestVote", "AppendEntries", etc.
    term*: uint64
    success*: Option[bool]  # for responses
    details*: JsonNode

  JsonFaultEvent* = object
    timeMs*: int64
    faultType*: string  # "partition", "node_stop", "node_restart", etc.
    details*: JsonNode

  JsonMessageStats* = object
    totalMessages*: int
    messagesByType*: Table[string, int]
    messagesByNode*: Table[string, Table[string, int]]  # from -> to -> count
    networkStats*: JsonNode  # dropped, duplicated, delivered counts
    avgLatencyMs*: float
    minLatencyMs*: int64
    maxLatencyMs*: int64

  JsonCommittedEvent* = object
    timeMs*: int64
    nodeId*: int
    entryIndex*: uint64
    entryTerm*: uint64
    entryData*: string
    recommit*: bool

  JsonInvariantViolation* = object
    timeMs*: int64
    invariant*: string
    description*: string
    details*: JsonNode

  JsonTrace* = object
    schemaVersion*: string
    config*: ScenarioYaml
    seed*: uint64
    startTime*: int64
    endTime*: int64
    success*: bool
    totalTicks*: int64
    clusterStates*: seq[JsonClusterState]
    rpcEvents*: seq[JsonRpcEvent]
    faultEvents*: seq[JsonFaultEvent]
    committedEvents*: seq[JsonCommittedEvent]
    invariantViolations*: seq[JsonInvariantViolation]
    messageStats*: JsonMessageStats
    finalStats*: JsonNode

  JsonTraceWriter* = ref object
    trace*: JsonTrace
    config*: ScenarioYaml
    seed*: uint64
    # Monotonic trackers (by node id) to report ever-seen bounds across restarts/wipes
    maxLastIdxByNode: Table[int, uint64]
    # Per-node set of indices already seen committed (to flag recommits)
    seenCommittedByNode: Table[int, Table[uint64, bool]]

proc newJsonTraceWriter*(config: ScenarioYaml, seed: uint64): JsonTraceWriter =
  ## Create a new JSON trace writer
  let trace = JsonTrace(
    schemaVersion: "1.0",
    config: config,
    seed: seed,
    startTime: 0,
    endTime: 0,
    success: false,
    totalTicks: 0,
    clusterStates: @[],
    rpcEvents: @[],
    faultEvents: @[],
    committedEvents: @[],
    invariantViolations: @[],
    messageStats: JsonMessageStats(
      totalMessages: 0,
      messagesByType: initTable[string, int](),
      messagesByNode: initTable[string, Table[string, int]](),
      networkStats: %*{"dropped": 0, "duplicated": 0, "delivered": 0},
      avgLatencyMs: 0.0,
      minLatencyMs: high(int64),
      maxLatencyMs: 0
    ),
    finalStats: %*{}
  )
  result = JsonTraceWriter(
    trace: trace,
    config: config,
    seed: seed,
    maxLastIdxByNode: initTable[int, uint64](),
    seenCommittedByNode: initTable[int, Table[uint64, bool]]()
  )

proc startTrace*(writer: JsonTraceWriter, startTime: int64) =
  ## Mark the start of tracing
  writer.trace.startTime = startTime

proc recordClusterState*(writer: JsonTraceWriter, timeMs: int64, nodes: seq[NodeHost], totalCommits: int) =
  ## Record the cluster state at a specific time
  var jsonNodes: seq[JsonNodeState] = @[]
  var leaderId: Option[int] = none(int)

  for node in nodes:
    let state = node.getState()
    var roleStr = case state.role
      of Follower: "follower"
      of Candidate: "candidate"
      of Leader: "leader"

    if state.role == Leader:
      leaderId = some(parseInt(state.id.id))

    # Compute absolute log indices (snapshot-aware)
    let hasLog = state.log.len > 0
    let firstIdxAbs = (if hasLog: state.log[0].index.uint64 else: state.snapshotIndex.uint64 + 1'u64)
    let lastIdxAbs = (if hasLog: state.log[^1].index.uint64 else: state.snapshotIndex.uint64)

    # Enforce monotonic lastLogIndex per node across the whole run by tracking
    # the maximum ever observed tail index, even if the node is wiped/restarted.
    let nodeIdInt = parseInt(state.id.id)
    var monotonicLast = lastIdxAbs
    if writer.maxLastIdxByNode.hasKey(nodeIdInt):
      monotonicLast = max(monotonicLast, writer.maxLastIdxByNode[nodeIdInt])
    writer.maxLastIdxByNode[nodeIdInt] = monotonicLast

    var jsonNode = JsonNodeState(
      id: parseInt(state.id.id),
      role: roleStr,
      currentTerm: state.currentTerm.uint64,
      votedFor: if state.votedFor.isSome: some(parseInt(state.votedFor.get.id)) else: none(int),
      commitIndex: state.commitIndex.uint64,
      lastApplied: state.lastApplied.uint64,
      logLen: state.log.len,
      firstLogIndex: firstIdxAbs,
      lastLogIndex: monotonicLast,
      alive: node.isAlive()
    )

    # Add leader-specific state
    if state.role == Leader:
      for nodeId, nextIdx in state.nextIndex:
        jsonNode.nextIndex.add(JsonTableEntry(nodeId: parseInt(nodeId.id), index: nextIdx.uint64))
      for nodeId, matchIdx in state.matchIndex:
        jsonNode.matchIndex.add(JsonTableEntry(nodeId: parseInt(nodeId.id), index: matchIdx.uint64))

    jsonNodes.add(jsonNode)

  let clusterState = JsonClusterState(
    timeMs: timeMs,
    nodes: jsonNodes,
    leaderId: leaderId,
    totalCommits: totalCommits,
    invariantsOk: true  # Will be updated if violations occur
  )

  writer.trace.clusterStates.add(clusterState)

proc updateMessageStats*(writer: JsonTraceWriter, rpcType: string, fromNode, toNode: RaftNodeId) =
  ## Update message statistics counters
  writer.trace.messageStats.totalMessages += 1

  # Count by type
  writer.trace.messageStats.messagesByType[rpcType] =
    writer.trace.messageStats.messagesByType.getOrDefault(rpcType, 0) + 1

  # Count by node pairs
  let fromId = fromNode.id
  let toId = toNode.id
  if not writer.trace.messageStats.messagesByNode.hasKey(fromId):
    writer.trace.messageStats.messagesByNode[fromId] = initTable[string, int]()
  writer.trace.messageStats.messagesByNode[fromId][toId] =
    writer.trace.messageStats.messagesByNode[fromId].getOrDefault(toId, 0) + 1

proc recordRpcEvent*(writer: JsonTraceWriter, timeMs: int64, rpc: RaftRpcMessage, fromNode, toNode: RaftNodeId) =
  ## Record an RPC event
  var rpcType: string
  var term: uint64 = 0
  var success: Option[bool] = none(bool)
  var details = newJObject()

  # TODO: Update RPC event recording for RaftRpcMessage
  case rpc.kind:
  of VoteRequest:
    rpcType = "RequestVote"
    term = rpc.currentTerm.uint64
  of VoteReply:
    rpcType = "RequestVoteReply"
    term = rpc.currentTerm.uint64
    success = some(rpc.voteReply.voteGranted)
  of AppendRequest:
    rpcType = "AppendEntries"
    term = rpc.currentTerm.uint64
  of AppendReply:
    rpcType = "AppendEntriesReply"
    term = rpc.currentTerm.uint64
    success = some(rpc.appendReply.result == Accepted)
  of InstallSnapshot:
    rpcType = "InstallSnapshot"
    term = rpc.currentTerm.uint64
  of SnapshotReply:
    rpcType = "InstallSnapshotReply"
    term = rpc.currentTerm.uint64
    success = some(rpc.snapshotReply.success)

  let event = JsonRpcEvent(
    timeMs: timeMs,
    fromNode: parseInt(fromNode.id),
    toNode: parseInt(toNode.id),
    rpcType: rpcType,
    term: term,
    success: success,
    details: details
  )

  writer.trace.rpcEvents.add(event)

  # Update message statistics
  writer.updateMessageStats(rpcType, fromNode, toNode)

proc recordFaultEvent*(writer: JsonTraceWriter, timeMs: int64, faultType: string, details: JsonNode) =
  ## Record a fault event
  let event = JsonFaultEvent(
    timeMs: timeMs,
    faultType: faultType,
    details: details
  )

  writer.trace.faultEvents.add(event)

proc recordInvariantViolation*(writer: JsonTraceWriter, timeMs: int64, violation: InvariantViolation) =
  ## Record an invariant violation
  # Extract invariant name from description (simplified approach)
  var invariantName = "unknown"
  if violation.description.contains("Election Safety"):
    invariantName = "election_safety"
  elif violation.description.contains("Log Matching"):
    invariantName = "log_matching"
  elif violation.description.contains("Leader Append-Only"):
    invariantName = "leader_append_only"
  elif violation.description.contains("State Machine Safety"):
    invariantName = "state_machine_safety"
  elif violation.description.contains("Leader Completeness"):
    invariantName = "leader_completeness"
  elif violation.description.contains("Snapshot Sanity"):
    invariantName = "snapshot_sanity"
  elif violation.description.contains("deadlock"):
    invariantName = "liveness_deadlock"

  let event = JsonInvariantViolation(
    timeMs: timeMs,
    invariant: invariantName,
    description: violation.description,
    details: %violation.details
  )

  writer.trace.invariantViolations.add(event)

  # Mark the most recent cluster state as having invariant violations
  if writer.trace.clusterStates.len > 0:
    writer.trace.clusterStates[^1].invariantsOk = false

proc recordCommittedEvent*(writer: JsonTraceWriter, timeMs: int64, nodeId: RaftNodeId, entry: LogEntry) =
  ## Record a committed entry event
  let entryData = if entry.kind == rletCommand:
                    $cast[string](entry.command.data)
                  else:
                    ""  # Config entries have no data
  let idx = entry.index.uint64
  let nid = parseInt(nodeId.id)
  if not writer.seenCommittedByNode.hasKey(nid):
    writer.seenCommittedByNode[nid] = initTable[uint64, bool]()
  let wasSeen = writer.seenCommittedByNode[nid].hasKey(idx)
  writer.seenCommittedByNode[nid][idx] = true

  let event = JsonCommittedEvent(
    timeMs: timeMs,
    nodeId: nid,
    entryIndex: idx,
    entryTerm: entry.term.uint64,
    entryData: entryData,
    recommit: wasSeen
  )
  writer.trace.committedEvents.add(event)

proc finalizeMessageStats*(writer: JsonTraceWriter) =
  ## Finalize message statistics with calculated metrics
  # Calculate latency statistics from RPC events (if available in finalStats)
  if writer.trace.finalStats.hasKey("networkStats"):
    let netStats = writer.trace.finalStats["networkStats"]
    writer.trace.messageStats.networkStats = netStats

  # Calculate average latency (simplified - could be enhanced)
  if writer.trace.rpcEvents.len > 0:
    var totalLatency = 0.0
    var count = 0
    for event in writer.trace.rpcEvents:
      # For now, we'll use a placeholder calculation
      # In a real implementation, we'd need to track send time vs delivery time
      count += 1
    if count > 0:
      writer.trace.messageStats.avgLatencyMs = totalLatency / float(count)
    else:
      writer.trace.messageStats.avgLatencyMs = 0.0

proc finishTrace*(writer: JsonTraceWriter, endTime: int64, success: bool, totalTicks: int64, finalStats: JsonNode) =
  ## Mark the end of tracing
  writer.trace.endTime = endTime
  writer.trace.success = success
  writer.trace.totalTicks = totalTicks
  writer.trace.finalStats = finalStats

  # Finalize message statistics
  writer.finalizeMessageStats()

proc writeToFile*(writer: JsonTraceWriter, path: string) =
  ## Write the complete trace to a JSON file
  let json = %writer.trace
  writeFile(path, $json)

proc `$`*(writer: JsonTraceWriter): string =
  ## String representation of the trace writer
  fmt"JsonTraceWriter(seed={writer.seed}, events={writer.trace.rpcEvents.len + writer.trace.faultEvents.len})"
