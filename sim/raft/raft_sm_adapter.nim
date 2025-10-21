## RaftStateMachineRef Adapter for Simulation
##
## Adapts the RaftStateMachineRef from the main codebase to implement
## the RaftNode interface expected by the simulation framework.

import std/options
import std/times
import std/tables
import std/strutils
import std/strformat

# Import main codebase types and RaftStateMachineRef
import ../../src/raft/types
import ../../src/raft/log
import ../../src/raft/consensus_state_machine
import ../../src/raft/config
import ../../src/raft/state
import ../../src/raft/poll_state

# Import simulation types and interfaces
import ../core/types
from ../core/types import data
import ../core/types as sim_types
import ../core/sim_rng
import ../storage/sim_storage
import raft_interface
import ../run/scenario_config

type
  RaftSmAdapter* = ref object of RaftNode
    ## Adapter that wraps RaftStateMachineRef to implement RaftNode interface
    raft*: RaftStateMachineRef
    callbacks*: RaftHostCallbacks
    nodeId*: RaftNodeId
    config*: ScenarioYaml

    # State for simulation integration
    pendingCommands*: seq[seq[byte]]  # Commands waiting to be proposed
    lastPolledTime*: int64
    electionTimeoutMs*: int64
    heartbeatTimeoutMs*: int64

    # Configuration tracking for stepdown logic
    currentConfig*: RaftConfig
    isVotingMember*: bool
    lastSeenCommitIndex*: RaftLogIndex


# Types are now used directly - no conversion needed

proc sendRpcCallback(adapter: RaftSmAdapter, target: RaftNodeId, msg: RaftRpcMessage) =
  ## Callback to send RPC messages through simulation framework
  echo fmt"RAFT: Node {adapter.nodeId} sending {msg.kind} to {target} in term {msg.currentTerm}"
  adapter.callbacks.sendRpc(target, msg)

proc getTimeCallback(adapter: RaftSmAdapter): int64 =
  ## Get current time from simulation
  adapter.callbacks.getTime()

method initialize*(adapter: RaftSmAdapter, nodeId: RaftNodeId, callbacks: RaftHostCallbacks, clusterConfig: seq[RaftNodeId]) =
  ## Initialize the RaftStateMachineRef adapter
  adapter.nodeId = nodeId
  adapter.callbacks = callbacks

  # Create initial configuration with all cluster nodes
  let initialConfig = RaftConfig(currentSet: clusterConfig)
  adapter.currentConfig = initialConfig
  adapter.isVotingMember = true  # Start as voting member

  # Load persisted disk state (term, votedFor, log). Snapshot ignored for now.
  var persisted = adapter.callbacks.loadDisk(false)

  #adapter.callbacks.recordSimLog(NodeLifecycle, fmt"Node {nodeId} initializing, persisted={persisted}")
  # Create initial log snapshot from persisted snapshot if available
  let initialSnapshot =
    if persisted.snapshot.isSome:
      let s = persisted.snapshot.get()
      RaftSnapshot(index: s.lastIncludedIndex, term: s.lastIncludedTerm, config: initialConfig)
    else:
      RaftSnapshot(index: 0, term: 0, config: initialConfig)
  let initialLog = RaftLog.init(initialSnapshot, persisted.log)


  # Create RaftStateMachineRef with correct initial time
  let initialTimeMs = adapter.callbacks.getTime()
  let initialNow = times.fromUnix(initialTimeMs div 1000).utc + times.initDuration(milliseconds = initialTimeMs mod 1000)
  adapter.raft = RaftStateMachineRef.new(
    id = nodeId,
    currentTerm = persisted.currentTerm,
    log = initialLog,
    commitIndex = 0,
    now = initialNow,
    randomizedElectionTime = times.initDuration(milliseconds = adapter.electionTimeoutMs),
    heartbeatTime = times.initDuration(milliseconds = adapter.heartbeatTimeoutMs)
  )
  adapter.lastPolledTime = adapter.callbacks.getTime()
  # Reset commit-index tracker across (re)initializations so we don't
  # compare against a previous process epoch after restart/wipe.
  adapter.lastSeenCommitIndex = adapter.raft.commitIndex

method step*(adapter: RaftSmAdapter, rpc: RaftRpcMessage) =
  ## Process an incoming RPC message
  let currentTimeMs = adapter.callbacks.getTime()

  # Convert current time to DateTime for raft (include milliseconds)
  let now = times.fromUnix(currentTimeMs div 1000).utc + times.initDuration(milliseconds = currentTimeMs mod 1000)
  adapter.raft.advance(rpc, now)

  # Poll once to surface outputs produced by this RPC (e.g., InstallSnapshot)
  let output = adapter.raft.poll()

  # Sanity: a leader's commitIndex must not decrease within a single
  # process epoch. After restart/wipe, lastSeenCommitIndex is reset in initialize.
  if adapter.raft.isLeader and adapter.raft.commitIndex < adapter.lastSeenCommitIndex:
    # Log commit index decrease to eventTrace for debugging
    if adapter.callbacks.recordDebugLog != nil:
      let debugEntry = DebugLogEntry(
        level: DebugLogLevel.Warning,
        time: times.fromUnix(currentTimeMs div 1000).utc + times.initDuration(milliseconds = currentTimeMs mod 1000),
        nodeId: adapter.nodeId,
        term: adapter.raft.term,
        state: adapter.raft.state.state,
        msg: fmt"Commit index decreased on leader: {adapter.lastSeenCommitIndex} -> {adapter.raft.commitIndex}"
      )
      adapter.callbacks.recordDebugLog(debugEntry)
  if adapter.raft.commitIndex > adapter.lastSeenCommitIndex:
    adapter.lastSeenCommitIndex = adapter.raft.commitIndex

  # Forward internal debug logs to event trace (ring buffer) if available
  if output.debugLogs.len > 0 and adapter.callbacks.recordDebugLog != nil:
    for d in output.debugLogs:
      adapter.callbacks.recordDebugLog(d)
  
  # Persist any new log entries
  for entry in output.logEntries:
    adapter.callbacks.persistLogEntry(entry)

  # Send any outgoing messages immediately
  for msg in output.messages:
    adapter.callbacks.sendRpc(msg.receiver, msg)

  # Apply any committed entries to storage/metrics
  for entry in output.committed:
    # Record committed event for all kinds to preserve first-commit order per index
    if adapter.callbacks.onCommitted != nil:
      adapter.callbacks.onCommitted(adapter.nodeId, entry)
    if entry.kind == rletConfig:
      adapter.currentConfig = entry.config
      adapter.isVotingMember = entry.config.contains(adapter.nodeId)
      if not adapter.isVotingMember and adapter.raft.state.state == RaftNodeState.rnsLeader:
        # Node stepping down - removed from configuration
        # Could log to eventTrace if needed for debugging
        discard


  # Keep term/votedFor durable
  adapter.callbacks.persistTerm(adapter.raft.term)
  adapter.callbacks.persistVotedFor(output.votedFor)

  # Persist any applied snapshot immediately to avoid races
  if output.applyedSnapshots.isSome:
    let snap = output.applyedSnapshots.get()
    let simSnap = Snapshot(
      lastIncludedIndex: snap.index,
      lastIncludedTerm: snap.term,
      data: @[]
    )
    adapter.callbacks.saveSnapshot(simSnap)

method tick*(adapter: RaftSmAdapter) =
  ## Process a timer tick
  let currentTimeMs = adapter.callbacks.getTime()

  let now = times.fromUnix(0).utc + times.initDuration(milliseconds = currentTimeMs)
  adapter.raft.tick(now)

  # Poll the state machine to get any pending messages and apply commits
  let output = adapter.raft.poll()

  # Forward internal debug logs to event trace (ring buffer) if available
  if output.debugLogs.len > 0 and adapter.callbacks.recordDebugLog != nil:
    for d in output.debugLogs:
      adapter.callbacks.recordDebugLog(d)

  # Persist any new log entries
  for entry in output.logEntries:
    adapter.callbacks.persistLogEntry(entry)

  # Send any outgoing messages
  for msg in output.messages:
    adapter.callbacks.sendRpc(msg.receiver, msg)

  # Apply any committed entries to storage
  for entry in output.committed:
    # Record committed event for metrics (all kinds)
    if adapter.callbacks.onCommitted != nil:
      adapter.callbacks.onCommitted(adapter.nodeId, entry)
    if entry.kind == rletConfig:
      # Update current configuration
      adapter.currentConfig = entry.config
      # Check if this node is still a voting member
      adapter.isVotingMember = entry.config.contains(adapter.nodeId)

      # If this node is no longer in the configuration and was leader, it should step down
      if not adapter.isVotingMember and adapter.raft.state.state == RaftNodeState.rnsLeader:
        # The core Raft implementation should handle stepdown
        # Could log to eventTrace if needed for debugging
        discard

  

  # Update term and votedFor if changed
  # Persist current term on every tick to ensure durability across restarts
  adapter.callbacks.persistTerm(adapter.raft.term)

  adapter.callbacks.persistVotedFor(output.votedFor)

  # Persist snapshots applied by the core (now exposed via applyedSnapshots)
  if output.applyedSnapshots.isSome:
    let snap = output.applyedSnapshots.get()
    let simSnap = Snapshot(
      lastIncludedIndex: snap.index,
      lastIncludedTerm: snap.term,
      data: @[]
    )
    adapter.callbacks.saveSnapshot(simSnap)

method propose*(adapter: RaftSmAdapter, cmd: seq[byte]): bool =
  ## Propose a new command to the cluster
  if not adapter.raft.isLeader:
    return false

  let command = Command(data: cmd)
  discard adapter.raft.addEntry(command)
  return true

method readIndex*(adapter: RaftSmAdapter): Option[seq[byte]] =
  ## Perform a read-only query using ReadIndex
  # Simplified implementation - just return empty for now
  some(@[byte(42)])

method getState*(adapter: RaftSmAdapter): NodeState =
  ## Get current Raft state for testing/invariants
  let role = if adapter.raft.state.isLeader:
               sim_types.Leader
             elif adapter.raft.state.isCandidate:
               sim_types.Candidate
             else:
               sim_types.Follower

  # Collect log entries from the Raft log
  let log = adapter.raft.log

  var logEntries: seq[LogEntry] = log.logEntries

  # Read votedFor from observed state to avoid poll() side effects
  let vf = adapter.raft.observedState.votedFor()
  let votedFor = if vf == RaftNodeId.empty: none(RaftNodeId) else: some(vf)

  # Diagnostic: detect inconsistent state where commitIndex exceeds raw lastIndex
  let rawLastIdx = adapter.raft.log.lastIndex
  let rawFirstIdx = adapter.raft.log.firstIndex
  let rawSnapIdx = adapter.raft.log.snapshot.index

  NodeState(    
    id: adapter.nodeId,
    role: role,
    currentTerm: RaftNodeTerm(adapter.raft.term),
    votedFor: votedFor,
    log: logEntries,
    commitIndex: RaftLogIndex(adapter.raft.commitIndex),
    lastApplied: RaftLogIndex(adapter.raft.commitIndex),  # Simplified
    snapshotIndex: adapter.raft.log.snapshot.index,
    snapshotTerm: adapter.raft.log.snapshot.term,
    nextIndex: initTable[RaftNodeId, RaftLogIndex](),  # Would need to be populated from leader state
    matchIndex: initTable[RaftNodeId, RaftLogIndex]()   # Would need to be populated from leader state
  )

  

method getCommitIndex*(adapter: RaftSmAdapter): RaftLogIndex =
  ## Get current commit index
  RaftLogIndex(adapter.raft.commitIndex)

method getLastApplied*(adapter: RaftSmAdapter): RaftLogIndex =
  ## Get last applied index
  RaftLogIndex(adapter.raft.commitIndex)  # Simplified

method forceLocalSnapshot*(adapter: RaftSmAdapter, upTo: RaftLogIndex): bool =
  ## Force a local snapshot up to the given index (bounded by commitIndex)
  let snapIndex = min(upTo, RaftLogIndex(adapter.raft.commitIndex))
  if snapIndex <= adapter.raft.log.snapshot.index:
    return false
  let termOpt = adapter.raft.log.termForIndex(snapIndex)
  if termOpt.isNone:
    return false
  let snap = RaftSnapshot(
    index: snapIndex,
    term: termOpt.get(),
    config: adapter.raft.log.configuration
  )
  let applied = adapter.raft.applySnapshot(snap, true)
  if applied:
    # Persist snapshot immediately to avoid races in invariants
    let simSnap = Snapshot(
      lastIncludedIndex: snap.index,
      lastIncludedTerm: snap.term,
      data: @[]
    )
    adapter.callbacks.saveSnapshot(simSnap)
  return true

proc newRaftSmAdapter*(config: ScenarioYaml, nodeId: RaftNodeId, electionRng: SimRng): RaftSmAdapter =
  ## Create a new RaftStateMachineRef adapter
  let electionTimeoutRange = config.cluster.election_timeout_ms
  # Use shared RNG to get a random timeout in the configured range
  # Each node will get a different value since they consume from the same RNG sequentially
  let electionTimeoutMs = electionRng.nextInt(electionTimeoutRange.min, electionTimeoutRange.max)
  RaftSmAdapter(
    config: config,
    pendingCommands: @[],
    lastPolledTime: 0,
    electionTimeoutMs: electionTimeoutMs,
    heartbeatTimeoutMs: config.cluster.heartbeat_ms
  )
