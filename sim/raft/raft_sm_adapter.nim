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

  # Create initial log with empty snapshot
  let initialSnapshot = RaftSnapshot(
    index: 0,
    term: 0,
    config: initialConfig
  )
  let initialLog = RaftLog.init(initialSnapshot)

  # Create RaftStateMachineRef with correct initial time
  let initialTimeMs = adapter.callbacks.getTime()
  let initialNow = times.fromUnix(initialTimeMs div 1000).utc + times.initDuration(milliseconds = initialTimeMs mod 1000)
  adapter.raft = RaftStateMachineRef.new(
    id = nodeId,
    currentTerm = 0,
    log = initialLog,
    commitIndex = 0,
    now = initialNow,
    randomizedElectionTime = times.initDuration(milliseconds = adapter.electionTimeoutMs),
    heartbeatTime = times.initDuration(milliseconds = adapter.heartbeatTimeoutMs)
  )
  adapter.lastPolledTime = adapter.callbacks.getTime()

method step*(adapter: RaftSmAdapter, rpc: RaftRpcMessage) =
  ## Process an incoming RPC message
  let currentTimeMs = adapter.callbacks.getTime()

  # Convert current time to DateTime for raft (include milliseconds)
  let now = times.fromUnix(currentTimeMs div 1000).utc + times.initDuration(milliseconds = currentTimeMs mod 1000)
  adapter.raft.advance(rpc, now)

method tick*(adapter: RaftSmAdapter) =
  ## Process a timer tick
  let currentTimeMs = adapter.callbacks.getTime()

  # Pass correct time to raft - it will handle its own election timeouts
  let now = times.fromUnix(currentTimeMs div 1000).utc + times.initDuration(milliseconds = currentTimeMs mod 1000)
  adapter.raft.tick(now)

  # Poll the state machine to get any pending messages and apply commits
  let output = adapter.raft.poll()

  # Send any outgoing messages
  for msg in output.messages:
    adapter.callbacks.sendRpc(msg.receiver, msg)

  # Apply any committed entries to storage
  for entry in output.committed:
    if entry.kind == rletCommand:
      # In a real implementation, this would apply the command to the state machine
      discard

  # Persist any new log entries
  for entry in output.logEntries:
    adapter.callbacks.persistLogEntry(entry)

  # Update term and votedFor if changed
  if output.term != adapter.raft.term:
    adapter.callbacks.persistTerm(RaftNodeTerm(output.term))

  adapter.callbacks.persistVotedFor(output.votedFor)

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
  var logEntries: seq[LogEntry] = @[]
  let log = adapter.raft.log
  for i in log.firstIndex .. log.lastIndex:
    logEntries.add(log.getEntryByIndex(i))

  # Poll to get current votedFor (this is a read-only operation for invariants)
  let output = adapter.raft.poll()
  let votedFor = output.votedFor

  NodeState(
    id: adapter.nodeId,
    role: role,
    currentTerm: RaftNodeTerm(adapter.raft.term),
    votedFor: votedFor,
    log: logEntries,
    commitIndex: RaftLogIndex(adapter.raft.commitIndex),
    lastApplied: RaftLogIndex(adapter.raft.commitIndex),  # Simplified
    nextIndex: initTable[RaftNodeId, RaftLogIndex](),  # Would need to be populated from leader state
    matchIndex: initTable[RaftNodeId, RaftLogIndex]()   # Would need to be populated from leader state
  )

method getCommitIndex*(adapter: RaftSmAdapter): RaftLogIndex =
  ## Get current commit index
  RaftLogIndex(adapter.raft.commitIndex)

method getLastApplied*(adapter: RaftSmAdapter): RaftLogIndex =
  ## Get last applied index
  RaftLogIndex(adapter.raft.commitIndex)  # Simplified

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
