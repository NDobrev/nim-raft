## NodeHost - Raft node adapter for simulation
##
## Thin shim implementing the expected host callbacks for Raft SM:
## timers, persistence, stable/random bytes, and logging.

import std/options
import std/strformat

import ../core/sim_clock
import ../core/sim_rng
import ../core/sim_scheduler
import ../storage/sim_storage
import ../net/sim_net
import ../core/types
import raft_interface
import ../../src/raft/types
import ../../src/raft/consensus_state_machine

type
  NodeLifecycleCallback* = proc(nodeId: RaftNodeId, event: LifecycleEvent) {.gcsafe, closure.}

  RpcEventCallback* = proc(timeMs: int64, rpc: RaftRpcMessage, fromNode, toNode: RaftNodeId) {.gcsafe, closure.}

  EventLogCallback* = proc(kind: EventTraceKind, nodeId: RaftNodeId, description: string, details: string = "") {.gcsafe, closure.}

  CommittedEventCallback* = proc(timeMs: int64, nodeId: RaftNodeId, entry: LogEntry) {.gcsafe, closure.}

  NodeHost* = ref object
    id*: RaftNodeId
    clock*: SimClock
    scheduler*: SimScheduler
    rng*: SimRng
    storage*: SimStorage
    net*: SimNet
    alive*: bool
    lifecycleCallback*: Option[NodeLifecycleCallback]
    clusterConfig*: seq[RaftNodeId]
    rpcEventCallback*: Option[RpcEventCallback]
    eventLogCallback*: Option[EventLogCallback]
    committedEventCallback*: Option[CommittedEventCallback]

    # Timer generation counters for versioning
    electionTimerGeneration*: int
    heartbeatTimerGeneration*: int

    # Raft implementation
    raft*: RaftNode

proc newNodeHost*(id: RaftNodeId, clock: SimClock, scheduler: SimScheduler,
                 rng: SimRng, storage: SimStorage, net: SimNet,
                 raftImpl: RaftNode, clusterConfig: seq[RaftNodeId],
                 rpcEventCallback: Option[RpcEventCallback] = none(RpcEventCallback),
                 lifecycleCallback: Option[NodeLifecycleCallback] = none(NodeLifecycleCallback),
                 eventLogCallback: Option[EventLogCallback] = none(EventLogCallback),
                 committedEventCallback: Option[CommittedEventCallback] = none(CommittedEventCallback)): NodeHost =
  # Create the host as a mutable variable
  var host = NodeHost(
    id: id,
    clock: clock,
    scheduler: scheduler,
    rng: rng,
    storage: storage,
    net: net,
    alive: true,
    lifecycleCallback: lifecycleCallback,
    clusterConfig: clusterConfig,
    rpcEventCallback: rpcEventCallback,
    eventLogCallback: eventLogCallback,
    committedEventCallback: committedEventCallback,
    electionTimerGeneration: 0,
    heartbeatTimerGeneration: 0,
    raft: raftImpl
  )

  # Create host callbacks for the Raft implementation
  let callbacks = RaftHostCallbacks(
    sendRpc: proc(target: RaftNodeId, rpc: RaftRpcMessage) =
      if host.alive:
        # Record the RPC event for statistics
        if host.rpcEventCallback.isSome:
          host.rpcEventCallback.get()(host.clock.nowMs, rpc, host.id, target)
        host.net.send(host.clock, rpc, host.id, target),
    persistTerm: proc(term: RaftNodeTerm) = host.storage.persistTerm(host.id, term),
    persistVotedFor: proc(votedFor: Option[RaftNodeId]) = host.storage.persistVotedFor(host.id, votedFor),
    persistLogEntry: proc(entry: LogEntry) = host.storage.persistLogEntry(host.id, entry),
    truncateLog: proc(fromIndex: RaftLogIndex) = host.storage.truncateLog(host.id, fromIndex),
    saveSnapshot: proc(snapshot: Snapshot) = host.storage.saveSnapshot(host.id, snapshot),
    getSnapshot: proc(): Option[Snapshot] = host.storage.getSnapshot(host.id),
    scheduleTimer: proc(delayMs: int64, callback: TimerCallback): TimerId = host.clock.scheduleTimer(delayMs, callback),
    cancelTimer: proc(timerId: TimerId): bool = host.clock.cancelTimer(timerId),
    randomBytes: proc(n: int): seq[byte] =
      var bytes = newSeq[byte](n)
      for i in 0..<n:
        bytes[i] = byte(host.rng.next() and 0xFF)
      return bytes,
    randomInt: proc(max: int): int = host.rng.nextInt(max),
    getTime: proc(): int64 = host.clock.nowMs,
    onCommitted: proc(nodeId: RaftNodeId, entry: LogEntry) =
      # Record committed event to JSON writer
      if host.committedEventCallback.isSome:
        host.committedEventCallback.get()(host.clock.nowMs, nodeId, entry)
      # Forward to event log callback if available
      if host.eventLogCallback.isSome:
        host.eventLogCallback.get()(EntryCommitted, nodeId, fmt"Entry {entry.index} committed", fmt"term={entry.term}")
  )

  # Initialize the Raft implementation
  raftImpl.initialize(id, callbacks, clusterConfig)

  return host

proc step*(host: NodeHost, rpc: RaftRpcMessage) =
  ## Handle incoming RPC message
  if not host.alive:
    return

  # Forward to Raft implementation
  host.raft.step(rpc)

proc tick*(host: NodeHost) =
  ## Process a simulation tick
  if not host.alive:
    return

  # Forward to Raft implementation
  host.raft.tick()

proc propose*(host: NodeHost, cmd: seq[byte]): bool =
  ## Propose a new command to the Raft cluster
  if not host.alive:
    return false

  # Forward to Raft implementation
  return host.raft.propose(cmd)

proc readIndex*(host: NodeHost): Option[seq[byte]] =
  ## Perform a read-only query using ReadIndex
  if not host.alive:
    return none(seq[byte])

  # Forward to Raft implementation
  return host.raft.readIndex()

proc sendRpc*(host: NodeHost, target: RaftNodeId, rpc: RaftRpcMessage) =
  ## Send an RPC to another node
  if not host.alive:
    return

  host.net.send(host.clock, rpc, host.id, target)

proc stop*(host: var NodeHost) =
  ## Stop this node (simulates crash/failure)
  if not host.alive:
    return

  host.alive = false

  # Notify lifecycle callback
  if host.lifecycleCallback.isSome:
    let event = LifecycleEvent(
      nodeId: host.id,
      state: Down,
      atTime: host.clock.nowMs,
      wipedDb: false
    )
    host.lifecycleCallback.get()(host.id, event)

proc start*(host: var NodeHost, wipeDb: bool = false) =
  ## Start/restart this node
  host.alive = true

  # For restart, we need to reload persisted state
  # The Raft implementation will handle state restoration through its initialize method
  # We call initialize again to restore from disk state

  # Create callbacks that capture the host ref
  let hostRef = host
  let callbacks = RaftHostCallbacks(
    sendRpc: proc(target: RaftNodeId, rpc: RaftRpcMessage) =
      if hostRef.alive:
        hostRef.net.send(hostRef.clock, rpc, hostRef.id, target),
    persistTerm: proc(term: RaftNodeTerm) = hostRef.storage.persistTerm(hostRef.id, term),
    persistVotedFor: proc(votedFor: Option[RaftNodeId]) = hostRef.storage.persistVotedFor(hostRef.id, votedFor),
    persistLogEntry: proc(entry: LogEntry) = hostRef.storage.persistLogEntry(hostRef.id, entry),
    truncateLog: proc(fromIndex: RaftLogIndex) = hostRef.storage.truncateLog(hostRef.id, fromIndex),
    saveSnapshot: proc(snapshot: Snapshot) = hostRef.storage.saveSnapshot(hostRef.id, snapshot),
    getSnapshot: proc(): Option[Snapshot] = hostRef.storage.getSnapshot(hostRef.id),
    scheduleTimer: proc(delayMs: int64, callback: TimerCallback): TimerId = hostRef.clock.scheduleTimer(delayMs, callback),
    cancelTimer: proc(timerId: TimerId): bool = hostRef.clock.cancelTimer(timerId),
    randomBytes: proc(n: int): seq[byte] =
      var bytes = newSeq[byte](n)
      for i in 0..<n:
        bytes[i] = byte(hostRef.rng.next() and 0xFF)
      return bytes,
    randomInt: proc(max: int): int = hostRef.rng.nextInt(max),
    getTime: proc(): int64 = hostRef.clock.nowMs,
    onCommitted: proc(nodeId: RaftNodeId, entry: LogEntry) =
      # Record committed event to JSON writer
      if hostRef.committedEventCallback.isSome:
        hostRef.committedEventCallback.get()(hostRef.clock.nowMs, nodeId, entry)
      # Forward to event log callback if available
      if hostRef.eventLogCallback.isSome:
        hostRef.eventLogCallback.get()(EntryCommitted, nodeId, fmt"Entry {entry.index} committed", fmt"term={entry.term}")
  )

  # Re-initialize the Raft implementation (this will load persisted state)
  host.raft.initialize(host.id, callbacks, host.clusterConfig)

  # Notify lifecycle callback
  if host.lifecycleCallback.isSome:
    let event = LifecycleEvent(
      nodeId: host.id,
      state: Up,
      atTime: host.clock.nowMs,
      wipedDb: wipeDb
    )
    host.lifecycleCallback.get()(host.id, event)

proc isAlive*(host: NodeHost): bool =
  ## Check if node is currently alive
  host.alive

proc getState*(host: NodeHost): NodeState =
  ## Get current Raft state (for testing/invariants)
  var state = host.raft.getState()
  # Add the generation counters from the host
  state.electionTimerGeneration = host.electionTimerGeneration
  state.heartbeatTimerGeneration = host.heartbeatTimerGeneration
  return state

# Timer callbacks (to be used by Raft implementation)
proc scheduleTimer*(host: NodeHost, delayMs: int64, callback: TimerCallback): TimerId =
  ## Schedule a timer (election timeout, heartbeat, etc.)
  host.clock.scheduleTimer(delayMs, callback)

proc scheduleVersionedTimer*(host: var NodeHost, delayMs: int64, callback: TimerCallback, kind: TimerKind): TimerId =
  ## Schedule a timer with type information (for future versioning support)
  return host.clock.scheduleTimer(delayMs, callback, kind, host.id)

proc cancelVersionedTimer*(host: var NodeHost, timerId: TimerId, kind: TimerKind): bool =
  ## Cancel a timer (versioning not yet implemented, just basic cancellation)
  return host.clock.cancelTimer(timerId)

proc cancelTimer*(host: NodeHost, timerId: TimerId): bool =
  ## Cancel a scheduled timer
  host.clock.cancelTimer(timerId)

# Persistence callbacks (to be used by Raft implementation)
proc saveTerm*(host: NodeHost, term: RaftNodeTerm) =
  ## Persist current term
  host.storage.persistTerm(host.id, term)

proc saveVotedFor*(host: NodeHost, votedFor: Option[RaftNodeId]) =
  ## Persist voted-for candidate
  host.storage.persistVotedFor(host.id, votedFor)

proc appendLog*(host: NodeHost, entry: LogEntry) =
  ## Append log entry
  host.storage.persistLogEntry(host.id, entry)

proc truncateLog*(host: NodeHost, fromIndex: RaftLogIndex) =
  ## Truncate log from given index
  host.storage.truncateLog(host.id, fromIndex)

proc saveSnapshot*(host: NodeHost, snapshot: Snapshot) =
  ## Save snapshot
  host.storage.saveSnapshot(host.id, snapshot)

proc loadDisk*(host: NodeHost, wipe: bool = false): NodeDisk =
  ## Load persistent state
  host.storage.loadDisk(host.id, wipe)

# Randomness callbacks
proc randomBytes*(host: NodeHost, n: int): seq[byte] =
  ## Generate random bytes
  var bytes = newSeq[byte](n)
  for i in 0..<n:
    bytes[i] = byte(host.rng.next() and 0xFF)
  return bytes

proc randomInt*(host: NodeHost, max: int): int =
  ## Generate random int in [0, max)
  host.rng.nextInt(max)
