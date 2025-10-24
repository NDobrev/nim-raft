## Core types used throughout the simulation
##
## Defines fundamental types, RPC envelopes, and data structures
## used by the Raft simulation harness.

import std/tables
import std/options

# Import main codebase types to avoid duplication
import ../../src/raft/types
import ../../src/raft/log
import ../../src/raft/consensus_state_machine

# Re-export types from main codebase for convenience
export log.LogEntry

# Types are imported directly from main codebase

# Use RaftRpcMessage types directly from main codebase

type
  # Timer types (forward declared for SimEvent)
  TimerId* = distinct uint64
  TimerCallback* = proc(id: TimerId) {.gcsafe, closure.}

  # Node state for simulation
  NodeRole* = enum
    Follower
    Candidate
    Leader

  NodeState* = object
    id*: RaftNodeId
    role*: NodeRole
    currentTerm*: RaftNodeTerm
    votedFor*: Option[RaftNodeId]
    log*: seq[LogEntry]
    commitIndex*: RaftLogIndex
    lastApplied*: RaftLogIndex
    snapshotIndex*: RaftLogIndex
    snapshotTerm*: RaftNodeTerm
    # Timer generation counters for versioning
    electionTimerGeneration*: int
    heartbeatTimerGeneration*: int
    # Leader state
    nextIndex*: Table[RaftNodeId, RaftLogIndex]   # only valid when leader
    matchIndex*: Table[RaftNodeId, RaftLogIndex]  # only valid when leader

  # Unified event types for simulation
  EventKind* = enum
    TimerEvent
    NetworkEvent
    LifecycleEvt

  TimerKind* = enum
    ElectionTimeout
    HeartbeatTimeout
    CustomTimer

  TimerEventData* = object
    id*: TimerId
    callback*: TimerCallback
    kind*: TimerKind
    generation*: int  # for versioning/cancellation
    cancelled*: bool
    periodic*: bool
    interval*: int64
    # Node context for timer ownership (if applicable)
    nodeId*: RaftNodeId

  NetworkEventData* = object
    fromNode*: RaftNodeId
    toNode*: RaftNodeId
    rpc*: RaftRpcMessage
    duplicate*: bool   # true if this is a duplicate delivery

  LifecycleEventData* = object
    nodeId*: RaftNodeId
    state*: LifecycleState
    wipedDb*: bool

  SimEvent* = object
    deliverAt*: int64  # simulation time in ms
    case kind*: EventKind
    of TimerEvent: timer*: TimerEventData
    of NetworkEvent: network*: NetworkEventData
    of LifecycleEvt: lifecycle*: LifecycleEventData

  # Legacy NetEvent for backward compatibility (will be removed)
  NetEvent* = object
    fromNode*: RaftNodeId
    toNode*: RaftNodeId
    deliverAt*: int64  # simulation time in ms
    rpc*: RaftRpcMessage
    duplicate*: bool   # true if this is a duplicate delivery

  # Storage durability modes
  DurabilityMode* = enum
    Durable    # writes immediately visible, survive crash
    Async      # commit delays, can lose last K writes on crash
    Torn       # probabilistic partial writes

  # Node lifecycle state
  LifecycleState* = enum
    Up
    Down

  LifecycleEvent* = object
    nodeId*: RaftNodeId
    state*: LifecycleState
    atTime*: int64
    wipedDb*: bool  # true if DB was wiped on restart

  # Simulation configuration types
  ClusterConfig* = object
    nodeCount*: int
    electionTimeoutMin*: int64
    electionTimeoutMax*: int64
    heartbeatInterval*: int64

  StorageConfig* = object
    durability*: DurabilityMode
    snapshotEnabled*: bool
    snapshotMaxEntries*: int
    snapshotMaxBytes*: int64

  NetConfig* = object
    baseLatency*: int64
    jitter*: int64
    p99Latency*: int64
    dropPercent*: float
    duplicatePercent*: float
    reorderWindow*: int

  # Partition configuration
  Partition* = object
    components*: seq[seq[RaftNodeId]]  # disjoint sets of nodes
    startTime*: int64
    endTime*: Option[int64]       # none = permanent

  # Workload configuration
  WorkloadConfig* = object
    kvEnabled*: bool
    proposeRate*: float  # expected proposals per tick
    putPercent*: float
    getPercent*: float
    keySpace*: int
    zipfExponent*: float

  # Zipf distribution sampler for key selection
  ZipfSampler* = object
    cdf*: seq[float]  # monotonically increasing, last = 1.0
    N*: int           # number of keys

  # Stop conditions
  StopCondition* = object
    maxTime*: Option[int64]
    minCommits*: Option[int]

  # Event tracing system for diagnostics
  EventTraceKind* = enum
    MessageSend
    MessageReceive
    LeaderElected
    LogAppend
    LogCommit
    EntryCommitted
    TimerFired
    NodeLifecycle
    InvariantCheck
    DebugLog

  EventTraceEntry* = object
    timestamp*: int64
    kind*: EventTraceKind
    nodeId*: RaftNodeId
    description*: string
    details*: string

  EventTrace* = seq[EventTraceEntry]

# NodeId operators are inherited from RaftNodeId
# Term and LogIndex operators are inherited from the main codebase

# Event ordering for priority queue (earlier delivery time first, stable sort by kind)
proc `<`*(a, b: SimEvent): bool =
  if a.deliverAt != b.deliverAt:
    return a.deliverAt < b.deliverAt
  return ord(a.kind) < ord(b.kind)

proc newLogEntry*(term: RaftNodeTerm, index: RaftLogIndex, data: seq[byte]): LogEntry =
  LogEntry(term: term, index: index, kind: rletCommand, command: Command(data: data))

proc data*(entry: LogEntry): seq[byte] =
  ## Get the data from a LogEntry, handling the discriminated union structure
  case entry.kind:
  of rletCommand: entry.command.data
  of rletConfig: @[]  # Config entries have no command data
  of rletEmpty: @[]   # Empty entries have no data

proc isEmpty*(log: seq[LogEntry]): bool = log.len == 0

proc lastIndex*(log: seq[LogEntry]): RaftLogIndex =
  if log.isEmpty: RaftLogIndex(0) else: log[^1].index

proc lastTerm*(log: seq[LogEntry]): RaftNodeTerm =
  if log.isEmpty: RaftNodeTerm(0) else: log[^1].term

proc entryAt*(log: seq[LogEntry], index: RaftLogIndex): Option[LogEntry] =
  for entry in log:
    if entry.index == index:
      return some(entry)
  return none(LogEntry)
