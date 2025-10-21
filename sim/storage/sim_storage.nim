## SimStorage - Storage simulation with durability modes and snapshots
##
## Simulates different storage durability characteristics:
## - Durable: immediate persistence, survives crashes
## - Async: delayed commits, can lose recent writes
## - Torn: partial write corruption on crash

import std/tables
import std/options
import std/sequtils
import std/algorithm
import std/strformat

import ../core/sim_rng
import ../core/types
import ../../src/raft/log
import ../../src/raft/types


type
  NodeDisk* = ref object
    ## Persistent state for a single node
    currentTerm*: RaftNodeTerm
    votedFor*: Option[RaftNodeId]
    log*: seq[LogEntry]
    snapshot*: Option[Snapshot]

  Snapshot* = object
    ## Snapshot data
    lastIncludedIndex*: RaftLogIndex
    lastIncludedTerm*: RaftNodeTerm
    data*: seq[byte]  # FSM state

  SimStorage* = ref object
    rng*: SimRng
    mode*: DurabilityMode
    nodes*: Table[RaftNodeId, NodeDisk]
    pendingWrites*: Table[RaftNodeId, seq[proc() {.gcsafe.}] ]  # for Async mode
    tornWriteProb*: float  # probability of torn write in Torn mode

proc newSimStorage*(rng: SimRng, mode: DurabilityMode = Durable,
                   tornWriteProb: float = 0.05): SimStorage =
  SimStorage(
    rng: rng,
    mode: mode,
    nodes: initTable[RaftNodeId, NodeDisk](),
    pendingWrites: initTable[RaftNodeId, seq[proc() {.gcsafe.}]](),
    tornWriteProb: tornWriteProb
  )

proc getNodeDisk*(storage: SimStorage, nodeId: RaftNodeId): NodeDisk =
  ## Get or create disk state for a node
  if not storage.nodes.contains(nodeId):
    storage.nodes[nodeId] = NodeDisk(
      currentTerm: RaftNodeTerm(0),
      votedFor: none(RaftNodeId),
      log: @[],
      snapshot: none(Snapshot)
    )
  return storage.nodes[nodeId]

proc persistTerm*(storage: SimStorage, nodeId: RaftNodeId, term: RaftNodeTerm) =
  ## Persist currentTerm for a node
  case storage.mode:
  of Durable:
    storage.getNodeDisk(nodeId).currentTerm = term
  of Async:
    # Add to pending writes, will be applied later
    storage.pendingWrites.mgetOrPut(nodeId, @[]).add(proc() {.gcsafe.} =
      storage.getNodeDisk(nodeId).currentTerm = term)
  of Torn:
    # Immediate write but may be corrupted
    if storage.rng.rngFor("storage-torn").bernoulli(storage.tornWriteProb):
      # Torn write - don't update
      discard
    else:
      storage.getNodeDisk(nodeId).currentTerm = term

proc persistVotedFor*(storage: SimStorage, nodeId: RaftNodeId, votedFor: Option[RaftNodeId]) =
  ## Persist votedFor for a node
  case storage.mode:
  of Durable:
    storage.getNodeDisk(nodeId).votedFor = votedFor
  of Async:
    storage.pendingWrites.mgetOrPut(nodeId, @[]).add(proc() {.gcsafe.} =
      storage.getNodeDisk(nodeId).votedFor = votedFor)
  of Torn:
    if storage.rng.rngFor("storage-torn").bernoulli(storage.tornWriteProb):
      discard
    else:
      storage.getNodeDisk(nodeId).votedFor = votedFor

proc persistLogEntry*(storage: SimStorage, nodeId: RaftNodeId, entry: LogEntry) =
  ## Append a log entry for a node
  case storage.mode:
  of Durable:
    var disk = storage.getNodeDisk(nodeId)
    # Truncate any conflicting suffix at or after entry.index, then append.
    let startIdx = disk.log.lowerBound(
      LogEntry(index: entry.index, term: RaftNodeTerm(0), kind: rletEmpty),
      proc(a, b: LogEntry): int = cmp(a.index.int, b.index.int))
    if startIdx < disk.log.len and disk.log[startIdx].index == entry.index and disk.log[startIdx].term == entry.term:
      # Idempotent write; keep as-is
      discard
    else:
      if startIdx < disk.log.len:
        disk.log.setLen(startIdx)
      disk.log.add(entry)
  of Async:
    storage.pendingWrites.mgetOrPut(nodeId, @[]).add(proc() {.gcsafe.} =
      var disk = storage.getNodeDisk(nodeId)
      let startIdx = disk.log.lowerBound(
        LogEntry(index: entry.index, term: RaftNodeTerm(0), kind: rletEmpty),
        proc(a, b: LogEntry): int = cmp(a.index.int, b.index.int))
      if startIdx < disk.log.len and disk.log[startIdx].index == entry.index and disk.log[startIdx].term == entry.term:
        discard
      else:
        if startIdx < disk.log.len:
          disk.log.setLen(startIdx)
        disk.log.add(entry))
  of Torn:
    if storage.rng.rngFor("storage-torn").bernoulli(storage.tornWriteProb):
      # Partial write - add corrupted entry
      var corrupted = entry
      if corrupted.kind == rletCommand:
        let maxLen = min(corrupted.command.data.len, storage.rng.rngFor("storage-torn").nextInt(1, corrupted.command.data.len + 1))
        corrupted.command.data.setLen(maxLen)
      var disk = storage.getNodeDisk(nodeId)
      let startIdx = disk.log.lowerBound(
        LogEntry(index: entry.index, term: RaftNodeTerm(0), kind: rletEmpty),
        proc(a, b: LogEntry): int = cmp(a.index.int, b.index.int))
      if startIdx < disk.log.len:
        disk.log.setLen(startIdx)
      disk.log.add(corrupted)
    else:
      var disk = storage.getNodeDisk(nodeId)
      let startIdx = disk.log.lowerBound(
        LogEntry(index: entry.index, term: RaftNodeTerm(0), kind: rletEmpty),
        proc(a, b: LogEntry): int = cmp(a.index.int, b.index.int))
      if startIdx < disk.log.len and disk.log[startIdx].index == entry.index and disk.log[startIdx].term == entry.term:
        discard
      else:
        if startIdx < disk.log.len:
          disk.log.setLen(startIdx)
        disk.log.add(entry)

proc truncateLog*(storage: SimStorage, nodeId: RaftNodeId, fromIndex: RaftLogIndex) =
  ## Truncate log from the given index onward
  var disk = storage.getNodeDisk(nodeId)
  let startIdx = disk.log.lowerBound(LogEntry(index: fromIndex, term: RaftNodeTerm(0), kind: rletEmpty),
    proc(a, b: LogEntry): int = cmp(a.index.int, b.index.int))
  if startIdx < disk.log.len:
    disk.log.setLen(startIdx)

proc saveSnapshot*(storage: SimStorage, nodeId: RaftNodeId, snapshot: Snapshot) =
  ## Save a snapshot for a node
  var disk = storage.getNodeDisk(nodeId)
  disk.snapshot = some(snapshot)
  

  disk = storage.getNodeDisk(nodeId)

  # Truncate log entries that are now included in the snapshot
  let snapIndex = snapshot.lastIncludedIndex
  let startIdx = disk.log.lowerBound(LogEntry(index: snapIndex + 1, term: RaftNodeTerm(0), kind: rletEmpty),
    proc(a, b: LogEntry): int = cmp(a.index.int, b.index.int))
  if startIdx > 0 and startIdx < disk.log.len:
    disk.log.delete(0, startIdx - 1)

proc loadDisk*(storage: SimStorage, nodeId: RaftNodeId, wipe: bool = false): NodeDisk {.gcsafe.} =
  ## Load disk state for a node, optionally wiping it (simulating DB loss)
  if wipe:
    # Simulate complete DB loss - return empty state
    return NodeDisk(
      currentTerm: RaftNodeTerm(0),
      votedFor: none(RaftNodeId),
      log: @[],
      snapshot: none(Snapshot)
    )

  # Apply any pending writes for Async mode
  if storage.pendingWrites.contains(nodeId):
    for writeProc in storage.pendingWrites[nodeId]:
      writeProc()
    storage.pendingWrites.del(nodeId)

  return storage.getNodeDisk(nodeId)

proc commitPending*(storage: SimStorage, nodeId: RaftNodeId) =
  ## Force commit all pending writes for a node (for testing Async mode)
  if storage.pendingWrites.contains(nodeId):
    for writeProc in storage.pendingWrites[nodeId]:
      writeProc()
    storage.pendingWrites.del(nodeId)

proc simulateCrash*(storage: SimStorage, nodeId: RaftNodeId, loseRecentWrites: int = 0) =
  ## Simulate a crash during pending writes
  ## loseRecentWrites: number of recent writes to lose (for Async mode testing)
  if storage.pendingWrites.contains(nodeId):
    echo fmt"Simulating crash for node {nodeId}"
    let pending = storage.pendingWrites[nodeId]
    let numToLose = min(loseRecentWrites, pending.len)

    # Apply only the writes that survive the crash
    for i in 0..<(pending.len - numToLose):
      pending[i]()

    # Clear pending writes (simulating crash)
    storage.pendingWrites.del(nodeId)

proc hasSnapshot*(storage: SimStorage, nodeId: RaftNodeId): bool =
  ## Check if node has a snapshot
  storage.getNodeDisk(nodeId).snapshot.isSome

proc getSnapshot*(storage: SimStorage, nodeId: RaftNodeId): Option[Snapshot] =
  ## Get the snapshot for a node
  storage.getNodeDisk(nodeId).snapshot
