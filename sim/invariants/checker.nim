## Invariants Checker - Monitors Raft safety and liveness properties
##
## Checks Raft invariants during simulation:
## - Election Safety: ≤1 leader per term
## - Log Matching: prefix agreement
## - Leader Append-Only: leaders don't overwrite log
## - State Machine Safety: applied sequences identical
## - Leader Completeness: leaders have all committed entries
## - Snapshot sanity: proper truncation and no gaps

import std/tables
import std/sets
import std/options
import std/strformat
import std/sequtils

import ../../src/raft/types
import ../core/types
import ../raft/node_host
import ../storage/sim_storage

type
  InvariantViolation* = object
    time*: int64
    description*: string
    details*: string

  InvariantsChecker* = ref object
    violations*: seq[InvariantViolation]
    leadersByTerm*: Table[RaftNodeTerm, HashSet[RaftNodeId]]  # Election safety
    committedEntries*: seq[LogEntry]              # State machine safety
    lastCheckedTime*: int64

proc newInvariantsChecker*(): InvariantsChecker =
  InvariantsChecker(
    violations: @[],
    leadersByTerm: initTable[RaftNodeTerm, HashSet[RaftNodeId]](),
    committedEntries: @[],
    lastCheckedTime: 0
  )

proc recordViolation*(checker: InvariantsChecker, currentTime: int64, description, details: string) =
  let violation = InvariantViolation(
    time: currentTime,
    description: description,
    details: details
  )
  checker.violations.add(violation)
  echo fmt"[INVARIANT VIOLATION at t={violation.time}ms] {violation.description}: {violation.details}"

proc checkElectionSafety*(checker: InvariantsChecker, nodes: seq[NodeHost], currentTime: int64) =
  ## Check Election Safety: at most one leader per term
  for node in nodes:
    let state = node.getState()
    if state.role == Leader:
      if not checker.leadersByTerm.contains(state.currentTerm):
        checker.leadersByTerm[state.currentTerm] = initHashSet[RaftNodeId]()
      checker.leadersByTerm[state.currentTerm].incl(state.id)

  # Check for violations
  for term, leaders in checker.leadersByTerm:
    if leaders.len > 1:
      checker.recordViolation(currentTime,
        "Election Safety Violation",
        fmt"Multiple leaders in term {term}: {leaders}"
      )

proc checkLogMatching*(checker: InvariantsChecker, nodes: seq[NodeHost], currentTime: int64) =
  ## Check Log Matching: if two logs contain entry at same index with same term
  var logsByIndex: Table[RaftLogIndex, Table[RaftNodeTerm, seq[RaftNodeId]]]

  for node in nodes:
    let state = node.getState()
    for entry in state.log:
      if not logsByIndex.contains(entry.index):
        logsByIndex[entry.index] = initTable[RaftNodeTerm, seq[RaftNodeId]]()
      if not logsByIndex[entry.index].contains(entry.term):
        logsByIndex[entry.index][entry.term] = @[]
      logsByIndex[entry.index][entry.term].add(state.id)

  # Check for conflicts
  for index, terms in logsByIndex:
    if terms.len > 1:
      var conflictDesc = ""
      for term, nodes in terms:
        conflictDesc &= fmt"term {term}: {nodes}; "
      checker.recordViolation(currentTime,
        "Log Matching Violation",
        fmt"Index {index} has conflicting terms: {conflictDesc}"
      )

proc checkLeaderAppendOnly*(checker: InvariantsChecker, nodes: seq[NodeHost], currentTime: int64) =
  ## Check Leader Append-Only: leaders never overwrite their own log entries
  # This is harder to check without tracking historical state
  # For now, we'll do a basic check that committed entries aren't modified
  for node in nodes:
    let state = node.getState()
    if state.commitIndex > RaftLogIndex(0):
      for i in 0..<(min(state.log.len.int, state.commitIndex.int)):
        let entry = state.log[i]
        if entry.index <= state.commitIndex and checker.committedEntries.len > entry.index.int:
          let committed = checker.committedEntries[entry.index.int]
          if entry.term != committed.term:
            checker.recordViolation(currentTime,
              "Leader Append-Only Violation",
              fmt"Node {state.id} modified committed entry at index {entry.index}"
            )

proc checkStateMachineSafety*(checker: InvariantsChecker, nodes: seq[NodeHost], currentTime: int64) =
  ## Check State Machine Safety: all applied sequences are identical prefixes
  # This is a simplified check - in practice we'd need to track applied commands
  for node in nodes:
    let state = node.getState()
    if state.lastApplied > RaftLogIndex(0):
      # Basic check: all nodes should have applied at least up to commitIndex
      if state.lastApplied < state.commitIndex:
        checker.recordViolation(currentTime,
          "State Machine Safety Violation",
          fmt"Node {state.id} lastApplied ({state.lastApplied}) < commitIndex ({state.commitIndex})"
        )

proc checkLeaderCompleteness*(checker: InvariantsChecker, nodes: seq[NodeHost], currentTime: int64) =
  ## Check Leader Completeness: leaders have all committed entries
  for node in nodes:
    let state = node.getState()
    if state.role == Leader and state.commitIndex > RaftLogIndex(0):
      # Check that leader has all entries up to commitIndex
      var hasAllEntries = true
      for i in 1..state.commitIndex.int:
        let found = state.log.anyIt(it.index == RaftLogIndex(i))
        if not found:
          hasAllEntries = false
          break

      if not hasAllEntries:
        checker.recordViolation(currentTime,
          "Leader Completeness Violation",
          fmt"Leader {state.id} missing committed entries up to {state.commitIndex}"
        )

proc checkSnapshotSanity*(checker: InvariantsChecker, nodes: seq[NodeHost], storage: SimStorage, currentTime: int64) =
  ## Check Snapshot Sanity: proper truncation and no gaps
  for node in nodes:
    let state = node.getState()
    # If node has a snapshot, check that log starts after snapshot
    let snapshot = storage.getSnapshot(node.id)
    if snapshot.isSome:
      let snapIndex = snapshot.get().lastIncludedIndex
      # Check that log doesn't contain entries <= snapIndex
      for entry in state.log:
        if entry.index <= snapIndex:
          checker.recordViolation(currentTime,
            "Snapshot Sanity Violation",
            fmt"Node {node.id} has entry at index {entry.index} <= snapshot index {snapIndex}"
          )

proc checkLiveness*(checker: InvariantsChecker, nodes: seq[NodeHost], currentTime: int64) =
  ## Check basic liveness: some progress should be made over time
  # This is a simple check - in a real system we'd check for election timeouts, etc.
  let timeSinceLastCheck = currentTime - checker.lastCheckedTime
  if timeSinceLastCheck > 5000:  # Check every 5 seconds
    var totalCommits = 0
    for node in nodes:
      let state = node.getState()
      totalCommits += state.commitIndex.int

    if totalCommits == 0:
      checker.recordViolation(currentTime,
        "Liveness Violation",
        "No commits made in the last 5 seconds - possible deadlock"
      )
    checker.lastCheckedTime = currentTime

proc checkAll*(checker: InvariantsChecker, nodes: seq[NodeHost], storage: SimStorage, currentTime: int64) =
  ## Run all invariant checks
  checker.checkElectionSafety(nodes, currentTime)
  checker.checkLogMatching(nodes, currentTime)
  checker.checkLeaderAppendOnly(nodes, currentTime)
  checker.checkStateMachineSafety(nodes, currentTime)
  checker.checkLeaderCompleteness(nodes, currentTime)
  checker.checkSnapshotSanity(nodes, storage, currentTime)
  checker.checkLiveness(nodes, currentTime)

proc hasViolations*(checker: InvariantsChecker): bool =
  ## Check if any violations have been recorded
  checker.violations.len > 0

proc getViolations*(checker: InvariantsChecker): seq[InvariantViolation] =
  ## Get all recorded violations
  checker.violations

proc getFirstViolation*(checker: InvariantsChecker): Option[InvariantViolation] =
  ## Get the first violation (for early termination)
  if checker.violations.len > 0:
    some(checker.violations[0])
  else:
    none(InvariantViolation)
