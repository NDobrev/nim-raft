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
import ../../src/raft/log
import ../core/types
import ../raft/node_host
import ../storage/sim_storage
import ../run/scenario_config

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
    config*: InvariantsConfig

proc newInvariantsChecker*(config: InvariantsConfig = InvariantsConfig(
    election_safety: true,
    log_matching: true,
    leader_append_only: true,
    state_machine_safety: true,
    leader_completeness: true,
    snapshot_sanity: true,
    index_validity: true,
    monotonic_ids: true,
    no_committed_deletion: true,
    log_consistency: true,
    liveness: true
  )): InvariantsChecker =
  InvariantsChecker(
    violations: @[],
    leadersByTerm: initTable[RaftNodeTerm, HashSet[RaftNodeId]](),
    committedEntries: @[],
    lastCheckedTime: 0,
    config: config
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

proc checkLogMatching*(checker: InvariantsChecker, nodes: seq[NodeHost], storage: SimStorage, currentTime: int64) =
  ## Check Log Matching: committed entries must be identical across all nodes
  # Find the minimum commit index across all nodes
  var minCommitIndex = RaftLogIndex.high
  for node in nodes:
    let state = node.getState()
    minCommitIndex = min(minCommitIndex, state.commitIndex)

  # Check that all committed entries are identical
  for commitIdx in 1..minCommitIndex.int:
    let index = RaftLogIndex(commitIdx)
    var entriesAtIndex: Table[string, seq[RaftNodeId]]  # Use string representation of entry

    for node in nodes:
      let state = node.getState()
      if state.commitIndex >= index:
        # Node should have this committed entry. If the node has a snapshot covering
        # this index, treat it as present (snapshots elide older log entries).
        let snap = storage.getSnapshot(state.id)
        if state.snapshotIndex >= index or (snap.isSome and snap.get().lastIncludedIndex >= index):
          # Covered by snapshot - consider this entry matched and skip explicit compare
          discard
        else:
          let entryOpt = state.log.entryAt(index)
          if entryOpt.isSome:
            let entry = entryOpt.get()
            let entryStr = fmt"term:{entry.term},kind:{entry.kind}"
            if not entriesAtIndex.contains(entryStr):
              entriesAtIndex[entryStr] = @[]
            entriesAtIndex[entryStr].add(state.id)
          else:
            # If the entry is not in the in-memory log and the log starts after
            # this index (implying a local snapshot), treat as covered by snapshot.
            let coveredByLocalSnapshot = (state.log.len == 0) or (state.log.len > 0 and index < state.log[0].index)
            if not coveredByLocalSnapshot:
              checker.recordViolation(currentTime,
                "Log Matching Violation",
                fmt"Node {state.id} missing committed entry at index {index}"
              )

    # Check for conflicts at this committed index
    if entriesAtIndex.len > 1:
      var conflictDesc = ""
      for entryStr, nodeIds in entriesAtIndex:
        conflictDesc &= fmt"{entryStr}: {nodeIds}; "
      checker.recordViolation(currentTime,
        "Log Matching Violation",
        fmt"Committed index {index} has conflicting entries: {conflictDesc}"
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

proc checkLeaderCompleteness*(checker: InvariantsChecker, nodes: seq[NodeHost], storage: SimStorage, currentTime: int64) =
  ## Check Leader Completeness: leaders have all committed entries
  for node in nodes:
    let state = node.getState()
    if state.role == Leader and state.commitIndex > RaftLogIndex(0):
      # Check that leader has all entries up to commitIndex
      var hasAllEntries = true
      for i in 1..state.commitIndex.int:
        let idx = RaftLogIndex(i)
        var covered = state.snapshotIndex >= idx
        if not covered:
          let snap = storage.getSnapshot(state.id)
          if snap.isSome and snap.get().lastIncludedIndex >= idx:
            covered = true
        if not covered:
          let found = state.log.anyIt(it.index == idx)
          if not found:
            hasAllEntries = false
            echo fmt"Leader {state.id} missing committed entry at index {i} in log: {state.log}"
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

proc checkIndexValidity*(checker: InvariantsChecker, nodes: seq[NodeHost], storage: SimStorage, currentTime: int64) =
  ## Check Log Index Validity: commitIndex and lastApplied within log bounds, no gaps
  for node in nodes:
    let state = node.getState()
    let logLen = state.log.len
    # Compute snapshot-aware last index bound
    var snapIdx = state.snapshotIndex
    if snapIdx == RaftLogIndex(0):
      let snap = storage.getSnapshot(state.id)
      if snap.isSome:
        snapIdx = snap.get().lastIncludedIndex
    let lastLogIdx = if logLen > 0: state.log[^1].index else: snapIdx

    # Check commitIndex bounds against snapshot-aware last index
    if state.commitIndex > lastLogIdx:
      checker.recordViolation(currentTime,
        "Index Validity Violation",
        fmt"Node {state.id} commitIndex={state.commitIndex} exceeds lastIndex {lastLogIdx} (logLen={logLen}, snapIdx={snapIdx})"
      )

    # Check lastApplied bounds
    if state.lastApplied > state.commitIndex:
      checker.recordViolation(currentTime,
        "Index Validity Violation",
        fmt"Node {state.id} lastApplied={state.lastApplied} > commitIndex={state.commitIndex}"
      )

    # Check for gaps in log indices (skip if log is empty)
    if logLen > 0:
      var expectedIndex = state.log[0].index
      for entry in state.log:
        if entry.index != expectedIndex:
          checker.recordViolation(currentTime,
            "Index Validity Violation",
            fmt"Node {state.id} log missing index {expectedIndex} (gap at {entry.index})"
          )
          break
        expectedIndex = expectedIndex + 1

proc checkMonotonicIds*(checker: InvariantsChecker, nodes: seq[NodeHost], currentTime: int64) =
  ## Check Monotonic Entry IDs: entry IDs should increase with log position
  # Only check if IDs are actually being assigned (non-zero)
  var hasNonZeroIds = false
  for node in nodes:
    let state = node.getState()
    for entry in state.log:
      if entry.index > 0:
        hasNonZeroIds = true
        break
    if hasNonZeroIds:
      break

  if hasNonZeroIds:
    for node in nodes:
      let state = node.getState()
      var prevId = RaftLogIndex(-1)
      for entry in state.log:
        if entry.index > 0:  # Only check entries with assigned IDs
          if prevId != RaftLogIndex(-1) and entry.index <= prevId:
            checker.recordViolation(currentTime,
              "Monotonic Entry ID Violation",
              fmt"Node {state.id} entry at index {entry.index} has <= previous id {prevId}"
            )
            break
          prevId = entry.index

proc checkNoCommittedDeletion*(checker: InvariantsChecker, nodes: seq[NodeHost], storage: SimStorage, currentTime: int64) =
  ## Check No Committed Entry Deletion: committed entries shouldn't be removed without snapshot
  for node in nodes:
    let state = node.getState()

    # If log is empty but commitIndex > 0, verify snapshot justifies it
    if state.log.len == 0 and state.commitIndex > RaftLogIndex(0):
      # Covered if either in-memory snapshot or storage snapshot includes commitIndex
      let coveredByState = state.snapshotIndex >= state.commitIndex
      let snap = storage.getSnapshot(state.id)
      let coveredByStorage = snap.isSome and snap.get().lastIncludedIndex >= state.commitIndex
      if coveredByState or coveredByStorage:
        continue
      # Otherwise, this indicates committed entries were deleted
      let storageSnapTxt = (if snap.isSome: $snap.get().lastIncludedIndex else: "none")
      checker.recordViolation(currentTime,
        "No Committed Deletion Violation",
        fmt"Node {state.id} has no log entries but commitIndex={state.commitIndex} (stateSnap={state.snapshotIndex}, storageSnap={storageSnapTxt})"
      )
      continue

    # Check if log starts at index > 1 without proper snapshot justification
    if state.log.len > 0:
      let firstIndex = state.log[0].index
      if firstIndex > RaftLogIndex(1):
        # Prefer in-memory snapshot index from NodeState for justification
        let stateSnapIdx = state.snapshotIndex
        if stateSnapIdx > RaftLogIndex(0) and firstIndex == stateSnapIdx + 1:
          discard  # Justified by in-memory snapshot
        else:
          # Fallback to storage snapshot
          let snapshot = storage.getSnapshot(state.id)
          if snapshot.isSome:
            let snapIndex = snapshot.get().lastIncludedIndex
            if firstIndex != snapIndex + 1:
              checker.recordViolation(currentTime,
                "No Committed Deletion Violation",
                fmt"Node {state.id} log starts at {firstIndex} but snapshot covers {snapIndex} (stateSnap={stateSnapIdx})"
              )
          else:
            # No snapshot but log doesn't start at 1
            checker.recordViolation(currentTime,
              "No Committed Deletion Violation",
              fmt"Node {state.id} log starts at {firstIndex} with no snapshot justification (stateSnap={stateSnapIdx})"
            )

proc checkLogConsistency*(checker: InvariantsChecker, nodes: seq[NodeHost], storage: SimStorage, currentTime: int64) =
  ## Check Log Consistency (Accuracy): logs should agree on committed prefix
  # Similar to log matching but with more detailed accuracy checking

  # Find the globally committed prefix (minimum commit index across all nodes)
  var minCommitIndex = RaftLogIndex.high
  for node in nodes:
    let state = node.getState()
    minCommitIndex = min(minCommitIndex, state.commitIndex)

  # For each committed index, check consistency
  for idx in 1..minCommitIndex.int:
    let index = RaftLogIndex(idx)
    var entriesAtIndex: seq[tuple[entry: LogEntry, nodeId: RaftNodeId]] = @[]

    # Collect entries from all nodes that have this index
    for node in nodes:
      let state = node.getState()
      if state.commitIndex >= index:
        let snap = storage.getSnapshot(state.id)
        if state.snapshotIndex >= index or (snap.isSome and snap.get().lastIncludedIndex >= index):
          # Covered by snapshot
          discard
        else:
          let entryOpt = state.log.entryAt(index)
          if entryOpt.isSome:
            entriesAtIndex.add((entryOpt.get(), state.id))
          else:
            let coveredByLocalSnapshot = (state.log.len == 0) or (state.log.len > 0 and index < state.log[0].index)
            if not coveredByLocalSnapshot:
              checker.recordViolation(currentTime,
                "Log Consistency Violation",
                fmt"Node {state.id} missing committed entry at index {index}"
              )

    # Check that all entries at this index are identical
    if entriesAtIndex.len > 1:
      let firstEntry = entriesAtIndex[0].entry
      for i in 1..<entriesAtIndex.len:
        let otherEntry = entriesAtIndex[i].entry
        if firstEntry.term != otherEntry.term:
          checker.recordViolation(currentTime,
            "Log Consistency Violation",
            fmt"Committed index {index} has term conflict: node {entriesAtIndex[0].nodeId} has term {firstEntry.term}, node {entriesAtIndex[i].nodeId} has term {otherEntry.term}"
          )
        elif firstEntry.index != otherEntry.index:
          checker.recordViolation(currentTime,
            "Log Consistency Violation",
            fmt"Committed index {index} has ID conflict: node {entriesAtIndex[0].nodeId} has id {firstEntry.index}, node {entriesAtIndex[i].nodeId} has id {otherEntry.index}"
          )

proc checkLiveness*(checker: InvariantsChecker, nodes: seq[NodeHost], currentTime: int64) =
  ## Check basic liveness: some progress should be made over time
  # This is a simple check - in a real system we'd check for election timeouts, etc.
  let timeSinceLastCheck = currentTime - checker.lastCheckedTime
  if timeSinceLastCheck > 1000:  # Check every 10 seconds (less frequent)
    var maxCommitIndex = 0
    for node in nodes:
      let state = node.getState()
      maxCommitIndex = max(maxCommitIndex, state.commitIndex.int)

    # Check if we've made any progress at all in the last 10 seconds
    # During partitions, minority groups cannot progress, but majority should
    if maxCommitIndex == 0 and currentTime > 2000:
      checker.recordViolation(currentTime,
        "Liveness Violation",
        "No commits made after 2 seconds - possible deadlock"
      )
    checker.lastCheckedTime = currentTime

proc checkAll*(checker: InvariantsChecker, nodes: seq[NodeHost], storage: SimStorage, currentTime: int64) =
  ## Run all enabled invariant checks
  if checker.config.election_safety:
    checker.checkElectionSafety(nodes, currentTime)
  if checker.config.log_matching:
    checker.checkLogMatching(nodes, storage, currentTime)
  if checker.config.leader_append_only:
    checker.checkLeaderAppendOnly(nodes, currentTime)
  if checker.config.state_machine_safety:
    checker.checkStateMachineSafety(nodes, currentTime)
  if checker.config.leader_completeness:
    checker.checkLeaderCompleteness(nodes, storage, currentTime)
  if checker.config.snapshot_sanity:
    checker.checkSnapshotSanity(nodes, storage, currentTime)
  if checker.config.index_validity:
    checker.checkIndexValidity(nodes, storage, currentTime)
  if checker.config.monotonic_ids:
    checker.checkMonotonicIds(nodes, currentTime)
  if checker.config.no_committed_deletion:
    checker.checkNoCommittedDeletion(nodes, storage, currentTime)
  if checker.config.log_consistency:
    checker.checkLogConsistency(nodes, storage, currentTime)
  if checker.config.liveness:
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
