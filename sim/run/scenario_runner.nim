## Scenario Runner - Orchestrates Raft simulation scenarios
##
## Drives time advancement, injects workload, executes fault schedules,
## and checks invariants throughout the simulation.

import std/tables
import std/options
import std/sequtils
import std/strformat
import std/strutils
import std/json
import std/algorithm

import ../core/sim_clock
import ../core/sim_rng
import ../core/sim_scheduler
import ../storage/sim_storage
import ../net/sim_net
import ../raft/node_host
import ../raft/raft_sm_adapter
import ../raft/raft_interface
import ../invariants/checker
import ../core/types
import ../../src/raft/types
import ../../src/raft/consensus_state_machine
import ../report/json_writer
import ../report/html_timeline
import scenario_config

type
  ScenarioRunner* = ref object
    config*: ScenarioYaml
    clock*: SimClock
    rng*: SimRng
    scheduler*: SimScheduler
    storage*: SimStorage
    net*: SimNet
    nodes*: Table[RaftNodeId, NodeHost]
    lifecycleEvents*: seq[LifecycleEvent]
    invariants*: InvariantsChecker
    commitCount*: int  # Track total commits across all nodes
    proposalAttempts*: int  # Track total proposal attempts
    jsonWriter*: JsonTraceWriter
    eventTrace*: EventTrace  # Rolling event trace for diagnostics
    recordedViolationCount*: int  # how many violations have been pushed to trace
    # Heartbeat counters
    heartbeatsSent*: int
    heartbeatsDelivered*: int
    # Uptime accounting across the simulation (milliseconds)
    uptimeMs*: Table[RaftNodeId, int64]
    downtimeMs*: Table[RaftNodeId, int64]
    # Last lifecycle transition times
    lastUpAtMs*: Table[RaftNodeId, int64]
    lastDownAtMs*: Table[RaftNodeId, int64]
    # Concurrency limit bookkeeping for wipe-db restarts
    pendingWipes*: Table[RaftNodeId, bool]
    # Progress reporting (console)
    lastProgressPct*: int
    lastProgressLine*: string

proc scheduleSnapshotPolicy*(runner: ScenarioRunner)
proc getAllNodes*(runner: ScenarioRunner): seq[NodeHost]

const MaxTraceEntries = 400

proc dumpRecentEvents*(runner: ScenarioRunner): string =
  ## Dump the recent event trace for diagnostics
  var lines: seq[string]
  lines.add(fmt"Recent Events (last {runner.eventTrace.len}):")

  if runner.eventTrace.len == 0:
    lines.add("  No events recorded")
  else:
    # Collect events from ring buffer (already in chronological order)
    var events: seq[EventTraceEntry]
    for entry in runner.eventTrace.items():
      events.add(entry)
    
    # Sort by timestamp to ensure correct order
    events.sort(proc(a, b: EventTraceEntry): int = 
      cmp(a.timestamp, b.timestamp)
    )
    
    for entry in events:
      lines.add(fmt"  [{entry.timestamp}ms] {entry.nodeId}: {entry.kind} - {entry.description} {entry.details}")
      
  lines.add("")
  result = lines.join("\n")

proc logEvent*(runner: ScenarioRunner, timestamp: int64, kind: EventTraceKind, nodeId: RaftNodeId,
               description: string, details: string = "") =
  ## Log an event to the rolling trace buffer for diagnostics
  let entry = EventTraceEntry(
    timestamp: timestamp,
    kind: kind,
    nodeId: nodeId,
    description: description,
    details: details
  )
  # Ring buffer automatically handles size limit
  runner.eventTrace.add(entry)

proc newScenarioRunner*(config: ScenarioYaml): ScenarioRunner =
  let rng = newSimRng(config.seed)
  let clock = newSimClock()
  let scheduler = newSimScheduler(clock)
  # Map scenario storage durability to simulator storage mode
  let storageMode =
    case config.storage.durability
    of scenario_config.StorageDurability.Durable: DurabilityMode.Durable
    of scenario_config.StorageDurability.Async: DurabilityMode.Async
    of scenario_config.StorageDurability.Torn: DurabilityMode.Torn
  let storage = newSimStorage(rng.rngFor("storage"), storageMode)
  let net = newSimNet(rng.rngFor("net"))

  # Configure network fault policy based on scenario config
  let defaultPolicy = NetFaultPolicy(
    latencyBase: config.net.latency_ms.base,
    latencyJitter: config.net.latency_ms.jitter,
    latencyP99: config.net.latency_ms.p99,
    dropPercent: config.net.drop_pct / 100.0,  # Convert from percentage to fraction
    duplicatePercent: config.net.dup_pct / 100.0,  # Convert from percentage to fraction
    reorderWindow: config.net.reorder_window
  )
  net.defaultPolicy = defaultPolicy

  var runner = ScenarioRunner(
    config: config,
    clock: clock,
    rng: rng,
    scheduler: scheduler,
    storage: storage,
    net: net,
    nodes: initTable[RaftNodeId, NodeHost](),
    lifecycleEvents: @[],
    commitCount: 0,
    proposalAttempts: 0,
    jsonWriter: newJsonTraceWriter(config, config.seed),
    eventTrace: newRingBuffer[EventTraceEntry](MaxTraceEntries),
    recordedViolationCount: 0,
    uptimeMs: initTable[RaftNodeId, int64](),
    downtimeMs: initTable[RaftNodeId, int64](),
    lastUpAtMs: initTable[RaftNodeId, int64](),
    lastDownAtMs: initTable[RaftNodeId, int64]()
    ,pendingWipes: initTable[RaftNodeId, bool]()
    ,lastProgressPct: -1
    ,lastProgressLine: ""
  )

  # Initialize invariants checker
  let invariantsConfig = if config.invariants.isSome:
                           config.invariants.get()
                         else:
                           InvariantsConfig(
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
                           )
  runner.invariants = newInvariantsChecker(invariantsConfig)

  # Collect all node IDs first
  var allNodeIds: seq[RaftNodeId]
  for i in 0..<config.cluster.nodes:
    allNodeIds.add(newRaftNodeId($i))

  # Create a shared RNG for election timeouts to ensure each node gets a different timeout
  let electionRng = rng.rngFor("election-timeouts")

  # Create nodes
  for i in 0..<config.cluster.nodes:
    let nodeId = newRaftNodeId($i)
    let nodeRng = rng.rngFor(nodeId)
    let raftImpl = newRaftSmAdapter(config, nodeId, electionRng)
    let host = newNodeHost(
      nodeId,
      clock,
      scheduler,
      nodeRng,
      storage,
      net,
      raftImpl,
      allNodeIds,
      some(proc(timeMs: int64, rpc: RaftRpcMessage, fromNode, toNode: RaftNodeId) =
        runner.jsonWriter.recordRpcEvent(timeMs, rpc, fromNode, toNode)
        if rpc.kind == AppendRequest and rpc.appendRequest.entries.len == 0:
          runner.heartbeatsSent += 1
      ),
      some(proc(nodeId: RaftNodeId, event: LifecycleEvent) =
        runner.lifecycleEvents.add(event)
        # Track last lifecycle transition times
        if event.state == Down:
          runner.lastDownAtMs[nodeId] = event.atTime
        else:
          runner.lastUpAtMs[nodeId] = event.atTime
        # Emit fault events for visualization: node_stop/node_restart
        let faultType = if event.state == Down: "node_stop" else: "node_restart"
        let details = %*{ "nodeId": parseInt(nodeId.id), "state": $event.state, "wipedDb": event.wipedDb }
        runner.jsonWriter.recordFaultEvent(event.atTime, faultType, details)
      ),
      some(proc(kind: EventTraceKind, timeMs: int64, nodeId: RaftNodeId, description: string, details: string = "") =
        logEvent(runner, timeMs, kind, nodeId, description, details)),
      some(proc(timeMs: int64, nodeId: RaftNodeId, entry: LogEntry) =
        runner.jsonWriter.recordCommittedEvent(timeMs, nodeId, entry))
    )
    runner.nodes[nodeId] = host
    # Initialize per-node uptime/downtime counters and last transition
    runner.uptimeMs[nodeId] = 0
    runner.downtimeMs[nodeId] = 0
    runner.lastUpAtMs[nodeId] = 0
    runner.lastDownAtMs[nodeId] = -1

  # Schedule snapshot policy if configured
  runner.scheduleSnapshotPolicy()
  return runner

proc scheduleNodeRestart(runner: ScenarioRunner, nid: RaftNodeId, wdb: bool, delayMs: int64) =
  ## Schedule a single node restart after delayMs.
  ## Important: keep nid/wdb as parameters so each scheduled closure
  ## captures its own immutable copy and doesn't get aliasing issues.
  let id = runner.scheduler.scheduleOnce(delayMs, proc(id: TimerId) {.gcsafe.} =
    #echo fmt"Timer {id.uint64} fired, Restarting node {nid} with wipeDb={wdb} time={runner.clock.nowMs}ms"
    if runner.nodes.contains(nid) and not runner.nodes[nid].isAlive():
      runner.nodes[nid].start(wdb)
    else:
      #echo fmt"Failed to restart node {nid} with wipeDb={wdb} time={runner.clock.nowMs}ms, runner.nodes.contains(nid)={runner.nodes.contains(nid)} runner.nodes[nid].isAlive()={runner.nodes[nid].isAlive()}"
      discard
    # Clear pending wipe tracking regardless, so the limiter frees capacity
    if runner.pendingWipes.contains(nid):
      runner.pendingWipes.del(nid)
  )
  #echo fmt"Timer {id.uint64} scheduled, Restarting node {nid} with wipeDb={wdb} in {delayMs}ms"

proc scheduleSnapshotPolicy*(runner: ScenarioRunner) =
  ## Periodically check snapshot thresholds and trigger local snapshots
  if not runner.config.storage.snapshot.enabled:
    return
  # Determine entry threshold from max_log_entries
  let maxEntriesOpt = runner.config.storage.snapshot.max_log_entries
  let hasEntryThreshold = maxEntriesOpt.isSome
  let threshold = if maxEntriesOpt.isSome: maxEntriesOpt.get() else: 0
  # Determine scheduling period
  let periodMs = if runner.config.storage.snapshot.snapshot_each_ms.isSome:
                   runner.config.storage.snapshot.snapshot_each_ms.get()
                 else:
                   200'i64
  let runnerRef = runner
  discard runner.scheduler.schedulePeriodic("snapshot-policy", periodMs, proc(id: TimerId) {.gcsafe.} =
    # For each alive node, decide if snapshot should be taken
    for node in runnerRef.nodes.values:
      if node.isAlive():
        let state = node.getState()
        let snapOpt = runnerRef.storage.getSnapshot(state.id)
        let lastSnapIdx = if snapOpt.isSome: snapOpt.get().lastIncludedIndex else: RaftLogIndex(0)
        var shouldSnap = false
        if hasEntryThreshold:
          # Trigger based on current in-memory log length rather than commit distance.
          # max_log_entries describes how many entries we want to keep in memory.
          # After a snapshot, the log is compacted, so use log size as the signal.
          let logLen = state.log.len
          if logLen >= threshold and state.commitIndex > lastSnapIdx:
            shouldSnap = true
        else:
          # time-based only: snapshot each period
          shouldSnap = state.commitIndex > lastSnapIdx
        if shouldSnap:
          if node.raft.forceLocalSnapshot(state.commitIndex):
            # Record a snapshot event for visualization
            let details = %*{ "nodeId": parseInt(state.id.id),
                             "index": state.commitIndex.uint64,
                             "term": state.currentTerm.uint64 }
            runnerRef.jsonWriter.recordFaultEvent(runnerRef.clock.nowMs, "snapshot", details)
  )

proc getAllNodes*(runner: ScenarioRunner): seq[NodeHost] =
  ## Get all node hosts
  toSeq(runner.nodes.values)

proc deliverEvent*(runner: ScenarioRunner, event: SimEvent) =
  ## Deliver an event to the appropriate handler
  case event.kind:
  of NetworkEvent:
    # Convert to legacy NetEvent and deliver
    if runner.nodes.contains(event.network.toNode):
      let host = runner.nodes[event.network.toNode]
      #echo fmt"time: {runner.clock.nowMs} Delivering {event.network.rpc.currentTerm} from {event.network.fromNode} to {event.network.toNode}"
      host.step(event.network.rpc)
      # Update network stats
      runner.net.deliveredCount += 1
      # Count delivered heartbeats (AppendEntries with no entries)
      if event.network.rpc.kind == AppendRequest and event.network.rpc.appendRequest.entries.len == 0:
        runner.heartbeatsDelivered += 1
      # Log message receive event
      let rpcKind = case event.network.rpc.kind:
        of VoteRequest: "VoteRequest"
        of VoteReply: "VoteReply"
        of AppendRequest: "AppendRequest"
        of AppendReply: "AppendReply"
        of InstallSnapshot: "InstallSnapshot"
        of SnapshotReply: "SnapshotReply"
      runner.logEvent(event.deliverAt, MessageReceive, event.network.toNode,
                      fmt"Received {rpcKind} from {event.network.fromNode}",
                      fmt"term={event.network.rpc.currentTerm}")
  of TimerEvent:
    # Log timer fires with node attribution when available
    let nid = if event.timer.nodeId.id.len > 0: event.timer.nodeId else: newRaftNodeId("scheduler")
    runner.logEvent(event.deliverAt, TimerFired, nid,
                    fmt"Timer fired: {event.timer.kind}",
                    fmt"timerId={event.timer.id.uint64}")
  of LifecycleEvt:
    # Handle partition lifecycle events
    if event.lifecycle.nodeId.id == "partition-activate":
      # Apply partitions from scenario config that start at this time
      for partitionSpec in runner.config.partitions:
        if partitionSpec.at_ms == event.deliverAt:
          # Check if this is a healing partition (empty components)
          if partitionSpec.components.len == 0 or partitionSpec.components.allIt(it.len == 0):
            # This is a healing event - clear all partitions
            runner.net.clearPartitions()
            echo fmt"Healed all partitions at {partitionSpec.at_ms}ms"
            runner.logEvent(event.deliverAt, DebugLog, newRaftNodeId("partition"),
                            "Partitions healed", "all components reconnected")
          else:
            # This is a partitioning event - apply the partition
            var components: seq[seq[RaftNodeId]] = @[]
            for comp in partitionSpec.components:
              var nodeIds: seq[RaftNodeId] = @[]
              for nodeIdx in comp:
                nodeIds.add(newRaftNodeId($nodeIdx))
              components.add(nodeIds)

            let endTime = if partitionSpec.heal.isSome and partitionSpec.heal.get():
                         some(event.deliverAt + 5000)  # Heal after 5 seconds for now
                       else:
                         none(int64)

            let partition = Partition(
              components: components,
              startTime: partitionSpec.at_ms,
              endTime: endTime
            )
            runner.net.addPartition(partition)
            echo fmt"Applied partition at {partitionSpec.at_ms}ms: {partition.components}"
            # Record partition apply to event trace for context
            runner.logEvent(event.deliverAt, DebugLog, newRaftNodeId("partition"),
                            fmt"Partition applied", fmt"components={partition.components}")
          break

proc dumpState*(runner: ScenarioRunner): string =
  ## Dump the current cluster state for diagnostics
  var lines: seq[string]
  lines.add("Cluster State:")
  lines.add(fmt"  Time: {runner.clock.nowMs}ms")
  lines.add(fmt"  Total commits: {runner.commitCount}")
  lines.add(fmt"  Proposal attempts: {runner.proposalAttempts}")

  for node in runner.getAllNodes():
    if node.isAlive():
      let state = node.getState()
      let lastLogIndex = if state.log.len > 0: state.log[^1].index else: RaftLogIndex(0)
      lines.add(fmt"  Node {state.id}: {state.role}, term={state.currentTerm}, commit={state.commitIndex}")
      lines.add(fmt"    Log: {state.log.len} entries, lastLogIndex={lastLogIndex}, lastApplied={state.lastApplied}")
      if state.role == Leader:
        lines.add(fmt"    Leader state: nextIndex={state.nextIndex}, matchIndex={state.matchIndex}")

  lines.add("")
  result = lines.join("\n")

proc run*(runner: ScenarioRunner): bool =
  ## Run the scenario until completion or timeout

  # Set up partition scheduling from scenario config
  for partitionSpec in runner.config.partitions:
    let specCopy = partitionSpec  # Create a copy to capture
    let runnerRef = runner  # Capture the runner ref

    # Schedule partition activation
    runner.clock.scheduleEvent(SimEvent(
      deliverAt: specCopy.at_ms,
      kind: LifecycleEvt,
      lifecycle: LifecycleEventData(
        nodeId: newRaftNodeId("partition-activate"),  # Dummy node ID for partition events
        state: Up,  # Using Up to indicate partition activation
        wipedDb: false
      )
    ))

  # Set up lifecycle chaos from restart policies
  for policy in runner.config.node_lifecycle.restart_policies:
    let policyCopy = policy  # Create a copy to capture
    let runnerRef = runner  # Capture the runner ref
    discard runner.scheduler.schedulePeriodic(
      fmt"restart-policy-{policyCopy.selector}",
      policyCopy.period_ms,
      proc(id: TimerId) {.gcsafe.} =
        let chaosRng = runnerRef.rng.rngFor(fmt"restart-{policyCopy.selector}")
        let targetNodes = if policyCopy.selector == "any":
                           toSeq(runnerRef.nodes.keys)
                         else:
                           # Parse "node:X" format
                           let parts = policyCopy.selector.split(":")
                           if parts.len == 2 and parts[0] == "node":
                             @[newRaftNodeId(parts[1])]
                           else:
                             @[]

        for nodeId in targetNodes:
          if chaosRng.bernoulli(policyCopy.probability_per_period_pct / 100.0):
            # Check max concurrent down limit BEFORE deciding to stop
            if runnerRef.config.node_lifecycle.max_concurrent_down.isSome:
              let limit = runnerRef.config.node_lifecycle.max_concurrent_down.get()
              # Count currently down nodes
              var currentDown = 0
              for nid, host in runnerRef.nodes.pairs:
                if not host.isAlive():
                  currentDown += 1
              if currentDown >= limit:
                # Already at limit, skip this restart
                continue
            
            var wipeDb = chaosRng.bernoulli(policyCopy.wipe_db_probability_pct / 100.0)
            # Enforce max concurrent wipes limiter, if configured
            if wipeDb and runnerRef.config.node_lifecycle.max_concurrent_wipes.isSome:
              let limit = runnerRef.config.node_lifecycle.max_concurrent_wipes.get()
              let current = runnerRef.pendingWipes.len
              if current >= limit:
                wipeDb = false
            # Determine stop duration
            var stopMs = 200'i64  # default
            if policyCopy.stop_duration_ms.isSome:
              let rngStop = runnerRef.rng.rngFor(fmt"restart-stopdur-{nodeId.id}")
              let r = policyCopy.stop_duration_ms.get()
              # nextInt(min, maxExclusive), include max by +1
              stopMs = int64(rngStop.nextInt(r.min.int, (r.max.int + 1)))

            if runnerRef.nodes[nodeId].isAlive():
              # Respect scenario end: clamp stop duration so restart happens before end, if configured
              if runnerRef.config.stop.max_ms.isSome:
                let remaining = runnerRef.config.stop.max_ms.get() - runnerRef.clock.nowMs
                if remaining > 0 and stopMs >= remaining:
                  stopMs = max(remaining - 1, 1'i64)

              # Stop now, schedule a start after stopMs
              runnerRef.nodes[nodeId].stop()
              if runnerRef.nodes[nodeId].isAlive():
                echo fmt"Failed to stop node {nodeId} time={runnerRef.clock.nowMs}ms"
              # Schedule restart using helper to avoid closure capture aliasing
              if wipeDb:
                runnerRef.pendingWipes[nodeId] = true
              scheduleNodeRestart(runnerRef, nodeId, wipeDb, stopMs)
    )

  # Set up workload driver
  if runner.config.workload.`type` == "kv":
    let runnerRef = runner  # Capture the runner ref
    # Derive a stable substream for workload once
    let workloadRng = runnerRef.rng.rngFor("workload")
    discard runner.scheduler.schedulePeriodic(
      "workload-driver",
      10,  # Check every millisecond
      proc(id: TimerId) {.gcsafe.} =
        # Try to propose based on the rate
        if workloadRng.bernoulli(runnerRef.config.workload.rate.propose_per_tick):
          runnerRef.proposalAttempts += 1
          # Find the current leader node to propose to
          var leaderNode: Option[RaftNodeId] = none(RaftNodeId)
          for nodeId, nodeHost in runnerRef.nodes.pairs:
            if nodeHost.isAlive():
              let state = nodeHost.getState()
              if state.role == Leader:
                leaderNode = some(nodeId)
                break

          if leaderNode.isSome:
            let targetNode = leaderNode.get()

            # Generate a command based on mix
            let isPut = workloadRng.bernoulli(runnerRef.config.workload.mix.put.float / 100.0)
            let key = workloadRng.nextInt(runnerRef.config.workload.keys.space)
            let value = workloadRng.nextInt(1000)  # Simple value for testing

            let cmdStr = if isPut: fmt"PUT {key} {value}" else: fmt"GET {key}"
            let cmd = cast[seq[byte]](cmdStr)

            let success = runnerRef.nodes[targetNode].propose(cmd)

            #if not success:
            #  echo fmt"Proposal to leader {targetNode} failed"
            #  return
            #echo fmt"Proposal to leader {targetNode} succeeded"
          else:
            # No leader found - this is expected during leader elections
            discard
    )

  # Main simulation loop
  while true:
    # Check stop conditions
    let shouldStopByTime = runner.config.stop.max_ms.isSome and
                          runner.clock.nowMs >= runner.config.stop.max_ms.get()
    let shouldStopByCommits = runner.config.stop.min_commits.isSome and
                             runner.commitCount >= runner.config.stop.min_commits.get()

    if shouldStopByTime or shouldStopByCommits:
      break

    # Deliver any events scheduled for the current time
    runner.clock.tick(0, proc(event: SimEvent) =
      runner.deliverEvent(event))

    # Account uptime/downtime for this 1ms and tick alive nodes
    for nodeId, host in runner.nodes.pairs:
      if host.isAlive():
        runner.uptimeMs[nodeId] = runner.uptimeMs.getOrDefault(nodeId, 0'i64) + 1
        host.tick()
      else:
        runner.downtimeMs[nodeId] = runner.downtimeMs.getOrDefault(nodeId, 0'i64) + 1

    # Advance time by 1ms
    runner.clock.tick(1, proc(event: SimEvent) =
      runner.deliverEvent(event))

    # Check invariants periodically (every 100ms)
    if runner.clock.nowMs mod 100 == 0:
      let nodes = runner.getAllNodes()
      runner.invariants.checkAll(nodes, runner.storage, runner.clock.nowMs)

      # Record cluster state for timeline visualization first
      runner.jsonWriter.recordClusterState(runner.clock.nowMs, nodes, runner.commitCount)

      # Push any new invariant violations into the trace (for CI summary and HTML)
      let allViolations = runner.invariants.getViolations()
      if allViolations.len > runner.recordedViolationCount:
        for i in runner.recordedViolationCount ..< allViolations.len:
          runner.jsonWriter.recordInvariantViolation(runner.clock.nowMs, allViolations[i])
        runner.recordedViolationCount = allViolations.len


      # Update commit count (take max commit index across all nodes)
      var maxCommitIndex = 0'u64
      for node in nodes:
        if node.isAlive():
          let state = node.getState()
          if state.commitIndex.uint64 > maxCommitIndex:
            maxCommitIndex = state.commitIndex.uint64
      runner.commitCount = maxCommitIndex.int

      # Progress reporting (towards earliest stop condition)
      var timePct = 0
      var commitsPct = 0
      if runner.config.stop.max_ms.isSome:
        let maxMs = max(1'i64, runner.config.stop.max_ms.get())
        timePct = int((runner.clock.nowMs.float / maxMs.float) * 100.0)
      if runner.config.stop.min_commits.isSome:
        let minComm = max(1, runner.config.stop.min_commits.get())
        commitsPct = int((runner.commitCount.float / minComm.float) * 100.0)
      let progressPct = max(timePct, commitsPct)
      let boundedPct = min(100, max(0, progressPct))
      if boundedPct != runner.lastProgressPct:
        var details: seq[string] = @[]
        if runner.config.stop.max_ms.isSome:
          let maxMs = runner.config.stop.max_ms.get()
          let remaining = max(0'i64, maxMs - runner.clock.nowMs)
          details.add(fmt"time {runner.clock.nowMs}/{maxMs}ms (rem {remaining}ms)")
        if runner.config.stop.min_commits.isSome:
          let minComm = runner.config.stop.min_commits.get()
          let remainingC = max(0, minComm - runner.commitCount)
          details.add(fmt"commits {runner.commitCount}/{minComm} (rem {remainingC})")
        let line = fmt"Progress: {boundedPct}% [" & details.join(", ") & "]"
        runner.lastProgressPct = boundedPct
        runner.lastProgressLine = line
        echo line

      # If we have violations, optionally abort early when fail_fast
      if runner.invariants.hasViolations():
        let violations = runner.invariants.getViolations()
        if runner.config.stop.fail_fast:
          echo fmt"Invariant violations detected ({violations.len} total) at t={runner.clock.nowMs}ms, aborting (fail_fast)."
          # Dump detailed state and recent events for diagnostics
          echo runner.dumpState()
          echo runner.dumpRecentEvents()
          break
        else:
          echo fmt"Invariant violations detected ({violations.len} total) at t={runner.clock.nowMs}ms, continuing simulation..."
          # Dump detailed state and recent events for diagnostics
          echo runner.dumpState()
          echo runner.dumpRecentEvents()
          echo "  Recent violations:"
          for i, violation in violations:
            if i < 3:  # Show only the first 3 violations to avoid spam
              echo fmt"    [{i+1}] {violation.description}: {violation.details}"

  # Check final result
  if runner.invariants.hasViolations():
    let violations = runner.invariants.getViolations()
    echo fmt"Simulation completed with {violations.len} invariant violations:"

    # Dump final state and recent events for diagnostics
    echo runner.dumpState()
    echo runner.dumpRecentEvents()

    # Group violations by type
    var violationCounts = initTable[string, int]()
    for violation in violations:
      violationCounts[violation.description] = violationCounts.getOrDefault(violation.description, 0) + 1

    for desc, count in violationCounts:
      echo fmt"  - {desc}: {count} occurrences"

    # Finalize trace with network statistics
    let networkStats = %*{
      "dropped": runner.net.droppedCount,
      "duplicated": runner.net.duplicatedCount,
      "delivered": runner.net.deliveredCount
    }
    # Per-node uptime/downtime stats
    var nodeStats = newJArray()
    for nodeId, host in runner.nodes.pairs:
      let aliveMs = runner.uptimeMs.getOrDefault(nodeId, 0'i64)
      let downMs = runner.downtimeMs.getOrDefault(nodeId, 0'i64)
      let aliveAtEnd = host.isAlive()
      let lastUp = runner.lastUpAtMs.getOrDefault(nodeId, 0'i64)
      let lastDown = runner.lastDownAtMs.getOrDefault(nodeId, -1'i64)
      let sinceAlive = if aliveAtEnd: runner.clock.nowMs - lastUp else: 0'i64
      let sinceDown = if (not aliveAtEnd) and lastDown >= 0: runner.clock.nowMs - lastDown else: 0'i64
      nodeStats.add(%*{
        "id": parseInt(nodeId.id),
        "aliveMs": aliveMs,
        "downMs": downMs,
        "alivePct": (if runner.clock.nowMs > 0: aliveMs.float / runner.clock.nowMs.float else: 0.0),
        "aliveAtEnd": aliveAtEnd,
        "aliveSinceMs": sinceAlive,
        "downSinceMs": sinceDown
      })
    let finalStats = %*{
      "networkStats": networkStats,
      "totalCommits": runner.commitCount,
      "proposalAttempts": runner.proposalAttempts,
      "heartbeatsSent": runner.heartbeatsSent,
      "heartbeatsDelivered": runner.heartbeatsDelivered,
      "nodeStats": nodeStats
    }
    runner.jsonWriter.finishTrace(runner.clock.nowMs, false, runner.clock.nowMs.int64, finalStats)
    runner.jsonWriter.writeToFile(runner.config.artifacts.json)

    # Generate HTML timeline
    let htmlGenerator = newHtmlTimelineGenerator(runner.jsonWriter.trace)
    htmlGenerator.writeToFile(runner.config.artifacts.html)

    # Print node uptime summary to console
    echo "Node uptime summary:"
    for nodeId, host in runner.nodes.pairs:
      let aliveMs = runner.uptimeMs.getOrDefault(nodeId, 0'i64)
      let downMs = runner.downtimeMs.getOrDefault(nodeId, 0'i64)
      let pct = if runner.clock.nowMs > 0: (aliveMs.float / runner.clock.nowMs.float) * 100.0 else: 0.0
      echo fmt"  Node {parseInt(nodeId.id)}: aliveMs={aliveMs}, downMs={downMs}, alivePct={pct:.2f}%, aliveAtEnd={host.isAlive()}"

    return false
  else:
    # Finalize trace with network statistics
    let networkStats = %*{
      "dropped": runner.net.droppedCount,
      "duplicated": runner.net.duplicatedCount,
      "delivered": runner.net.deliveredCount
    }
    var nodeStats = newJArray()
    for nodeId, host in runner.nodes.pairs:
      let aliveMs = runner.uptimeMs.getOrDefault(nodeId, 0'i64)
      let downMs = runner.downtimeMs.getOrDefault(nodeId, 0'i64)
      let aliveAtEnd = host.isAlive()
      let lastUp = runner.lastUpAtMs.getOrDefault(nodeId, 0'i64)
      let lastDown = runner.lastDownAtMs.getOrDefault(nodeId, -1'i64)
      let sinceAlive = if aliveAtEnd: runner.clock.nowMs - lastUp else: 0'i64
      let sinceDown = if (not aliveAtEnd) and lastDown >= 0: runner.clock.nowMs - lastDown else: 0'i64
      nodeStats.add(%*{
        "id": parseInt(nodeId.id),
        "aliveMs": aliveMs,
        "downMs": downMs,
        "alivePct": (if runner.clock.nowMs > 0: aliveMs.float / runner.clock.nowMs.float else: 0.0),
        "aliveAtEnd": aliveAtEnd,
        "aliveSinceMs": sinceAlive,
        "downSinceMs": sinceDown
      })
    let finalStats = %*{
      "networkStats": networkStats,
      "totalCommits": runner.commitCount,
      "proposalAttempts": runner.proposalAttempts,
      "heartbeatsSent": runner.heartbeatsSent,
      "heartbeatsDelivered": runner.heartbeatsDelivered,
      "nodeStats": nodeStats
    }
    runner.jsonWriter.finishTrace(runner.clock.nowMs, true, runner.clock.nowMs.int64, finalStats)
    runner.jsonWriter.writeToFile(runner.config.artifacts.json)

    # Generate HTML timeline
    let htmlGenerator = newHtmlTimelineGenerator(runner.jsonWriter.trace)
    htmlGenerator.writeToFile(runner.config.artifacts.html)

    # Print node uptime summary to console
    echo "Node uptime summary:"
    for nodeId, host in runner.nodes.pairs:
      let aliveMs = runner.uptimeMs.getOrDefault(nodeId, 0'i64)
      let downMs = runner.downtimeMs.getOrDefault(nodeId, 0'i64)
      let pct = if runner.clock.nowMs > 0: (aliveMs.float / runner.clock.nowMs.float) * 100.0 else: 0.0
      echo fmt"  Node {parseInt(nodeId.id)}: aliveMs={aliveMs}, downMs={downMs}, alivePct={pct:.2f}%, aliveAtEnd={host.isAlive()}"

    return true

proc getNode*(runner: ScenarioRunner, id: RaftNodeId): NodeHost =
  ## Get a node host by ID
  runner.nodes[id]

proc getLifecycleEvents*(runner: ScenarioRunner): seq[LifecycleEvent] =
  ## Get all recorded lifecycle events
  runner.lifecycleEvents

proc getInvariantsChecker*(runner: ScenarioRunner): InvariantsChecker =
  ## Get the invariants checker
  runner.invariants

proc stopNode*(runner: ScenarioRunner, nodeId: RaftNodeId) =
  ## Stop a node
  if runner.nodes.contains(nodeId):
    runner.nodes[nodeId].stop()

proc startNode*(runner: ScenarioRunner, nodeId: RaftNodeId, wipeDb: bool = false) =
  ## Start/restart a node
  if runner.nodes.contains(nodeId):
    runner.nodes[nodeId].start(wipeDb)

proc sendCommand*(runner: ScenarioRunner, nodeId: RaftNodeId, cmd: seq[byte]): bool =
  ## Send a command proposal to a node
  if runner.nodes.contains(nodeId) and runner.nodes[nodeId].isAlive():
    return runner.nodes[nodeId].propose(cmd)
  return false
