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

import ../core/sim_clock
import ../core/sim_rng
import ../core/sim_scheduler
import ../storage/sim_storage
import ../net/sim_net
import ../raft/node_host
import ../raft/raft_sm_adapter
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

proc logEvent*(runner: ScenarioRunner, kind: EventTraceKind, nodeId: RaftNodeId,
               description: string, details: string = "") =
  ## Log an event to the rolling trace buffer for diagnostics
  let entry = EventTraceEntry(
    timestamp: runner.clock.nowMs,
    kind: kind,
    nodeId: nodeId,
    description: description,
    details: details
  )
  runner.eventTrace.add(entry)
  # Keep only the last 50 entries
  if runner.eventTrace.len > 50:
    runner.eventTrace = runner.eventTrace[^50..^1]

proc newScenarioRunner*(config: ScenarioYaml): ScenarioRunner =
  let rng = newSimRng(config.seed)
  let clock = newSimClock()
  let scheduler = newSimScheduler(clock)
  let storage = newSimStorage(rng.rngFor("storage"))
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
    jsonWriter: newJsonTraceWriter(config, rng.state),
    eventTrace: @[]
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
        runner.jsonWriter.recordRpcEvent(timeMs, rpc, fromNode, toNode)),
      some(proc(nodeId: RaftNodeId, event: LifecycleEvent) =
        runner.lifecycleEvents.add(event)),
      some(proc(kind: EventTraceKind, nodeId: RaftNodeId, description: string, details: string = "") =
        logEvent(runner, kind, nodeId, description, details)),
      some(proc(timeMs: int64, nodeId: RaftNodeId, entry: LogEntry) =
        runner.jsonWriter.recordCommittedEvent(timeMs, nodeId, entry))
    )
    runner.nodes[nodeId] = host

  return runner

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
      host.step(event.network.rpc)
      # Update network stats
      runner.net.deliveredCount += 1
      # Log message receive event
      let rpcKind = case event.network.rpc.kind:
        of VoteRequest: "VoteRequest"
        of VoteReply: "VoteReply"
        of AppendRequest: "AppendRequest"
        of AppendReply: "AppendReply"
        of InstallSnapshot: "InstallSnapshot"
        of SnapshotReply: "SnapshotReply"
      runner.logEvent(MessageReceive, event.network.toNode,
                      fmt"Received {rpcKind} from {event.network.fromNode}",
                      fmt"term={event.network.rpc.currentTerm}")
  of TimerEvent:
    # Timer events are handled by the clock.tick method directly
    discard
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

proc dumpRecentEvents*(runner: ScenarioRunner): string =
  ## Dump the recent event trace for diagnostics
  var lines: seq[string]
  lines.add("Recent Events (last 50):")

  if runner.eventTrace.len == 0:
    lines.add("  No events recorded")
  else:
    for i in countdown(runner.eventTrace.high, 0):
      let entry = runner.eventTrace[i]
      lines.add(fmt"  [{entry.timestamp}ms] {entry.nodeId}: {entry.kind} - {entry.description}")
      if entry.details.len > 0:
        lines.add(fmt"    {entry.details}")

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
      proc() {.gcsafe.} =
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
            let wipeDb = chaosRng.bernoulli(policyCopy.wipe_db_probability_pct / 100.0)
            if runnerRef.nodes[nodeId].isAlive():
              runnerRef.nodes[nodeId].stop()
            else:
              runnerRef.nodes[nodeId].start(wipeDb)
    )

  # Set up workload driver
  if runner.config.workload.`type` == "kv":
    let runnerRef = runner  # Capture the runner ref
    discard runner.scheduler.schedulePeriodic(
      "workload-driver",
      10,  # Check every millisecond
      proc() {.gcsafe.} =
        let workloadRng = runnerRef.rng.rngFor("workload")
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

            if not success:
              echo fmt"Proposal to leader {targetNode} failed"
              return
            echo fmt"Proposal to leader {targetNode} succeeded"
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

    # Tick all alive nodes (must happen every millisecond for Raft timeouts to work)
    for host in runner.nodes.values:
      if host.isAlive():
        host.tick()

    # Advance time by 1ms
    runner.clock.tick(1, proc(event: SimEvent) =
      runner.deliverEvent(event))

    # Check invariants periodically (every 100ms)
    if runner.clock.nowMs mod 100 == 0:
      let nodes = runner.getAllNodes()
      runner.invariants.checkAll(nodes, runner.storage, runner.clock.nowMs)

      # Record cluster state for timeline visualization
      runner.jsonWriter.recordClusterState(runner.clock.nowMs, nodes, runner.commitCount)


      # Update commit count (take max commit index across all nodes)
      var maxCommitIndex = 0'u64
      for node in nodes:
        if node.isAlive():
          let state = node.getState()
          if state.commitIndex.uint64 > maxCommitIndex:
            maxCommitIndex = state.commitIndex.uint64
      runner.commitCount = maxCommitIndex.int

      # If we have violations, we could abort early
      if runner.invariants.hasViolations():
        let violations = runner.invariants.getViolations()
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
    let finalStats = %*{
      "networkStats": networkStats,
      "totalCommits": runner.commitCount,
      "proposalAttempts": runner.proposalAttempts
    }
    runner.jsonWriter.finishTrace(runner.clock.nowMs, false, runner.clock.nowMs.int64, finalStats)
    runner.jsonWriter.writeToFile(runner.config.artifacts.json)

    # Generate HTML timeline
    let htmlGenerator = newHtmlTimelineGenerator(runner.jsonWriter.trace)
    htmlGenerator.writeToFile(runner.config.artifacts.html)

    return false
  else:
    # Finalize trace with network statistics
    let networkStats = %*{
      "dropped": runner.net.droppedCount,
      "duplicated": runner.net.duplicatedCount,
      "delivered": runner.net.deliveredCount
    }
    let finalStats = %*{
      "networkStats": networkStats,
      "totalCommits": runner.commitCount,
      "proposalAttempts": runner.proposalAttempts
    }
    runner.jsonWriter.finishTrace(runner.clock.nowMs, true, runner.clock.nowMs.int64, finalStats)
    runner.jsonWriter.writeToFile(runner.config.artifacts.json)

    # Generate HTML timeline
    let htmlGenerator = newHtmlTimelineGenerator(runner.jsonWriter.trace)
    htmlGenerator.writeToFile(runner.config.artifacts.html)

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
