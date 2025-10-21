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
    jsonWriter*: JsonTraceWriter

proc newScenarioRunner*(config: ScenarioYaml): ScenarioRunner =
  let rng = newSimRng(config.seed)
  let clock = newSimClock()
  let scheduler = newSimScheduler(clock)
  let storage = newSimStorage(rng.rngFor("storage"))
  let net = newSimNet(rng.rngFor("net"))

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
    jsonWriter: newJsonTraceWriter(config, rng.state)
  )

  # Initialize invariants checker
  runner.invariants = newInvariantsChecker()

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
        runner.lifecycleEvents.add(event))
    )
    runner.nodes[nodeId] = host

  return runner

proc getAllNodes*(runner: ScenarioRunner): seq[NodeHost] =
  ## Get all node hosts
  toSeq(runner.nodes.values)

proc deliverRpc*(runner: ScenarioRunner, event: NetEvent) =
  ## Deliver an RPC to the appropriate node
  if runner.nodes.contains(event.toNode):
    let host = runner.nodes[event.toNode]
    host.step(event.rpc)

proc run*(runner: ScenarioRunner): bool =
  ## Run the scenario until completion or timeout

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

    # Advance to next event time or by 1ms if no events
    let nextEventTime = runner.scheduler.nextEventTime()
    if nextEventTime.isSome:
      let delta = nextEventTime.get() - runner.clock.nowMs
      runner.clock.tick(delta)
    else:
      runner.clock.tick(1)

    # Deliver any pending network messages
    runner.net.pump(runner.clock.nowMs, proc(event: NetEvent) =
      runner.deliverRpc(event))

    # Tick all alive nodes
    for host in runner.nodes.values:
      if host.isAlive():
        host.tick()

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

        # Show current cluster state
        echo "  Current cluster state:"
        for node in nodes:
          if node.isAlive():
            let state = node.getState()
            echo fmt"    Node {state.id}: {state.role}, term={state.currentTerm}, commit={state.commitIndex}"

        echo "  Recent violations:"
        for i, violation in violations:
          if i < 3:  # Show only the first 3 violations to avoid spam
            echo fmt"    [{i+1}] {violation.description}: {violation.details}"

  # Check final result
  if runner.invariants.hasViolations():
    let violations = runner.invariants.getViolations()
    echo fmt"Simulation completed with {violations.len} invariant violations:"

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
      "totalCommits": runner.commitCount
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
      "totalCommits": runner.commitCount
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
