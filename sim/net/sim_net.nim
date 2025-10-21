## SimNet - Network simulation with fault injection
##
## Provides deterministic network simulation with configurable faults:
## latency, drops, duplicates, reordering, and partitions.

import std/deques
import std/tables
import std/options
import std/math
import std/strformat

import ../core/sim_rng
import ../core/types
import ../../src/raft/types
import ../../src/raft/consensus_state_machine

type
  NetFaultPolicy* = object
    ## Network fault configuration for a link
    latencyBase*: int64      # base latency in ms
    latencyJitter*: int64    # random jitter ± this amount
    latencyP99*: int64       # 99th percentile tail latency
    dropPercent*: float      # probability of dropping message [0.0, 1.0]
    duplicatePercent*: float # probability of duplicating message
    reorderWindow*: int      # max reorder distance (0 = no reorder)

  LinkPolicy* = object
    ## Policy for a specific from->to link
    fromNode*: RaftNodeId
    toNode*: RaftNodeId
    policy*: NetFaultPolicy

  SimNet* = ref object
    rng*: SimRng
    defaultPolicy*: NetFaultPolicy
    linkPolicies*: Table[(RaftNodeId, RaftNodeId), NetFaultPolicy]
    eventQueue*: Deque[NetEvent]  # sorted by deliverAt
    partitions*: seq[Partition]
    droppedCount*: uint64
    duplicatedCount*: uint64
    deliveredCount*: uint64

proc newNetFaultPolicy*(baseLatency: int64 = 20, jitter: int64 = 10,
                       p99Latency: int64 = 120, dropPercent: float = 0.02,
                       duplicatePercent: float = 0.01, reorderWindow: int = 5): NetFaultPolicy =
  NetFaultPolicy(
    latencyBase: baseLatency,
    latencyJitter: jitter,
    latencyP99: p99Latency,
    dropPercent: dropPercent,
    duplicatePercent: duplicatePercent,
    reorderWindow: reorderWindow
  )

proc newSimNet*(rng: SimRng, defaultPolicy: NetFaultPolicy = newNetFaultPolicy()): SimNet =
  SimNet(
    rng: rng,
    defaultPolicy: defaultPolicy,
    linkPolicies: initTable[(RaftNodeId, RaftNodeId), NetFaultPolicy](),
    eventQueue: initDeque[NetEvent](),
    partitions: @[],
    droppedCount: 0,
    duplicatedCount: 0,
    deliveredCount: 0
  )

proc setLinkPolicy*(net: SimNet, fromNode, toNode: RaftNodeId, policy: NetFaultPolicy) =
  ## Set fault policy for a specific link
  net.linkPolicies[(fromNode, toNode)] = policy

proc getPolicy*(net: SimNet, fromNode, toNode: RaftNodeId): NetFaultPolicy =
  ## Get the fault policy for a link, falling back to default
  net.linkPolicies.getOrDefault((fromNode, toNode), net.defaultPolicy)

proc canCommunicate*(net: SimNet, fromNode, toNode: RaftNodeId, atTime: int64): bool =
  ## Check if two nodes can communicate at the given time (partition check)
  for partition in net.partitions:
    if atTime >= partition.startTime and
       (partition.endTime.isNone or atTime < partition.endTime.get()):
      # Find which component each node is in
      var fromComponent = -1
      var toComponent = -1
      for i, component in partition.components:
        if fromNode in component: fromComponent = i
        if toNode in component: toComponent = i
      # Can only communicate within same component
      if fromComponent != toComponent:
        return false
  return true

proc calculateLatency*(net: SimNet, fromNode, toNode: RaftNodeId): int64 =
  ## Calculate delivery latency for a message, including faults
  let policy = net.getPolicy(fromNode, toNode)
  let rng = net.rng.rngFor("net-latency")

  # Base latency + jitter
  let jitter = rng.nextInt(-policy.latencyJitter, policy.latencyJitter + 1)
  var latency = policy.latencyBase + jitter

  # Occasionally add tail latency
  if rng.bernoulli(0.01):  # 1% chance of tail latency
    latency += policy.latencyP99

  return max(latency, 1)  # minimum 1ms latency

proc send*(net: SimNet, rpc: RaftRpcMessage, fromNode, toNode: RaftNodeId, sendTime: int64) =
  ## Send an RPC from one node to another, applying network faults
  let policy = net.getPolicy(fromNode, toNode)

  # Check if nodes can communicate (partition)
  if not net.canCommunicate(fromNode, toNode, sendTime):
    net.droppedCount += 1
    return

  let rng = net.rng.rngFor("net-faults-" & fromNode.id & "-" & toNode.id)

  # Drop check
  if rng.bernoulli(policy.dropPercent):
    net.droppedCount += 1
    return

  let baseLatency = net.calculateLatency(fromNode, toNode)
  var deliverTime = sendTime + baseLatency

  # Duplicate check
  let willDuplicate = rng.bernoulli(policy.duplicatePercent)

  # Reorder check - can delay delivery by up to reorderWindow events
  if policy.reorderWindow > 0 and rng.bernoulli(0.1):  # 10% chance to reorder
    let reorderDelay = rng.nextInt(1, policy.reorderWindow + 1)
    deliverTime += int64(reorderDelay)

  # Create and queue the event
  let event = NetEvent(
    fromNode: fromNode,
    toNode: toNode,
    deliverAt: deliverTime,
    rpc: rpc,
    duplicate: false
  )

  net.eventQueue.addLast(event)

  # Add duplicate if needed
  if willDuplicate:
    let dupEvent = NetEvent(
      fromNode: fromNode,
      toNode: toNode,
      deliverAt: deliverTime + rng.nextInt(1, 11),  # duplicate within 10ms
      rpc: rpc,
      duplicate: true
    )
    net.eventQueue.addLast(dupEvent)
    net.duplicatedCount += 1

proc pump*(net: SimNet, currentTime: int64, deliverCallback: proc(event: NetEvent)) =
  ## Deliver all events that are due at or before currentTime
  while net.eventQueue.len > 0 and net.eventQueue[0].deliverAt <= currentTime:
    let event = net.eventQueue.popFirst()
    deliverCallback(event)
    net.deliveredCount += 1

proc addPartition*(net: SimNet, partition: Partition) =
  ## Add a partition configuration
  net.partitions.add(partition)

proc clearPartitions*(net: SimNet) =
  ## Remove all partitions
  net.partitions.setLen(0)

proc pendingEvents*(net: SimNet): int =
  ## Return number of pending network events
  net.eventQueue.len
