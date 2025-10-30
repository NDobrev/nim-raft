## Scenario Configuration - YAML loader and types for Raft simulation scenarios
##
## Defines the configuration structure for scenarios and provides YAML loading.

import std/options
import std/tables
import std/strformat
import std/json
import std/jsonutils

import ../core/types

type
  # Basic configuration types
  TimeoutRange* = object
    min*: int64
    max*: int64

  LatencyConfig* = object
    base*: int64
    jitter*: int64
    p99*: int64

  # Storage configuration
  StorageDurability* = enum
    Durable, Async, Torn

  SnapshotConfig* = object
    enabled*: bool
    max_log_entries*: Option[int]
    or_bytes*: Option[int64]
    snapshot_each_ms*: Option[int64]

  StorageConfig* = object
    durability*: StorageDurability
    snapshot*: SnapshotConfig

  # Workload configuration
  WorkloadMix* = object
    put*: int
    get*: int

  WorkloadRate* = object
    propose_per_tick*: float

  KeySpace* = object
    space*: int
    zipf_s*: float

  WorkloadConfig* = object
    `type`*: string  # "kv" or "blackhole"
    rate*: WorkloadRate
    mix*: WorkloadMix
    keys*: KeySpace

  # Network configuration
  NetConfig* = object
    latency_ms*: LatencyConfig
    drop_pct*: float
    dup_pct*: float
    reorder_window*: int

  # Partition configuration
  PartitionComponent* = seq[int]
  PartitionSpec* = object
    at_ms*: int64
    components*: seq[PartitionComponent]
    heal*: Option[bool]

  # Lifecycle configuration
  RestartPolicy* = object
    selector*: string  # "any" or "node:X"
    period_ms*: int64
    stop_duration_ms*: Option[TimeoutRange]  # if not specified, use default
    probability_per_period_pct*: float
    wipe_db_probability_pct*: float

  NodeLifecycleConfig* = object
    restart_policies*: seq[RestartPolicy]
    # Optional limiter: cap how many nodes may be wiped concurrently
    # If none, no explicit cap is enforced by the runner
    max_concurrent_wipes*: Option[int]
    # Optional limiter: cap how many nodes can be down simultaneously
    # If none, no explicit cap is enforced by the runner
    max_concurrent_down*: Option[int]

  # Fuzz configuration
  FuzzConfig* = object
    enabled*: bool
    knobs*: Table[string, bool]  # vary_timeouts, burst_drops_pct, tail_latency_events

  # Stop conditions
  StopConfig* = object
    max_ms*: Option[int64]
    min_commits*: Option[int]
    fail_fast*: bool

  # Artifacts configuration
  ArtifactsConfig* = object
    html*: string
    json*: string

  # Cluster configuration
  ClusterConfig* = object
    nodes*: int
    election_timeout_ms*: TimeoutRange
    heartbeat_ms*: int64

  # Invariants configuration
  InvariantsConfig* = object
    election_safety*: bool
    log_matching*: bool
    leader_append_only*: bool
    state_machine_safety*: bool
    leader_completeness*: bool
    snapshot_sanity*: bool
    index_validity*: bool
    monotonic_ids*: bool
    no_committed_deletion*: bool
    log_consistency*: bool
    liveness*: bool

  # Complete scenario configuration
  ScenarioYaml* = object
    seed*: uint64
    cluster*: ClusterConfig
    storage*: StorageConfig
    workload*: WorkloadConfig
    net*: NetConfig
    partitions*: seq[PartitionSpec]
    node_lifecycle*: NodeLifecycleConfig
    fuzz*: FuzzConfig
    stop*: StopConfig
    artifacts*: ArtifactsConfig
    invariants*: Option[InvariantsConfig]

proc loadScenarioFromYaml*(path: string): ScenarioYaml =
  ## Load a scenario configuration from a YAML/JSON file
  try:
    let jsonContent = readFile(path)
    let jsonNode = parseJson(jsonContent)
    result = jsonNode.to(ScenarioYaml)
  except JsonParsingError as e:
    raise newException(ValueError, fmt"Failed to parse scenario file '{path}': {e.msg}")
  except IOError as e:
    raise newException(ValueError, fmt"Could not read scenario file '{path}': {e.msg}")

proc loadScenarioFromString*(content: string): ScenarioYaml =
  ## Load a scenario configuration from a JSON string
  try:
    let jsonNode = parseJson(content)
    result = jsonNode.to(ScenarioYaml)
  except JsonParsingError as e:
    raise newException(ValueError, fmt"Failed to parse scenario string: {e.msg}")

proc saveScenarioToYaml*(scenario: ScenarioYaml, path: string) =
  ## Save a scenario configuration to a JSON file
  try:
    let jsonNode = %scenario
    writeFile(path, $jsonNode)
  except IOError as e:
    raise newException(ValueError, fmt"Could not write scenario file '{path}': {e.msg}")

proc `$`*(scenario: ScenarioYaml): string =
  ## Convert scenario to a readable string representation
  result = fmt"Scenario(seed={scenario.seed}, nodes={scenario.cluster.nodes})"

# Validation functions
proc validateScenario*(scenario: ScenarioYaml): seq[string] =
  ## Validate a scenario configuration and return a list of error messages
  result = @[]

  # Basic validations
  if scenario.cluster.nodes < 1:
    result.add("Cluster must have at least 1 node")

  if scenario.cluster.election_timeout_ms.min <= 0 or scenario.cluster.election_timeout_ms.max <= 0:
    result.add("Election timeout must be positive")

  if scenario.cluster.election_timeout_ms.min >= scenario.cluster.election_timeout_ms.max:
    result.add("Election timeout min must be less than max")

  if scenario.cluster.heartbeat_ms <= 0:
    result.add("Heartbeat interval must be positive")

  if scenario.workload.mix.put + scenario.workload.mix.get != 100:
    result.add("Workload mix percentages must sum to 100")

  if scenario.workload.rate.propose_per_tick <= 0.0 or scenario.workload.rate.propose_per_tick > 1.0:
    result.add("Propose rate per tick must be between 0 and 1")

  if scenario.net.drop_pct < 0.0 or scenario.net.drop_pct > 100.0:
    result.add("Drop percentage must be between 0 and 100")

  if scenario.net.dup_pct < 0.0 or scenario.net.dup_pct > 100.0:
    result.add("Duplicate percentage must be between 0 and 100")

  if scenario.net.reorder_window < 0:
    result.add("Reorder window must be non-negative")

  # Partition validation (TODO: implement proper partition semantics)
  # for i, partition in scenario.partitions:
  #   # Skip validation for heal partitions (they have special semantics)
  #   if partition.heal.isSome and partition.heal.get:
  #     continue
  #
  #   var totalNodes = 0
  #   for component in partition.components:
  #     totalNodes += component.len
  #   if totalNodes != scenario.cluster.nodes:
  #     result.add(fmt"Partition {i} has {totalNodes} nodes, expected {scenario.cluster.nodes}")

  # Lifecycle validation
  for i, policy in scenario.node_lifecycle.restart_policies:
    if policy.probability_per_period_pct < 0.0 or policy.probability_per_period_pct > 100.0:
      result.add(fmt"Restart policy {i} probability must be between 0 and 100")

    if policy.wipe_db_probability_pct < 0.0 or policy.wipe_db_probability_pct > 100.0:
      result.add(fmt"Restart policy {i} wipe probability must be between 0 and 100")

proc isValidScenario*(scenario: ScenarioYaml): bool =
  ## Check if a scenario configuration is valid
  validateScenario(scenario).len == 0
