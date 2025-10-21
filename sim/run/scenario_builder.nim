## Scenario Builder - Fluent API for programmatic scenario creation
##
## Provides a Nim DSL for building Raft simulation scenarios programmatically.

import std/options
import std/tables
import std/strformat

import scenario_config

type
  ScenarioBuilder* = ref object
    scenario*: ScenarioYaml

proc newScenarioBuilder*(seed: uint64 = 0): ScenarioBuilder =
  ## Create a new scenario builder with default values
  let scenario = ScenarioYaml(
    seed: if seed != 0: seed else: 12345'u64,
    cluster: ClusterConfig(
      nodes: 3,
      election_timeout_ms: TimeoutRange(min: 150, max: 300),
      heartbeat_ms: 50
    ),
    storage: StorageConfig(
      durability: Durable,
      snapshot: SnapshotConfig(
        enabled: false,
        max_log_entries: none(int),
        or_bytes: none(int64)
      )
    ),
    workload: WorkloadConfig(
      `type`: "kv",
      rate: WorkloadRate(propose_per_tick: 0.6),
      mix: WorkloadMix(put: 80, get: 20),
      keys: KeySpace(space: 1000, zipf_s: 1.1)
    ),
    net: NetConfig(
      latency_ms: LatencyConfig(base: 20, jitter: 10, p99: 120),
      drop_pct: 2.0,
      dup_pct: 1.0,
      reorder_window: 5
    ),
    partitions: @[],
    node_lifecycle: NodeLifecycleConfig(restart_policies: @[]),
    fuzz: FuzzConfig(enabled: false, knobs: initTable[string, bool]()),
    stop: StopConfig(
      max_ms: some(30000'i64),
      min_commits: some(1000),
      fail_fast: false
    ),
    artifacts: ArtifactsConfig(
      html: "artifacts/scenario.html",
      json: "artifacts/scenario.json"
    )
  )
  result = ScenarioBuilder(scenario: scenario)

# Cluster configuration methods
proc cluster*(builder: ScenarioBuilder, nodes: int): ScenarioBuilder =
  ## Set cluster size
  builder.scenario.cluster.nodes = nodes
  builder

proc cluster*(builder: ScenarioBuilder, nodes: int, electionTimeout: TimeoutRange, heartbeat: int64): ScenarioBuilder =
  ## Set cluster configuration
  builder.scenario.cluster.nodes = nodes
  builder.scenario.cluster.election_timeout_ms = electionTimeout
  builder.scenario.cluster.heartbeat_ms = heartbeat
  builder

proc electionTimeout*(builder: ScenarioBuilder, minMs, maxMs: int64): ScenarioBuilder =
  ## Set election timeout range
  builder.scenario.cluster.election_timeout_ms = TimeoutRange(min: minMs, max: maxMs)
  builder

proc heartbeat*(builder: ScenarioBuilder, ms: int64): ScenarioBuilder =
  ## Set heartbeat interval
  builder.scenario.cluster.heartbeat_ms = ms
  builder

# Storage configuration methods
proc storage*(builder: ScenarioBuilder, durability: StorageDurability): ScenarioBuilder =
  ## Set storage durability mode
  builder.scenario.storage.durability = durability
  builder

proc snapshot*(builder: ScenarioBuilder, enabled: bool): ScenarioBuilder =
  ## Enable/disable snapshots
  builder.scenario.storage.snapshot.enabled = enabled
  builder

proc snapshot*(builder: ScenarioBuilder, enabled: bool, maxEntries: int, orBytes: int64): ScenarioBuilder =
  ## Configure snapshots with triggers
  builder.scenario.storage.snapshot.enabled = enabled
  builder.scenario.storage.snapshot.max_log_entries = some(maxEntries)
  builder.scenario.storage.snapshot.or_bytes = some(orBytes)
  builder

# Network configuration methods
proc net*(builder: ScenarioBuilder, latency: LatencyConfig, dropPct: float = 2.0, dupPct: float = 1.0, reorderWin: int = 5): ScenarioBuilder =
  ## Set network configuration
  builder.scenario.net = NetConfig(
    latency_ms: latency,
    drop_pct: dropPct,
    dup_pct: dupPct,
    reorder_window: reorderWin
  )
  builder

proc latency*(builder: ScenarioBuilder, base: int64, jitter: int64 = 10, p99: int64 = 120): ScenarioBuilder =
  ## Set network latency configuration
  builder.scenario.net.latency_ms = LatencyConfig(base: base, jitter: jitter, p99: p99)
  builder

# Workload configuration methods
proc workload*(builder: ScenarioBuilder, workloadType: string): ScenarioBuilder =
  ## Set workload type ("kv" or "blackhole")
  builder.scenario.workload.`type` = workloadType
  builder

proc workload*(builder: ScenarioBuilder, workloadType: string, rate: float, putPct: int = 80, getPct: int = 20): ScenarioBuilder =
  ## Set workload configuration
  builder.scenario.workload.`type` = workloadType
  builder.scenario.workload.rate.propose_per_tick = rate
  builder.scenario.workload.mix.put = putPct
  builder.scenario.workload.mix.get = getPct
  builder

proc keys*(builder: ScenarioBuilder, space: int, zipf: float = 1.1): ScenarioBuilder =
  ## Set key space configuration
  builder.scenario.workload.keys.space = space
  builder.scenario.workload.keys.zipf_s = zipf
  builder

# Partition configuration methods
proc partition*(builder: ScenarioBuilder, atMs: int64, components: seq[seq[int]]): ScenarioBuilder =
  ## Add a partition at the specified time
  let partition = PartitionSpec(
    at_ms: atMs,
    components: components,
    heal: none(bool)
  )
  builder.scenario.partitions.add(partition)
  builder

proc heal*(builder: ScenarioBuilder, atMs: int64): ScenarioBuilder =
  ## Heal partitions at the specified time
  let partition = PartitionSpec(
    at_ms: atMs,
    components: @[],
    heal: some(true)
  )
  builder.scenario.partitions.add(partition)
  builder

# Lifecycle configuration methods
proc lifecycle*(builder: ScenarioBuilder, policy: RestartPolicy): ScenarioBuilder =
  ## Add a restart policy
  builder.scenario.node_lifecycle.restart_policies.add(policy)
  builder

proc RestartPolicy*(selector: string, periodMs: int64, stopDur: TimeoutRange, probPct: float, wipeDbPct: float = 0.0): RestartPolicy =
  ## Create a restart policy
  RestartPolicy(
    selector: selector,
    period_ms: periodMs,
    stop_duration_ms: some(stopDur),
    probability_per_period_pct: probPct,
    wipe_db_probability_pct: wipeDbPct
  )

proc RestartPolicy*(selector: string, periodMs: int64, stopDurMs: int64, probPct: float, wipeDbPct: float = 0.0): RestartPolicy =
  ## Create a restart policy with fixed stop duration
  let stopDur = TimeoutRange(min: stopDurMs, max: stopDurMs)
  RestartPolicy(
    selector: selector,
    period_ms: periodMs,
    stop_duration_ms: some(stopDur),
    probability_per_period_pct: probPct,
    wipe_db_probability_pct: wipeDbPct
  )

# Fuzz configuration methods
proc fuzz*(builder: ScenarioBuilder, enabled: bool): ScenarioBuilder =
  ## Enable/disable fuzzing
  builder.scenario.fuzz.enabled = enabled
  builder

proc fuzz*(builder: ScenarioBuilder, enabled: bool, knobs: Table[string, bool]): ScenarioBuilder =
  ## Configure fuzzing with specific knobs
  builder.scenario.fuzz.enabled = enabled
  builder.scenario.fuzz.knobs = knobs
  builder

# Stop conditions methods
proc stop*(builder: ScenarioBuilder, maxMs: int64, minCommits: int = 1000): ScenarioBuilder =
  ## Set stop conditions
  builder.scenario.stop.max_ms = some(maxMs)
  builder.scenario.stop.min_commits = some(minCommits)
  builder

proc failFast*(builder: ScenarioBuilder, enabled: bool = true): ScenarioBuilder =
  ## Enable/disable fail-fast behavior
  builder.scenario.stop.fail_fast = enabled
  builder

# Artifacts configuration methods
proc artifacts*(builder: ScenarioBuilder, htmlPath: string, jsonPath: string): ScenarioBuilder =
  ## Set artifact output paths
  builder.scenario.artifacts.html = htmlPath
  builder.scenario.artifacts.json = jsonPath
  builder

# Build method
proc build*(builder: ScenarioBuilder): ScenarioYaml =
  ## Build and return the scenario configuration
  builder.scenario

# Preset builders
proc happy3Node*(): ScenarioYaml =
  ## Create a happy 3-node scenario
  newScenarioBuilder(seed = 12345)
    .cluster(3)
    .storage(Durable)
    .snapshot(false)
    .workload("kv", 0.6)
    .keys(1000, 1.1)
    .net(LatencyConfig(base: 20, jitter: 10, p99: 120))
    .stop(30000, 2000)
    .artifacts("artifacts/happy_3node.html", "artifacts/happy_3node.json")
    .build()

proc minorityPartition*(): ScenarioYaml =
  ## Create a minority partition scenario
  newScenarioBuilder(seed = 54321)
    .cluster(5)
    .partition(5000, @[@[0, 1, 2], @[3, 4]])
    .heal(12000)
    .stop(20000, 1500)
    .artifacts("artifacts/minority_partition.html", "artifacts/minority_partition.json")
    .build()

proc restartAndWipe*(): ScenarioYaml =
  ## Create a restart and wipe scenario
  newScenarioBuilder(seed = 98765)
    .cluster(3)
    .lifecycle(RestartPolicy("any", 4000, TimeoutRange(min: 50, max: 400), 40.0, 15.0))
    .storage(Durable)
    .snapshot(true, 500, 2_097_152)  # 2MB
    .stop(60000, 3000)
    .artifacts("artifacts/restart_and_wipe.html", "artifacts/restart_and_wipe.json")
    .build()

# Utility functions
proc TimeoutRange*(min, max: int64): TimeoutRange =
  ## Create a timeout range
  TimeoutRange(min: min, max: max)

proc Latency*(base: int64, jitter: int64 = 10, p99: int64 = 120): LatencyConfig =
  ## Create a latency configuration
  LatencyConfig(base: base, jitter: jitter, p99: p99)

# Export the types for convenience
export scenario_config
