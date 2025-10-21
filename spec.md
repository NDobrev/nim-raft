Perfect—got it. Here’s a lean, Raft-only simulation & test harness spec tailored to your answers: simple FSM, Raft-standard invariants, full control over faults, reproducible + fuzz runs, snapshots enabled, and lifecycle/storage chaos (node restarts, DB loss, bootstrap-from-disk).

# Simulation & Test Harness — Design Spec (Raft only)

## Goals (what this tool guarantees)

* **Deterministic & reproducible**: every run is keyed by a single seed; same seed ⇒ identical behavior and outputs.
* **Configurable chaos**: you control how often nodes stop/restart, DB loss rates, message delay/drop/reorder/dupe, partitions.
* **Raft-standard safety checks** only (no BLS): election safety, log matching, leader append-only, state machine safety, leader completeness.
* **Snapshots & recovery**: exercise snapshot creation/apply, crash/restart, bootstrap from on-disk state—or from **empty** (DB loss).
* **Outputs for CI**: JSON artifacts, a small HTML timeline/report, and a CI-friendly pass/fail summary.

---

## Architecture (modules)

1. **SimClock**

   * Virtual, manually advanced time (`tick()`), no wall-clock.
   * Schedules timers (election, heartbeat, RPC timeouts) with deterministic ordering.
   * `SimClock.now()` is the sole time source.

2. **SimRng**

   * `SimRng(seed: uint64)` with deterministic streams.
   * Substreams for net, storage, scheduler so runs are stable even if you add metrics.

3. **SimNet**

   * Event queue delivering Raft RPCs (`RequestVote`, `AppendEntries`, `InstallSnapshot`).
   * Fault knobs:

     * Latency: fixed, uniform, normal, or p90/p99 tail parameters.
     * Drop %, duplication %, reordering window, jitter.
     * Partitions: static, scheduled, or stochastic (Markov flapping).
   * Per-link or per-node policies (e.g., “node 2 is slow to node 4 only”).
   * Programmatic controls to **stop** / **restart** a node on a schedule (see Scenario DSL).
   * All deliveries are recorded to a trace for postmortem.

4. **SimStorage**

   * In-memory “disk” per node with durability modes:

     * `Durable`: writes immediately visible and survive crash.
     * `Async`: commit → journal delays (can lose last K writes on crash).
     * `Torn`: probabilistic partial write (for testing journal logic if applicable).
   * **DB loss**: a restart action may be tagged `wipe=true`, simulating machine reimage / lost disk.
   * **Snapshot support**:

     * Log compaction thresholds (size or index horizon).
     * `saveSnapshot(term, index, state)`, `installSnapshot(...)`.
     * On restart, node loads `currentTerm`, `votedFor`, log, and the latest snapshot if present.

5. **NodeHost (Adapter)**

   * Thin shim implementing your repo’s expected host callbacks:

     * timers (using `SimClock`), persistence (using `SimStorage`),
     * stable/random bytes (via `SimRng`), and logging.
   * Exposes `step(msg)`, `tick()`, `propose(cmd)`, `readIndex()` as your Raft SM needs.

6. **FSMs**

   * Start with **Simple KV** (set/get; GETs use ReadIndex).
   * Optional “blackhole apply” FSM for pure protocol testing (no user state).

7. **Scenario Runner**

   * Drives time advancement, injects workload (proposes), and executes fault schedules.
   * Provides **Fuzz Driver**: randomized workload + faults under seed.
   * Stop conditions: committed ops target, time budget, or invariant breach.

8. **Invariants & Oracles**

   * **Safety** (per Raft):

     * Election safety (≤1 leader per term).
     * Log matching property.
     * Leader append-only.
     * State machine safety (all applied sequences identical).
     * Leader completeness.
   * **Liveness (weak)**:

     * Eventually a leader exists given a majority partition.
     * Progress detected (commit index increases) when a majority is healthy.
   * Snapshot sanity: after install, follower’s log prefix truncated correctly; no gaps in committed sequence.

9. **Outputs**

   * **JSON**: run config, seed, per-tick node states, events, metrics, invariant verdicts.
   * **HTML**: compact single-file timeline (leaders, elections, commits, faults), plus quick verdict summary.
   * **CI summary**: minimal text (pass/fail + seed + first failure cause).

---

## Configuration — Scenario DSL

You can do this in YAML (easy for CI) or Nim builders; below shows YAML and the equivalent Nim builder idea.

### YAML (example)

```yaml
seed: 12345678
cluster:
  nodes: 5
  election_timeout_ms: {min: 150, max: 300}
  heartbeat_ms: 50
storage:
  durability: Async
  snapshot:
    enabled: true
    trigger:
      max_log_entries: 500        # take snapshot after 500 applied
      or_bytes: 2097152           # or 2MB of FSM state/log
workload:
  type: kv
  rate:
    propose_per_tick: 0.6         # expected value; SimRng drives actual Bernoulli
  mix:
    put: 80                       # %
    get: 20
  keys:
    space: 1000
    zipf_s: 1.1
net:
  latency_ms: {base: 20, jitter: 10, p99: 120}
  drop_pct: 2
  dup_pct: 1
  reorder_window: 5               # up to 5 events can reorder
partitions:
  - at_ms: 5000
    components:
      - [0, 1, 2]                 # majority partition
      - [3, 4]
  - at_ms: 12000
    heal: true
node_lifecycle:
  restart_policies:
    - selector: any
      period_ms: 4000             # every ~4s, randomly pick a node to stop/start
      stop_duration_ms: {min: 50, max: 400}
      probability_per_period_pct: 40
      wipe_db_probability_pct: 15 # DB loss events
    - selector: node:3
      cron: "*/7"                 # every 7 simulated seconds
      stop_duration_ms: 300
      wipe_db_probability_pct: 0
fuzz:
  enabled: true
  knobs:
    vary_timeouts: true
    burst_drops_pct: 10           # short bursts where drop_pct spikes to 10%
    tail_latency_events: 0.01     # occasional p999 spikes
stop:
  max_ms: 30000
  min_commits: 2000
fail_fast: false
artifacts:
  html: "artifacts/run.html"
  json: "artifacts/run.json"
```

### Nim builder (sketch)

```nim
let scn = Scenario.new(seed = 12345678)
  .cluster(nodes=5, electionTimeout=(150,300), heartbeat=50)
  .storage(durability=Async, snapshot=SnapshotCfg(enabled=true, maxEntries=500, orBytes=2.MiB))
  .net(latency=Latency(base=20, jitter=10, p99=120), dropPct=2, dupPct=1, reorderWin=5)
  .partition(at=5.seconds, comps=[[0,1,2],[3,4]])
  .heal(at=12.seconds)
  .lifecycle(RestartPolicy.any(period=4.seconds, stopDur=(50.ms, 400.ms), p=0.40, wipeDbP=0.15))
  .lifecycle(RestartPolicy.node(3).cron(every=7.seconds, stopDur=300.ms))
  .workload(KV(mixPut=80, keys=1000, zipf=1.1, rate=0.6))
  .fuzz(varyTimeouts=true, burstDrops=10, tailLatency=0.01)
  .stop(max=30.seconds, minCommits=2000)
```

---

## Reproducibility & Fuzzing

* **Single seed** drives:

  * scenario random decisions (workload, lifecycle, network faults),
  * per-node substreams (e.g., leader elections) derived as `H(seed, “node-i”)`.
* **Fuzz mode**:

  * Given a base scenario, the fuzzer perturbs: timeouts, latencies, drop spikes, restart timings, and key skew.
  * Outputs the **winning seed** for any failing run.
* CLI examples:

  * `nim r sim_main.nim --scenario path=scenarios/chaos.yaml --seed 42`
  * `nim r sim_main.nim --preset=minority-snapshots --fuzz=100 --fail-fast`

---

## Node lifecycle & storage chaos (exact behaviors)

* **Stop**: node stops processing timers & RPCs; outbound queue cleared or retained (configurable). Inbound messages are dropped by SimNet.
* **Restart**:

  * `wipe=false`: Node reloads `currentTerm`, `votedFor`, log, snapshot from `SimStorage`. Continues as follower.
  * `wipe=true` (DB loss): Node starts empty:

    * If snapshotting exists in cluster, it **catches up** via `InstallSnapshot` + log replication.
    * If no snapshot is available, follower eventually catches up from leader’s log (bounded by leader’s retention in your impl).
* **Crash during write** (Async/Torn modes): last N ops may be absent in log/metadata; invariants verify recovery is safe.

---

## Metrics & Tracing (what we record)

Per tick & per node:

* term, role, commitIndex, lastApplied, lastLogIndex/Term
* nextIndex[]/matchIndex[] (leaders)
* outstanding RPCs, message counts (sent/recv/dropped/duped/reordered)
* storage: log size, snapshot index/term, persisted meta (term/votedFor)
* lifecycle: up/down, reason, wipe flag

Cluster-level:

* leader timeline (who led when)
* election counts per term
* commit throughput, commit latency (ticks/op)
* invariant checks (first failure tick, details)

---

## Invariants (implementation hints)

* **Election Safety**: Track `leadersByTerm: Table[Term, HashSet[NodeId]]`; assert `card ≤ 1`.
* **Log Matching**: For any index `i` where two nodes have entries, assert `(termA[i] == termB[i])`.
* **Leader Append-Only**: Leader never overwrites its own log at indexes it had when becoming leader.
* **State Machine Safety**: Compare applied sequences (KV op IDs); all nodes’ applied logs must be a prefix of the majority’s committed sequence.
* **Leader Completeness**: When a leader for term T commits entry (i), every future leader’s log contains (i).

Snapshot-specific:

* After `InstallSnapshot(snapIndex)`, follower has no entries `< snapIndex+1`.
* Applying snapshot + subsequent entries yields the same FSM state as replaying the original committed entries.

---

## Developer UX & CI

* **One command**:

  * `nimble test` runs fast smoke scenarios: no faults, light chaos, snapshot basic, restart basic.
  * `nimble chaos` runs a curated set with fuzz trials (bounded time).
* **Artifacts**:

  * `artifacts/run.json` (full trace + verdicts),
  * `artifacts/run.html` (clickable timeline, node panels).
* **CI**:

  * Fails on any invariant breach or “no progress” within configured budget.
  * Stores failing seed(s) as CI output for reproduction: `--seed <failing_seed>`.

---

## Minimal skeleton (Nim-style sketch)

```nim
# sim_clock.nim
type SimClock* = ref object
  nowMs*: int64
proc tick*(c: SimClock, dtMs=1) =
  c.nowMs += dtMs
  # run due timers in deterministic order

# sim_rng.nim
type SimRng* = ref object
  state: uint64
proc next*(r: SimRng): uint64 = discard
proc choose*(r: SimRng, n: int): int = discard    # [0..n)
proc bernoulli*(r: SimRng, pct: float): bool = discard

# sim_net.nim
type NetEvent = object
  from, to: int
  deliverAt: int64
  payload: Rpc
  dup: bool
type SimNet* = ref object
  policy: NetPolicy
  queue: seq[NetEvent]
proc send*(net: SimNet, ev: NetEvent) = discard
proc pump*(net: SimNet, now: int64, deliver: proc (ev: NetEvent)) = discard

# sim_storage.nim
type Durability = enum Durable, Async, Torn
type SimStorage* = ref object
  mode: Durability
  nodes: Table[int, NodeDisk]
proc persistLog*(st: SimStorage, id: int, e: LogEntry) = discard
proc reload*(st: SimStorage, id: int, wipe=false): NodeDisk = discard

# node_host.nim
type NodeHost* = ref object
  id: int
  clock: SimClock
  net: SimNet
  storage: SimStorage
  raft: RaftNode    # from your repo
proc step*(h: NodeHost, m: Rpc) = discard
proc tick*(h: NodeHost) = discard
proc restart*(h: NodeHost, wipe=false) = discard

# scenario_runner.nim
proc run*(scn: Scenario): Verdict =
  initAll()
  while not stopCondition():
    scheduleFaults(); driveWorkload()
    for n in nodes: if n.alive: n.tick()
    net.pump(clock.nowMs, deliver=routeToNode)
    checkInvariants()
    clock.tick()
  emitArtifacts(); return verdict
```

---

## Ready-made presets to ship first

1. **happy_3node.yaml** — baseline, no faults, snapshot threshold small.
2. **minority_partition.yaml** — split [0,1] vs [2,3,4], then heal; verify no commits in minority, log repair after heal.
3. **restart_and_wipe.yaml** — periodic restarts with 20% DB wipes; ensure wiped nodes catch up (snapshot or logs).
4. **tail_latency_bursts.yaml** — p99 spikes + reorder; ensure election stability and progress.
5. **snapshot_stress.yaml** — heavy writes to trigger frequent snapshots and installs with follower restarts.

---

## A few decisions I still need from you

1. **Storage API you want NodeHost to use**: do you prefer a key/value log API mirroring your repo’s current abstractions, or a minimal interface (`saveMeta`, `appendLog`, `truncate`, `saveSnapshot`, `loadAll`)?
2. **Exact RPC envelope types**: if your repo’s Raft wire types are already set, I’ll mirror those; otherwise I’ll define minimal types inside the simulator.
3. **HTML timeline**: okay to bundle a small static page (no external deps), or prefer pure JSON + a tiny Nim/JS generator?

If you’re good with this plan, I’ll draft:

* the concrete Nim module skeletons,
* a working `happy_3node.yaml`, `restart_and_wipe.yaml`,
* and the invariant checker stubs (with failing-seed reporting).
