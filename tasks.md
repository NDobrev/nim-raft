Awesome. Here’s a crisp, execution-ready task plan to build the Raft-only simulator/harness we just scoped. It’s grouped by milestones, with concrete tasks, dependencies, deliverables, and “done” checks. Copy this into an issue tracker and go.

# Milestone 0 — Repo scaffolding & CI (Day 0 bootstraps)

* [x] Create `sim/` workspace layout

  * `sim/core/` (clock, rng, net, storage, types)
  * `sim/raft/` (node host adapter, wrappers)
  * `sim/run/` (scenario runner, CLI)
  * `sim/invariants/` (checkers)
  * `sim/report/` (json, html)
  * `scenarios/` (yaml presets)
  * `tests/` (unit + smoke)
* [x] Add `nimble tasks`: `sim`, `sim:chaos`, `test`
* [ ] CI job (GitHub Actions) running: unit tests + 2 smoke scenarios; upload artifacts
* [x] Coding standards: deterministic PRNG rule, no wall-clock, single seed policy
  **DoD:** `nimble test` green; CI stores `artifacts/run.json` & `run.html` from a smoke run.

---

# Milestone 1 — Deterministic core (clock, rng, scheduler)

* [x] `SimClock` with timer queue (stable ordering on ties)
* [x] `SimRng` with substreams (`rngFor("net"|"storage"|"workload"|nodeId)`)
* [x] Event scheduler utilities (next tick hooks, periodic tasks)
  **DoD:** Unit tests proving determinism: same seed ⇒ identical timer firings; snapshot of RNG stream.

---

# Milestone 2 — Network & RPC plumbing (no faults yet)

* [x] Define minimal Raft RPC envelopes (RequestVote, AppendEntries, InstallSnapshot)
* [x] `SimNet` event queue + delivery to nodes
* [x] Node registry & addressing
* [x] Node lifecycle: `stop()`, `start()` (no wipe yet)
  **DoD:** 3-node happy path commits a few PUTs (manual workload), no faults.

---

# Milestone 3 — Storage & restart semantics (incl. DB loss)

* [x] `SimStorage` modes: `Durable | Async | Torn`
* [x] Persist: term, votedFor, log entries, snapshots
* [x] Restart with `wipe=false` loads disk; `wipe=true` starts empty
* [x] Crash-during-write simulation for Async/Torn (bounded loss)
  **DoD:** Scenario where a node restarts (with/without wipe) and catches up from leader; data survives durable restarts.

---

# Milestone 4 — Raft node host adapter

* [x] `NodeHost` implementing your repo's callbacks for time, IO, logging
* [x] Wrapper exposing `tick()`, `step(msg)`, `propose(cmd)`, `readIndex()`
* [x] Hook `SimNet`↔Raft RPC translation
  **DoD:** 5-node cluster commits under steady load; ReadIndex GETs return correct values.

---

# Milestone 5 — Fault injection & partitions

* [x] Net faults: latency (base+jitter+tail), drop%, dup%, reorder window
* [x] Link-local policies (per from→to)
* [x] Partitions: static, scheduled, flapping (2+ components)
* [x] Lifecycle chaos: scheduled stops/restarts with `wipeDbProbability`
  **DoD:** Minority partition prevents commits; heals cleanly; periodic restarts don't violate safety.

---

# Milestone 6 — Snapshots & install

* [x] Snapshot trigger (by entries/bytes/interval)
* [x] `saveSnapshot` & `installSnapshot` paths in storage
* [x] Log truncation rules after snapshot
  **DoD:** Followers apply snapshot then incremental logs; wiped node rejoins via snapshot if available.

---

# Milestone 7 — Invariants & liveness checks

* [x] Election Safety (≤1 leader/term)
* [x] Log Matching (prefix agreement)
* [x] Leader Append-Only
* [x] State-Machine Safety (applied sequences identical prefixes)
* [x] Leader Completeness
* [x] Liveness: eventual leader/progress under majority health
* [x] Snapshot sanity (no gaps; truncation below snap index)
  **DoD:** Invariants run every tick; failing run aborts with first classified breach.

---

# Milestone 8 — Scenario DSL & fuzzing

* [x] YAML loader for scenario config (cluster, net, storage, lifecycle, workload, partitions, stop conditions)
* [x] Nim builder API mirroring YAML
* [ ] Fuzz driver: perturb selected knobs deterministically from seed
* [x] CLI: `--scenario`, `--preset`, `--seed`, `--fuzz=N`, `--fail-fast`
  **DoD:** `nim r sim/run/main.nim --scenario scenarios/restart_and_wipe.yaml --seed 42` reproducibly replicates behavior.

---

# Milestone 9 — Reporting (JSON + HTML)

* [x] Structured JSON trace: config, seed, per-tick node/cluster state, events, faults, invariants
* [x] Minimal single-file HTML timeline (leaders over time, commits, partitions, restarts, failures)
* [x] CI summary line: PASS/FAIL, seed, first-failure, path to artifacts
  **DoD:** Open HTML locally; see timeline; JSON schema documented and versioned. ✅

---

# Milestone 10 — Test suites & presets

* [x] `scenarios/happy_3node.json`
* [x] `scenarios/minority_partition.json`
* [x] `scenarios/restart_and_wipe.json`
* [x] `scenarios/tail_latency_bursts.json`
* [x] `scenarios/snapshot_stress.json`
* [ ] Golden traces for stable scenarios (snapshot + diff on PR)
  **DoD:** All presets pass invariants; CI runs two fast ones on every PR.

---

# Nice-to-have (after core is green)

* [ ] Linearizability checker for KV (history capture + verifier)
* [ ] HTML diff viewer for two seeds
* [ ] Trace shrinking (delta-debugging) to minimize failing seeds
* [ ] Perf counters (commit latency distribution in ticks)

---

## Concrete issue stubs (copy/paste)

**/sim/core/sim_clock.nim**

* [x] Implement `SimClock` (+ unit tests: timers ordering, driftless tick)

**/sim/core/sim_rng.nim**

* [x] Implement split streams + tests (stable sequence by name)

**/sim/net/sim_net.nim**

* [x] Basic delivery queue
* [x] Fault policies & reordering
* [x] Partition manager

**/sim/storage/sim_storage.nim**

* [x] Durable/Async/Torn writes
* [x] Snapshot save/load/install
* [x] Restart(wipe) semantics

**/sim/raft/node_host.nim**

* [x] Time & IO adapters to your Raft SM
* [x] RPC encode/decode glue

**/sim/invariants/checker.nim**

* [x] Implement 6 invariants + snapshot sanity
* [x] Failure reporter (first failure with context)

**/sim/run/scenario_runner.nim & main.nim**

* [x] Tick loop, workload driver, stop conditions
* [x] YAML loader & builder API
* [ ] Fuzz knob perturbations

**/sim/report/**

* [x] JSON writer (schema v1)
* [x] HTML timeline (no external deps)

**/tests/**

* [x] Determinism tests (seed equality)
* [x] Happy-path commit
* [x] Minority partition no-commit
* [ ] Restart + wipe catch-up
* [x] Snapshot install correctness

---

## Initial presets (drop in now)

`scenarios/happy_3node.yaml`, `scenarios/restart_and_wipe.yaml`, `scenarios/minority_partition.yaml` exactly as in the spec you approved (you can reuse those samples).

---

## Definition of Done (project)

* All invariants implemented and enforced per tick.
* Reproducible runs: single seed governs everything; failing seeds emitted.
* Supports: stops/restarts, DB loss, partitions, latency/drop/dup/reorder, snapshots & install.
* JSON + HTML artifacts produced; CI executes smoke presets on PRs.

✅ **NEARLY COMPLETE**: Milestones 0-9 and most of 10 have been successfully implemented. The simulation framework has comprehensive reporting, scenario loading, CLI, workload driver, invariant checking, and generates JSON traces, HTML timelines, and CI summaries. Remaining tasks: fuzz driver, additional tests, and CI setup.
