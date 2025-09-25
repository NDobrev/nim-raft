# SSZ Serialization for Raft Messages — In‑Place Mutation Plan

<!-- ## Testing

- Make sure that `nimble test` is compiled and run correctly -->

## Code quality guidelines

- ALWAYS strive to achieve high code quality.
- ALWAYS write secure code.
- ALWAYS make sure the code is well tested and edge cases are covered. Design the code for testability and be extremely thorough.
- ALWAYS write defensive code and make sure all potential errors are handled.
- ALWAYS strive to write highly reusable code with routines that have high fan in and low fan out.
- ALWAYS keep the code DRY.
- Aim for low coupling and high cohesion. Encapsulate and hide implementation details.
- When creating executable, ALWAYS make sure the functionality can also be used as a library.
  To achieve this, avoid global variables, raise/return errors instead of terminating the program, and think whether the use case of the library requires more control over logging
  and metrics from the application that integrates the library.


Got it — we’ll **change the existing Raft types themselves** to be SSZ-encodable and make SSZ the **only** binary serialization path. Below is a practical, junior-friendly, **atomic step plan**. Every step says which files to touch, what to change, and how to test it (unit + integration where it helps). I’ve based file names on your repo layout (top level has `src`, `tests`, `raft.nimble`, etc.) — if any file differs locally, search by the symbols noted (e.g., `type LogEntry`, `RequestVoteReq`). ([GitHub][1])

---

# Refactor plan: make Raft RPCs & LogEntries encode/decode via SSZ

## Ground rules (for the whole refactor)

* **No mirror types.** We modify the existing Raft types to be SSZ-friendly.
* **SSZ only.** All binary (de)serialization code paths use SSZ (no feature flags, no legacy codec).
* **Minimal API change.** Keep field names/semantics; only adjust field **types** where SSZ requires fixed width / byte sequences / bounded lists. (SSZ needs explicit bounds for variable-length items; we’ll add internal `const` limits where needed.) ([ethereum.github.io][2])
* **Every step compiles, tests pass.** Commit each step with tests.

---

## Step 1 — Add SSZ lib & make `LogEntry` SSZ-friendly

**Status:** ✅ Completed (`src/raft/ssz.nim`, `tests/test_ssz_basic_types.nim`)

* Added `MaxLogEntryPayloadBytes` guardrails and SSZ validation helpers for `LogEntry` serialization.
* Introduced `tests/test_ssz_basic_types.nim` covering command/config/empty round-trips and payload bound checks.

**Goal:** Introduce `nim-ssz-serialization` and ensure `LogEntry` can round-trip with SSZ.

**Files to change**

* `raft.nimble`: add dependency

  * `requires "https://github.com/status-im/nim-ssz-serialization >= 0.9.0"` (or the version you target)
* `src/raft/<where LogEntry is defined>.nim` (search `type LogEntry`)
* New test: `tests/test_ssz_basic_types.nim`

**Implementation**

1. `import ssz_serialization` in the module that defines `LogEntry`.
2. Ensure SSZ-compatible field types:

   * `term`, `index`: use fixed-width unsigned (e.g., `uint64`).
   * `payload`/`data`: use `seq[byte]` (opaque bytes).
   * If `LogEntry` contains nested types, ensure they are also SSZ-friendly (fixed ints / `seq[byte]` / `seq[T]` with a known max).
3. Add internal limits SSZ needs for variable sizes:

   ```nim
   const
     MaxEntryPayload = 1_048_576  # 1 MiB, adjust as your protocol allows
   ```

   Enforce on encode (assert/return error) and check on decode.
4. Provide `proc` pair on the **type you already have** (no mirrors):

   ```nim
   proc toSsz*(x: LogEntry): seq[byte] =
     ## serialize with SSZ
     # build an SSZ container with the same fields and call the library encoder

   proc fromSsz*(T: type LogEntry, bytes: openArray[byte]): LogEntry =
     ## deserialize with SSZ
   ```

   (Name them however you standardize — just keep them **on the real type**.)

**Tests**

* `tests/test_ssz_basic_types.nim`

  * Round-trip: random payload sizes (0, small, near `MaxEntryPayload`).
  * Canonicality: `toSsz(LogEntry.fromSsz(b)) == b` for valid `b`.
  * Bounds: payload > `MaxEntryPayload` must fail decode/encode with a clear error.

---

## Step 2 — Make `RequestVote{Req,Resp}` SSZ-friendly & SSZ-only

**Goal:** Change the existing request/response types (no mirrors), add SSZ encode/decode, do **not** wire it into transport yet.

**Files**

* `src/raft/<where RequestVoteReq/Resp are defined>.nim` (search `type RequestVoteReq`)
* New test: `tests/t_ssz_requestvote.nim`

**Implementation**

1. `import ssz_serialization` in this module.
2. Ensure SSZ types:

   * `term`, `lastLogTerm`, `lastLogIndex` → `uint64`.
   * `candidateId` / `nodeId`: choose SSZ-friendly representation (if it’s bytes, use `array[N, byte]` for fixed size or `seq[byte]` with a bound; if numeric, make it `uint64`).
3. Add:

   ```nim
   proc toSsz*(x: RequestVoteReq): seq[byte]
   proc fromSsz*(T: type RequestVoteReq, b: openArray[byte]): RequestVoteReq

   proc toSsz*(x: RequestVoteResp): seq[byte]
   proc fromSsz*(T: type RequestVoteResp, b: openArray[byte]): RequestVoteResp
   ```

**Tests**

* `tests/t_ssz_requestvote.nim`

  * Round-trip both types with edge term/index values (`0`, `high(uint64)`).
  * Boolean flags (e.g., `voteGranted`) survive roundtrip.

---

## Step 3 — Make `AppendEntries{Req,Resp}` SSZ-friendly (entries=0 or 1)

**Goal:** Get the structure right with 0 or 1 entry to keep it small and surgical.

**Files**

* `src/raft/<where AppendEntriesReq/Resp are defined>.nim`
* New test: `tests/t_ssz_appendentries_basic.nim`

**Implementation**

1. Ensure types are SSZ-friendly:

   * `term`, `prevLogIndex`, `prevLogTerm`, `leaderCommit` → `uint64`.
   * `entries: seq[LogEntry]` is fine, but **add bounds**:

     ```nim
     const MaxEntriesPerMsg = 1024
     ```

     Enforce in (de)serialization.
2. Add `toSsz/fromSsz` for both request and response (like Step 2).
3. For now, restrict tests to 0 or 1 entry to validate end-to-end shape.

**Tests**

* `tests/t_ssz_appendentries_basic.nim`

  * Round-trip with no entries.
  * Round-trip with one minimal `LogEntry`.
  * Fail if `entries.len > MaxEntriesPerMsg`.

---

## Step 4 — `AppendEntries` with many entries (+payload bounds)

**Goal:** Exercise larger, variable lists and big payloads.

**Files**

* Same module as Step 3 (no new public API).
* New test: `tests/t_ssz_appendentries_many.nim`

**Implementation**

* No new procs; just ensure encoder/decoder stream through the list efficiently.
* Validate total byte volume if your protocol desires (optional but recommended):

  ```nim
  const MaxAppendEntriesBytes = 4 * 1024 * 1024
  ```

  (Reject oversized messages on encode/decode.)

**Tests**

* Randomized generation:

  * `entries.len` in `[0, 8, 64, MaxEntriesPerMsg]`.
  * Mix payload sizes (0, small, near `MaxEntryPayload`).
* Round-trip + size/bound failures.

---

## Step 5 — Make `InstallSnapshot{Req,Resp}` (if present) SSZ-friendly

**Goal:** Cover snapshot RPC.

**Files**

* `src/raft/<where InstallSnapshot* are defined>.nim`
* New test: `tests/t_ssz_snapshot.nim`

**Implementation**

* Typical fields: `term:uint64`, `lastIncludedIndex:uint64`, `lastIncludedTerm:uint64`, `offset:uint64`, `data: seq[byte]`, `done: bool`.
* Add size bounds for `data` (e.g., `MaxSnapshotChunk = 4 * 1024 * 1024`) and enforce.

**Tests**

* Round-trip for chunks (`offset` progression) and `done=true/false`.
* Bounds tests for chunk size.

---

## Step 6 — Replace all binary serialization calls with SSZ

**Goal:** Wire SSZ into the transport and storage — **single codec path**.

**Files**

* `src/raft/network/*.nim` or wherever outbound/inbound RPC bytes are built/parsed.
* `src/raft/storage/*.nim` or wherever log entries are persisted/read.
* If there’s a central “codec” or “wire” module, update it; otherwise inline.

**Implementation**

* **Sending RPCs**: replace previous binary writer with `req.toSsz()`.
* **Receiving RPCs**: replace previous parser with `RequestVoteReq.fromSsz(bytes)` etc., switch on the message type as you already do (your current framing likely carries a message kind — keep that framing).
* **Log persistence**: write/read `LogEntry` via `toSsz/fromSsz`.
* Remove (or comment and then delete in a later step) any legacy binary encode/decode helpers.

**Tests**

* Existing integration tests should pass unchanged (behaviorally).
* Add `tests/t_wire_roundtrip.nim`:

  * Simulate “send → bytes → recv” for each RPC kind.
  * Simulate log write/read round-trip.

---

## Step 7 — Node-to-node integration test (leader election + 1 entry)

**Goal:** End-to-end sanity with SSZ in place.

**Files**

* New: `tests/it_ssz_cluster_basic.nim`

**Implementation (test harness)**

* Spin up a 3-node in-proc cluster using your existing test utilities (the repo ships tests and CI; leverage same patterns). ([GitHub][1])
* Let it elect a leader, append a small `LogEntry`, wait until committed across majority.
* Assert:

  * One leader; terms monotonic.
  * Logs identical on quorum.
  * No decode/encode errors surfaced.

---

## Step 8 — Large replication integration test

**Goal:** Stress SSZ lists/bytes in real replication.

**Files**

* New: `tests/it_ssz_cluster_stress.nim`

**Implementation (test harness)**

* 3-node cluster, append:

  * 1k entries of small payloads
  * 50 entries near `MaxEntryPayload`
* Verify commitment, equality, and timing within reasonable bounds.

---

## Step 9 — Remove dead code & document the format switch

**Goal:** Clean up and make it obvious to users.

**Files**

* Remove any old binary helpers in `src/raft/*` (search for old encode/decode).
* `README.md`: add a short “Wire format” section: “All Raft RPCs and LogEntries are serialized with SSZ via `nim-ssz-serialization`.”
* If you have persisted non-SSZ logs from earlier versions, add a one-line note in README: “**Breaking**: storage files created before commit `<sha>` are incompatible and must be cleared or migrated.”

**Tests**

* `nimble test` must be green.
* Grep the tree to ensure no old codec calls remain.

---

## Step 10 — Benchmark (optional but recommended)

**Goal:** Ensure SSZ didn’t regress throughput/latency.

**Files**

* `tests/bench/bench_ssz.nim` (or under `bench/` if you keep benches separate)

**Implementation**

* Measure:

  * `toSsz/fromSsz` cost for LogEntry (sizes: 0, 1KiB, 1MiB).
  * AppendEntries pack/unpack with {1, 64, 512} entries.

**Output acceptance**

* Keep results printed; no hard thresholds in CI unless you already have a bench framework.

---

## Practical tips for the junior dev

* **Finding the right files quickly**

  * `ripgrep` or `grep -R`:

    * `rg "type LogEntry" src`
    * `rg "type RequestVoteReq" src`
    * `rg "AppendEntriesReq" src`
  * Transport glue often sits under `src/raft/network` or a “wire” module; storage under `src/raft/storage`.

* **Choosing SSZ field shapes**

  * **Numbers** → fixed width (`uint64` is a safe default for term/index).
  * **Opaque blobs** → `seq[byte]` (with an explicit `Max...` constant).
  * **Lists** → `seq[T]` with an explicit `Max...` for validation.
  * **IDs**: if your `NodeId` isn’t numeric, prefer **fixed-length bytes** (`array[N, byte]`) so SSZ stays canonical. If you can’t fix the length, use `seq[byte]` with a bound and validate. (SSZ spec distinguishes numbers vs. bytes; bytes are intended for opaque data.) ([ethereum.github.io][2])

* **Validation**

  * Enforce max sizes *before* calling the encoder and *after* decoding.
  * Return your project’s standard error type (do not `quit`; bubble up).

* **Style**

  * Follow the Status Nim style guide (imports ordering, naming, error handling). ([status-im.github.io][3])

---

## Example skeletons (adapt to the exact modules)

```nim
# In the module that defines LogEntry
import ssz_serialization

const
  MaxEntryPayload = 1_048_576

type
  LogEntry* = object
    term*: uint64
    index*: uint64
    payload*: seq[byte]    # opaque

proc toSsz*(x: LogEntry): seq[byte] =
  doAssert x.payload.len <= MaxEntryPayload
  # Build an SSZ container inline (fields encoded in order)
  # Depending on the library API, this may be a single call like:
  # result = sszEncode(x)          # <- replace with the actual API you use
  discard

proc fromSsz*(T: type LogEntry, b: openArray[byte]): LogEntry =
  # result = sszDecode[LogEntry](b)  # <- replace with the actual API you use
  doAssert result.payload.len <= MaxEntryPayload
  discard
```

```nim
# In your transport module (sending a RequestVote)
let bytes = req.toSsz()
transportSend(kind = RpcKind.RequestVote, payload = bytes)

# Upon receiving:
let req = RequestVoteReq.fromSsz(incomingPayload)
```

*(Use the exact SSZ encode/decode procs from `nim-ssz-serialization`; method names above are placeholders — the step tests will lock this down for your codebase.)*

---

### Deliverables checklist per step

* Code changes committed.
* New/updated tests committed.
* CI green (`nimble test`).
* Short commit message referencing the step (e.g., `SSZ(1): LogEntry SSZ encode/decode with bounds`).

If you want, I can turn this into **GitHub issues** (one per step) with copy-pasteable checklists for your junior.

[1]: https://github.com/NDobrev/nim-raft "GitHub - NDobrev/nim-raft: A modified version of the Raft consensus protocol for highly-efficient cooperative Ethereum SSV implementations"
[2]: https://ethereum.github.io/consensus-specs/ssz/simple-serialize/?utm_source=chatgpt.com "SimpleSerialize (SSZ) - Ethereum Consensus Specs"
[3]: https://status-im.github.io/nim-style-guide/?utm_source=chatgpt.com "Introduction - The Status Nim style guide"
