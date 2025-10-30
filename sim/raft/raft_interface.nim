## Raft Interface - Abstract interface for Raft implementations
##
## Defines the contract that Raft implementations must follow
## to integrate with the simulation framework.

import std/options
import ../core/sim_clock
import ../core/types
import ../storage/sim_storage
import ../../src/raft/types
import ../../src/raft/consensus_state_machine

type
  RaftNode* = ref object of RootObj
    ## Abstract base class for Raft implementations

  RaftHostCallbacks* = ref object
    ## Callbacks provided by the simulation framework to Raft implementations
    sendRpc*: proc(target: RaftNodeId, rpc: RaftRpcMessage) {.gcsafe, closure.}
    persistTerm*: proc(term: RaftNodeTerm) {.gcsafe, closure.}
    persistVotedFor*: proc(votedFor: Option[RaftNodeId]) {.gcsafe, closure.}
    persistLogEntry*: proc(entry: LogEntry) {.gcsafe, closure.}
    truncateLog*: proc(fromIndex: RaftLogIndex) {.gcsafe, closure.}
    saveSnapshot*: proc(snapshot: Snapshot) {.gcsafe, closure.}
    getSnapshot*: proc(): Option[Snapshot] {.gcsafe, closure.}
    loadDisk*: proc(wipe: bool): NodeDisk {.gcsafe, closure.}
    scheduleTimer*: proc(delayMs: int64, callback: TimerCallback): TimerId {.gcsafe, closure.}
    cancelTimer*: proc(timerId: TimerId): bool {.gcsafe, closure.}
    randomBytes*: proc(n: int): seq[byte] {.gcsafe, closure.}
    randomInt*: proc(max: int): int {.gcsafe, closure.}
    getTime*: proc(): int64 {.gcsafe, closure.}
    onCommitted*: proc(nodeId: RaftNodeId, entry: LogEntry) {.gcsafe, closure.}
    # Optional: record internal debug logs produced by Raft poll()
    recordDebugLog*: proc(entry: DebugLogEntry) {.gcsafe, closure.}

method initialize*(raft: RaftNode, nodeId: RaftNodeId, callbacks: RaftHostCallbacks, clusterConfig: seq[RaftNodeId]) {.base, gcsafe.} =
  ## Initialize the Raft node with its ID, host callbacks, and cluster configuration
  doAssert false, "initialize method not implemented"

method step*(raft: RaftNode, rpc: RaftRpcMessage) {.base, gcsafe.} =
  ## Process an incoming RPC message
  doAssert false, "step method not implemented"

method tick*(raft: RaftNode) {.base, gcsafe.} =
  ## Process a timer tick
  doAssert false, "tick method not implemented"

method propose*(raft: RaftNode, cmd: seq[byte]): bool {.base, gcsafe.} =
  ## Propose a new command to the cluster
  doAssert false, "propose method not implemented"

method readIndex*(raft: RaftNode): Option[seq[byte]] {.base, gcsafe.} =
  ## Perform a read-only query using ReadIndex
  doAssert false, "readIndex method not implemented"

method getState*(raft: RaftNode): NodeState {.base, gcsafe.} =
  ## Get current Raft state (for testing/invariants)
  doAssert false, "getState method not implemented"

method getCommitIndex*(raft: RaftNode): RaftLogIndex {.base, gcsafe.} =
  ## Get current commit index
  doAssert false, "getCommitIndex method not implemented"

method getLastApplied*(raft: RaftNode): RaftLogIndex {.base, gcsafe.} =
  ## Get last applied index
  doAssert false, "getLastApplied method not implemented"

method forceLocalSnapshot*(raft: RaftNode, upTo: RaftLogIndex): bool {.base, gcsafe.} =
  ## Instruct the node to create/apply a local snapshot up to the given index
  doAssert false, "forceLocalSnapshot method not implemented"
