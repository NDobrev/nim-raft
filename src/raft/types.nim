# nim-raft
# Copyright (c) 2023 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

import std/hashes

type
  RaftNodeId* = object
    id*: string # uuid4 uniquely identifying every Raft Node

  RaftNodeTerm* = uint64 # Raft Node Term Type
  RaftLogIndex* = uint64 # Raft Node Log Index Type
  RaftSnapshotId* = uint32
  ConfigMemberSet* = seq[RaftNodeId]
  ConfigDiff* = object
    joining*: ConfigMemberSet
    leaving*: ConfigMemberSet

  RaftConfig* = object
    currentSet*: ConfigMemberSet
    previousSet*: ConfigMemberSet

  ReftConfigRef* = ref RaftConfig

proc empty*(t: typedesc[RaftNodeId]): RaftNodeId =
  RaftNodeId()

proc newRaftNodeId*(s: string): RaftNodeId =
  RaftNodeId(id: s)

# Ensure stable hashing/equality for use as Table keys
proc `==`*(a, b: RaftNodeId): bool {.inline.} = a.id == b.id

proc hash*(x: RaftNodeId): Hash {.inline.} =
  ## Hash based on the node id string contents
  result = hash(x.id)
