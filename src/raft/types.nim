# nim-raft
# Copyright (c) 2023 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

# Raft Node Public Types

# import std/rlocks
# import options

# import chronos

# export results, options, rlocks, chronos

# type
#   RaftNodeId* = object
#     id*: string # uuid4 uniquely identifying every Raft Node

#   RaftNodeTerm* = uint64 # Raft Node Term Type
#   RaftLogIndex* = uint64 # Raft Node Log Index Type
#   RaftSnapshotId* = uint32
#   ConfigMemberSet* = seq[RaftNodeId]
#   ConfigDiff* = object
#     joining*: ConfigMemberSet
#     leaving*: ConfigMemberSet

#   RaftConfig* = object
#     currentSet*: ConfigMemberSet
#     previousSet*: ConfigMemberSet

#   ReftConfigRef* = ref RaftConfig

# func `$`*(r: RaftNodeId): string =
#   if r.id.len > 8:
#     return $r.id[0 .. 8] & ".."
#   $r.id



import std/[rlocks]
import options

import chronos, tables

export results, options, rlocks, chronos

type
  RaftNodeId* = object
    id*: string # uuid4 uniquely identifying every Raft Node
  StringCache = ref object
    cache*: Table[string, string]       # string -> interned string
    revCache*: Table[ptr char, string]  # pointer -> original string

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
