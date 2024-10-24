# nim-raft
# Copyright (c) 2023 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

# Raft Node Public Types

import std/rlocks
import options
import stew/results
import uuids
import chronos

export
  results,
  options,
  rlocks,
  uuids,
  chronos

const
  DefaultUUID* = initUUID(0, 0)             # 00000000-0000-0000-0000-000000000000

type
  RaftNodeId* = object
    id*: string                              # uuid4 uniquely identifying every Raft Node
  RaftNodeTerm* = int64                       # Raft Node Term Type
  RaftLogIndex* = int64                       # Raft Node Log Index Type
  ConfigMemberSet* = seq[RaftNodeId]
  ConfigDiff* = object
    joining*: ConfigMemberSet
    leaving*: ConfigMemberSet

  RaftConfig* = object
    currentSet*: ConfigMemberSet
    previousSet*: ConfigMemberSet

  ReftConfigRef* = ref RaftConfig

func `$`*(r: RaftNodeId): string =
  if r.id.len > 8:
    return $r.id[0..8] & ".."
  else:
    return $r.id