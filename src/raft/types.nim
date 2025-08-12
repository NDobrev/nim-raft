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
  #RaftNodeId* = object
  #  idPtr*: ptr char # uuid4 uniquely identifying every Raft Node
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

# proc `=destroy`*(dest: var RaftNodeId) =
#   echo $cast[uint](dest.idPtr) & " =destroy"
#   dest.idPtr = nil
# # proc `=copy`*(dest: var RaftNodeId, src: RaftNodeId) =
# #   dest.idPtr = src.idPtr
#   #echo $cast[uint](dest.idPtr) & " =copy " & $cast[uint](src.idPtr)

# proc `=dup`*(src: RaftNodeId): RaftNodeId =
#   result.idPtr = src.idPtr
#   echo $cast[uint](result.idPtr) & " =dup " & $cast[uint](src.idPtr)

# proc `=trace`*(dest: var RaftNodeId, env: pointer) =
#   discard
#   #echo $cast[uint](dest.idPtr) & " =trace " & $cast[uint](env)

# proc `=sink`*(dest: var RaftNodeId, src: RaftNodeId) =
#   dest.idPtr = src.idPtr

# proc `wasMoved`*(x: var RaftNodeId) =
#   x.idPtr = nil



# var
#   globalStringCache*: StringCache = nil
#   cacheLock*: Lock

# proc `$`*(r: RaftNodeId): string =
#   {.gcsafe, noSideEffect.}:
#     if globalStringCache.isNil:
#       return "<uninitialized cache>"
#     elif r.idPtr == nil:
#       # Empty string
#       return "<empty RaftNodeId>"
#     else:
#       # Lookup in reverse cache
#       globalStringCache.revCache.withValue(r.idPtr, val):
#         return val[]
#       do:
#         doAssert false
#         return "<invalid RaftNodeId>"

# proc initGlobalStringCache*() =
#   if globalStringCache.isNil:
#     initLock(cacheLock) 
#     globalStringCache = StringCache(
#       cache: initTable[string, string](),
#       revCache: initTable[ptr char, string]()
#     )

# proc intern*(s: string): ptr char =
#   {.gcsafe, noSideEffect.}:
#     initGlobalStringCache()
#     withLock cacheLock:
#       globalStringCache.cache.withValue(s, v):
#         if v[].len > 0:
#           return v[0].addr
#         else:
#           return nil
#       do:
#         let interned = globalStringCache.cache.mgetOrPut(s,s)
#         if interned.len > 0:
#           globalStringCache.revCache[interned[0].addr] = interned
#           return interned[0].addr
#         else:
#           # For empty string, store nil pointer mapping for completeness (optional)
#           globalStringCache.revCache[nil] = ""
#           return nil
        

proc empty*(t: typedesc[RaftNodeId]): RaftNodeId =
  RaftNodeId()

proc newRaftNodeId*(s: string): RaftNodeId =
  RaftNodeId(id: s)
