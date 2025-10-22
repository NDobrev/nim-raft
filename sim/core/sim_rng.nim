## SimRng - Deterministic pseudo-random number generator for simulation
##
## Provides deterministic randomness using a seed, with substreams for
## different simulation components (net, storage, workload, etc.) to ensure
## stable behavior even when code changes add/remove random calls.

import std/hashes
import std/math

import types
import ../../src/raft/types
import std/strformat

type
  SimRng* = ref object
    state*: uint64

proc murmurHash64*(x: uint64): uint64 =
  ## Simple 64-bit murmur hash for seed derivation
  const m: uint64 = 0xc6a4a7935bd1e995'u64
  const r: uint32 = 47
  var h: uint64 = 0xdeadbeefdeadbeef'u64 xor (8'u64 * m)

  var k = x
  k = k * m
  k = k xor (k shr r)
  k = k * m
  h = h xor k
  h = h * m

  h = h xor (h shr r)
  h = h * m
  h = h xor (h shr r)

  return h

proc newSimRng*(seed: uint64): SimRng =
  ## Create a new SimRng with the given seed
  SimRng(state: seed)

proc next*(rng: SimRng): uint64 =
  ## Generate next random uint64 using xorshift* algorithm
  # xorshift* generator with good statistical properties
  rng.state = rng.state xor (rng.state shr 12)
  rng.state = rng.state xor (rng.state shl 25)
  rng.state = rng.state xor (rng.state shr 27)
  result = rng.state * 0x2545F4914F6CDD1D'u64

proc rngFor*(baseRng: SimRng, domain: string): SimRng =
  ## Create a substream RNG for a specific domain/component
  ## This ensures stable random sequences even if other domains change
  let domainHash = hash(domain).uint64
  # Use the next random value from base RNG as additional entropy
  # This ensures different calls get different seeds even for the same domain
  let entropy = baseRng.next()
  let subSeed = murmurHash64(baseRng.state xor domainHash xor entropy)
  SimRng(state: subSeed)

proc rngFor*(baseRng: SimRng, nodeId: int): SimRng =
  ## Create a substream RNG for a specific node
  baseRng.rngFor("node-" & $nodeId)

proc rngFor*(baseRng: SimRng, nodeId: RaftNodeId): SimRng =
  ## Create a substream RNG for a specific node (RaftNodeId overload)
  baseRng.rngFor("node-" & nodeId.id)

proc nextFloat*(rng: SimRng): float =
  ## Generate random float in [0.0, 1.0)
  let u = rng.next()
  return cast[float](u shr 11) * (1.0 / cast[float](1'u64 shl 53))

proc nextInt*(rng: SimRng, max: int): int =
  ## Generate random int in [0, max)
  if max <= 0:
    return 0
  let u = rng.next()
  return int(u mod cast[uint64](max))

proc nextInt*(rng: SimRng, min, max: int): int =
  ## Generate random int in [min, max)
  let range = max - min
  return min + rng.nextInt(range)

proc bernoulli*(rng: SimRng, probability: float): bool =
  ## Return true with given probability [0.0, 1.0]
  return rng.nextFloat() < probability

proc choose*[T](rng: SimRng, items: openArray[T]): T =
  ## Randomly choose an item from the array
  if items.len == 0:
    raise newException(ValueError, "Cannot choose from empty array")
  return items[rng.nextInt(items.len)]

proc shuffle*[T](rng: SimRng, items: var openArray[T]) =
  ## Shuffle array in-place using Fisher-Yates algorithm
  for i in countdown(items.len - 1, 1):
    let j = rng.nextInt(i + 1)
    swap(items[i], items[j])

proc exponential*(rng: SimRng, rate: float): float =
  ## Generate exponential random variable with given rate (1/mean)
  -ln(rng.nextFloat()) / rate

proc normal*(rng: SimRng, mean: float, stddev: float): float =
  ## Generate normal random variable using Box-Muller transform
  let u1 = rng.nextFloat()
  let u2 = rng.nextFloat()
  let z0 = sqrt(-2.0 * ln(u1)) * cos(2.0 * PI * u2)
  return mean + z0 * stddev
