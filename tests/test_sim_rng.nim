## Unit tests for SimRng

import std/unittest
import std/sets
import ../sim/core/sim_rng

suite "SimRng":
  test "same seed produces identical sequence":
    let rng1 = newSimRng(12345)
    let rng2 = newSimRng(12345)

    for i in 0..10:
      check rng1.next() == rng2.next()

  test "different seeds produce different sequences":
    let rng1 = newSimRng(12345)
    let rng2 = newSimRng(54321)

    var sameCount = 0
    for i in 0..100:
      if rng1.next() == rng2.next():
        sameCount += 1

    # Should be very unlikely to have many matches
    check sameCount < 10

  test "substreams are deterministic":
    let base1 = newSimRng(12345)
    let base2 = newSimRng(12345)

    let sub1a = base1.rngFor("test")
    let sub1b = base1.rngFor("test")
    let sub2a = base2.rngFor("test")

    # Same base seed + same domain should give same sequence
    for i in 0..10:
      check sub1a.next() == sub2a.next()

    # Different instances of same domain should be independent
    check sub1a.next() != sub1b.next()

  test "node substreams":
    let base = newSimRng(12345)
    let node0 = base.rngFor(0)
    let node1 = base.rngFor(1)

    # Node streams should be different
    check node0.next() != node1.next()

    # Fresh instances should produce same sequence
    let base2 = newSimRng(12345)
    let base3 = newSimRng(12345)
    let node0_2 = base2.rngFor(0)
    let node0_3 = base3.rngFor(0)
    check node0_2.next() == node0_3.next()
    check node0_2.next() == node0_3.next()

  test "nextInt range":
    let rng = newSimRng(12345)

    for i in 0..100:
      let val = rng.nextInt(10)
      check val >= 0 and val < 10

  test "nextInt min-max":
    let rng = newSimRng(12345)

    for i in 0..100:
      let val = rng.nextInt(5, 15)
      check val >= 5 and val < 15

  test "bernoulli probability":
    let rng = newSimRng(12345)
    var trueCount = 0
    let trials = 10000

    for i in 0..trials:
      if rng.bernoulli(0.3):
        trueCount += 1

    let proportion = trueCount.float / trials.float
    # Should be roughly 0.3 with some tolerance
    check proportion > 0.25 and proportion < 0.35

  test "choose from array":
    let rng = newSimRng(12345)
    let items = [1, 2, 3, 4, 5]

    var seen = initHashSet[int]()
    for i in 0..100:
      let choice = rng.choose(items)
      check choice in items
      seen.incl(choice)

    # Should have seen all items with high probability
    check seen.len == 5

  test "shuffle modifies array":
    let rng = newSimRng(12345)
    var arr1 = [1, 2, 3, 4, 5]
    var arr2 = [1, 2, 3, 4, 5]

    rng.shuffle(arr1)
    # Different seed should likely give different result
    let rng2 = newSimRng(54321)
    rng2.shuffle(arr2)

    # Same seed should give same shuffle
    var arr3 = [1, 2, 3, 4, 5]
    let rng3 = newSimRng(12345)
    rng3.shuffle(arr3)

    check arr1 == arr3
    # arr2 might be same by chance, but arr1 should be different from original
    check arr1 != [1, 2, 3, 4, 5]

  test "exponential distribution":
    let rng = newSimRng(12345)
    var sum = 0.0
    let samples = 1000

    for i in 0..samples:
      let val = rng.exponential(2.0)  # rate = 2.0
      sum += val
      check val >= 0.0

    let mean = sum / samples.float
    # For exponential with rate 2, mean should be 0.5
    check mean > 0.4 and mean < 0.6

  test "normal distribution":
    let rng = newSimRng(12345)
    var sum = 0.0
    let samples = 1000

    for i in 0..samples:
      let val = rng.normal(10.0, 2.0)  # mean=10, stddev=2
      sum += val

    let mean = sum / samples.float
    # Should be close to 10.0
    check mean > 9.0 and mean < 11.0
