## Unit tests for ZipfSampler

import std/unittest
import std/tables
import std/math
import ../sim/core/types
import ../sim/core/sim_rng
import ../sim/run/scenario_runner

suite "ZipfSampler":
  test "uniform sampling (zipf_s = 0)":
    let sampler = buildZipfSampler(100, 0.0)
    let rng = newSimRng(12345)

    # Collect frequency counts
    var counts = newTable[int, int]()
    let samples = 100000  # Increased sample size for better statistics

    for i in 0..<samples:
      let key = sampleKey(sampler, rng)
      counts[key] = counts.getOrDefault(key, 0) + 1

    # Check that all keys are sampled
    check counts.len == 100

    # Check that frequencies are roughly uniform
    let expectedPerKey = samples.float / 100.0
    let tolerance = expectedPerKey * 0.5  # 50% tolerance for uniform distribution

    for count in counts.values:
      check count.float > expectedPerKey - tolerance
      check count.float < expectedPerKey + tolerance

  test "skewed sampling (zipf_s > 0)":
    let sampler = buildZipfSampler(100, 1.2)
    let rng = newSimRng(12345)

    # Collect frequency counts
    var counts = newTable[int, int]()
    let samples = 100000  # Increased sample size

    for i in 0..<samples:
      let key = sampleKey(sampler, rng)
      counts[key] = counts.getOrDefault(key, 0) + 1

    # Check that low-index keys get more samples than high-index keys
    # Key 0 should have significantly more samples than key 99
    check counts[0] > counts[99] * 5  # Key 0 should have at least 5x more samples

    # Check that early keys generally get more samples than later keys
    # Compare averages of first half vs second half
    var firstHalfTotal = 0
    var secondHalfTotal = 0
    for i in 0..<50:
      firstHalfTotal += counts[i]
    for i in 50..<100:
      secondHalfTotal += counts[i]

    check firstHalfTotal.float > secondHalfTotal.float * 2.0  # First half should have 2x more samples

    # Check that top keys get disproportionately more samples
    var top10Total = 0
    for i in 0..<10:
      top10Total += counts[i]
    let top10Percent = top10Total.float / samples.float
    check top10Percent > 0.6  # Top 10% of keys should get >60% of samples

  test "deterministic sampling":
    let sampler1 = buildZipfSampler(100, 1.0)
    let sampler2 = buildZipfSampler(100, 1.0)
    let rng1 = newSimRng(12345)
    let rng2 = newSimRng(12345)

    # Same sampler and same RNG should produce identical sequences
    for i in 0..<100:
      let key1 = sampler1.sampleKey(rng1)
      let key2 = sampler2.sampleKey(rng2)
      check key1 == key2

  test "different parameters produce different distributions":
    let uniformSampler = buildZipfSampler(100, 0.0)
    let skewedSampler = buildZipfSampler(100, 1.5)
    let rng = newSimRng(12345)

    # Sample from both
    var uniformCounts = newTable[int, int]()
    var skewedCounts = newTable[int, int]()
    let samples = 50000  # Increased sample size

    for i in 0..<samples:
      let uniformKey = sampleKey(uniformSampler, rng)
      let skewedKey = sampleKey(skewedSampler, rng)
      uniformCounts[uniformKey] = uniformCounts.getOrDefault(uniformKey, 0) + 1
      skewedCounts[skewedKey] = skewedCounts.getOrDefault(skewedKey, 0) + 1

    # Skewed distribution should have much higher concentration on low keys
    var uniformTop10Percent = 0
    var skewedTop10Percent = 0
    for i in 0..<10:
      uniformTop10Percent += uniformCounts[i]
      skewedTop10Percent += skewedCounts[i]

    let uniformRatio = uniformTop10Percent.float / samples.float
    let skewedRatio = skewedTop10Percent.float / samples.float

    check skewedRatio > uniformRatio * 3.0  # Skewed should be much more concentrated

  test "key bounds":
    let sampler = buildZipfSampler(50, 1.1)
    let rng = newSimRng(12345)

    for i in 0..<1000:
      let key = sampleKey(sampler, rng)
      check key >= 0
      check key < 50

  test "empty CDF for uniform":
    let sampler = buildZipfSampler(10, 0.0)
    # Uniform sampler should have empty CDF
    check sampler.cdf.len == 0
    check sampler.N == 10

  test "CDF construction for skewed":
    let sampler = buildZipfSampler(5, 1.0)
    # Skewed sampler should have CDF
    check sampler.cdf.len == 5
    check sampler.N == 5

    # CDF should be monotonically increasing
    for i in 1..<sampler.cdf.len:
      check sampler.cdf[i] >= sampler.cdf[i-1]

    # Last element should be 1.0 (approximately)
    check abs(sampler.cdf[^1] - 1.0) < 1e-10
