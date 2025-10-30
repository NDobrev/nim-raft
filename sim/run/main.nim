## Simulation Main - CLI entry point for Raft simulation
##
## Parses command line arguments and runs scenarios.

import std/os
import std/strutils
import std/options
import std/strformat

import scenario_runner
import scenario_config
import ../report/ci_summary

proc printUsage() =
  echo "Raft Simulation Runner"
  echo ""
  echo "Usage:"
  echo "  sim_main --scenario <path> [--seed <seed>] [--fuzz <trials>] [--fail-fast]"
  echo "  sim_main --preset <name> [--seed <seed>] [--fuzz <trials>] [--fail-fast]"
  echo ""
  echo "Options:"
  echo "  --scenario <path>    Path to YAML scenario file"
  echo "  --preset <name>      Use built-in preset:"
  echo "                       happy_3node, minority_partition, restart_and_wipe,"
  echo "                       tail_latency_bursts, snapshot_stress"
  echo "  --seed <seed>        Random seed for reproducible runs (overrides scenario seed)"
  echo "  --fuzz <trials>      Number of fuzz trials to run (default: 1)"
  echo "  --fail-fast          Stop on first failure during fuzzing"
  echo "  --help               Show this help"

proc getPresetPath(presetName: string): string =
  ## Get the path for a built-in preset
  case presetName
  of "happy_3node":
    "scenarios/happy_3node.json"
  of "minority_partition":
    "scenarios/minority_partition.json"
  of "restart_and_wipe":
    "scenarios/restart_and_wipe.json"
  of "tail_latency_bursts":
    "scenarios/tail_latency_bursts.json"
  of "snapshot_stress":
    "scenarios/snapshot_stress.json"
  else:
    raise newException(ValueError, fmt"Unknown preset: {presetName}")

proc main() =
  var
    scenarioPath = ""
    preset = ""
    seed: uint64 = 0  # 0 means use seed from scenario file
    fuzzTrials = 1
    failFast = false

  # Parse command line arguments
  var args = commandLineParams()
  var i = 0
  while i < args.len:
    let arg = args[i]
    if arg.startsWith("--"):
      let opt = arg[2..^1]
      case opt
      of "scenario":
        i += 1
        if i >= args.len: echo "Error: --scenario requires a value"; quit(1)
        scenarioPath = args[i]
      of "preset":
        i += 1
        if i >= args.len: echo "Error: --preset requires a value"; quit(1)
        preset = args[i]
      of "seed":
        i += 1
        if i >= args.len: echo "Error: --seed requires a value"; quit(1)
        seed = parseUInt(args[i])
      of "fuzz":
        i += 1
        if i >= args.len: echo "Error: --fuzz requires a value"; quit(1)
        fuzzTrials = parseInt(args[i])
      of "fail-fast":
        failFast = true
      of "help", "h":
        printUsage()
        return
      else:
        echo "Unknown option: --", opt
        printUsage()
        quit(1)
    else:
      echo "Unexpected argument: ", arg
      printUsage()
      quit(1)
    i += 1

  # Validate arguments
  if scenarioPath == "" and preset == "":
    echo "Error: Must specify either --scenario or --preset"
    printUsage()
    quit(1)

  # Get scenario path
  let actualScenarioPath = if scenarioPath != "":
                             scenarioPath
                           else:
                             getPresetPath(preset)

  # Load scenario configuration
  var scenario: ScenarioYaml
  try:
    scenario = loadScenarioFromYaml(actualScenarioPath)
  except ValueError as e:
    echo "Error loading scenario: ", e.msg
    quit(1)

  # Override seed if specified
  if seed != 0:
    scenario.seed = seed

  # Validate scenario
  let validationErrors = validateScenario(scenario)
  if validationErrors.len > 0:
    echo "Scenario validation errors:"
    for error in validationErrors:
      echo "  ", error
    quit(1)

  echo "Starting Raft simulation..."
  echo "Scenario: ", actualScenarioPath
  echo "Seed: ", scenario.seed
  echo "Nodes: ", scenario.cluster.nodes
  echo "Fuzz trials: ", fuzzTrials
  if scenario.stop.max_ms.isSome:
    echo "Max time: ", scenario.stop.max_ms.get(), "ms"
  if scenario.stop.min_commits.isSome:
    echo "Min commits: ", scenario.stop.min_commits.get()

  # Run scenario (with fuzzing support)
  var allPassed = true
  var lastRunner: ScenarioRunner
  for trial in 1..fuzzTrials:
    if fuzzTrials > 1:
      echo fmt"\n--- Trial {trial}/{fuzzTrials} ---"
      # For fuzzing, we could modify the scenario here based on fuzz knobs

    let runner = newScenarioRunner(scenario)
    lastRunner = runner
    let success = runner.run()

    if not success:
      allPassed = false
      if failFast:
        break

  let success = allPassed

  echo ""
  if success:
    echo "✓ Simulation completed successfully"
  else:
    echo "✗ Simulation failed"

  # Generate and print CI summary
  if lastRunner != nil:
    let summary = generateCiSummary(lastRunner.jsonWriter.trace)
    printToStdout(summary)

    # Print some basic statistics from the last runner
    echo "Total time simulated: ", lastRunner.clock.nowMs, "ms"
    echo "Lifecycle events: ", lastRunner.lifecycleEvents.len
    echo fmt"Heartbeats sent: {lastRunner.heartbeatsSent}"
    echo fmt"Heartbeats delivered: {lastRunner.heartbeatsDelivered}"

when isMainModule:
  main()
