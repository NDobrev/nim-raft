## CI Summary Generator - Machine-readable summary for CI systems
##
## Generates concise PASS/FAIL summaries with failure details and artifact paths
## for integration with CI/CD systems.

import std/json
import std/strformat
import std/options

import json_writer
type
  CiSummary* = object
    status*: string  # "PASS" or "FAIL"
    seed*: uint64
    totalTimeMs*: int64
    firstFailure*: Option[string]
    violationCount*: int
    jsonArtifact*: string
    htmlArtifact*: string
    details*: string

proc generateCiSummary*(trace: JsonTrace): CiSummary =
  ## Generate CI summary from a trace
  let firstViolation = if trace.invariantViolations.len > 0:
                         some(trace.invariantViolations[0].description)
                       else:
                         none(string)

  let status = if trace.success: "PASS" else: "FAIL"

  let totalMessages = if trace.messageStats.totalMessages > 0: trace.messageStats.totalMessages else: 0
  let networkDropped = if trace.finalStats.hasKey("networkStats") and trace.finalStats["networkStats"].hasKey("dropped"): trace.finalStats["networkStats"]["dropped"].getInt() else: 0
  let networkDelivered = if trace.finalStats.hasKey("networkStats") and trace.finalStats["networkStats"].hasKey("delivered"): trace.finalStats["networkStats"]["delivered"].getInt() else: 0

  let details = fmt"Seed: {trace.seed}, Time: {trace.totalTicks}ms, Violations: {trace.invariantViolations.len}, Messages: {totalMessages}, Dropped: {networkDropped}, Delivered: {networkDelivered}"

  CiSummary(
    status: status,
    seed: trace.seed,
    totalTimeMs: trace.totalTicks,
    firstFailure: firstViolation,
    violationCount: trace.invariantViolations.len,
    jsonArtifact: trace.config.artifacts.json,
    htmlArtifact: trace.config.artifacts.html,
    details: details
  )

proc toJson*(summary: CiSummary): JsonNode =
  ## Convert summary to JSON
  %summary

proc toString*(summary: CiSummary): string =
  ## Convert summary to human-readable string
  let failureInfo = if summary.firstFailure.isSome:
                      fmt", First failure: {summary.firstFailure.get}"
                    else:
                      ""
  fmt"{summary.status}: {summary.details}{failureInfo}"

proc toCiFormat*(summary: CiSummary): string =
  ## Convert to CI-friendly format (single line)
  let failurePart = if summary.firstFailure.isSome:
                      fmt" | first_failure='{summary.firstFailure.get}'"
                    else:
                      ""
  fmt"RAFT_TEST={summary.status} seed={summary.seed} time_ms={summary.totalTimeMs} violations={summary.violationCount}{failurePart} json='{summary.jsonArtifact}' html='{summary.htmlArtifact}'"

proc writeToFile*(summary: CiSummary, path: string) =
  ## Write summary to a file
  writeFile(path, summary.toCiFormat() & "\n")

proc printToStdout*(summary: CiSummary) =
  ## Print summary to stdout (for CI systems)
  echo summary.toCiFormat()
