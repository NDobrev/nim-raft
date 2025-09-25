import std/[json, strformat, options]

import ../src/raft/ssz
import ../src/raft/log
import ../src/raft/consensus_state_machine
import ../src/raft/types

proc configToJson(cfg: RaftConfig): JsonNode =
  result = newJObject()
  let currentMembers = newJArray()
  for member in cfg.currentSet:
    currentMembers.add(%member.id)
  result["currentSet"] = currentMembers

  let previousMembers = newJArray()
  for member in cfg.previousSet:
    previousMembers.add(%member.id)
  result["previousSet"] = previousMembers

proc logEntryToJson(entry: LogEntry): JsonNode =
  result = newJObject()
  result["term"] = %entry.term
  result["index"] = %entry.index
  result["kind"] = %entry.kind.int
  case entry.kind
  of RaftLogEntryType.rletCommand:
    result["command"] = %entry.command.data
  of RaftLogEntryType.rletEmpty:
    discard
  of RaftLogEntryType.rletConfig:
    result["configMembers"] = configToJson(entry.config)

proc appendRequestToJson(req: RaftRpcAppendRequest): JsonNode =
  result = newJObject()
  result["previousTerm"] = %req.previousTerm
  result["previousLogIndex"] = %req.previousLogIndex
  result["commitIndex"] = %req.commitIndex
  let entries = newJArray()
  for entry in req.entries:
    entries.add(logEntryToJson(entry))
  result["entries"] = entries

proc voteRequestToJson(req: RaftRpcVoteRequest): JsonNode =
  result = newJObject()
  result["currentTerm"] = %req.currentTerm
  result["lastLogIndex"] = %req.lastLogIndex
  result["lastLogTerm"] = %req.lastLogTerm
  result["force"] = %req.force

proc voteReplyToJson(rep: RaftRpcVoteReply): JsonNode =
  result = newJObject()
  result["currentTerm"] = %rep.currentTerm
  result["voteGranted"] = %rep.voteGranted

proc appendReplyToJson(rep: RaftRpcAppendReply): JsonNode =
  result = newJObject()
  result["commitIndex"] = %rep.commitIndex
  result["term"] = %rep.term
  result["result"] = %rep.result.int
  case rep.result
  of RaftRpcCode.Accepted:
    result["lastNewIndex"] = %rep.accepted.lastNewIndex
  of RaftRpcCode.Rejected:
    result["nonMatchingIndex"] = %rep.rejected.nonMatchingIndex
    result["lastIdx"] = %rep.rejected.lastIdx
    if rep.rejected.conflictTerm.isSome:
      result["conflictTerm"] = %rep.rejected.conflictTerm.get()
    result["conflictIndex"] = %rep.rejected.conflictIndex

proc installSnapshotToJson(req: RaftInstallSnapshot): JsonNode =
  result = newJObject()
  result["term"] = %req.term
  result["snapshotIndex"] = %req.snapshot.index
  result["snapshotTerm"] = %req.snapshot.term
  result["config"] = configToJson(req.snapshot.config)

proc snapshotReplyToJson(rep: RaftSnapshotReply): JsonNode =
  result = newJObject()
  result["term"] = %rep.term
  result["success"] = %rep.success

proc rpcMessageToJson(msg: RaftRpcMessage): JsonNode =
  result = newJObject()
  result["currentTerm"] = %msg.currentTerm
  result["sender"] = %msg.sender.id
  result["receiver"] = %msg.receiver.id
  result["kind"] = %msg.kind.int
  case msg.kind
  of RaftRpcMessageType.VoteRequest:
    result["voteRequest"] = voteRequestToJson(msg.voteRequest)
  of RaftRpcMessageType.VoteReply:
    result["voteReply"] = voteReplyToJson(msg.voteReply)
  of RaftRpcMessageType.AppendRequest:
    result["appendRequest"] = appendRequestToJson(msg.appendRequest)
  of RaftRpcMessageType.AppendReply:
    result["appendReply"] = appendReplyToJson(msg.appendReply)
  of RaftRpcMessageType.InstallSnapshot:
    result["installSnapshot"] = installSnapshotToJson(msg.installSnapshot)
  of RaftRpcMessageType.SnapshotReply:
    result["snapshotReply"] = snapshotReplyToJson(msg.snapshotReply)

proc sampleLogEntry(): LogEntry =
  var data = newSeq[byte](64)
  for i in 0..<data.len:
    data[i] = byte(i)
  LogEntry(
    term: RaftNodeTerm(42),
    index: RaftLogIndex(100),
    kind: RaftLogEntryType.rletCommand,
    command: Command(data: data)
  )

proc sampleConfig(): RaftConfig =
  RaftConfig(
    currentSet: @[RaftNodeId(id: "node-01"), RaftNodeId(id: "node-02"), RaftNodeId(id: "node-03")],
    previousSet: @[RaftNodeId(id: "node-00")],
  )

proc sampleAppendRequest(): RaftRpcAppendRequest =
  var entries: seq[LogEntry] = @[]
  for i in 0..3:
    var entry = sampleLogEntry()
    entry.index = RaftLogIndex(200 + i)
    entry.command.data.setLen(128 + i)
    for j in 0..<entry.command.data.len:
      entry.command.data[j] = byte((i*17 + j) mod 256)
    entries.add(entry)
  RaftRpcAppendRequest(
    previousTerm: RaftNodeTerm(41),
    previousLogIndex: RaftLogIndex(150),
    commitIndex: RaftLogIndex(180),
    entries: entries,
  )

proc sampleVoteRequest(): RaftRpcVoteRequest =
  RaftRpcVoteRequest(
    currentTerm: RaftNodeTerm(56),
    lastLogIndex: RaftLogIndex(320),
    lastLogTerm: RaftNodeTerm(55),
    force: false,
  )

proc sampleVoteReply(granted = true): RaftRpcVoteReply =
  RaftRpcVoteReply(
    currentTerm: RaftNodeTerm(56),
    voteGranted: granted,
  )

proc sampleAppendReplyAccepted(): RaftRpcAppendReply =
  RaftRpcAppendReply(
    commitIndex: RaftLogIndex(220),
    term: RaftNodeTerm(55),
    result: RaftRpcCode.Accepted,
    accepted: RaftRpcAppendReplyAccepted(lastNewIndex: RaftLogIndex(224)),
  )

proc sampleAppendReplyRejected(): RaftRpcAppendReply =
  RaftRpcAppendReply(
    commitIndex: RaftLogIndex(210),
    term: RaftNodeTerm(55),
    result: RaftRpcCode.Rejected,
    rejected: RaftRpcAppendReplyRejected(
      nonMatchingIndex: RaftLogIndex(199),
      lastIdx: RaftLogIndex(215),
      conflictTerm: some(RaftNodeTerm(53)),
      conflictIndex: RaftLogIndex(198),
    ),
  )

proc sampleSnapshot(): RaftSnapshot =
  RaftSnapshot(
    index: RaftLogIndex(1024),
    term: RaftNodeTerm(60),
    config: sampleConfig(),
  )

proc sampleInstallSnapshot(): RaftInstallSnapshot =
  RaftInstallSnapshot(
    term: RaftNodeTerm(61),
    snapshot: sampleSnapshot(),
  )

proc sampleSnapshotReply(success = true): RaftSnapshotReply =
  RaftSnapshotReply(
    term: RaftNodeTerm(61),
    success: success,
  )

proc sampleRpcMessage(kind: RaftRpcMessageType): RaftRpcMessage =
  case kind
  of RaftRpcMessageType.VoteRequest:
    RaftRpcMessage(
      currentTerm: RaftNodeTerm(62),
      sender: RaftNodeId(id: "node-04"),
      receiver: RaftNodeId(id: "node-05"),
      kind: kind,
      voteRequest: sampleVoteRequest(),
    )
  of RaftRpcMessageType.VoteReply:
    RaftRpcMessage(
      currentTerm: RaftNodeTerm(62),
      sender: RaftNodeId(id: "node-04"),
      receiver: RaftNodeId(id: "node-05"),
      kind: kind,
      voteReply: sampleVoteReply(),
    )
  of RaftRpcMessageType.AppendRequest:
    RaftRpcMessage(
      currentTerm: RaftNodeTerm(62),
      sender: RaftNodeId(id: "node-04"),
      receiver: RaftNodeId(id: "node-05"),
      kind: kind,
      appendRequest: sampleAppendRequest(),
    )
  of RaftRpcMessageType.AppendReply:
    RaftRpcMessage(
      currentTerm: RaftNodeTerm(62),
      sender: RaftNodeId(id: "node-04"),
      receiver: RaftNodeId(id: "node-05"),
      kind: kind,
      appendReply: sampleAppendReplyAccepted(),
    )
  of RaftRpcMessageType.InstallSnapshot:
    RaftRpcMessage(
      currentTerm: RaftNodeTerm(62),
      sender: RaftNodeId(id: "node-04"),
      receiver: RaftNodeId(id: "node-05"),
      kind: kind,
      installSnapshot: sampleInstallSnapshot(),
    )
  of RaftRpcMessageType.SnapshotReply:
    RaftRpcMessage(
      currentTerm: RaftNodeTerm(62),
      sender: RaftNodeId(id: "node-04"),
      receiver: RaftNodeId(id: "node-05"),
      kind: kind,
      snapshotReply: sampleSnapshotReply(),
    )

proc printComparison(name: string, jsonVal: JsonNode, sszBytes: openArray[byte]) =
  let jsonBytes = $jsonVal
  echo fmt"{name} JSON bytes: {jsonBytes.len}"
  echo fmt"{name} SSZ  bytes: {sszBytes.len}"
  let reduction = (1 - (sszBytes.len.float / jsonBytes.len.float)) * 100
  echo fmt"SSZ saves {reduction:.2f}% compared to JSON"
  echo "---"

when isMainModule:
  let logEntry = sampleLogEntry()
  printComparison("LogEntry", logEntryToJson(logEntry), encodeLogEntry(logEntry))

  let appendReq = sampleAppendRequest()
  printComparison("AppendRequest", appendRequestToJson(appendReq), toSsz(appendReq))

  let voteReq = sampleVoteRequest()
  printComparison("VoteRequest", voteRequestToJson(voteReq), toSsz(voteReq))

  let voteRep = sampleVoteReply()
  printComparison("VoteReply", voteReplyToJson(voteRep), toSsz(voteRep))

  let appendAccepted = sampleAppendReplyAccepted()
  printComparison("AppendReplyAccepted", appendReplyToJson(appendAccepted), toSsz(appendAccepted))

  let appendRejected = sampleAppendReplyRejected()
  printComparison("AppendReplyRejected", appendReplyToJson(appendRejected), toSsz(appendRejected))

  let installSnapshot = sampleInstallSnapshot()
  printComparison("InstallSnapshot", installSnapshotToJson(installSnapshot), encodeInstallSnapshot(installSnapshot))

  let snapshotRep = sampleSnapshotReply()
  printComparison("SnapshotReply", snapshotReplyToJson(snapshotRep), encodeSnapshotReply(snapshotRep))

  for kind in [
    RaftRpcMessageType.VoteRequest,
    RaftRpcMessageType.AppendRequest,
    RaftRpcMessageType.AppendReply,
    RaftRpcMessageType.InstallSnapshot,
  ]:
    let rpcMsg = sampleRpcMessage(kind)
    printComparison(
      fmt"RaftRpcMessage({kind})",
      rpcMessageToJson(rpcMsg),
      encodeRpcMessage(rpcMsg),
    )
