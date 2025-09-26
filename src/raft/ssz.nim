# nim-raft
# Copyright (c) 2023 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

import
  std/options,
  ssz_serialization,
  ./consensus_state_machine,
  ./log,
  ./types

export SszError

const
  voteRequestSszLen = 8 * 3 + 1
  voteReplySszLen = 8 + 1
  snapshotConfigBytes* = 64 * 1024
  maxAppendEntries* = 1024
  maxAppendEntriesBytes* = 4 * 1024 * 1024
  maxRpcPayloadBytes = maxAppendEntriesBytes + snapshotConfigBytes + 1024
  maxLogEntryPayloadBytes* = 1 shl 20
  maxNodeIdBytes = 64

type
  LogEntrySsz* = object
    term*: uint64
    index*: uint64
    kind*: uint8
    commandData*: ByteList[maxLogEntryPayloadBytes]

  AppendRequestSsz* = object
    previousTerm*: uint64
    previousLogIndex*: uint64
    commitIndex*: uint64
    entries*: List[LogEntrySsz, maxAppendEntries]

  AppendReplyRejectedSsz = object
    nonMatchingIndex: uint64
    lastIdx: uint64
    hasConflictTerm: bool
    conflictTerm: uint64
    conflictIndex: uint64

  AppendReplySsz = object
    commitIndex: uint64
    term: uint64
    result: uint8
    acceptedLastNewIndex: uint64
    rejected: AppendReplyRejectedSsz

  RaftRpcMessageEnvelope = object
    currentTerm: uint64
    sender: ByteList[maxNodeIdBytes]
    receiver: ByteList[maxNodeIdBytes]
    kind: uint8
    payload: ByteList[maxRpcPayloadBytes]

  SnapshotSsz = object
    index: uint64
    term: uint64
    config: ByteList[snapshotConfigBytes]

  InstallSnapshotSsz = object
    term: uint64
    snapshot: SnapshotSsz

  SnapshotReplySsz = object
    term: uint64
    success: bool

template toSszError(msg: string): untyped =
  newException(SszError, msg)

proc nodeIdToBytes(node: RaftNodeId): seq[byte] {.raises: [SszError].} =
  if node.id.len > maxNodeIdBytes:
    raise toSszError("node id exceeds SSZ limit")
  result = newSeq[byte](node.id.len)
  for i, ch in node.id:
    result[i] = byte(ch)

proc bytesToNodeId(data: openArray[byte]): RaftNodeId {.raises: [SszError].} =
  if data.len > maxNodeIdBytes:
    raise toSszError("node id exceeds SSZ limit")
  var s = newString(data.len)
  for i in 0..<data.len:
    s[i] = char(data[i])
  RaftNodeId(id: s)

proc encodeSnapshot(snapshot: RaftSnapshot): SnapshotSsz {.raises: [SszError].} =
  # Placeholder: config serialization not yet implemented
  SnapshotSsz(
    index: uint64(snapshot.index),
    term: uint64(snapshot.term),
    config: default(ByteList[snapshotConfigBytes]),
  )

proc decodeSnapshot(container: SnapshotSsz): RaftSnapshot {.raises: [SszError].} =
  RaftSnapshot(
    index: RaftLogIndex(container.index),
    term: RaftNodeTerm(container.term),
    config: RaftConfig(),
  )

proc toSsz*(entry: LogEntry): LogEntrySsz {.raises: [SszError].} =
  result.term = uint64(entry.term)
  result.index = uint64(entry.index)
  result.kind = uint8(entry.kind)
  case entry.kind
  of RaftLogEntryType.rletCommand:
    if entry.command.data.len > maxLogEntryPayloadBytes:
      raise toSszError("command payload exceeds SSZ limit")
    result.commandData = init(ByteList[maxLogEntryPayloadBytes], entry.command.data)
  of RaftLogEntryType.rletEmpty:
    discard
  of RaftLogEntryType.rletConfig:
    raise toSszError("SSZ encoding for config log entries not supported yet")

proc toLogEntry*(entry: LogEntrySsz): LogEntry {.raises: [SszError].} =
  let kind = RaftLogEntryType(entry.kind)
  case kind
  of RaftLogEntryType.rletCommand:
    if entry.commandData.len > maxLogEntryPayloadBytes:
      raise toSszError("command payload exceeds SSZ limit on decode")
    LogEntry(
      term: RaftNodeTerm(entry.term),
      index: RaftLogIndex(entry.index),
      kind: kind,
      command: Command(data: entry.commandData.asSeq),
    )
  of RaftLogEntryType.rletEmpty:
    LogEntry(
      term: RaftNodeTerm(entry.term),
      index: RaftLogIndex(entry.index),
      kind: kind,
    )
  of RaftLogEntryType.rletConfig:
    raise toSszError("SSZ decoding for config log entries not supported yet")

proc encodeLogEntry*(entry: LogEntry): seq[byte] {.raises: [SszError].} =
  let container = entry.toSsz()
  try:
    encode(SSZ, container)
  except SszError as exc:
    raise exc
  except SerializationError as exc:
    raise toSszError(exc.msg)

proc decodeLogEntry*(bytes: openArray[byte]): LogEntry {.raises: [SszError].} =
  let container =
    try:
      decode(SSZ, bytes, LogEntrySsz)
    except SszError as exc:
      raise exc
  except SerializationError as exc:
    raise toSszError(exc.msg)
  container.toLogEntry()

proc encodeInstallSnapshot*(snapshot: RaftInstallSnapshot): seq[byte] {.raises: [SszError].} =
  let container = InstallSnapshotSsz(
    term: uint64(snapshot.term),
    snapshot: encodeSnapshot(snapshot.snapshot),
  )
  try:
    encode(SSZ, container)
  except SszError as exc:
    raise exc
  except SerializationError as exc:
    raise toSszError(exc.msg)

proc decodeInstallSnapshot*(bytes: openArray[byte]): RaftInstallSnapshot {.raises: [SszError].} =
  let container =
    try:
      decode(SSZ, bytes, InstallSnapshotSsz)
    except SszError as exc:
      raise exc
    except SerializationError as exc:
      raise toSszError(exc.msg)
  RaftInstallSnapshot(
    term: RaftNodeTerm(container.term),
    snapshot: decodeSnapshot(container.snapshot),
  )

proc encodeSnapshotReply*(reply: RaftSnapshotReply): seq[byte] {.raises: [SszError].} =
  let container = SnapshotReplySsz(term: uint64(reply.term), success: reply.success)
  try:
    encode(SSZ, container)
  except SszError as exc:
    raise exc
  except SerializationError as exc:
    raise toSszError(exc.msg)

proc decodeSnapshotReply*(bytes: openArray[byte]): RaftSnapshotReply {.raises: [SszError].} =
  let container =
    try:
      decode(SSZ, bytes, SnapshotReplySsz)
    except SszError as exc:
      raise exc
    except SerializationError as exc:
      raise toSszError(exc.msg)
  RaftSnapshotReply(
    term: RaftNodeTerm(container.term),
    success: container.success,
  )

proc encodeRequest(value: RaftRpcVoteRequest): seq[byte] {.raises: [SszError].} =
  var stream = memoryOutput()
  try:
    var writer = SszWriter.init(stream)
    writer.writeValue(uint64(value.currentTerm))
    writer.writeValue(uint64(value.lastLogIndex))
    writer.writeValue(uint64(value.lastLogTerm))
    writer.writeValue(value.force)
    result = stream.getOutput(seq[byte])
  except IOError as exc:
    raise toSszError(exc.msg)
  except SerializationError as exc:
    raise toSszError(exc.msg)

proc encodeReply(value: RaftRpcVoteReply): seq[byte] {.raises: [SszError].} =
  var stream = memoryOutput()
  try:
    var writer = SszWriter.init(stream)
    writer.writeValue(uint64(value.currentTerm))
    writer.writeValue(value.voteGranted)
    result = stream.getOutput(seq[byte])
  except IOError as exc:
    raise toSszError(exc.msg)
  except SerializationError as exc:
    raise toSszError(exc.msg)

proc encodeAppendRequest(request: RaftRpcAppendRequest): seq[byte] {.raises: [SszError].} =
  if request.entries.len > maxAppendEntries:
    raise toSszError("AppendEntries entries length exceeds limit")
  var payloadBytes = 0
  var entryContainers: seq[LogEntrySsz] = @[]
  for entry in request.entries:
    case entry.kind:
      of RaftLogEntryType.rletConfig:
        raise toSszError("SSZ encoding for config log entries not supported yet")
      of RaftLogEntryType.rletEmpty:
        discard
      of RaftLogEntryType.rletCommand:
        payloadBytes += entry.command.data.len
    if payloadBytes > maxAppendEntriesBytes:
      raise toSszError("AppendEntries payload exceeds limit")
    entryContainers.add(entry.toSsz())

  let container = AppendRequestSsz(
    previousTerm: uint64(request.previousTerm),
    previousLogIndex: uint64(request.previousLogIndex),
    commitIndex: uint64(request.commitIndex),
    entries: init(List[LogEntrySsz, maxAppendEntries], entryContainers),
  )

  try:
    encode(SSZ, container)
  except SszError as exc:
    raise exc
  except SerializationError as exc:
    raise toSszError(exc.msg)

proc encodeAppendReply(reply: RaftRpcAppendReply): seq[byte] {.raises: [SszError].} =
  var container = AppendReplySsz(
    commitIndex: uint64(reply.commitIndex),
    term: uint64(reply.term),
    result: uint8(reply.result),
    acceptedLastNewIndex: 0,
    rejected: AppendReplyRejectedSsz(),
  )

  case reply.result
  of RaftRpcCode.Accepted:
    container.acceptedLastNewIndex = uint64(reply.accepted.lastNewIndex)
  of RaftRpcCode.Rejected:
    container.rejected = AppendReplyRejectedSsz(
      nonMatchingIndex: uint64(reply.rejected.nonMatchingIndex),
      lastIdx: uint64(reply.rejected.lastIdx),
      hasConflictTerm: reply.rejected.conflictTerm.isSome,
      conflictTerm: (
        if reply.rejected.conflictTerm.isSome:
          uint64(reply.rejected.conflictTerm.get())
        else:
          0'u64
      ),
      conflictIndex: uint64(reply.rejected.conflictIndex),
    )

  try:
    encode(SSZ, container)
  except SszError as exc:
    raise exc
  except SerializationError as exc:
    raise toSszError(exc.msg)

proc encodeRpcMessage*(msg: RaftRpcMessage): seq[byte] {.raises: [SszError].} =
  var payload: seq[byte]
  case msg.kind
  of RaftRpcMessageType.VoteRequest:
    payload = encodeRequest(msg.voteRequest)
  of RaftRpcMessageType.VoteReply:
    payload = encodeReply(msg.voteReply)
  of RaftRpcMessageType.AppendRequest:
    payload = encodeAppendRequest(msg.appendRequest)
  of RaftRpcMessageType.AppendReply:
    payload = encodeAppendReply(msg.appendReply)
  of RaftRpcMessageType.InstallSnapshot:
    payload = encodeInstallSnapshot(msg.installSnapshot)
  of RaftRpcMessageType.SnapshotReply:
    payload = encodeSnapshotReply(msg.snapshotReply)

  if payload.len > maxRpcPayloadBytes:
    raise toSszError("RPC payload exceeds SSZ limit")

  let senderBytes = nodeIdToBytes(msg.sender)
  let receiverBytes = nodeIdToBytes(msg.receiver)

  let envelope = RaftRpcMessageEnvelope(
    currentTerm: uint64(msg.currentTerm),
    sender: init(ByteList[maxNodeIdBytes], senderBytes),
    receiver: init(ByteList[maxNodeIdBytes], receiverBytes),
    kind: uint8(msg.kind),
    payload: init(ByteList[maxRpcPayloadBytes], payload),
  )

  try:
    encode(SSZ, envelope)
  except SszError as exc:
    raise exc
  except SerializationError as exc:
    raise toSszError(exc.msg)

proc decodeRequest(bytes: openArray[byte]): RaftRpcVoteRequest {.raises: [SszError].} =
  if bytes.len != voteRequestSszLen:
    raise toSszError("invalid RequestVote SSZ length")

  var stream = unsafeMemoryInput(bytes)
  try:
    var reader = SszReader.init(stream)
    var currentTerm: uint64
    reader.readValue(currentTerm)
    var lastLogIndex: uint64
    reader.readValue(lastLogIndex)
    var lastLogTerm: uint64
    reader.readValue(lastLogTerm)
    var force: bool
    reader.readValue(force)

    if stream.readable(1):
      raise toSszError("unexpected trailing bytes in RequestVote SSZ payload")

    RaftRpcVoteRequest(
      currentTerm: RaftNodeTerm(currentTerm),
      lastLogIndex: RaftLogIndex(lastLogIndex),
      lastLogTerm: RaftNodeTerm(lastLogTerm),
      force: force,
    )
  except IOError as exc:
    raise toSszError(exc.msg)
  except SerializationError as exc:
    raise toSszError(exc.msg)

proc decodeAppendRequest(bytes: openArray[byte]): RaftRpcAppendRequest {.raises: [SszError].} =
  let container =
    try:
      decode(SSZ, bytes, AppendRequestSsz)
    except SszError as exc:
      raise exc
    except SerializationError as exc:
      raise toSszError(exc.msg)

  if container.entries.len > maxAppendEntries:
    raise toSszError("decoded AppendEntries exceeds entry limit")

  var entries: seq[LogEntry] = @[]
  var payloadBytes = 0
  for entry in container.entries.asSeq:
    payloadBytes += entry.commandData.len
    if payloadBytes > maxAppendEntriesBytes:
      raise toSszError("decoded AppendEntries payload exceeds limit")
    entries.add(entry.toLogEntry())

  RaftRpcAppendRequest(
    previousTerm: RaftNodeTerm(container.previousTerm),
    previousLogIndex: RaftLogIndex(container.previousLogIndex),
    commitIndex: RaftLogIndex(container.commitIndex),
    entries: entries,
  )

proc decodeAppendReply(bytes: openArray[byte]): RaftRpcAppendReply {.raises: [SszError].} =
  let container =
    try:
      decode(SSZ, bytes, AppendReplySsz)
    except SszError as exc:
      raise exc
    except SerializationError as exc:
      raise toSszError(exc.msg)

  let resultCode =
    try:
      RaftRpcCode(container.result)
    except ValueError:
      raise toSszError("invalid AppendEntries reply result code")

  case resultCode
  of RaftRpcCode.Accepted:
    RaftRpcAppendReply(
      commitIndex: RaftLogIndex(container.commitIndex),
      term: RaftNodeTerm(container.term),
      result: resultCode,
      accepted: RaftRpcAppendReplyAccepted(
        lastNewIndex: RaftLogIndex(container.acceptedLastNewIndex),
      ),
    )
  of RaftRpcCode.Rejected:
    let conflictTerm =
      if container.rejected.hasConflictTerm:
        some(RaftNodeTerm(container.rejected.conflictTerm))
      else:
        none(RaftNodeTerm)

    RaftRpcAppendReply(
      commitIndex: RaftLogIndex(container.commitIndex),
      term: RaftNodeTerm(container.term),
      result: resultCode,
      rejected: RaftRpcAppendReplyRejected(
        nonMatchingIndex: RaftLogIndex(container.rejected.nonMatchingIndex),
        lastIdx: RaftLogIndex(container.rejected.lastIdx),
        conflictTerm: conflictTerm,
        conflictIndex: RaftLogIndex(container.rejected.conflictIndex),
      ),
    )

proc decodeReply(bytes: openArray[byte]): RaftRpcVoteReply {.raises: [SszError].} =
  if bytes.len != voteReplySszLen:
    raise toSszError("invalid RequestVote reply SSZ length")

  var stream = unsafeMemoryInput(bytes)
  try:
    var reader = SszReader.init(stream)
    var currentTerm: uint64
    reader.readValue(currentTerm)
    var voteGranted: bool
    reader.readValue(voteGranted)

    if stream.readable(1):
      raise toSszError("unexpected trailing bytes in RequestVote reply payload")

    RaftRpcVoteReply(
      currentTerm: RaftNodeTerm(currentTerm),
      voteGranted: voteGranted,
    )
  except IOError as exc:
    raise toSszError(exc.msg)
  except SerializationError as exc:
    raise toSszError(exc.msg)

proc decodeRpcMessage*(bytes: openArray[byte]): RaftRpcMessage {.raises: [SszError].} =
  let envelope =
    try:
      decode(SSZ, bytes, RaftRpcMessageEnvelope)
    except SszError as exc:
      raise exc
    except SerializationError as exc:
      raise toSszError(exc.msg)

  let kind =
    try:
      RaftRpcMessageType(envelope.kind)
    except ValueError:
      raise toSszError("invalid RaftRpcMessage kind")

  let sender = bytesToNodeId(envelope.sender.asSeq)
  let receiver = bytesToNodeId(envelope.receiver.asSeq)

  case kind
  of RaftRpcMessageType.VoteRequest:
    RaftRpcMessage(
      currentTerm: RaftNodeTerm(envelope.currentTerm),
      sender: sender,
      receiver: receiver,
      kind: kind,
      voteRequest: decodeRequest(envelope.payload.asSeq),
    )
  of RaftRpcMessageType.VoteReply:
    RaftRpcMessage(
      currentTerm: RaftNodeTerm(envelope.currentTerm),
      sender: sender,
      receiver: receiver,
      kind: kind,
      voteReply: decodeReply(envelope.payload.asSeq),
    )
  of RaftRpcMessageType.AppendRequest:
    RaftRpcMessage(
      currentTerm: RaftNodeTerm(envelope.currentTerm),
      sender: sender,
      receiver: receiver,
      kind: kind,
      appendRequest: decodeAppendRequest(envelope.payload.asSeq),
    )
  of RaftRpcMessageType.AppendReply:
    RaftRpcMessage(
      currentTerm: RaftNodeTerm(envelope.currentTerm),
      sender: sender,
      receiver: receiver,
      kind: kind,
      appendReply: decodeAppendReply(envelope.payload.asSeq),
    )
  of RaftRpcMessageType.InstallSnapshot:
    RaftRpcMessage(
      currentTerm: RaftNodeTerm(envelope.currentTerm),
      sender: sender,
      receiver: receiver,
      kind: kind,
      installSnapshot: decodeInstallSnapshot(envelope.payload.asSeq),
    )
  of RaftRpcMessageType.SnapshotReply:
    RaftRpcMessage(
      currentTerm: RaftNodeTerm(envelope.currentTerm),
      sender: sender,
      receiver: receiver,
      kind: kind,
      snapshotReply: decodeSnapshotReply(envelope.payload.asSeq),
    )

proc toSsz*(request: RaftRpcVoteRequest): seq[byte] {.raises: [SszError].} =
  encodeRequest(request)

proc fromSsz*(
    T: type RaftRpcVoteRequest, bytes: openArray[byte]
): RaftRpcVoteRequest {.raises: [SszError].} =
  decodeRequest(bytes)

proc toSsz*(reply: RaftRpcVoteReply): seq[byte] {.raises: [SszError].} =
  encodeReply(reply)

proc fromSsz*(
    T: type RaftRpcVoteReply, bytes: openArray[byte]
): RaftRpcVoteReply {.raises: [SszError].} =
  decodeReply(bytes)

proc toSsz*(request: RaftRpcAppendRequest): seq[byte] {.raises: [SszError].} =
  encodeAppendRequest(request)

proc fromSsz*(
    T: type RaftRpcAppendRequest, bytes: openArray[byte]
): RaftRpcAppendRequest {.raises: [SszError].} =
  decodeAppendRequest(bytes)

proc toSsz*(reply: RaftRpcAppendReply): seq[byte] {.raises: [SszError].} =
  encodeAppendReply(reply)

proc fromSsz*(
    T: type RaftRpcAppendReply, bytes: openArray[byte]
): RaftRpcAppendReply {.raises: [SszError].} =
  decodeAppendReply(bytes)
