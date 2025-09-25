import consensus_state_machine
import log
import ssz

proc toBinary*(msg: RaftRpcMessage): seq[byte] = encodeRpcMessage(msg)
proc toBinary*(msg: LogEntry): seq[byte] = encodeLogEntry(msg)
proc fromBinary*(T: type RaftRpcMessage, bytes: openArray[byte]): RaftRpcMessage =
  decodeRpcMessage(bytes)
proc fromBinary*(T: type LogEntry, bytes: openArray[byte]): LogEntry =
  decodeLogEntry(bytes)
