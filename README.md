# nim-raft

This repository provides an implementation of the [Raft consensus algorithm](https://raft.github.io/) that is well suited for application-specific customizations of the protocol.

We plan to leverage this implementation to create a highly-efficient setup for operating a redundant set of Nimbus beacon nodes and/or validator clients that rely on BLS threshold signatures to achieve improved resilience and security. Further details can be found in our roadmap here:

https://github.com/status-im/nimbus-eth2/issues/3416

Our implementation is heavily inspired by the Raft implementation in ScyllaDB 

https://github.com/scylladb/scylladb/tree/master/raft

## Design goals

The main goal is to separate implementation of the raft state machine from the other implementation details such as storage, network communication, etc.
In order to achieve this, we want to keep the state machine absolutely deterministic. Aspects such as networking, logging, acquiring current time, random number generation, disc operation, etc are delegated to the hosting application through the state machine interface. This ensures better testability and easier integration in arbitrary host application architectures.

### Wire format

All Raft RPCs and log entries are serialised with [SimpleSerialize (SSZ)](https://ethereum.github.io/consensus-specs/ssz/simple-serialize/)

## BLS-Raft

Besides the base Raft algorithm, this repository implements an extension of Raft that effectively authenticates all Raft messages with BLS signatures of the participants. Through the use of [BLS threshold signing](https://notes.status.im/BLS-Threshold-Signing#), this augments the act of reaching consensus with the creation of a corresponding group BLS signature in cluster configuration such as 2 out of 3, 3 out of 5, etc.

In the context of Ethereum distributed validating, the consensus represents the sequence of validator messages that are signed and submitted to the Ethereum network. Examined through the familiar context of using Raft for database replication, you can think of the proposed approach as a replication mechanism for the slashing protection database that is typically maintained by an Ethereum client.

### How does it work?

The immutability of entries in the Raft log poses a challenge when working with BLS threshold signing, particularly in generating message signatures.
Since the signature requires shares from other nodes in the network, partially signed messages cannot be added directly to the Raft log.
To overcome this, an alternative method for collecting signature shares from other nodes before adding the message to the log becomes necessary.
Although various approaches exist, they introduce delays and increased network traffic to the protocol.

To address this challenge, we propose utilizing Raft exclusively for replicating messages and introduce a second layer atop Raft to ensure signature collection. The process unfolds as follows:

1. The leader node generates a new message.
2. The leader signs the message.
3. The message and its signature are saved in the leader's state.
4. The message is added to the Raft log.
5. The Raft replication message is encapsulated within a signature request message.
6. Follower nodes sign each message, retaining the signatures in their states.
7. Upon creating a replication response message, the follower node attaches signatures for each uncommitted message in its log.
8. When the leader receives the response, it adds new signatures to its state. Once Raft indicates the commitment of a message, the leader reconstructs the BLS signature and submits the signed message to the network.
9. If the leader is down after successful replication but before submitting the message to the network, a new leader is elected. This new leader initiates replication
messages to gather information about the process, collect signatures for uncommitted entries, and prune messages not replicated to the threshold.

Since this approach doesn't introduce any new messages in the Raft protocol, but merely adds additional data to each message, it retains the round-trip efficiency of the base algorithm.

## Raft Simulation Framework

This repository includes a comprehensive simulation and testing framework for the Raft consensus algorithm. The framework allows you to:

- Test Raft implementations under various failure scenarios
- Inject network faults (latency, drops, partitions)
- Simulate storage failures and durability modes
- Verify safety and liveness properties
- Run deterministic, reproducible simulations

### Quick Start

```bash
# Run unit tests
make test

# Build and run a basic simulation
make sim

# Run a quick simulation (1 second)
make sim-quick

# Get help on available commands
make help
```

### Available Commands

- `make help` - Show all available commands
- `make build` - Build the simulation executable
- `make test` - Run unit tests
- `make sim` - Run default simulation (happy 3-node scenario)
- `make sim-quick` - Run very quick simulation for testing
- `make sim-happy-long` - Run longer happy scenario
- `make sim-custom` - Run with custom parameters
- `make clean` - Clean build artifacts
- `make info` - Show project information

### Custom Simulations

You can run simulations with custom parameters:

```bash
# Using make variables
make sim-custom SCENARIO=scenarios/happy_3node.yaml SEED=123 MAX_TIME=5000

# Or set environment variables
export SCENARIO=scenarios/happy_3node.yaml
export SEED=123
export MAX_TIME=5000
make sim-custom
```

### Architecture

The simulation framework consists of:

- **Core Components**: Deterministic clock, RNG, event scheduling
- **Network Simulation**: Fault injection, partitions, message delays
- **Storage Simulation**: Durability modes (Durable/Async/Torn), crash recovery
- **Raft Interface**: Pluggable Raft implementations via abstract interface
- **Invariant Checking**: Automatic verification of Raft safety properties

### Integration

To integrate your Raft implementation with the simulation framework:

1. Implement the `RaftNode` interface defined in `sim/raft/raft_interface.nim`
2. Create a `NodeHost` with your Raft implementation
3. Run simulations to test under various failure scenarios


## Contributing

### Style Guide

Please adhere to the [Status Nim Style Guide](https://status-im.github.io/nim-style-guide/) when making contributions.

### Running Tests

Make sure that all tests are passing when executed with `nimble test`.
