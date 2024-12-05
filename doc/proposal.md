Robust eth validator cluster

# ETH nodes

In the Ethereum network, validators play a crucial role in the consensus mechanism, particularly in Ethereum 2.0, which is the network's upgrade from the current proof-of-work (PoW) to proof-of-stake (PoS) consensus algorithm. Validators are responsible for securing the network, validating transactions, and creating new blocks. Each validator is responsible to several duties:

## Block Proposal:
Validators take turns proposing new blocks to be added to the blockchain. The right to propose a block is typically determined by a random or pseudo-random process, and validators are selected to create blocks based on the amount of cryptocurrency they "stake" as collateral.

## Transaction Validation:
Validators validate transactions by checking their legitimacy, ensuring that they adhere to the network's rules and that the sender has sufficient funds to perform the transaction. This helps maintain the integrity of the blockchain.

## Block Verification:
Validators verify the validity of the proposed blocks. This involves checking that the transactions within the block are legitimate, the block adheres to the consensus rules, and the validator has not acted maliciously.

## Consensus Participation:
Validators participate in the consensus process by proposing and attesting to blocks. In Ethereum 2.0's PoS, validators are chosen to propose blocks and then must attest to the validity of other proposed blocks. Consensus is reached when a supermajority of validators agree on a block.

## Staking:
Validators are required to lock up a certain amount of cryptocurrency (ETH) as collateral, known as staking. This collateral serves as an incentive for validators to act honestly, as they risk losing their staked funds if they validate fraudulent transactions or propose malicious blocks.

## Rewards and Penalties:
Validators are rewarded for their honest participation in the network. This includes receiving transaction fees and block rewards. However, validators may also face penalties for malicious behavior, such as attempting to validate conflicting transactions or going offline for extended periods.

## Network Security:
Validators contribute to the overall security of the Ethereum network by participating in the consensus algorithm. PoS is designed to be more energy-efficient than PoW, and validators are chosen to create blocks based on their stake, reducing the risk of centralization.


## Slashing events

To effectively perform its responsibilities, each validator node requires robust hosting infrastructure with ample system resources and reliable network connectivity.
Additionally, to actively engage in the network, each validator possesses a unique BLS key dedicated to signing various messages. The confidentiality of these BLS keys
is very important, as the malicious use of a validator's key to sign deceptive messages could trigger a slashing event. Slashing events entail consequences such as the
partial loss of the validator's stake, resulting in financial losses. Therefore, safeguarding the secrecy of BLS keys is crucial to prevent potential adversarial actions
and uphold the integrity of the validator's participation in the network.

Validators are expected to be online and actively participating in the consensus process. If a validator goes offline for an extended period without proper justification
or fails to propose blocks and attestations when it's their turn, they can be slashed. This encourages validators to maintain a reliable online presence.

To enhance the security and reliability of the node while mitigating potential financial losses for the validator operator, we can address two key challenges:

1. A vulnerability in each validator lies in its reliance on the network or cloud provider. If, for any reason, there is a disruption in network or electricity supply,
the validator may incur downtime penalties. One solution is to establish a cluster of nodes that operate collectively, reducing the impact of downtime. However,
this introduces a security trade-off, as each node within the cluster must share the same private key, thereby increasing the potential attack surface.

2. Building upon the previous point, another potential single point of failure is the BLS private key. If a malicious actor gains access to this key, the entire node
becomes compromised. This underscores the critical importance of safeguarding the BLS key to prevent unauthorized access and maintain the integrity of the node.


## ETH node cluster

Simply deploying multiple Ethereum nodes with identical BLS keys across diverse locations or cloud providers is not a viable strategy. This approach could lead to concurrency
issues, where different nodes generate and submit conflicting messages for the same epoch. Such discrepancies would trigger network slashing penalties, resulting in unintended 
financial losses. Achieving synchronization among nodes in the cluster is imperative to prevent these issues. To ensure cohesive and timely message generation, the cluster requires
a consensus protocol. Assuming cooperative behavior from nodes within the cluster, as they share the same operator, a non-Byzantine proof algorithm like RAFT could be a suitable 
choice for maintaining synchronization. This approach minimizes the risk of intentional malicious actions within the cluster, promoting reliable and secure operation.

While RAFT effectively addresses the synchronization challenge, it necessitates that each node in the cluster possesses the same BLS key. Fortunately, BLS cryptography offers a solution through multi-signature capabilities.
Specifically, Shamir's secret sharing scheme can be employed to generate/regenerate keys that represent fractions (shares) of the original key.
This allows each node in the cluster to hold a key representing 1/n (where n is the total number of shares) of the complete key. Moreover, Shamir's secret sharing facilitates k/n schemes (threashold signatures),
where only some part (k) of all shares (n) needs to be produced to reconstruct the signature of the original key


## RAFT-BLS cluster

Raft functions as a consensus algorithm designed to facilitate agreement on a shared state in distributed systems. In our context, this shared state pertains to the sequence of messages 
to be signed and submitted to the Ethereum network. To achieve this, each Raft node maintains an orderly log where entries are chronologically ordered. When a sufficient number of
nodes in the cluster possess the same entry in the correct position, the entry can be committed. In our case, each entry represents a message slated for signing and submission to
the Ethereum network. Committing an entry signifies that it can be submitted to the network, ensuring redundancy in case the current leader node is unavailable.

However, the immutability of entries in the Raft log poses a challenge when working with Shamir's secret sharing scheme, particularly in generating message signatures. Since
the signature requires shares from other nodes in the network, partially signed messages cannot be added directly to the Raft log. To overcome this, an alternative method for
collecting signature shares from other nodes before adding the message to the log becomes necessary. Although various approaches exist, they introduce delays and increased
network traffic to the protocol.

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




