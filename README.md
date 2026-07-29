# Distributed SQLite over Raft

A replicated relational database system that wraps SQLite and distributes it across a cluster using the Raft consensus protocol. It guarantees strong consistency (linearizability) for writes, leader election, and high availability in the presence of node failures.

## Features & Architecture

*   **Raft Consensus Engine:** Built-in consensus state machine tracking roles (Follower, Candidate, Leader), term epochs, and election timers.
*   **Leader Elections:** Heartbeats and randomized election timeouts (between 150ms and 300ms) triggering parallel vote request cycles.
*   **Log Replication:** Leaders accept writes, append them to local logs, replicate them to worker nodes via `AppendEntries` RPCs, and commit them once a quorum majority is reached.
*   **State Machine Replication:** Composed of a `database/sql` driver mapping committed log SQL write entries (`INSERT`, `UPDATE`, `DELETE`) onto isolated SQLite instances.
*   **Cluster Network Broker:** Registry router brokering RPC communications (`RequestVote`, `AppendEntries`) between nodes without requiring heavy socket ports binding.

## Getting Started

### Prerequisites

*   Go 1.20 or higher.
*   CGO enabled compiler (since the Go SQLite driver compiles C-level source files).

### Run Cluster Simulation

Run the local 3-node cluster simulation:

```bash
go run main.go
```

Output:
```text
==================================================
Starting Replicated SQLite Cluster Verification
==================================================
[Cluster Setup] Promoting node_1 to consensus Leader...
[Node node_1] Starting election for Term 1
[Node node_1] Elected Leader for Term 1
[Transactions] Executing write transaction: Set 'key_user' -> 'gpar377'...
[Consensus] Broadcasting write entry to peers for commit quorum...
[Node node_1] Committed log index up to 0
[node_2] Applying log index 0 to SQLite: INSERT INTO kv (key, value) VALUES ('key_user', 'gpar377');
[node_3] Applying log index 0 to SQLite: INSERT INTO kv (key, value) VALUES ('key_user', 'gpar377');
[node_1] Applying log index 0 to SQLite: INSERT INTO kv (key, value) VALUES ('key_user', 'gpar377');
[Verification] Reading state from node_2 SQLite database...
SUCCESS: Key 'key_user' correctly replicated to node_2 SQLite db!
==================================================
ALL DISTRIBUTED SQLITE VERIFICATIONS COMPLETED! 🎉
==================================================
```
