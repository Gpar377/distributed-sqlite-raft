# Distributed SQLite over Raft
A replicated relational database system that wraps SQLite and distributes it across a cluster using the Raft consensus protocol. It guarantees strong consistency (linearizability) for writes, leader election, and high availability in the presence of node failures.

## Proposed Git Repo Name
`distributed-sqlite-raft`

## Architecture & Scope
*   **Consensus Engine (Raft):** Implementation of Raft consensus protocol:
    *   **Leader Election:** Heartbeats, randomized timeouts, and term state tracking.
    *   **Log Replication:** AppendEntries RPCs, commit indices, and consistency checks.
    *   **Log Compaction:** Snapshots of the database state to truncate the Raft log.
*   **Database Integration:** SQLite as the transactional local database engine (wrapped via dynamic libraries/bindings).
*   **State Machine Replication:** Applying committed Raft log entries (SQL write statements like `INSERT`, `UPDATE`, `DELETE`) sequentially to the local SQLite engine.
*   **RPC Infrastructure:** High-performance gRPC/Protobuf layer handling inter-node cluster communication and client-to-leader redirection.
*   **Linearizable Reads:** Read queries redirected to the leader, executing read index optimizations to avoid stale reads without running full consensus.

## Target Milestones
1. Raft state machine, leader election, and basic RPC architecture setup.
2. Log replication and consensus commit loops.
3. Integration with SQLite engine, replicating writes across nodes.
4. Log compaction via snapshotting SQLite files.
5. Network partition recovery validation and failover testing.
