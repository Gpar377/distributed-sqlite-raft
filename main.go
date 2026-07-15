package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/Gpar377/distributed-sqlite-raft/raft"
)

// ReplicatedStateMachine coordinator bringing raft consensus and sqlite engine together
type ReplicatedStateMachine struct {
	nodeID   string
	raftNode *raft.Node
	sqliteDb *raft.Database
	shutdown chan bool
}

func NewReplicatedStateMachine(nodeID string, address string, peers map[string]string, dbPath string) (*ReplicatedStateMachine, error) {
	db, err := raft.NewDatabase(dbPath)
	if err != nil {
		return nil, err
	}

	rNode := raft.NewNode(nodeID, address, peers)

	return &ReplicatedStateMachine{
		nodeID:   nodeID,
		raftNode: rNode,
		sqliteDb: db,
		shutdown: make(chan bool),
	}, nil
}

func (r *ReplicatedStateMachine) Start() {
	// Initialize test database schema table
	err := r.sqliteDb.ExecuteWrite("CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT);")
	if err != nil {
		log.Fatalf("[%s] Schema initialization failed: %v", r.nodeID, err)
	}

	go r.runMainLoop()
}

func (r *ReplicatedStateMachine) Close() {
	close(r.shutdown)
	r.sqliteDb.Close()
}

// Write transactional query to leader (which replicates command across peers)
func (r *ReplicatedStateMachine) ExecuteWriteCommand(command string) bool {
	// Only leader accepts writes directly
	if r.raftNode.GetRole() != raft.Leader {
		return false
	}

	r.raftNode.Propose(command)
	return true
}

func (r *ReplicatedStateMachine) runMainLoop() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	// Randomized election timeout between 150ms and 300ms
	electionTimeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
	lastHeartbeat := time.Now()

	for {
		select {
		case <-r.shutdown:
			return
		case <-r.raftNode.GetHeartbeatChan():
			lastHeartbeat = time.Now()
		case <-ticker.C:
			// Check election timeout bounds for followers/candidates
			if r.raftNode.GetRole() != raft.Leader {
				if time.Since(lastHeartbeat) > electionTimeout {
					log.Printf("[%s] Election timeout elapsed. Triggering election...", r.nodeID)
					r.raftNode.StartElection()
					// Reset timeout for candidates
					lastHeartbeat = time.Now()
					electionTimeout = time.Duration(150+rand.Intn(150)) * time.Millisecond
				}
			}

			// Apply committed consensus logs to the SQLite state machine
			r.applyCommittedEntries()
		}
	}
}

func (r *ReplicatedStateMachine) applyCommittedEntries() {
	commitIdx := r.raftNode.GetCommitIndex()
	lastApplied := r.raftNode.GetLastApplied()

	if commitIdx > lastApplied {
		logSlice := r.raftNode.GetLog()
		for i := lastApplied + 1; i <= commitIdx; i++ {
			cmd := logSlice[i].Command
			log.Printf("[%s] Applying log index %d to SQLite: %s", r.nodeID, i, cmd)
			err := r.sqliteDb.ExecuteWrite(cmd)
			if err != nil {
				log.Printf("[%s] State machine apply error on query (%s): %v", r.nodeID, cmd, err)
			}
			r.raftNode.SetLastApplied(i)
		}
}

func (r *ReplicatedStateMachine) GetDatabase() *raft.Database {
	return r.sqliteDb
}

func main() {
	log.Println("==================================================")
	log.Println("Starting Replicated SQLite Cluster Verification")
	log.Println("==================================================")

	// Define addresses and nodes
	peers := map[string]string{
		"node_1": "127.0.0.1:9001",
		"node_2": "127.0.0.1:9002",
		"node_3": "127.0.0.1:9003",
	}

	// Initialize 3 nodes locally with separate SQLite databases
	rsm1, err := NewReplicatedStateMachine("node_1", "127.0.0.1:9001", peers, "./node1.db")
	if err != nil {
		log.Fatalf("Failed to initialize node 1: %v", err)
	}
	defer rsm1.Close()
	defer os.Remove("./node1.db")

	rsm2, err := NewReplicatedStateMachine("node_2", "127.0.0.1:9002", peers, "./node2.db")
	if err != nil {
		log.Fatalf("Failed to initialize node 2: %v", err)
	}
	defer rsm2.Close()
	defer os.Remove("./node2.db")

	rsm3, err := NewReplicatedStateMachine("node_3", "127.0.0.1:9003", peers, "./node3.db")
	if err != nil {
		log.Fatalf("Failed to initialize node 3: %v", err)
	}
	defer rsm3.Close()
	defer os.Remove("./node3.db")

	// Start all nodes
	rsm1.Start()
	rsm2.Start()
	rsm3.Start()

	// Simulate consensus election by forcing Node 1 to become leader
	log.Println("[Cluster Setup] Promoting node_1 to consensus Leader...")
	rsm1.raftNode.StartElection() // Self vote + candidate elevation
	
	// Wait for election convergence
	time.Sleep(100 * time.Millisecond)

	// Leader writes records to cluster
	log.Println("[Transactions] Executing write transaction: Set 'key_user' -> 'gpar377'...")
	success := rsm1.ExecuteWriteCommand("INSERT INTO kv (key, value) VALUES ('key_user', 'gpar377');")
	if !success {
		log.Println("WARNING: node_1 is not the leader yet! Retrying...")
		time.Sleep(100 * time.Millisecond)
		rsm1.ExecuteWriteCommand("INSERT INTO kv (key, value) VALUES ('key_user', 'gpar377');")
	}

	// Force log replication by sending heartbeats/log append entries
	log.Println("[Consensus] Broadcasting write entry to peers for commit quorum...")
	rsm1.raftNode.SendHeartbeats()

	// Wait for consensus apply loops to execute SQLite queries
	time.Sleep(200 * time.Millisecond)

	// Verify replicated reads on peer nodes
	log.Println("[Verification] Reading state from node_2 SQLite database...")
	results, err := rsm2.GetDatabase().ExecuteQuery("SELECT * FROM kv WHERE key = 'key_user';")
	if err != nil {
		log.Fatalf("Query failed on node 2: %v", err)
	}

	if len(results) > 0 && results[0]["value"] == "gpar377" {
		log.Println("SUCCESS: Key 'key_user' correctly replicated to node_2 SQLite db!")
	} else {
		log.Printf("FAIL: Replication verification failed! Results: %v", results)
	}

	log.Println("==================================================")
	log.Println("ALL DISTRIBUTED SQLITE VERIFICATIONS COMPLETED! 🎉")
	log.Println("==================================================")
}

