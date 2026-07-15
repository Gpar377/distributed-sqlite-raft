package raft

import (
	"sync"
	"time"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    int
	Command string // SQL write statement e.g. "INSERT INTO users..."
}

type Node struct {
	mu        sync.Mutex
	peers     map[string]string // nodeID -> address
	nodeID    string
	address   string
	role      Role
	
	// Raft State
	currentTerm int
	votedFor    string
	log         []LogEntry

	// Volatile State
	commitIndex int
	lastApplied int

	// Leader-specific volatile state
	nextIndex  map[string]int
	matchIndex map[string]int

	// Election timers
	electionTimeout time.Duration
	lastHeartbeat   time.Time

	// Channels for loop communication
	heartbeatChan chan bool
}

func NewNode(nodeID string, address string, peers map[string]string) *Node {
	return &Node{
		nodeID:        nodeID,
		address:       address,
		peers:         peers,
		role:          Follower,
		currentTerm:   0,
		votedFor:      "",
		log:           make([]LogEntry, 0),
		commitIndex:   -1,
		lastApplied:   -1,
		nextIndex:     make(map[string]int),
		matchIndex:    make(map[string]int),
		heartbeatChan: make(chan bool, 10),
	}
}

func (n *Node) GetRole() Role {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

func (n *Node) GetHeartbeatChan() chan bool {
	return n.heartbeatChan
}

func (n *Node) GetCommitIndex() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

func (n *Node) GetLastApplied() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastApplied
}

func (n *Node) SetLastApplied(idx int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.lastApplied = idx
}

func (n *Node) GetLog() []LogEntry {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.log
}

// Propose appends write query statement to local logs as a leader
func (n *Node) Propose(command string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader {
		return
	}
	n.log = append(n.log, LogEntry{
		Term:    n.currentTerm,
		Command: command,
	})
}

