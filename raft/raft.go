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
