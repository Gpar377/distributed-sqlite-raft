package raft

import (
	"log"
	"math/rand"
	"time"
)

// StartElection transitions follower/candidate states and triggers RequestVotes in parallel
func (n *Node) StartElection() {
	n.mu.Lock()
	n.role = Candidate
	n.currentTerm++
	n.votedFor = n.nodeID
	term := n.currentTerm
	nodeID := n.nodeID
	
	// Determine last log state
	lastLogIndex := len(n.log) - 1
	lastLogTerm := -1
	if lastLogIndex >= 0 {
		lastLogTerm = n.log[lastLogIndex].Term
	}
	n.mu.Unlock()

	log.Printf("[Node %s] Starting election for Term %d", nodeID, term)

	votesReceived := 1 // Vote for self
	voteMutex := sync.Mutex{}

	for peerID, addr := range n.peers {
		if peerID == nodeID {
			continue
		}

		go func(peer string, peerAddr string) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateID:  nodeID,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			var reply RequestVoteReply

			// In production, make a network RPC call. Here we simulate or route to remote clients.
			err := n.sendRequestVote(peerAddr, args, &reply)
			if err == nil {
				n.mu.Lock()
				defer n.mu.Unlock()

				// If reply has higher term, fall back to follower
				if reply.Term > n.currentTerm {
					n.currentTerm = reply.Term
					n.role = Follower
					n.votedFor = ""
					return
				}

				if n.role == Candidate && reply.VoteGranted && reply.Term == n.currentTerm {
					voteMutex.Lock()
					votesReceived++
					if votesReceived > (len(n.peers)/2) && n.role == Candidate {
						n.role = Leader
						log.Printf("[Node %s] Elected Leader for Term %d", nodeID, n.currentTerm)
						n.initializeLeaderState()
					}
					voteMutex.Unlock()
				}
			}
		}(peerID, addr)
	}
}

// RequestVote RPC handler
func (n *Node) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	// 1. Reply false if Term < currentTerm
	if args.Term < n.currentTerm {
		reply.Term = n.currentTerm
		reply.VoteGranted = false
		return nil
	}

	// If term is higher, step down
	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.role = Follower
		n.votedFor = ""
	}

	// 2. If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote
	lastLogIndex := len(n.log) - 1
	lastLogTerm := -1
	if lastLogIndex >= 0 {
		lastLogTerm = n.log[lastLogIndex].Term
	}

	logOk := false
	if args.LastLogTerm > lastLogTerm {
		logOk = true
	} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
		logOk = true
	}

	if (n.votedFor == "" || n.votedFor == args.CandidateID) && logOk {
		n.votedFor = args.CandidateID
		reply.VoteGranted = true
		n.heartbeatChan <- true // Reset election timer
	} else {
		reply.VoteGranted = false
	}

	reply.Term = n.currentTerm
	return nil
}

// Dummy helper simulating network RPC calls
func (n *Node) sendRequestVote(address string, args RequestVoteArgs, reply *RequestVoteReply) error {
	// Simulated direct call
	return nil
}

func (n *Node) initializeLeaderState() {
	for peerID := range n.peers {
		n.nextIndex[peerID] = len(n.log)
		n.matchIndex[peerID] = -1
	}
	go n.startHeartbeatLoop()
}

func (n *Node) startHeartbeatLoop() {
	for {
		n.mu.Lock()
		if n.role != Leader {
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()

		n.SendHeartbeats()
		time.Sleep(100 * time.Millisecond)
	}
}
