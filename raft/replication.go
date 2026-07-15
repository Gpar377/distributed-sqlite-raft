package raft

import (
	"log"
)

// SendHeartbeats sends AppendEntries RPCs to all peers in the cluster
func (n *Node) SendHeartbeats() {
	n.mu.Lock()
	term := n.currentTerm
	nodeID := n.nodeID
	commitIndex := n.commitIndex
	n.mu.Unlock()

	for peerID, addr := range n.peers {
		if peerID == nodeID {
			continue
		}

		go func(peer string, peerAddr string) {
			n.mu.Lock()
			prevLogIndex := n.nextIndex[peer] - 1
			prevLogTerm := -1
			if prevLogIndex >= 0 && prevLogIndex < len(n.log) {
				prevLogTerm = n.log[prevLogIndex].Term
			}

			entries := make([]LogEntry, 0)
			if n.nextIndex[peer] < len(n.log) {
				entries = n.log[n.nextIndex[peer]:]
			}
			n.mu.Unlock()

			args := AppendEntriesArgs{
				Term:         term,
				LeaderID:     nodeID,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: commitIndex,
			}
			var reply AppendEntriesReply

			err := n.sendAppendEntries(peerAddr, args, &reply)
			if err == nil {
				n.mu.Lock()
				defer n.mu.Unlock()

				if reply.Term > n.currentTerm {
					n.currentTerm = reply.Term
					n.role = Follower
					n.votedFor = ""
					return
				}

				if n.role == Leader && reply.Term == n.currentTerm {
					if reply.Success {
						n.nextIndex[peer] = prevLogIndex + len(entries) + 1
						n.matchIndex[peer] = prevLogIndex + len(entries)
						n.updateCommitIndex()
					} else {
						// Step back index on consistency mismatch failure
						if n.nextIndex[peer] > 0 {
							n.nextIndex[peer]--
						}
					}
				}
			}
		}(peerID, addr)
	}
}

// AppendEntries RPC Handler (Log Replication and Heartbeats)
func (n *Node) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply.Success = false
	reply.Term = n.currentTerm

	// 1. Reply false if Term < currentTerm
	if args.Term < n.currentTerm {
		return nil
	}

	// Step down if higher term or to recognize current leader
	if args.Term > n.currentTerm || n.role == Candidate {
		n.currentTerm = args.Term
		n.role = Follower
		n.votedFor = ""
	}

	n.heartbeatChan <- true // Reset election timer

	// 2. Reply false if log doesn't contain entry at prevLogIndex matching prevLogTerm
	if args.PrevLogIndex >= 0 {
		if args.PrevLogIndex >= len(n.log) || n.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			return nil
		}
	}

	// 3. If existing entry conflicts with new one, delete existing entry and all that follow it
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx < len(n.log) {
			if n.log[idx].Term != entry.Term {
				n.log = n.log[:idx]
				n.log = append(n.log, entry)
			}
		} else {
			// 4. Append any new entries not already in the log
			n.log = append(n.log, entry)
		}
	}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > n.commitIndex {
		lastNewIndex := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit < lastNewIndex {
			n.commitIndex = args.LeaderCommit
		} else {
			n.commitIndex = lastNewIndex
		}
	}

	reply.Success = true
	reply.Term = n.currentTerm
	return nil
}

// Update leader commit index based on peer matchIndex majorities
func (n *Node) updateCommitIndex() {
	for i := len(n.log) - 1; i > n.commitIndex; i-- {
		if n.log[i].Term != n.currentTerm {
			continue
		}
		
		matches := 1 // Count leader self
		for peer, idx := range n.matchIndex {
			if idx >= i {
				matches++
			}
		}

		if matches > (len(n.peers) / 2) {
			n.commitIndex = i
			log.Printf("[Node %s] Committed log index up to %d", n.nodeID, i)
			break
		}
	}
}

// Dummy helper simulating network RPC calls via cluster registry
func (n *Node) sendAppendEntries(address string, args AppendEntriesArgs, reply *AppendEntriesReply) error {
	peerNode, err := GetNode(address)
	if err != nil {
		return err
	}
	return peerNode.AppendEntries(args, reply)
}

