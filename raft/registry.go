package raft

import (
	"errors"
	"sync"
)

// Registry manages simulated local memory connection routing for testing without TCP port binds
type Registry struct {
	mu    sync.RWMutex
	nodes map[string]*Node
}

var clusterRegistry = &Registry{
	nodes: make(map[string]*Node),
}

func RegisterNode(address string, node *Node) {
	clusterRegistry.mu.Lock()
	defer clusterRegistry.mu.Unlock()
	clusterRegistry.nodes[address] = node
}

func GetNode(address string) (*Node, error) {
	clusterRegistry.mu.RLock()
	defer clusterRegistry.mu.RUnlock()
	node, exists := clusterRegistry.nodes[address]
	if !exists {
		return nil, errors.New("node address not found in registry")
	}
	return node, nil
}
