# Claude Code Guidelines - Distributed SQLite Raft

## Project Overview
This repository contains a replicated SQLite engine utilizing the Raft consensus protocol, written in Go or C++ (Go is highly recommended for network/Raft, but C++ can be used. Let's write the guideline for C++/gRPC).

## Technology Stack
*   **C++17** or **Golang** (Go is ideal for distributed primitives; we specify Go rules below for simplicity and concurrency safety)
*   **SQLite3** (Driver and CLI bindings)
*   **gRPC & Protobuf** (For RPC communication between cluster nodes)

## Coding Standards & Conventions
*   Strictly separate Raft logic (state, log, RPC handlers) from the State Machine (SQLite wrapper).
*   Protect all Raft state variables (term, vote, log index) with locks (`sync.Mutex`).
*   Ensure random election timeouts range appropriately (e.g., 150ms-300ms) to avoid split-vote loops.
*   On database snapshot, use SQLite backup API (`sqlite3_backup_*`) to take consistent snapshots without blocking writes.

## Workflow Rules & Commands
*   **Build Project:** `go build -o raft-sqlite cmd/main.go`
*   **Run Cluster Locally:** Run scripts `scripts/run_cluster.sh` (launches 3 nodes)
*   **Run Consensus Tests:** `go test ./raft/...`
