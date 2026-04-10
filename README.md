# Distributed Replicated Object Store

A fault-tolerant, distributed key-value store built in Python with gRPC, implementing majority-commit replication, deterministic primary election, and concurrent write safety. Exposes three interfaces: a gRPC backend, a REST proxy, and an interactive CLI proxy.

Built as part of CS 417: Distributed Systems at Rutgers University.

---

## Architecture

```
                        ┌─────────────┐
                        │  CLI Proxy  │  (cli.py)
                        │  stdin/out  │
                        └──────┬──────┘
                               │ gRPC
                               ▼
┌──────────────┐      ┌─────────────────┐      ┌──────────────────┐
│ REST Proxy   │─────▶│  Primary Node   │─────▶│  Replica Node 1  │
│ (HTTP/JSON)  │ gRPC │  (server.py)    │ gRPC │  (server.py)     │
└──────────────┘      │                 │      └──────────────────┘
                       │  - Handles all  │           │ ApplyWrite
                       │    writes       │      ┌──────────────────┐
                       │  - Fan-out to   │─────▶│  Replica Node 2  │
                       │    replicas     │      │  (server.py)     │
                       └─────────────────┘      └──────────────────┘
```

The system runs as a cluster of N server processes. One node is designated the **primary** and all others are **replicas**. All writes go to the primary; reads can go to any node.

---

## Features

- **Majority-commit replication** — a write returns `OK` only after a majority of nodes acknowledge it
- **Deterministic primary election** — no coordination needed; every node applies the same lexicographic sort on the cluster list and independently agrees on who the primary is
- **Concurrent write safety** — `threading.Lock` protects all in-memory state; `ThreadPoolExecutor` fans out `ApplyWrite` calls to replicas in parallel
- **Replica write rejection** — replicas immediately return `FAILED_PRECONDITION` on any write attempt, redirecting the client to the primary
- **REST proxy** — full HTTP→gRPC translation with round-robin read routing across all nodes
- **CLI proxy** — interactive terminal interface with write/read routing and `shlex` parsing for quoted values
- **Concurrent correctness test** — 20 threads × 200 mixed put/get/delete operations with final state consistency assertions

---

## Tech Stack

- **Python 3** — server, proxies, benchmarks
- **gRPC / Protocol Buffers** — backend RPC interface (`objectstore.proto`)
- **HTTP** — REST proxy built on Python's `http.server`
- **`concurrent.futures.ThreadPoolExecutor`** — parallel replica fan-out
- **`threading.Lock`** — in-memory store synchronization

---

## Project Structure

```
├── server.py               # gRPC object store server (primary + replica logic)
├── restproxy.py            # REST-to-gRPC proxy with round-robin reads
├── cli.py                  # Interactive CLI proxy
├── objectstore.proto       # gRPC service definition (unmodified)
├── objectstore_pb2.py      # Generated message classes
├── objectstore_pb2_grpc.py # Generated gRPC stubs
├── requirements.txt
├── bench/
│   ├── bench1.py           # Throughput vs. concurrency benchmark (1–32 clients)
│   ├── bench2.py           # Replication cost benchmark (1/2/3-node configs)
│   └── bench_worker.py     # Per-process benchmark worker
└── test/
    └── test_concurrent.py  # Concurrent correctness test (20 threads)
```

---

## gRPC Interface

Defined in `objectstore.proto`:

| RPC | Description |
|-----|-------------|
| `Put(key, value)` | Store a new key-value pair. Returns `ALREADY_EXISTS` if key exists. |
| `Get(key)` | Retrieve value. Returns `NOT_FOUND` if missing. |
| `Delete(key)` | Remove a key. Returns `NOT_FOUND` if missing. |
| `Update(key, value)` | Replace value of existing key. Returns `NOT_FOUND` if missing. |
| `List()` | Return all keys and their value sizes. |
| `Reset()` | Clear all keys and reset all counters. |
| `Stats()` | Return live object count, total bytes, and per-operation counters. |
| `ApplyWrite(op)` | Primary→replica replication call. Clients must never call this directly. |

Key constraints: printable ASCII only, max 128 chars. Value max 1 MiB.

---

## Replication Model

### Primary Election

Every node applies the same deterministic rule at startup — no leader election protocol needed:

```python
endpoints = sorted(e.lower().strip() for e in cluster_arg.split(","))
primary   = endpoints[0]   # lexicographically smallest
replicas  = endpoints[1:]
```

Since every node in the cluster receives the same `--cluster` list, they all independently agree on who the primary is.

### Write Path

1. Client sends write RPC to the primary.
2. Primary validates and applies the write locally.
3. Primary fans out an `ApplyWrite` to all replicas in parallel via `ThreadPoolExecutor`.
4. Primary counts acknowledgments (its own local apply counts as 1).
5. If `total_acks >= majority`, primary returns `OK` to the client.
6. If majority is not reached, primary returns `UNAVAILABLE` and the write is not committed.

```python
def _majority(self):
    cluster_size = 1 + len(self._replicas)
    return cluster_size // 2 + 1
```

For a 3-node cluster, majority = 2. One replica can be down and writes still succeed.

### Read Path

Reads (`Get`, `List`, `Stats`) are routed round-robin across all nodes by the proxies. The primary is also a valid read target.

### Failure Semantics

- A downed replica does not block writes as long as majority still responds.
- If the primary goes down, writes return `UNAVAILABLE` until it is restarted (no automatic failover — by design).
- No restart recovery: in-memory state is lost on restart.

---

## Running the Cluster

### Generate gRPC stubs (one-time)

```bash
pip install -r requirements.txt
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. objectstore.proto
```

### Single-node (development)

```bash
python3 server.py --listen localhost:50051 --cluster localhost:50051
```

### 3-node cluster (separate terminals)

```bash
python3 server.py --listen localhost:50051 --cluster localhost:50051,localhost:50052,localhost:50053
python3 server.py --listen localhost:50052 --cluster localhost:50051,localhost:50052,localhost:50053
python3 server.py --listen localhost:50053 --cluster localhost:50051,localhost:50052,localhost:50053
```

`localhost:50051` is lexicographically smallest, so it becomes the primary.

### CLI Proxy

```bash
python3 cli.py --cluster localhost:50051,localhost:50052,localhost:50053
> put mykey hello
OK
> get mykey
OK: hello
> stats
live=1 bytes=5 puts=1 gets=1 deletes=0 updates=0
> list
mykey  (5 bytes)
```

### REST Proxy

```bash
python3 restproxy.py --cluster localhost:50051,localhost:50052,localhost:50053 --port 8080

# Put
curl -X PUT http://localhost:8080/objects/mykey --data-binary "hello"

# Get
curl http://localhost:8080/objects/mykey

# List
curl http://localhost:8080/objects

# Stats
curl http://localhost:8080/stats

# Delete
curl -X DELETE http://localhost:8080/objects/mykey

# Reset
curl -X DELETE http://localhost:8080/objects
```

---

## Benchmarks

All benchmarks were run on Rutgers iLab machines with server and client processes on separate machines.

### Benchmark 1 — Throughput vs. Concurrency

Fixed 4 KiB objects, 30-second sustained workload per concurrency level.

```bash
python3 bench/bench1.py --cluster <primary>
```

Reports `ops/s` and `p99 latency (ms)` for both `put` and `get` at 1, 2, 4, 8, 16, and 32 concurrent client processes.

### Benchmark 2 — Replication Cost

Fixed 8 concurrent clients, 4 KiB objects, 30-second runs across three configurations.

```bash
python3 bench/bench2.py --node1 <ep1> --node2 <ep2> --node3 <ep3>
```

Compares throughput and p99 latency across 1-node (no replication), 2-node (majority=2), and 3-node (majority=2) configurations.

---

## Concurrency & Synchronization

All access to `self._store` and `self._stats` is protected by a single `threading.Lock`. This was chosen over finer-grained locking because:

- Python's GIL already serializes CPU-bound bytecode, so lock contention is low.
- A single lock eliminates the risk of deadlock from lock ordering.
- The `List` RPC builds its entire response under the lock and releases before returning — it never holds the lock while doing network I/O.

`ApplyWrite` fan-out uses `ThreadPoolExecutor` so replicas are contacted in parallel rather than sequentially, keeping write latency proportional to the slowest responding replica rather than the sum.

---

## Concurrent Correctness Test

```bash
python3 test/test_concurrent.py
```

Spawns 20 threads, each performing 200 mixed `put`/`get`/`delete` operations on an overlapping key space against a live single-node server. After all threads complete, asserts:

- No phantom keys (keys that were deleted still appear)
- No corrupted values (values don't match what was written)
- Stats counters match the number of successful operations recorded by the threads
