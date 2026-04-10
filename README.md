# Replicated-Distributed-Object-Store
Fault-tolerant distributed key-value store in Python with gRPC. Implements majority-commit replication across a 3-node cluster, deterministic primary election via lexicographic sorting, and concurrent write safety with parallel replica fan-out. Exposes a REST proxy and interactive CLI in addition to the gRPC backend.
