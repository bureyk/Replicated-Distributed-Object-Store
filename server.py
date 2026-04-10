#!/usr/bin/env python3
# server.py
# gRPC object store server — runs as a single node or in a cluster
# primary handles all writes and forwards them to replicas, reads go to anyone

import argparse
import sys
import threading
from concurrent import futures

import grpc
from google.protobuf import empty_pb2

import objectstore_pb2 as pb
import objectstore_pb2_grpc as pb_grpc

# limits from the spec
MAX_KEY_LEN   = 128
MAX_VALUE_LEN = 1024 * 1024  # 1 MiB

EMPTY = empty_pb2.Empty()


def parse_cluster(cluster_arg):
    # normalize and sort so the smallest endpoint is always the primary
    endpoints = sorted(e.lower().strip() for e in cluster_arg.split(","))
    return endpoints[0], endpoints


def validate_key(key):
    # printable ASCII only (0x21-0x7E), no spaces, max 128 chars
    if not key:
        return "key can't be empty"
    if len(key) > MAX_KEY_LEN:
        return f"key too long ({len(key)} chars, max {MAX_KEY_LEN})"
    for ch in key:
        if ord(ch) < 0x21 or ord(ch) > 0x7E:
            return f"invalid character in key: {repr(ch)}"
    return None


def validate_value(value):
    # cap at 1 MiB
    if len(value) > MAX_VALUE_LEN:
        return f"value too large ({len(value)} bytes, max {MAX_VALUE_LEN})"
    return None


# ---------------------------------------------------------------------------
# ObjectStore service implementation
# ---------------------------------------------------------------------------

class ObjectStoreServicer(pb_grpc.ObjectStoreServicer):
    # handles all gRPC calls for the object store
    # uses a threading.Lock to protect the store and stats counters
    # plain Lock is fine here since no handler calls another handler internally

    def __init__(self, listen_ep, primary_ep, replica_eps):
        # listen_ep  - address this instance is bound to
        # primary_ep - smallest endpoint in the cluster, handles all writes
        # replica_eps - everyone else
        
        self._listen = listen_ep
        self._primary = primary_ep
        self._replicas = replica_eps
        self._is_primary = (listen_ep == primary_ep)

        self._lock  = threading.Lock()
        self._store = {}
        self._stats = {"puts": 0, "gets": 0, "deletes": 0, "updates": 0}

        # build replica stubs on first write so startup doesn't fail if replicas aren't up yet
        self._replica_stubs = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_replica_stubs(self):
        # lazy init so we don't fail at startup if replicas aren't up yet
        if self._replica_stubs is None:
            self._replica_stubs = [
                pb_grpc.ObjectStoreStub(grpc.insecure_channel(ep))
                for ep in self._replicas
            ]
        return self._replica_stubs

    def _reject_if_replica(self, context):
        # replicas aren't allowed to handle writes, redirect to primary
        if not self._is_primary:
            context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                f"this node is a replica, send writes to {self._primary}"
            )
            return True
        return False

    def _fan_out(self, write_op):
        # send the write to all replicas in parallel, return how many acked
        stubs = self._get_replica_stubs()
        if not stubs:
            return 0

        ack_count = 0    
        
        def send_one(stub):
            nonlocal ack_count
            try:
                stub.ApplyWrite(write_op, timeout=5.0)
                ack_count += 1
            except grpc.RpcError as e:
                print(f"replica unreachable: {e.details()}", file=sys.stderr)

        with futures.ThreadPoolExecutor(max_workers=len(stubs)) as pool:
            list(pool.map(send_one, stubs))
       
        return ack_count


    def _majority(self):
        # how many acks we need to commit (primary counts as 1)
        cluster_size = 1 + len(self._replicas)
        return cluster_size // 2 + 1

    def _commit_write(self, write_op, context):
        # fan out then check if we hit majority, abort with UNAVAILABLE if not
        replica_acks = self._fan_out(write_op)
        total_acks   = 1 + replica_acks
        if total_acks < self._majority():
            context.abort(
                grpc.StatusCode.UNAVAILABLE,
                f"only {total_acks}/{1 + len(self._replicas)} nodes acked, need {self._majority()}"
            )
            return False
        return True

    # ------------------------------------------------------------------
    # Client-facing RPCs
    # ------------------------------------------------------------------

    def Put(self, request, context):
        if self._reject_if_replica(context):
            return EMPTY

        err = validate_key(request.key)
        if err:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, err)
            return EMPTY
        err = validate_value(request.value)
        if err:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, err)
            return EMPTY

        with self._lock:
            if request.key in self._store:
                context.abort(grpc.StatusCode.ALREADY_EXISTS, f"key already exists: {request.key}")
                return EMPTY
            self._store[request.key] = request.value
            self._stats["puts"] += 1

        self._commit_write(pb.WriteOp(type=pb.PUT, key=request.key, value=request.value), context)
        return EMPTY

    def Get(self, request, context):
        with self._lock:
            if request.key not in self._store:
                context.abort(grpc.StatusCode.NOT_FOUND, f"key not found: {request.key}")
                return pb.GetResponse()
            value = self._store[request.key]
            self._stats["gets"] += 1
        return pb.GetResponse(value=value)

    def Delete(self, request, context):
        if self._reject_if_replica(context):
            return EMPTY

        with self._lock:
            if request.key not in self._store:
                context.abort(grpc.StatusCode.NOT_FOUND, f"key not found: {request.key}")
                return EMPTY
            del self._store[request.key]
            self._stats["deletes"] += 1

        self._commit_write(pb.WriteOp(type=pb.DELETE, key=request.key), context)
        return EMPTY

    def Update(self, request, context):
        if self._reject_if_replica(context):
            return EMPTY

        err = validate_value(request.value)
        if err:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, err)
            return EMPTY

        with self._lock:
            if request.key not in self._store:
                context.abort(grpc.StatusCode.NOT_FOUND, f"key not found: {request.key}")
                return EMPTY
            self._store[request.key] = request.value
            self._stats["updates"] += 1

        self._commit_write(pb.WriteOp(type=pb.UPDATE, key=request.key, value=request.value), context)
        return EMPTY

    def List(self, request, context):
        # lock for the whole build so we don't return a half-modified snapshot
        with self._lock:
            entries = [pb.ListEntry(key=k, size_bytes=len(v)) for k, v in self._store.items()]
        return pb.ListResponse(entries=entries)

    def Reset(self, request, context):
        if self._reject_if_replica(context):
            return EMPTY

        with self._lock:
            self._store.clear()
            self._stats = {"puts": 0, "gets": 0, "deletes": 0, "updates": 0}

        self._commit_write(pb.WriteOp(type=pb.RESET), context)
        return EMPTY

    def Stats(self, request, context):
        with self._lock:
            live = len(self._store)
            total = sum(len(v) for v in self._store.values())
            puts = self._stats["puts"]
            gets = self._stats["gets"]
            dels = self._stats["deletes"]
            upds = self._stats["updates"]
        return pb.StatsResponse(
            live_objects=live, total_bytes=total,
            puts=puts, gets=gets, deletes=dels, updates=upds,
        )

    def ApplyWrite(self, request, context):
        # primary calls this on replicas to keep them in sync
        with self._lock:
            if request.type == pb.PUT:
                self._store[request.key] = request.value
                self._stats["puts"] += 1
            elif request.type == pb.DELETE:
                self._store.pop(request.key, None)
                self._stats["deletes"] += 1
            elif request.type == pb.UPDATE:
                self._store[request.key] = request.value
                self._stats["updates"] += 1
            elif request.type == pb.RESET:
                self._store.clear()
                self._stats = {"puts": 0, "gets": 0, "deletes": 0, "updates": 0}
        return EMPTY


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen", required=True, help="host:port to bind to")
    parser.add_argument("--cluster", required=True, help="comma-separated list of all endpoints")
    args = parser.parse_args()

    # validate each endpoint in the cluster list
    raw_endpoints = [e.lower().strip() for e in args.cluster.split(",")]
    for ep in raw_endpoints:
        parts = ep.split(":")
        if len(parts) != 2 or not parts[1].isdigit():
            print(f"invalid endpoint: {repr(ep)}", file=sys.stderr)
            sys.exit(1)

    listen_ep = args.listen.lower().strip()
    if listen_ep not in raw_endpoints:
        print(f"--listen {listen_ep!r} not found in --cluster list", file=sys.stderr)
        sys.exit(1)

    primary_ep, all_eps = parse_cluster(args.cluster)
    replica_eps = [ep for ep in all_eps if ep != primary_ep]

    role = "primary" if listen_ep == primary_ep else "replica"
    print(f"listen: {listen_ep} | role: {role} | primary: {primary_ep} | replicas: {replica_eps}", file=sys.stderr)

    servicer = ObjectStoreServicer(listen_ep, primary_ep, replica_eps)
    grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    pb_grpc.add_ObjectStoreServicer_to_server(servicer, grpc_server)
    grpc_server.add_insecure_port(listen_ep)
    grpc_server.start()
    print(f"server up on {listen_ep}", file=sys.stderr)

    try:
        grpc_server.wait_for_termination()
    except KeyboardInterrupt:
        print("shutting down", file=sys.stderr)
        grpc_server.stop(grace=2)


if __name__ == "__main__":
    main()