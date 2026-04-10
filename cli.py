#!/usr/bin/env python3
# cli.py
# command-line interface for the object store
# reads commands from stdin, forwards them to the cluster over gRPC
# writes go to the primary, reads round-robin across all nodes

import argparse
import shlex
import sys

import grpc
from google.protobuf import empty_pb2

import objectstore_pb2 as pb
import objectstore_pb2_grpc as pb_grpc


def parse_cluster(cluster_arg):
    # same rule as the server — smallest endpoint after sorting is the primary
    endpoints = sorted(e.lower().strip() for e in cluster_arg.split(","))
    return endpoints[0], endpoints


def make_stub(endpoint):
    return pb_grpc.ObjectStoreStub(grpc.insecure_channel(endpoint))


def cmd_put(stub, args):
    if len(args) < 2:
        print("usage: put <key> <value>")
        return
    try:
        stub.Put(pb.PutRequest(key=args[0], value=" ".join(args[1:]).encode()))
        print("OK")
    except grpc.RpcError as e:
        print(f"ERROR {e.code().name}: {e.details()}")


def cmd_get(stub, args):
    if len(args) < 1:
        print("usage: get <key>")
        return
    try:
        resp = stub.Get(pb.GetRequest(key=args[0]))
        try:
            print(f"OK: {resp.value.decode()}")
        except UnicodeDecodeError:
            print(f"OK: ({len(resp.value)} bytes, binary)")
    except grpc.RpcError as e:
        print(f"ERROR {e.code().name}: {e.details()}")


def cmd_delete(stub, args):
    if len(args) < 1:
        print("usage: delete <key>")
        return
    try:
        stub.Delete(pb.DeleteRequest(key=args[0]))
        print("OK")
    except grpc.RpcError as e:
        print(f"ERROR {e.code().name}: {e.details()}")


def cmd_update(stub, args):
    if len(args) < 2:
        print("usage: update <key> <value>")
        return
    try:
        stub.Update(pb.UpdateRequest(key=args[0], value=" ".join(args[1:]).encode()))
        print("OK")
    except grpc.RpcError as e:
        print(f"ERROR {e.code().name}: {e.details()}")


def cmd_list(stub, args):
    try:
        resp = stub.List(empty_pb2.Empty())
        if not resp.entries:
            print("(empty)")
        else:
            for entry in sorted(resp.entries, key=lambda e: e.key):
                print(f"{entry.key}  ({entry.size_bytes} bytes)")
    except grpc.RpcError as e:
        print(f"ERROR {e.code().name}: {e.details()}")


def cmd_reset(stub, args):
    try:
        stub.Reset(empty_pb2.Empty())
        print("OK")
    except grpc.RpcError as e:
        print(f"ERROR {e.code().name}: {e.details()}")


def cmd_stats(stub, args):
    try:
        r = stub.Stats(empty_pb2.Empty())
        print(f"live={r.live_objects} bytes={r.total_bytes} puts={r.puts} gets={r.gets} deletes={r.deletes} updates={r.updates}")
    except grpc.RpcError as e:
        print(f"ERROR {e.code().name}: {e.details()}")


class RoundRobin:
    # cycles through stubs for read distribution
    def __init__(self, stubs):
        self._stubs = stubs
        self._idx = 0

    def next(self):
        stub = self._stubs[self._idx % len(self._stubs)]
        self._idx += 1
        return stub


# (handler, is_write) — writes go to primary, reads round-robin
COMMANDS = {
    "put":    (cmd_put,    True),
    "get":    (cmd_get,    False),
    "delete": (cmd_delete, True),
    "update": (cmd_update, True),
    "list":   (cmd_list,   False),
    "reset":  (cmd_reset,  True),
    "stats":  (cmd_stats,  False),
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cluster", required=True, help="comma-separated host:port endpoints")
    args = parser.parse_args()

    primary_ep, all_eps = parse_cluster(args.cluster)
    primary_stub = make_stub(primary_ep)
    rr = RoundRobin([make_stub(ep) for ep in all_eps])

    interactive = sys.stdin.isatty()

    while True:
        if interactive:
            print("> ", end="", flush=True)

        try:
            line = input()
        except EOFError:
            break

        line = line.strip()
        if not line or line.startswith("#"):
            continue

        try:
            tokens = shlex.split(line)
        except ValueError as e:
            print(f"parse error: {e}")
            continue

        cmd = tokens[0].lower()
        rest = tokens[1:]

        if cmd not in COMMANDS:
            print(f"unknown command: {cmd!r} — valid: {', '.join(COMMANDS)}")
            continue

        handler, is_write = COMMANDS[cmd]
        handler(primary_stub if is_write else rr.next(), rest)


if __name__ == "__main__":
    main()