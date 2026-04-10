#!/usr/bin/env python3
# test_concurrent.py
# launches 20 threads each doing 200 mixed put/get/delete ops on overlapping keys
# after all threads finish, checks that the final state is consistent

import argparse
import os
import random
import string
import sys
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import grpc
from google.protobuf import empty_pb2

import objectstore_pb2 as pb
import objectstore_pb2_grpc as pb_grpc

NUM_THREADS = 20
OPS_PER_THREAD = 200
KEY_POOL = [f"key{i:03d}" for i in range(30)]


def make_stub(endpoint):
    return pb_grpc.ObjectStoreStub(grpc.insecure_channel(endpoint))


def random_value(length=64):
    return "".join(random.choices(string.ascii_letters + string.digits, k=length)).encode()


def worker(stub, thread_id, results):
    puts, gets, deletes = 0, 0, 0

    for _ in range(OPS_PER_THREAD):
        key = random.choice(KEY_POOL)
        op = random.choice(["put", "get", "delete"])

        if op == "put":
            try:
                stub.Put(pb.PutRequest(key=key, value=random_value()))
                puts += 1
            except grpc.RpcError:
                pass
        elif op == "get":
            try:
                stub.Get(pb.GetRequest(key=key))
                gets += 1
            except grpc.RpcError:
                pass
        elif op == "delete":
            try:
                stub.Delete(pb.DeleteRequest(key=key))
                deletes += 1
            except grpc.RpcError:
                pass

    results[thread_id] = {"puts": puts, "gets": gets, "deletes": deletes}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cluster", default="localhost:50051")
    args = parser.parse_args()

    endpoint = sorted(e.lower().strip() for e in args.cluster.split(","))[0]
    stub = make_stub(endpoint)

    print("resetting server...")
    try:
        stub.Reset(empty_pb2.Empty())
    except grpc.RpcError as e:
        print(f"could not connect: {e.details()}")
        sys.exit(1)

    print(f"launching {NUM_THREADS} threads x {OPS_PER_THREAD} ops...")
    results = [None] * NUM_THREADS
    threads = [threading.Thread(target=worker, args=(stub, i, results)) for i in range(NUM_THREADS)]
    for t in threads: t.start()
    for t in threads: t.join()

    print("checking final state...")

    total_puts = sum(r["puts"] for r in results)
    total_deletes = sum(r["deletes"] for r in results)

    list_resp = stub.List(empty_pb2.Empty())
    stats_resp = stub.Stats(empty_pb2.Empty())
    live_keys = {e.key for e in list_resp.entries}

    failures = []

    unexpected = live_keys - set(KEY_POOL)
    if unexpected:
        failures.append(f"phantom keys: {unexpected}")

    for key in live_keys:
        try:
            resp = stub.Get(pb.GetRequest(key=key))
            if len(resp.value) == 0:
                failures.append(f"{key} is live but has empty value")
        except grpc.RpcError as e:
            failures.append(f"{key} in list but get returned {e.code().name}")

    if stats_resp.live_objects != len(live_keys):
        failures.append(f"stats.live_objects={stats_resp.live_objects} but list has {len(live_keys)}")

    if stats_resp.puts != total_puts:
        failures.append(f"stats.puts={stats_resp.puts} but threads recorded {total_puts}")

    if stats_resp.deletes != total_deletes:
        failures.append(f"stats.deletes={stats_resp.deletes} but threads recorded {total_deletes}")

    total_bytes_list = sum(e.size_bytes for e in list_resp.entries)
    if stats_resp.total_bytes != total_bytes_list:
        failures.append(f"stats.total_bytes={stats_resp.total_bytes} but list sums to {total_bytes_list}")

    print()
    print(f"  threads:          {NUM_THREADS}")
    print(f"  ops per thread:   {OPS_PER_THREAD}")
    print(f"  successful puts:  {total_puts}")
    print(f"  successful dels:  {total_deletes}")
    print(f"  live keys:        {len(live_keys)}")
    print()

    if failures:
        print(f"FAIL — {len(failures)} assertion(s) failed:")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)
    else:
        print("PASS — all assertions satisfied")
        sys.exit(0)


if __name__ == "__main__":
    main()