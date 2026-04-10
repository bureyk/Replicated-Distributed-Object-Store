#!/usr/bin/env python3
"""
testclient.py - Interactive test suite for the CS 417 distributed object store.

Walks you through two test phases:
  Phase 1: Single-node server — tests all basic operations.
  Phase 2: Three-node cluster — tests replication and replica behavior.

Run this from the same directory as your objectstore_pb2.py stubs.

Usage:
    python3 testclient.py
"""

import sys
import time

import grpc

try:
    import objectstore_pb2 as pb
    import objectstore_pb2_grpc as pb_grpc
    from google.protobuf import empty_pb2
except ModuleNotFoundError as _e:
    print(f"""
ERROR: Missing generated gRPC stubs ({_e.name}).

Run this command from the directory that contains objectstore.proto:

    python3 -m grpc_tools.protoc \
        -I. \
        --python_out=. \
        --grpc_python_out=. \
        objectstore.proto

This produces objectstore_pb2.py and objectstore_pb2_grpc.py.
Then run testclient.py from that same directory.
""")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Terminal colors
# ---------------------------------------------------------------------------

class C:
    RESET   = "\033[0m"
    BOLD    = "\033[1m"
    RED     = "\033[91m"
    GREEN   = "\033[92m"
    MAGENTA  = "\033[35m"
    CYAN    = "\033[96m"
    WHITE   = "\033[97m"
    DIM     = "\033[2m"

def bold(s):    return f"{C.BOLD}{s}{C.RESET}"
def cyan(s):    return f"{C.CYAN}{s}{C.RESET}"
def magenta(s):  return f"{C.MAGENTA}{s}{C.RESET}"
def dim(s):     return f"{C.DIM}{s}{C.RESET}"

def passed(label):
    print(f"  {C.GREEN}PASS{C.RESET}  {label}")

def failed(label, detail=""):
    msg = f"  {C.RED}FAIL{C.RESET}  {label}"
    if detail:
        msg += f"\n        {C.DIM}{detail}{C.RESET}"
    print(msg)

def skipped(label, reason=""):
    msg = f"  {C.MAGENTA}SKIP{C.RESET}  {label}"
    if reason:
        msg += f"  {C.DIM}({reason}){C.RESET}"
    print(msg)

def section(title):
    print(f"\n{C.CYAN}{C.BOLD}{'─' * 50}{C.RESET}")
    print(f"{C.CYAN}{C.BOLD}  {title}{C.RESET}")
    print(f"{C.CYAN}{C.BOLD}{'─' * 50}{C.RESET}")

def info(msg):
    print(f"  {C.DIM}→{C.RESET}  {msg}")

def warn(msg):
    print(f"  {C.MAGENTA}!{C.RESET}  {msg}")

def prompt(msg):
    print(f"\n{C.MAGENTA}{C.BOLD}{msg}{C.RESET}")

# ---------------------------------------------------------------------------
# Test state
# ---------------------------------------------------------------------------

PASS_COUNT = 0
FAIL_COUNT = 0
SKIP_COUNT = 0


def record(ok: bool):
    global PASS_COUNT, FAIL_COUNT
    if ok:
        PASS_COUNT += 1
    else:
        FAIL_COUNT += 1


# ---------------------------------------------------------------------------
# gRPC helpers
# ---------------------------------------------------------------------------

_MSG_OPTIONS = [
    ('grpc.max_send_message_length',    1024 * 1024 + 65536),
    ('grpc.max_receive_message_length', 1024 * 1024 + 65536),
]

def make_stub(endpoint: str) -> pb_grpc.ObjectStoreStub:
    ch = grpc.insecure_channel(endpoint, options=_MSG_OPTIONS)
    return pb_grpc.ObjectStoreStub(ch)


def check_ok(label: str, fn) -> bool:
    """Call fn(). Pass if it returns without raising."""
    try:
        fn()
        passed(label)
        record(True)
        return True
    except grpc.RpcError as e:
        failed(label, f"unexpected {e.code().name}: {e.details()}")
        record(False)
        return False


def check_code(label: str, expected: grpc.StatusCode, fn) -> bool:
    """Call fn(). Pass if it raises with the expected status code."""
    try:
        fn()
        failed(label, f"expected {expected.name} but got OK")
        record(False)
        return False
    except grpc.RpcError as e:
        if e.code() == expected:
            passed(label)
            record(True)
            return True
        else:
            failed(label, f"expected {expected.name}, got {e.code().name}: {e.details()}")
            record(False)
            return False


def check_value(label: str, expected: bytes, fn) -> bool:
    """Call fn() and pass if the returned value matches expected."""
    try:
        result = fn()
        if result == expected:
            passed(label)
            record(True)
            return True
        else:
            failed(label,
                   f"expected {expected!r}, got {result!r}")
            record(False)
            return False
    except grpc.RpcError as e:
        failed(label, f"unexpected {e.code().name}: {e.details()}")
        record(False)
        return False


def server_ready(endpoint: str, timeout: int = 15) -> bool:
    """Poll until the server responds to Stats or timeout expires."""
    stub = make_stub(endpoint)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            stub.Stats(empty_pb2.Empty(), timeout=1.0)
            return True
        except grpc.RpcError:
            time.sleep(0.5)
    return False


# ---------------------------------------------------------------------------
# Input helpers
# ---------------------------------------------------------------------------

def ask_port(prompt_text: str) -> int:
    while True:
        raw = input(f"{C.MAGENTA}  {prompt_text}: {C.RESET}").strip()
        if raw.isdigit():
            port = int(raw)
            if 1024 <= port <= 65535:
                return port
        print(f"  {C.RED}Please enter a valid port number (1024–65535).{C.RESET}")


def ask_three_ports() -> tuple[int, int, int]:
    print()
    print("  Enter three different port numbers for the cluster nodes.")
    while True:
        p1 = ask_port("Port for node 1 (will be primary)")
        p2 = ask_port("Port for node 2")
        p3 = ask_port("Port for node 3")
        if len({p1, p2, p3}) == 3:
            return p1, p2, p3
        print(f"  {C.RED}All three ports must be different. Try again.{C.RESET}")


def pause(msg="Press Enter when ready..."):
    input(f"\n  {C.MAGENTA}{msg}{C.RESET}")


# ---------------------------------------------------------------------------
# Phase 1: Single-node tests
# ---------------------------------------------------------------------------

def phase1_single_node():
    print(f"""
{C.BOLD}{'═' * 60}{C.RESET}
{C.BOLD}  PHASE 1: Single-Node Server{C.RESET}
{C.BOLD}{'═' * 60}{C.RESET}

  This phase tests all basic operations against a single server.
""")

    port = ask_port("Pick a port number for the server 1024-65535  (don't start it yet)")
    endpoint = f"localhost:{port}"
    cluster  = f"localhost:{port}"

    print(f"""
  Start your server with this command (in a separate terminal):

  {C.GREEN}{C.BOLD}python3 server.py --listen {endpoint} --cluster {cluster}{C.RESET}

  Leave it running and come back here.
""")
    pause("Press Enter once the server is running...")

    info(f"Checking that server is up at {endpoint} ...")
    if not server_ready(endpoint):
        print(f"\n  {C.RED}Could not reach server at {endpoint} after 15 seconds.{C.RESET}")
        print("  Make sure it started without errors and is listening on the right port.")
        sys.exit(1)
    info("Server is up.\n")

    stub = make_stub(endpoint)

    # Reset first so we start clean regardless of prior state.
    try:
        stub.Reset(empty_pb2.Empty())
    except grpc.RpcError:
        pass

    # ------------------------------------------------------------------
    section("put — store new objects")
    # ------------------------------------------------------------------

    check_ok(
        "put 'fruit' = 'apple'",
        lambda: stub.Put(pb.PutRequest(key="fruit", value=b"apple"))
    )
    check_ok(
        "put 'veggie' = 'carrot'",
        lambda: stub.Put(pb.PutRequest(key="veggie", value=b"carrot"))
    )
    check_ok(
        "put 'grain' = 'rice'",
        lambda: stub.Put(pb.PutRequest(key="grain", value=b"rice"))
    )

    # ------------------------------------------------------------------
    section("put — error cases")
    # ------------------------------------------------------------------

    check_code(
        "put 'fruit' again → ALREADY_EXISTS",
        grpc.StatusCode.ALREADY_EXISTS,
        lambda: stub.Put(pb.PutRequest(key="fruit", value=b"orange"))
    )
    check_code(
        "put with empty key → INVALID_ARGUMENT",
        grpc.StatusCode.INVALID_ARGUMENT,
        lambda: stub.Put(pb.PutRequest(key="", value=b"v"))
    )
    check_code(
        "put with space in key → INVALID_ARGUMENT",
        grpc.StatusCode.INVALID_ARGUMENT,
        lambda: stub.Put(pb.PutRequest(key="bad key", value=b"v"))
    )
    check_code(
        "put with value over 1 MiB → INVALID_ARGUMENT",
        grpc.StatusCode.INVALID_ARGUMENT,
        lambda: stub.Put(pb.PutRequest(key="bigval", value=b"x" * (1024 * 1024 + 1)))
    )

    # ------------------------------------------------------------------
    section("get — retrieve objects")
    # ------------------------------------------------------------------

    check_value(
        "get 'fruit' → 'apple'",
        b"apple",
        lambda: stub.Get(pb.GetRequest(key="fruit")).value
    )
    check_value(
        "get 'veggie' → 'carrot'",
        b"carrot",
        lambda: stub.Get(pb.GetRequest(key="veggie")).value
    )
    check_code(
        "get 'missing' → NOT_FOUND",
        grpc.StatusCode.NOT_FOUND,
        lambda: stub.Get(pb.GetRequest(key="missing"))
    )

    # ------------------------------------------------------------------
    section("update — replace a value")
    # ------------------------------------------------------------------

    check_ok(
        "update 'fruit' = 'mango'",
        lambda: stub.Update(pb.UpdateRequest(key="fruit", value=b"mango"))
    )
    check_value(
        "get 'fruit' after update → 'mango'",
        b"mango",
        lambda: stub.Get(pb.GetRequest(key="fruit")).value
    )
    check_code(
        "update 'missing' → NOT_FOUND",
        grpc.StatusCode.NOT_FOUND,
        lambda: stub.Update(pb.UpdateRequest(key="missing", value=b"v"))
    )

    # ------------------------------------------------------------------
    section("delete — remove an object")
    # ------------------------------------------------------------------

    check_ok(
        "delete 'grain'",
        lambda: stub.Delete(pb.DeleteRequest(key="grain"))
    )
    check_code(
        "get 'grain' after delete → NOT_FOUND",
        grpc.StatusCode.NOT_FOUND,
        lambda: stub.Get(pb.GetRequest(key="grain"))
    )
    check_code(
        "delete 'grain' again → NOT_FOUND",
        grpc.StatusCode.NOT_FOUND,
        lambda: stub.Delete(pb.DeleteRequest(key="grain"))
    )

    # ------------------------------------------------------------------
    section("list — enumerate stored objects")
    # ------------------------------------------------------------------

    # At this point: 'fruit' (mango, 5 bytes), 'veggie' (carrot, 6 bytes)
    try:
        resp = stub.List(empty_pb2.Empty())
        keys  = {e.key for e in resp.entries}
        sizes = {e.key: e.size_bytes for e in resp.entries}

        ok = "fruit" in keys and "veggie" in keys and "grain" not in keys
        if ok:
            passed("list returns 'fruit' and 'veggie', not 'grain'")
            record(True)
        else:
            failed("list returns correct keys",
                   f"got keys: {sorted(keys)}")
            record(False)

        size_ok = sizes.get("fruit") == 5 and sizes.get("veggie") == 6
        if size_ok:
            passed("list reports correct sizes (fruit=5, veggie=6)")
            record(True)
        else:
            failed("list reports correct sizes",
                   f"fruit={sizes.get('fruit')}, veggie={sizes.get('veggie')}")
            record(False)

    except grpc.RpcError as e:
        failed("list call", f"{e.code().name}: {e.details()}")
        record(False)

    # ------------------------------------------------------------------
    section("stats — operation counters")
    # ------------------------------------------------------------------

    try:
        s = stub.Stats(empty_pb2.Empty())
        info(f"live_objects={s.live_objects}  total_bytes={s.total_bytes}  "
             f"puts={s.puts}  gets={s.gets}  deletes={s.deletes}  updates={s.updates}")

        # We did: 3 puts, 2+1+1 gets (fruit+veggie+missing non-counted?),
        # 1 update, 1 delete. gets: fruit, veggie, missing(NOT_FOUND=no count?),
        # fruit after update. Only successful ops count.
        # puts: 3 ok + 1 already_exists(fail) + 2 error = 3
        # gets: fruit(ok) + veggie(ok) + missing(fail) + fruit-after-update(ok) + grain-after-delete(fail) = 3
        # updates: fruit(ok) + missing(fail) = 1
        # deletes: grain(ok) + grain-again(fail) = 1
        # live_objects should be 2 (fruit, veggie)

        ok_live = s.live_objects == 2
        if ok_live:
            passed("live_objects == 2")
            record(True)
        else:
            failed("live_objects == 2", f"got {s.live_objects}")
            record(False)

        ok_puts = s.puts == 3
        if ok_puts:
            passed("puts == 3 (only successful puts counted)")
            record(True)
        else:
            failed("puts == 3", f"got {s.puts}")
            record(False)

        ok_updates = s.updates == 1
        if ok_updates:
            passed("updates == 1")
            record(True)
        else:
            failed("updates == 1", f"got {s.updates}")
            record(False)

        ok_deletes = s.deletes == 1
        if ok_deletes:
            passed("deletes == 1")
            record(True)
        else:
            failed("deletes == 1", f"got {s.deletes}")
            record(False)

    except grpc.RpcError as e:
        failed("stats call", f"{e.code().name}: {e.details()}")
        record(False)

    # ------------------------------------------------------------------
    section("reset — clear all data and counters")
    # ------------------------------------------------------------------

    check_ok(
        "reset()",
        lambda: stub.Reset(empty_pb2.Empty())
    )
    check_code(
        "get 'fruit' after reset → NOT_FOUND",
        grpc.StatusCode.NOT_FOUND,
        lambda: stub.Get(pb.GetRequest(key="fruit"))
    )
    check_code(
        "get 'veggie' after reset → NOT_FOUND",
        grpc.StatusCode.NOT_FOUND,
        lambda: stub.Get(pb.GetRequest(key="veggie"))
    )

    try:
        s = stub.Stats(empty_pb2.Empty())
        ok = (s.live_objects == 0 and s.puts == 0 and s.gets == 0
              and s.deletes == 0 and s.updates == 0)
        if ok:
            passed("all stats counters are zero after reset")
            record(True)
        else:
            failed("all stats counters are zero after reset",
                   f"live_objects={s.live_objects} puts={s.puts} "
                   f"gets={s.gets} deletes={s.deletes} updates={s.updates}")
            record(False)
    except grpc.RpcError as e:
        failed("stats after reset", f"{e.code().name}: {e.details()}")
        record(False)

    try:
        resp = stub.List(empty_pb2.Empty())
        if len(resp.entries) == 0:
            passed("list is empty after reset")
            record(True)
        else:
            failed("list is empty after reset",
                   f"got {len(resp.entries)} entries: "
                   f"{[e.key for e in resp.entries]}")
            record(False)
    except grpc.RpcError as e:
        failed("list after reset", f"{e.code().name}: {e.details()}")
        record(False)

    print(f"""
  {C.GREEN}{C.BOLD}Phase 1 complete.{C.RESET}
  Now kill the server (Ctrl-C in its terminal) before moving to Phase 2.
""")
    pause("Press Enter once the single-node server is stopped...")


# ---------------------------------------------------------------------------
# Phase 2: Three-node cluster tests
# ---------------------------------------------------------------------------

def phase2_cluster():
    print(f"""
{C.BOLD}{'═' * 60}{C.RESET}
{C.BOLD}  PHASE 2: Three-Node Cluster{C.RESET}
{C.BOLD}{'═' * 60}{C.RESET}

  This phase tests replication: writes go to the primary; reads
  from replicas should return the same data. It also verifies that
  replicas correctly reject client writes.
""")

    p1, p2, p3 = ask_three_ports()

    ep1 = f"localhost:{p1}"
    ep2 = f"localhost:{p2}"
    ep3 = f"localhost:{p3}"

    # The primary is lexicographically smallest. Sort to find it.
    sorted_eps = sorted([ep1, ep2, ep3])
    primary_ep = sorted_eps[0]
    replica_eps = sorted_eps[1:]

    cluster_arg = f"localhost:{p1},localhost:{p2},localhost:{p3}"

    print(f"""
  Start three server instances, one per terminal:

  {C.GREEN}{C.BOLD}python3 server.py --listen {ep1} --cluster {cluster_arg}{C.RESET}

  {C.GREEN}{C.BOLD}python3 server.py --listen {ep2} --cluster {cluster_arg}{C.RESET}

  {C.GREEN}{C.BOLD}python3 server.py --listen {ep3} --cluster {cluster_arg}{C.RESET}

  {C.DIM}The primary will be {primary_ep} (lexicographically smallest).{C.RESET}
  {C.DIM}Replicas: {replica_eps[0]}, {replica_eps[1]}{C.RESET}
""")
    pause("Press Enter once all three servers are running...")

    # Check all three are reachable.
    for ep in [ep1, ep2, ep3]:
        info(f"Checking {ep} ...")
        if not server_ready(ep, timeout=15):
            print(f"\n  {C.RED}Could not reach {ep} after 15 seconds.{C.RESET}")
            print("  Make sure all three servers started without errors.")
            sys.exit(1)
    info("All three nodes are up.\n")

    primary  = make_stub(primary_ep)
    replica1 = make_stub(replica_eps[0])
    replica2 = make_stub(replica_eps[1])

    # Reset cluster state.
    try:
        primary.Reset(empty_pb2.Empty())
    except grpc.RpcError:
        pass

    # ------------------------------------------------------------------
    section("basic writes to the primary")
    # ------------------------------------------------------------------

    check_ok(
        f"put 'city' = 'Newark' via primary ({primary_ep})",
        lambda: primary.Put(pb.PutRequest(key="city", value=b"Newark"))
    )
    check_ok(
        "put 'river' = 'Raritan' via primary",
        lambda: primary.Put(pb.PutRequest(key="river", value=b"Raritan"))
    )

    # Give replicas a moment to receive the writes.
    time.sleep(0.2)

    # ------------------------------------------------------------------
    section("reads from all nodes return consistent data")
    # ------------------------------------------------------------------

    info("Reading 'city' from the primary...")
    check_value(
        f"get 'city' from primary ({primary_ep}) → 'Newark'",
        b"Newark",
        lambda: primary.Get(pb.GetRequest(key="city")).value
    )

    info(f"Reading 'city' from replica {replica_eps[0]}...")
    check_value(
        f"get 'city' from replica 1 ({replica_eps[0]}) → 'Newark'",
        b"Newark",
        lambda: replica1.Get(pb.GetRequest(key="city")).value
    )

    info(f"Reading 'city' from replica {replica_eps[1]}...")
    check_value(
        f"get 'city' from replica 2 ({replica_eps[1]}) → 'Newark'",
        b"Newark",
        lambda: replica2.Get(pb.GetRequest(key="city")).value
    )

    # ------------------------------------------------------------------
    section("update propagates to replicas")
    # ------------------------------------------------------------------

    check_ok(
        "update 'city' = 'New Brunswick' via primary",
        lambda: primary.Update(pb.UpdateRequest(key="city", value=b"New Brunswick"))
    )
    time.sleep(0.2)

    check_value(
        "get 'city' from replica 1 after update → 'New Brunswick'",
        b"New Brunswick",
        lambda: replica1.Get(pb.GetRequest(key="city")).value
    )
    check_value(
        "get 'city' from replica 2 after update → 'New Brunswick'",
        b"New Brunswick",
        lambda: replica2.Get(pb.GetRequest(key="city")).value
    )

    # ------------------------------------------------------------------
    section("delete propagates to replicas")
    # ------------------------------------------------------------------

    check_ok(
        "delete 'river' via primary",
        lambda: primary.Delete(pb.DeleteRequest(key="river"))
    )
    time.sleep(0.2)

    check_code(
        "get 'river' from replica 1 after delete → NOT_FOUND",
        grpc.StatusCode.NOT_FOUND,
        lambda: replica1.Get(pb.GetRequest(key="river"))
    )
    check_code(
        "get 'river' from replica 2 after delete → NOT_FOUND",
        grpc.StatusCode.NOT_FOUND,
        lambda: replica2.Get(pb.GetRequest(key="river"))
    )

    # ------------------------------------------------------------------
    section("replicas reject client writes")
    # ------------------------------------------------------------------

    info("Sending write RPCs directly to the replicas (should all fail)...")

    check_code(
        f"put directly to replica 1 ({replica_eps[0]}) → FAILED_PRECONDITION",
        grpc.StatusCode.FAILED_PRECONDITION,
        lambda: replica1.Put(pb.PutRequest(key="x", value=b"y"))
    )
    check_code(
        f"update directly to replica 1 → FAILED_PRECONDITION",
        grpc.StatusCode.FAILED_PRECONDITION,
        lambda: replica1.Update(pb.UpdateRequest(key="city", value=b"z"))
    )
    check_code(
        f"delete directly to replica 2 ({replica_eps[1]}) → FAILED_PRECONDITION",
        grpc.StatusCode.FAILED_PRECONDITION,
        lambda: replica2.Delete(pb.DeleteRequest(key="city"))
    )
    check_code(
        f"reset directly to replica 2 → FAILED_PRECONDITION",
        grpc.StatusCode.FAILED_PRECONDITION,
        lambda: replica2.Reset(empty_pb2.Empty())
    )

    # ------------------------------------------------------------------
    section("majority commit: writing with one replica down")
    # ------------------------------------------------------------------

    warn("For this test, kill ONE of the two replicas.")
    warn(f"That means stopping either {replica_eps[0]} or {replica_eps[1]}.")
    warn("Leave the primary and the other replica running.")
    print()
    pause("Press Enter once one replica has been stopped...")

    info("Attempting writes with one replica down (majority = 2 of 3 = still reachable)...")

    check_ok(
        "put 'county' = 'Middlesex' with one replica down",
        lambda: primary.Put(pb.PutRequest(key="county", value=b"Middlesex"))
    )
    check_value(
        "get 'county' from primary → 'Middlesex'",
        b"Middlesex",
        lambda: primary.Get(pb.GetRequest(key="county")).value
    )

    info("Checking the surviving replica still serves reads...")
    # Try both replicas; one is down so one will fail — that is expected.
    got_replica_read = False
    for label, stub in [(replica_eps[0], replica1), (replica_eps[1], replica2)]:
        try:
            val = stub.Get(pb.GetRequest(key="county")).value
            if val == b"Middlesex":
                passed(f"get 'county' from surviving replica ({label}) → 'Middlesex'")
                record(True)
                got_replica_read = True
            else:
                failed(f"get 'county' from {label}", f"got {val!r}")
                record(False)
            break
        except grpc.RpcError:
            info(f"{label} appears to be the stopped replica — trying the other one.")

    if not got_replica_read:
        failed("could not reach any surviving replica",
               "Make sure only ONE replica is stopped.")
        record(False)

    # ------------------------------------------------------------------
    section("list and stats from a replica")
    # ------------------------------------------------------------------

    info("Verifying read-only RPCs work from a replica...")
    # Find the surviving replica.
    surviving = None
    for label, stub in [(replica_eps[0], replica1), (replica_eps[1], replica2)]:
        try:
            stub.Stats(empty_pb2.Empty(), timeout=2.0)
            surviving = (label, stub)
            break
        except grpc.RpcError:
            pass

    if surviving is None:
        warn("Could not reach any replica for list/stats test. "
             "Make sure exactly one replica is still running.")
        skipped("list from replica")
        skipped("stats from replica")
        global SKIP_COUNT
        SKIP_COUNT += 2
    else:
        label, stub = surviving
        try:
            resp = stub.List(empty_pb2.Empty())
            keys = {e.key for e in resp.entries}
            if "city" in keys and "county" in keys:
                passed(f"list from replica ({label}) returns expected keys")
                record(True)
            else:
                failed(f"list from replica ({label})",
                       f"got keys: {sorted(keys)}")
                record(False)
        except grpc.RpcError as e:
            failed(f"list from replica ({label})",
                   f"{e.code().name}: {e.details()}")
            record(False)

        try:
            s = stub.Stats(empty_pb2.Empty())
            if s.live_objects >= 2:
                passed(f"stats from replica ({label}) returns live_objects >= 2")
                record(True)
            else:
                failed(f"stats from replica ({label})",
                       f"live_objects={s.live_objects}")
                record(False)
        except grpc.RpcError as e:
            failed(f"stats from replica ({label})",
                   f"{e.code().name}: {e.details()}")
            record(False)

    # ------------------------------------------------------------------
    section("no majority: writes must fail, reads from primary still work")
    # ------------------------------------------------------------------

    if surviving is not None:
        surviving_label = surviving[0]
        warn(f"Now kill the remaining replica ({surviving_label}).")
    else:
        warn("Now kill whichever replica is still running.")
    warn("The primary must stay up.")
    warn("With both replicas down, the primary cannot reach a majority (needs")
    warn("2 of 3 nodes; only 1 is reachable), so writes must return UNAVAILABLE.")
    print()
    pause("Press Enter once the second replica has been stopped...")

    info("Attempting writes — should all fail with UNAVAILABLE...")
    check_code(
        "put 'state' = 'NJ' with no majority -> UNAVAILABLE",
        grpc.StatusCode.UNAVAILABLE,
        lambda: primary.Put(pb.PutRequest(key="state", value=b"NJ"))
    )
    check_code(
        "update 'county' with no majority -> UNAVAILABLE",
        grpc.StatusCode.UNAVAILABLE,
        lambda: primary.Update(pb.UpdateRequest(key="county", value=b"Monmouth"))
    )
    check_code(
        "delete 'county' with no majority -> UNAVAILABLE",
        grpc.StatusCode.UNAVAILABLE,
        lambda: primary.Delete(pb.DeleteRequest(key="county"))
    )

    info("Reads from the primary should still succeed (reads do not require majority)...")
    check_value(
        "get 'city' from primary with no majority -> 'New Brunswick'",
        b"New Brunswick",
        lambda: primary.Get(pb.GetRequest(key="city")).value
    )
    try:
        resp = primary.List(empty_pb2.Empty())
        keys = {e.key for e in resp.entries}
        if "city" in keys:
            passed("list from primary with no majority returns expected keys")
            record(True)
        else:
            failed("list from primary with no majority",
                   f"got keys: {sorted(keys)}")
            record(False)
    except grpc.RpcError as e:
        failed("list from primary with no majority",
               f"{e.code().name}: {e.details()}")
        record(False)

    info("Verifying both replicas are unreachable...")
    for ep, stub in [(replica_eps[0], replica1), (replica_eps[1], replica2)]:
        try:
            stub.Stats(empty_pb2.Empty(), timeout=2.0)
            warn(f"{ep} is still responding — did you stop both replicas?")
        except grpc.RpcError:
            passed(f"replica {ep} is unreachable (expected)")
            record(True)


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def print_summary():
    total = PASS_COUNT + FAIL_COUNT + SKIP_COUNT
    print(f"""
{C.BOLD}{'═' * 60}{C.RESET}
{C.BOLD}  RESULTS{C.RESET}
{C.BOLD}{'═' * 60}{C.RESET}

  {C.GREEN}Passed : {PASS_COUNT}{C.RESET}
  {C.RED}Failed : {FAIL_COUNT}{C.RESET}
  {C.MAGENTA}Skipped: {SKIP_COUNT}{C.RESET}
  {C.DIM}Total  : {total}{C.RESET}
""")
    if FAIL_COUNT == 0:
        print(f"  {C.GREEN}{C.BOLD}All tests passed. Good work.{C.RESET}\n")
    else:
        print(f"  {C.RED}{C.BOLD}{FAIL_COUNT} test(s) failed. "
              f"Review the FAIL lines above and check your implementation.{C.RESET}\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    print(f"""
{C.BOLD}{'═' * 60}{C.RESET}
{C.BOLD}  CS 417 — Distributed Object Store Test Client{C.RESET}
{C.BOLD}{'═' * 60}{C.RESET}

  This script tests your server implementation in two phases.
  You will need two or more terminal windows open.

  Make sure objectstore_pb2.py and objectstore_pb2_grpc.py
  are in the same directory as this script.
""")
    try:
        phase1_single_node()
        phase2_cluster()
    except KeyboardInterrupt:
        print(f"\n\n  {C.MAGENTA}Interrupted.{C.RESET}")

    print_summary()
    sys.exit(0 if FAIL_COUNT == 0 else 1)


if __name__ == "__main__":
    main()
