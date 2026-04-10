CS 417: Object Store Test Client
=================================

testclient.py is an interactive test script that checks your server
implementation against the project specification. It is not a substitute
for the conformance suite we run during grading, but it covers all the
basic operations and will catch the most common implementation errors
before you submit.

Run it from the same directory as your objectstore_pb2.py and
objectstore_pb2_grpc.py stubs:

    python3 testclient.py


WHAT YOU NEED BEFORE RUNNING
------------------------------

- Your server.py implemented and working (or partially working — the
  script will tell you what passes and what fails).
- objectstore_pb2.py and objectstore_pb2_grpc.py in the same directory.
- At least two terminal windows: one to run the test script, one or more
  to run server instances.


PHASE 1: SINGLE-NODE SERVER
-----------------------------

The script asks for a port number and then prints the exact command to
start a single-node server. Open a separate terminal, run that command,
and come back. The script will wait for you and will automatically check
that the server is up before running any tests.

The following operations are tested in order:

put — success cases
    Stores three keys ('fruit', 'veggie', 'grain') and checks that each
    returns OK.

put — error cases
    Verifies that putting a key that already exists returns ALREADY_EXISTS,
    that an empty key returns INVALID_ARGUMENT, that a key containing a
    space returns INVALID_ARGUMENT, and that a value larger than 1 MiB
    returns INVALID_ARGUMENT.

get
    Retrieves 'fruit' and 'veggie' and checks that the values match what
    was stored. Also checks that getting a key that does not exist returns
    NOT_FOUND.

update
    Replaces the value of 'fruit' and then reads it back to confirm the
    new value is stored. Also checks that updating a key that does not
    exist returns NOT_FOUND.

delete
    Deletes 'grain', then verifies that getting or deleting it again both
    return NOT_FOUND.

list
    Checks that the list response contains exactly the keys that should be
    present at that point ('fruit' and 'veggie', not 'grain'), and that
    the reported sizes match the actual value lengths.

stats
    Reads the stats counters and checks them against the exact number of
    successful operations performed so far. Only successful operations
    should increment a counter — a put that returns ALREADY_EXISTS must
    not increment the put counter, for example.

reset
    Calls reset and then verifies that all three of the following are true:
    getting any previously stored key returns NOT_FOUND, list returns an
    empty response, and all stats counters are zero.


PHASE 2: THREE-NODE CLUSTER
-----------------------------

The script asks for three distinct port numbers. It then prints three
separate server start commands — one per terminal window. The primary is
determined by the same lexicographic rule your server uses: the
lexicographically smallest endpoint in the cluster list. The script tells
you which node will be the primary so you know what to expect.

Start all three servers and press Enter. The script checks that all three
are reachable before running any tests.

The following are tested:

Writes to the primary
    Stores two keys via the primary and checks that both return OK.

Reads from all nodes return the same data
    Reads each key from the primary and from both replicas and checks
    that all three return the same value. This verifies that the primary
    is propagating writes to replicas correctly.

Update propagation
    Updates a value via the primary, then reads it from both replicas to
    confirm the new value arrived.

Delete propagation
    Deletes a key via the primary, then checks that both replicas return
    NOT_FOUND for that key.

Replicas reject client writes
    Sends put, update, delete, and reset RPCs directly to the replicas
    (bypassing the primary). Each must return FAILED_PRECONDITION. If any
    of these return OK, your replica is not checking whether it is the
    primary before processing writes.

Majority commit with one replica down
    The script asks you to kill one of the two replicas. Once you have
    done that and pressed Enter, it attempts a write to the primary. With
    one replica down, a 3-node cluster still has a majority of 2 (the
    primary plus the surviving replica), so the write must succeed. The
    script then reads the value back from the primary and from whichever
    replica is still running to confirm both agree.

List and stats from a replica
    Verifies that the surviving replica correctly serves list and stats
    requests, returning consistent data.


PHASE 3: NO MAJORITY
----------------------

Still within the three-node cluster, the script now asks you to kill the
second (surviving) replica, leaving only the primary running. With both
replicas down, the primary can reach only 1 of 3 nodes — itself — which
is below the majority threshold of 2. Writes must now fail.

The following are tested:

Writes fail with UNAVAILABLE
    Attempts put, update, and delete operations against the primary. Each
    must return UNAVAILABLE. If any return OK, your primary is committing
    writes without verifying that a majority acknowledged, which violates
    the replication protocol.

Reads from the primary still succeed
    Reads (get and list) do not require majority acknowledgment, so they
    must still work even when both replicas are down. The script reads a
    key that was committed successfully before any replicas were killed,
    so the result should be definitive. If get returns NOT_FOUND here,
    your server is either not persisting the write correctly or is rolling
    back state it should not touch.

Both replicas are unreachable
    The script probes both replica endpoints and expects each to refuse
    the connection. This is a sanity check to confirm you actually stopped
    the right processes before pressing Enter.


UNDERSTANDING THE OUTPUT
-------------------------

Each test prints one of three results:

    PASS   The operation behaved as specified.
    FAIL   The operation returned the wrong result or the wrong status
           code. A brief explanation is printed below the line.
    SKIP   The test could not run, usually because a server was
           unreachable at that point. This is not a pass — fix whatever
           caused the skip and run again.

At the end, the script prints a summary of pass, fail, and skip counts
and exits with code 0 if everything passed, or code 1 if anything failed.


COMMON FAILURE PATTERNS
-------------------------

"put 'fruit' again -> ALREADY_EXISTS" fails with OK
    Your server is not checking whether a key already exists before
    storing it.

Stats counter tests fail
    The most common cause is incrementing a counter before checking
    whether the operation will succeed, rather than after. Only
    successful operations should increment counters.

"get 'fruit' after reset -> NOT_FOUND" fails with the old value
    Your reset() implementation is not clearing the store, or it is
    clearing a local copy rather than the shared data structure.

Replica read tests fail with NOT_FOUND right after a write
    Your primary is not sending ApplyWrite to the replicas, or it is
    returning OK to the client before the replicas have received and
    applied the write.

Replica write rejection tests fail with OK
    Your replica is not checking whether it is the primary at the top
    of each write handler. The check must happen before any state is
    modified.

Majority commit test fails with UNAVAILABLE
    Your primary is requiring all replicas to acknowledge rather than
    a majority. For a 3-node cluster, majority is 2, so the primary
    plus one replica is sufficient.

No-majority write tests pass with OK instead of UNAVAILABLE
    Your primary is not counting acknowledgments correctly, or it is
    not checking the count before returning OK. The fan-out to replicas
    should time out or fail, leaving the primary with only its own
    acknowledgment (1 of 3), which is below the majority of 2.

No-majority read tests fail with NOT_FOUND
    This usually means your server is applying the rollback from a
    failed write to the wrong key, or context.abort() is being called
    before the rollback runs rather than after it. The key being read
    was committed in an earlier successful write and should not be
    affected by the failing writes in this phase.


WHAT THIS SCRIPT DOES NOT TEST
--------------------------------

This script tests the correctness of individual operations under normal
conditions. It does not test concurrent access, large values, binary
values, or every edge case covered by the grading conformance suite.
Passing this script is a good sign, but it is not a guarantee that
your implementation is complete. Run your own concurrent correctness
test before submitting.
