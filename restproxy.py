#!/usr/bin/env python3
"""
restproxy.py - REST front end for the distributed object store.

This file is provided as a working skeleton. The HTTP server infrastructure,
routing, gRPC stub setup, and status-code translation are already implemented.
Your job is to fill in the handler methods marked TODO.

Usage:
    python restproxy.py --cluster host1:port1,host2:port2,... [--port 8080]

Examples:
    # Single-node
    python restproxy.py --cluster localhost:50051

    # Three-node cluster
    python restproxy.py --cluster ilab1.cs.rutgers.edu:50051,ilab2.cs.rutgers.edu:50051,ilab3.cs.rutgers.edu:50051
"""

import argparse
import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse

import grpc
import objectstore_pb2 as pb
import objectstore_pb2_grpc as pb_grpc
from google.protobuf import empty_pb2


# ---------------------------------------------------------------------------
# Cluster helpers  (do not modify)
# ---------------------------------------------------------------------------

def parse_cluster(cluster_arg):
    """
    Parse --cluster and return (primary_endpoint, list_of_all_endpoints).
    The primary is defined as the lexicographically smallest endpoint string
    after normalization. This must match the rule used by server.py.
    """
    endpoints = sorted(e.lower().strip() for e in cluster_arg.split(","))
    return endpoints[0], endpoints


def make_stub(endpoint):
    """Open an insecure gRPC channel to endpoint and return a client stub."""
    channel = grpc.insecure_channel(endpoint)
    return pb_grpc.ObjectStoreStub(channel)


# ---------------------------------------------------------------------------
# gRPC status code -> HTTP status code  (do not modify)
# ---------------------------------------------------------------------------

GRPC_TO_HTTP = {
    grpc.StatusCode.OK:                  200,
    grpc.StatusCode.NOT_FOUND:           404,
    grpc.StatusCode.ALREADY_EXISTS:      409,
    grpc.StatusCode.INVALID_ARGUMENT:    400,
    grpc.StatusCode.UNAVAILABLE:         503,
    grpc.StatusCode.FAILED_PRECONDITION: 412,
}

def grpc_status_to_http(code):
    return GRPC_TO_HTTP.get(code, 500)


# ---------------------------------------------------------------------------
# Request handler
# ---------------------------------------------------------------------------

class ObjectStoreHandler(BaseHTTPRequestHandler):
    """
    HTTP request handler for the object store REST proxy.

    The HTTP server creates a new instance of this class for every incoming
    request. Instance attributes are therefore per-request. State shared
    across requests (gRPC stubs, round-robin index) lives on self.server,
    which is the ObjectStoreHTTPServer instance that owns this handler.

    Useful self.server attributes:
        self.server.primary_stub   gRPC stub pointing at the primary node
        self.server.all_stubs      list of gRPC stubs for all nodes (reads)
        self.server.read_index     integer; increment for round-robin reads
    """

    # ------------------------------------------------------------------
    # Internal helpers  (do not modify)
    # ------------------------------------------------------------------

    def _read_stub(self):
        """Return a stub for a read operation, round-robined across all nodes."""
        idx = self.server.read_index % len(self.server.all_stubs)
        self.server.read_index += 1
        return self.server.all_stubs[idx]

    def _send(self, status: int, content_type: str, body: bytes):
        """Write a complete HTTP response."""
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, status: int, data):
        """Serialize data as JSON and send it."""
        self._send(status, "application/json", json.dumps(data).encode())

    def _send_grpc_error(self, rpc_error: grpc.RpcError):
        """Translate a gRPC error into an HTTP error response."""
        code = rpc_error.code()
        http_status = grpc_status_to_http(code)
        msg = rpc_error.details() or code.name
        self._send(http_status, "text/plain", msg.encode())

    def _key_from_path(self, path: str) -> str:
        """
        Extract the key from a path like /objects/<key>.
        Returns the key string, or None if the path has no key component.
        """
        suffix = path[len("/objects/"):]
        return suffix if suffix else None

    def _read_body(self) -> bytes:
        """Read the full request body as bytes."""
        length = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(length)

    def log_message(self, fmt, *args):
        # Comment out the next line to suppress per-request logging.
        sys.stderr.write(f"{self.address_string()} - {fmt % args}\n")

    # ------------------------------------------------------------------
    # GET /objects/<key>   -- retrieve a single object
    # GET /objects         -- list all objects
    # GET /stats           -- server statistics
    # ------------------------------------------------------------------

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")

        if path == "/stats":
            self._handle_stats()
        elif path == "/objects":
            self._handle_list()
        elif path.startswith("/objects/"):
            key = self._key_from_path(path)
            self._handle_get(key)
        else:
            self._send(404, "text/plain", b"Not found")

    def _handle_get(self, key: str):
        """
        Call Get(key) on a read node.
        On success, return the raw value bytes with Content-Type
        application/octet-stream and HTTP 200.
        On error, call self._send_grpc_error(e).
        """
        # TODO: implement this method.
        try:
            resp = self._read_stub().Get(pb.GetRequest(key=key))
            self._send(200, "application/octet-stream", resp.value)
        except grpc.RpcError as e:
            self._send_grpc_error(e)


    def _handle_list(self):
        """
        Call List() on a read node.
        On success, return a JSON array of objects, each with "key" and
        "size_bytes" fields, and HTTP 200.
        On error, call self._send_grpc_error(e).

        Example response body:
            [{"key": "foo", "size_bytes": 12}, {"key": "bar", "size_bytes": 4}]
        """
        # TODO: implement this method.
        try:
            resp = self._read_stub().List(empty_pb2.Empty())
            data = [{"key": e.key, "size_bytes": e.size_bytes} for e in resp.entries]
            self._send_json(200, data)
        except grpc.RpcError as e:
            self._send_grpc_error(e)

    def _handle_stats(self):
        """
        Call Stats() on a read node.
        On success, return a JSON object with the StatsResponse fields and
        HTTP 200.
        On error, call self._send_grpc_error(e).

        Example response body:
            {"live_objects": 3, "total_bytes": 128, "puts": 5, ...}
        """
        # TODO: implement this method.
        try:
            r = self._read_stub().Stats(empty_pb2.Empty())
            data = {
                "live_objects": r.live_objects,
                "total_bytes": r.total_bytes,
                "puts": r.puts,
                "gets": r.gets,
                "deletes": r.deletes,
                "updates": r.updates,
            }
            self._send_json(200, data)
        except grpc.RpcError as e:
            self._send_grpc_error(e)

    # ------------------------------------------------------------------
    # PUT /objects/<key>   -- store a new object
    # ------------------------------------------------------------------

    def do_PUT(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")

        if path.startswith("/objects/"):
            key = self._key_from_path(path)
            if not key:
                self._send(400, "text/plain", b"Key required")
                return
            self._handle_put(key)
        else:
            self._send(404, "text/plain", b"Not found")

    def _handle_put(self, key: str):
        """
        Read the raw request body, then call Put(key, value) on the primary.
        Use self._read_body() to get the value bytes.
        On success, send HTTP 200 with body b"OK".
        On error, call self._send_grpc_error(e).
        """
        value = self._read_body()
        try:
            self.server.primary_stub.Put(pb.PutRequest(key=key, value=value))
            self._send(200, "text/plain", b"OK")
        except grpc.RpcError as e:
            self._send_grpc_error(e)

    # ------------------------------------------------------------------
    # PATCH /objects/<key>  -- update an existing object's value
    # ------------------------------------------------------------------

    def do_PATCH(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")

        if path.startswith("/objects/"):
            key = self._key_from_path(path)
            if not key:
                self._send(400, "text/plain", b"Key required")
                return
            self._handle_update(key)
        else:
            self._send(404, "text/plain", b"Not found")

    def _handle_update(self, key: str):
        """
        Read the raw request body, then call Update(key, value) on the primary.
        On success, send HTTP 200 with body b"OK".
        On error, call self._send_grpc_error(e).
        """
        # TODO: implement this method.
        value = self._read_body()
        try:
            self.server.primary_stub.Update(pb.UpdateRequest(key=key, value=value))
            self._send(200, "text/plain", b"OK")
        except grpc.RpcError as e:
            self._send_grpc_error(e)

    # ------------------------------------------------------------------
    # DELETE /objects/<key>  -- delete a single object
    # DELETE /objects        -- reset (clear all objects)
    # ------------------------------------------------------------------

    def do_DELETE(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")

        if path == "/objects":
            self._handle_reset()
        elif path.startswith("/objects/"):
            key = self._key_from_path(path)
            self._handle_delete(key)
        else:
            self._send(404, "text/plain", b"Not found")

    def _handle_delete(self, key: str):
        """
        Call Delete(key) on the primary.
        On success, send HTTP 200 with body b"OK".
        On error, call self._send_grpc_error(e).
        """
        # TODO: implement this method.
        try:
            self.server.primary_stub.Delete(pb.DeleteRequest(key=key))
            self._send(200, "text/plain", b"OK")
        except grpc.RpcError as e:
            self._send_grpc_error(e)
        
        try:
            self.server.primary_stub.Reset(empty_pb2.Empty())
            self._send(200, "text/plain", b"OK")
        except grpc.RpcError as e:
            self._send_grpc_error(e)

    def _handle_reset(self):
        """
        Call Reset() on the primary.
        Always returns HTTP 200 with body b"OK".
        """
        # TODO: implement this method.
        try:
            self.server.primary_stub.Reset(empty_pb2.Empty())
            self._send(200, "text/plain", b"OK")
        except grpc.RpcError as e:
            self._send_grpc_error(e)


# ---------------------------------------------------------------------------
# HTTP server subclass  (do not modify)
# ---------------------------------------------------------------------------

class ObjectStoreHTTPServer(HTTPServer):
    """
    Extends HTTPServer to hold shared state: gRPC stubs and the round-robin
    read index. The handler accesses these via self.server.<attribute>.
    """
    def __init__(self, server_address, handler, primary, all_endpoints):
        super().__init__(server_address, handler)
        self.primary_stub = make_stub(primary)
        self.all_stubs    = [make_stub(ep) for ep in all_endpoints]
        self.read_index   = 0


# ---------------------------------------------------------------------------
# Entry point  (do not modify)
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="REST proxy for the object store")
    parser.add_argument(
        "--cluster", required=True,
        help="Comma-separated list of host:port endpoints (same format as server.py)"
    )
    parser.add_argument(
        "--port", type=int, default=8080,
        help="HTTP port to listen on (default: 8080)"
    )
    args = parser.parse_args()

    primary, all_endpoints = parse_cluster(args.cluster)
    print(f"Primary node : {primary}", file=sys.stderr)
    print(f"All nodes    : {all_endpoints}", file=sys.stderr)

    server = ObjectStoreHTTPServer(
        ("", args.port),
        ObjectStoreHandler,
        primary,
        all_endpoints,
    )
    print(f"REST proxy listening on http://0.0.0.0:{args.port}", file=sys.stderr)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.", file=sys.stderr)


if __name__ == "__main__":
    main()
