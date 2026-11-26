#!/usr/bin/env python3
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Minimal CONNECT proxy for Actae conformance tests.

Proxies `CONNECT host:port` tunnels (what HTTP clients use for HTTPS
targets when `HTTPS_PROXY`/`ALL_PROXY` is set) and logs each tunnel to a
file so tests can assert that traffic actually flowed through the proxy.

Usage:
    python3 tests/connect_proxy.py --port 9080 --log /tmp/proxy.log
"""

import argparse
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

class ProxyHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        with open(LOG_PATH, "a") as f:
            f.write("%s\n" % (fmt % args))

    def do_CONNECT(self):
        host, _, port = self.path.partition(":")
        try:
            port = int(port) if port else 443
            upstream = socket.create_connection((host, port), timeout=10)
        except OSError as e:
            self.send_error(502, "upstream connect failed: %s" % e)
            return

        self.send_response(200, "Connection established")
        self.end_headers()

        # Full-duplex relay: TLS handshakes interleave reads/writes in both
        # directions, so a sequential pump is not enough.
        def pump(src, dst, name):
            try:
                src.settimeout(30)
                while True:
                    data = src.recv(65536)
                    if not data:
                        break
                    dst.sendall(data)
            except OSError:
                pass
            finally:
                try:
                    dst.shutdown(socket.SHUT_WR)
                except OSError:
                    pass

        try:
            t1 = threading.Thread(target=pump, args=(self.connection, upstream, "c2u"), daemon=True)
            t2 = threading.Thread(target=pump, args=(upstream, self.connection, "u2c"), daemon=True)
            t1.start()
            t2.start()
            t1.join()
            t2.join()
        finally:
            upstream.close()

    def do_GET(self):
        # Plain-HTTP targets through the proxy (unused by the tests, but
        # keeps the fixture honest for manual debugging).
        import urllib.request

        url = self.path
        req = urllib.request.Request(url, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                body = resp.read()
                self.send_response(resp.status)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
        except Exception as e:
            self.send_error(502, str(e))


def main():
    global LOG_PATH
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=9080)
    parser.add_argument("--log", default="/tmp/actae_proxy.log")
    args = parser.parse_args()

    LOG_PATH = args.log
    open(LOG_PATH, "w").close()

    server = ThreadingHTTPServer(("127.0.0.1", args.port), ProxyHandler)
    print("connect proxy listening on 127.0.0.1:%d (log: %s)" % (args.port, args.log))
    server.serve_forever()


if __name__ == "__main__":
    main()
