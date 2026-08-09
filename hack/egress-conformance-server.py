#!/usr/bin/env python3
"""Small fixture-only dual-stack TCP acceptor and UDP echo server."""

import selectors
import socket
import sys


selector = selectors.DefaultSelector()


def register(family: socket.AddressFamily, kind: socket.SocketKind, port: int) -> None:
    sock = socket.socket(family, kind)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if family == socket.AF_INET6:
        sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
    sock.bind(("::" if family == socket.AF_INET6 else "0.0.0.0", port))
    if kind == socket.SOCK_STREAM:
        sock.listen(128)
        sock.setblocking(False)
        selector.register(sock, selectors.EVENT_READ, "tcp")
    else:
        sock.setblocking(False)
        selector.register(sock, selectors.EVENT_READ, "udp")


for argument in sys.argv[1:]:
    protocol, raw_ports = argument.split(":", 1)
    kind = {"tcp": socket.SOCK_STREAM, "udp": socket.SOCK_DGRAM}[protocol]
    for raw_port in raw_ports.split(","):
        for address_family in (socket.AF_INET, socket.AF_INET6):
            register(address_family, kind, int(raw_port))

open("/tmp/ready", "w").close()
while True:
    for key, _ in selector.select():
        if key.data == "tcp":
            connection, _ = key.fileobj.accept()
            connection.close()
        else:
            payload, peer = key.fileobj.recvfrom(65535)
            key.fileobj.sendto(payload, peer)
