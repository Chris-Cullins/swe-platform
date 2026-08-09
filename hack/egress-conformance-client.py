#!/usr/bin/env python3
"""Structured one-shot and continuous TCP/UDP conformance client."""

import ipaddress
import os
import socket
import sys
import time


mode, protocol, raw_address, raw_port = sys.argv[1:]
address = ipaddress.ip_address(raw_address)
family = socket.AF_INET6 if address.version == 6 else socket.AF_INET
kind = socket.SOCK_DGRAM if protocol == "udp" else socket.SOCK_STREAM
target = (raw_address, int(raw_port))


def reachable() -> bool:
    try:
        with socket.socket(family, kind) as sock:
            sock.settimeout(2)
            if protocol == "tcp":
                sock.connect(target)
            else:
                nonce = f"swe-{os.getpid()}-{time.monotonic_ns()}".encode()
                sock.sendto(nonce, target)
                payload, peer = sock.recvfrom(65535)
                if payload != nonce or peer[0] != raw_address or peer[1] != int(raw_port):
                    return False
        return True
    except (OSError, TimeoutError):
        return False


if mode == "once":
    print("REACHED" if reachable() else "UNREACHED", flush=True)
elif mode == "continuous":
    while True:
        if not reachable():
            print("FAILED", flush=True)
            raise SystemExit(1)
        print("OK", flush=True)
        time.sleep(0.05)
else:
    raise SystemExit("unknown mode")
