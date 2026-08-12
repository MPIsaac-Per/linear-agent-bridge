#!/usr/bin/env python3
"""Tiny TCP forwarder: listen on 127.0.0.1:LISTEN_PORT, pipe to HOST:PORT.

Useful when the bridge host can't terminate public TLS itself (for
example, a brand-new `tailscale funnel` that can't mint a certificate):
run this on any box that already has a working public HTTPS ingress and
point it at the bridge over your private network:
ingress :8443 -> 127.0.0.1:8899 -> (private net) -> bridge host :3979.

Usage: tcp_forward.py <listen_port> <target_host> <target_port>
"""
import asyncio
import sys


async def pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while True:
            data = await reader.read(65536)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except (ConnectionResetError, BrokenPipeError):
        pass
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


async def handle(client_r: asyncio.StreamReader, client_w: asyncio.StreamWriter) -> None:
    try:
        remote_r, remote_w = await asyncio.open_connection(TARGET_HOST, TARGET_PORT)
    except OSError:
        client_w.close()
        return
    await asyncio.gather(pipe(client_r, remote_w), pipe(remote_r, client_w))


async def main() -> None:
    server = await asyncio.start_server(handle, "127.0.0.1", LISTEN_PORT)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    LISTEN_PORT = int(sys.argv[1])
    TARGET_HOST = sys.argv[2]
    TARGET_PORT = int(sys.argv[3])
    asyncio.run(main())
