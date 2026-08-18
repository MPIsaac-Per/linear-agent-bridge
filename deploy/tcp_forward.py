#!/usr/bin/env python3
"""Diagnostic-only TCP forwarder bound to 127.0.0.1.

Use temporarily while diagnosing a private last hop; it is not the supported
production ingress. Run it on a host with an existing HTTPS ingress and point
it at the bridge over a private network:
ingress :8443 -> 127.0.0.1:8899 -> (private net) -> bridge host :3979.

Usage: tcp_forward.py <listen_port> <target_host> <target_port>
"""
import asyncio
import secrets
import sys

CONNECT_TIMEOUT_SECONDS = 10
IDLE_TIMEOUT_SECONDS = 300


def lifecycle(connection_id: str, event: str, detail: str = "") -> None:
    suffix = f" {detail}" if detail else ""
    print(f"connection={connection_id} event={event}{suffix}", flush=True)


async def pipe(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    direction: str,
) -> str:
    try:
        while True:
            data = await asyncio.wait_for(
                reader.read(65536), timeout=IDLE_TIMEOUT_SECONDS
            )
            if not data:
                return f"{direction}_eof"
            writer.write(data)
            await writer.drain()
    except asyncio.TimeoutError:
        return f"{direction}_idle_timeout"
    except (ConnectionResetError, BrokenPipeError):
        return f"{direction}_reset"


async def close_writer(writer: asyncio.StreamWriter) -> None:
    try:
        writer.close()
        await writer.wait_closed()
    except Exception:
        pass


async def handle(client_r: asyncio.StreamReader, client_w: asyncio.StreamWriter) -> None:
    connection_id = secrets.token_hex(8)
    lifecycle(connection_id, "start")
    try:
        remote_r, remote_w = await asyncio.wait_for(
            asyncio.open_connection(TARGET_HOST, TARGET_PORT),
            timeout=CONNECT_TIMEOUT_SECONDS,
        )
    except (asyncio.TimeoutError, OSError) as error:
        lifecycle(connection_id, "upstream_failure", f"error={type(error).__name__}")
        await close_writer(client_w)
        lifecycle(connection_id, "close", "reason=upstream_failure")
        return
    lifecycle(connection_id, "upstream_connected")

    client_to_upstream = asyncio.create_task(
        pipe(client_r, remote_w, "client")
    )
    upstream_to_client = asyncio.create_task(
        pipe(remote_r, client_w, "upstream")
    )
    tasks = {client_to_upstream, upstream_to_client}
    reason = "pipe_failure"
    try:
        done, _pending = await asyncio.wait(
            tasks, return_when=asyncio.FIRST_COMPLETED
        )
        reason = next(iter(done)).result()
    except asyncio.CancelledError:
        reason = "cancelled"
        raise
    except Exception as error:
        lifecycle(connection_id, "pipe_failure", f"error={type(error).__name__}")
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await close_writer(remote_w)
        await close_writer(client_w)
        lifecycle(connection_id, "close", f"reason={reason}")


async def main() -> None:
    server = await asyncio.start_server(handle, "127.0.0.1", LISTEN_PORT)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    LISTEN_PORT = int(sys.argv[1])
    TARGET_HOST = sys.argv[2]
    TARGET_PORT = int(sys.argv[3])
    asyncio.run(main())
