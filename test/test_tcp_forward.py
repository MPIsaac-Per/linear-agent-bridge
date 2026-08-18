import asyncio
import contextlib
import importlib.util
import io
import pathlib
import subprocess
import sys
import unittest
from unittest import mock


MODULE_PATH = pathlib.Path(__file__).parents[1] / "deploy" / "tcp_forward.py"
SPEC = importlib.util.spec_from_file_location("tcp_forward", MODULE_PATH)
tcp_forward = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(tcp_forward)
tcp_forward.TARGET_HOST = "upstream.test"
tcp_forward.TARGET_PORT = 3979


class FakeWriter:
    def __init__(self):
        self.closed = False

    def write(self, _data):
        pass

    async def drain(self):
        pass

    def close(self):
        self.closed = True

    async def wait_closed(self):
        pass


class EofReader:
    async def read(self, _size):
        return b""


class BlockingReader:
    def __init__(self):
        self.cancelled = False

    async def read(self, _size):
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            self.cancelled = True
            raise


class ErrorReader:
    async def read(self, _size):
        raise RuntimeError("sensitive payload-like diagnostic")


class TcpForwardTests(unittest.IsolatedAsyncioTestCase):
    def test_cli_rejects_a_non_loopback_upstream(self):
        result = subprocess.run(
            [
                sys.executable,
                str(MODULE_PATH),
                "8899",
                "100.64.0.2",
                "3979",
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )

        self.assertEqual(result.returncode, 2)
        self.assertIn("local tunnel endpoint at 127.0.0.1", result.stderr)

    async def test_logs_bounded_lifecycle_and_cancels_sibling_on_half_close(self):
        client_reader = EofReader()
        upstream_reader = BlockingReader()
        client_writer = FakeWriter()
        upstream_writer = FakeWriter()

        with mock.patch.object(
            tcp_forward.asyncio,
            "open_connection",
            new=mock.AsyncMock(return_value=(upstream_reader, upstream_writer)),
        ), contextlib.redirect_stdout(io.StringIO()) as output:
            await tcp_forward.handle(client_reader, client_writer)

        logged = output.getvalue()
        self.assertRegex(logged, r"connection=[0-9a-f]{16} event=start")
        self.assertIn("event=upstream_connected", logged)
        self.assertIn("event=close reason=client_eof", logged)
        self.assertNotIn("payload", logged)
        self.assertTrue(upstream_reader.cancelled)
        self.assertTrue(client_writer.closed)
        self.assertTrue(upstream_writer.closed)

    async def test_idle_connection_is_bounded_and_closed(self):
        client_reader = BlockingReader()
        upstream_reader = BlockingReader()
        client_writer = FakeWriter()
        upstream_writer = FakeWriter()

        with mock.patch.object(tcp_forward, "IDLE_TIMEOUT_SECONDS", 0.001), mock.patch.object(
            tcp_forward.asyncio,
            "open_connection",
            new=mock.AsyncMock(return_value=(upstream_reader, upstream_writer)),
        ), contextlib.redirect_stdout(io.StringIO()) as output:
            await tcp_forward.handle(client_reader, client_writer)

        self.assertRegex(
            output.getvalue(),
            r"event=close reason=(client|upstream)_idle_timeout",
        )
        self.assertTrue(client_writer.closed)
        self.assertTrue(upstream_writer.closed)

    async def test_unexpected_pipe_failure_still_cleans_up_with_bounded_log(self):
        client_writer = FakeWriter()
        upstream_writer = FakeWriter()
        upstream_reader = BlockingReader()
        with mock.patch.object(
            tcp_forward.asyncio,
            "open_connection",
            new=mock.AsyncMock(return_value=(upstream_reader, upstream_writer)),
        ), contextlib.redirect_stdout(io.StringIO()) as output:
            await tcp_forward.handle(ErrorReader(), client_writer)

        logged = output.getvalue()
        self.assertIn("event=pipe_failure error=RuntimeError", logged)
        self.assertIn("event=close reason=pipe_failure", logged)
        self.assertNotIn("sensitive payload-like diagnostic", logged)
        self.assertTrue(upstream_reader.cancelled)
        self.assertTrue(client_writer.closed)
        self.assertTrue(upstream_writer.closed)

    async def test_upstream_failure_logs_only_bounded_error_class_and_close(self):
        client_writer = FakeWriter()
        with mock.patch.object(
            tcp_forward.asyncio,
            "open_connection",
            new=mock.AsyncMock(side_effect=OSError("sensitive upstream details")),
        ), contextlib.redirect_stdout(io.StringIO()) as output:
            await tcp_forward.handle(EofReader(), client_writer)

        logged = output.getvalue()
        self.assertIn("event=upstream_failure error=OSError", logged)
        self.assertIn("event=close reason=upstream_failure", logged)
        self.assertNotIn("sensitive upstream details", logged)
        self.assertTrue(client_writer.closed)


if __name__ == "__main__":
    unittest.main()
