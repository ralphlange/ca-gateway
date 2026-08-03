"""
Testing the Gateway's caPutLog network integration (gateLoadPutLogText/gateLoadPutLogJson).

Puts a trapped write through the Gateway and checks that both the traditional-text and
JSON-format sinks deliver the expected log message to a local TCP listener standing in for a
real caPutLog log server.
"""
from __future__ import annotations

import contextlib
import json
import socket
import tempfile
import textwrap
import threading
import time
from typing import Generator, Tuple

import pytest
from epics import ca

from . import conftest


@pytest.fixture(autouse=True)
def _require_caputlog(caputlog_supported: bool):
    if not caputlog_supported:
        pytest.skip("caPutLog support not built in (CAPUTLOG not set in RELEASE.local)")


# TRAPWRITE is required on the ASG rule for asTrapWriteWithData()/asTrapWriteAfter() to invoke
# any registered asTrapWriteListener at all (see GateLogic.cpp's caPutLog integration comment).
ACCESS_WITH_TRAPWRITE = textwrap.dedent(
    """\
    ASG(DEFAULT) {
        RULE(1,WRITE,TRAPWRITE)
    }
    """
)


@contextlib.contextmanager
def tcp_listener() -> Generator[Tuple[int, bytearray], None, None]:
    """Starts a plain TCP listener (standing in for a caPutLog log server) on localhost with a
    system-chosen port, and yields (port, received_bytes). received_bytes is filled in live as
    data arrives; read it only after the `with` block exits (or after all expected traffic has
    already landed) since it is written from a background thread.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    sock.listen(1)
    port = sock.getsockname()[1]
    received = bytearray()
    stop = threading.Event()

    def run():
        sock.settimeout(1.0)
        conn = None
        while not stop.is_set() and conn is None:
            try:
                conn, _ = sock.accept()
            except socket.timeout:
                continue
        if conn is None:
            return
        conn.settimeout(0.2)
        while not stop.is_set():
            try:
                chunk = conn.recv(4096)
                if not chunk:
                    break
                received.extend(chunk)
            except socket.timeout:
                continue
        conn.close()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    try:
        yield port, received
    finally:
        stop.set()
        thread.join(timeout=2.0)
        sock.close()


def test_caputlog_text_and_json_sinks():
    """
    Gateway configured with both a text and a JSON caPutLog sink -- a trapped put through the
    Gateway should be logged, with the real PV name/user/host/old/new value, to both.
    """
    with tcp_listener() as (text_port, text_received), tcp_listener() as (json_port, json_received):
        access_fp = tempfile.NamedTemporaryFile(mode="wt", suffix=".acf", delete=False)
        try:
            access_fp.write(ACCESS_WITH_TRAPWRITE)
            access_fp.close()

            with conftest.run_gateway(
                access=access_fp.name,
                put_log_text_addr=f"127.0.0.1:{text_port}",
                put_log_json_addr=f"127.0.0.1:{json_port}",
            ) as gw_proc, conftest.run_ioc() as ioc_proc:
                with conftest.local_channel_access():
                    gw = ca.create_channel("gateway:passive0")
                    assert ca.connect_channel(gw, timeout=5.0)

                    ca.put(gw, 42.0, wait=True)
                    time.sleep(1.0)  # logClientSend()/Flush() deliver asynchronously
        finally:
            import os

            os.unlink(access_fp.name)

    text = text_received.decode("latin-1")
    assert "gateway:passive0" in text
    assert "new=42" in text
    assert "old=0" in text

    json_lines = [line for line in json_received.decode("latin-1").splitlines() if line]
    assert json_lines, "expected at least one JSON put-log message"
    msg = json.loads(json_lines[0])
    assert msg["pv"] == "gateway:passive0"
    assert msg["new"] == 42
    assert msg["old"] == 0
    assert "user" in msg and msg["user"]
    assert "host" in msg and msg["host"]


def test_caputlog_on_change_filter_skips_unchanged_put():
    """
    config=0 (caPutLogOnChange) should suppress a put that doesn't actually change the value.
    """
    with tcp_listener() as (text_port, text_received):
        access_fp = tempfile.NamedTemporaryFile(mode="wt", suffix=".acf", delete=False)
        try:
            access_fp.write(ACCESS_WITH_TRAPWRITE)
            access_fp.close()

            with conftest.run_gateway(
                access=access_fp.name,
                put_log_text_addr=f"127.0.0.1:{text_port}",
                put_log_text_config=0,
            ) as gw_proc, conftest.run_ioc() as ioc_proc:
                with conftest.local_channel_access():
                    gw = ca.create_channel("gateway:passive0")
                    assert ca.connect_channel(gw, timeout=5.0)

                    # passive0's initial value is 0.0 -- putting the same value should be
                    # filtered out under caPutLogOnChange.
                    ca.put(gw, 0.0, wait=True)
                    time.sleep(1.0)
        finally:
            import os

            os.unlink(access_fp.name)

    assert text_received == b"", (
        "expected the on-change filter to suppress an unchanged-value put, got: "
        + text_received.decode("latin-1")
    )
