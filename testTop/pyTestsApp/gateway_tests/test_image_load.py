"""
Small-scalar performance through the gateway while it streams large arrays (images).

The IOC serves a few large "image" waveforms that post a full-size monitor update at a fixed
rate. Helper processes subscribe to them through the gateway, so the gateway constantly
receives (upstream) and fans out (downstream) a high-volume array stream. Meanwhile, this
process measures the latency of four operations on small scalar PVs:

- read:            ca_get round trip through the gateway (served from the gateway's cache)
- write:           ca_put without callback through the gateway, until the new value is seen
                   by a direct monitor on the IOC (i.e. until it actually arrived upstream)
- write-complete:  ca_put_callback round trip through the gateway (until completion arrives)
- monitor (other IOC):
                   a second IOC, which carries no array traffic at all, serves a few more
                   scalars that this process keeps subscribed to through the gateway; a new
                   value is put directly on that IOC, and timed until the gateway delivers
                   the update. This shows whether one IOC's array stream slows down the
                   gateway's traffic for unrelated IOCs.

These are measured in four phases on the same gateway/IOC setup:

- idle:            no image subscribers yet (the gateway carries no array traffic)
- loaded:          image subscribers active, measuring client is not subscribed to images
- loaded (direct): the same operations directly against the IOCs (subscribing to the
                   second IOC directly, too), as a reference for how much of the slowdown
                   the gateway itself adds
- loaded (shared): the measuring client also monitors the images through the gateway, so
                   its scalar traffic, including the other IOC's monitor updates, shares a
                   TCP circuit (and gateway event queue) with the array stream

The load (``GATEWAY_PERF_IMAGES``, ``GATEWAY_PERF_IMAGE_WIDTH``/``_HEIGHT``,
``GATEWAY_PERF_IMAGE_PERIOD`` in seconds, ``GATEWAY_PERF_SUBSCRIBERS``) and the measurement
(``GATEWAY_PERF_SAMPLES``, ``GATEWAY_PERF_OP_TIMEOUT``) can be set from the environment.

A statistics table is printed (run pytest with ``-s`` or ``-rP`` to see it) and attached to
the junit XML as the ``perf_report`` property. Assertions are deliberately generous so this
doesn't turn flaky on a busy machine: every operation must complete, the subscribers must
actually have received the array stream, and the p95 latency of each operation under load
must stay below ``GATEWAY_PERF_P95_LIMIT`` seconds (default 0.5).

The same file runs unchanged against both gateway implementations, so their results can be
compared: the PCAS-based V2 gateway (``master``'s test harness, which passes command-line
arguments to the gateway) and the rsrv-based reimplementation (whose harness drives the
gateway with iocsh commands instead). Which one is in use is detected from ``conftest``.
"""

import contextlib
import inspect
import itertools
import logging
import multiprocessing
import os
import statistics
import tempfile
import threading
import time
from typing import Any, Dict, Generator, Iterator, List, Tuple

import epics
import numpy as np
import pytest

from . import config, conftest

logger = logging.getLogger(__name__)

# Image stream: IMAGE_COUNT waveforms of IMAGE_WIDTH x IMAGE_HEIGHT USHORT pixels, each
# processed (and posting a full-size monitor) every IMAGE_PERIOD seconds, read by SUBSCRIBERS
# separate client processes through the gateway. All can be overridden from the environment,
# e.g. to find out how much load a gateway implementation copes with.
IMAGE_COUNT = int(os.environ.get("GATEWAY_PERF_IMAGES", "2"))
IMAGE_WIDTH = int(os.environ.get("GATEWAY_PERF_IMAGE_WIDTH", "1024"))
IMAGE_HEIGHT = int(os.environ.get("GATEWAY_PERF_IMAGE_HEIGHT", "1024"))
# Must be one of the periodic scan rates of menuScan.
IMAGE_PERIOD = os.environ.get("GATEWAY_PERF_IMAGE_PERIOD", ".1")
IMAGE_SCAN = f"{IMAGE_PERIOD} second"
IMAGE_RATE = 1.0 / float(IMAGE_PERIOD)  # Hz
SUBSCRIBERS = int(os.environ.get("GATEWAY_PERF_SUBSCRIBERS", "3"))

IMAGE_ELEMENTS = IMAGE_WIDTH * IMAGE_HEIGHT
IMAGE_BYTES = IMAGE_ELEMENTS * 2
# Room for one full image plus protocol overhead.
MAX_ARRAY_BYTES = str(IMAGE_BYTES + 65536)

SAMPLES = int(os.environ.get("GATEWAY_PERF_SAMPLES", "200"))
P95_LIMIT = float(os.environ.get("GATEWAY_PERF_P95_LIMIT", "0.5"))
OP_TIMEOUT = 10.0
# Per measured operation: one that takes longer counts as a timeout (and enters the statistics
# with this value, i.e. as a lower bound). A phase is cut short after MAX_PHASE_TIMEOUTS.
MEASURE_TIMEOUT = float(os.environ.get("GATEWAY_PERF_OP_TIMEOUT", "5.0"))
MAX_PHASE_TIMEOUTS = 10
# Fraction of the nominal image rate each subscriber must have received for the load to count.
MIN_RATE_FRACTION = 0.5

IMAGE_NAMES = [f"perf:image{i}" for i in range(IMAGE_COUNT)]
SCALAR_NAME = "perf:scalar"

# The second IOC: OTHER_SCALARS scalars, served by the gateway as "other:..." (from
# "ioc2:..." upstream), all subscribed to by the measuring client for the whole test.
OTHER_IOC_PORT = config.default_ioc_port + 2
OTHER_SCALARS = 10
OTHER_NAMES = [f"perf:scalar{i}" for i in range(OTHER_SCALARS)]

DB_CONTENTS = "\n".join(
    f"""
record(waveform, "ioc:{name}") {{
  field(DESC, "Large array posted at a fixed rate")
  field(SCAN, "{IMAGE_SCAN}")
  field(NELM, "{IMAGE_ELEMENTS}")
  field(FTVL, "USHORT")
}}
"""
    for name in IMAGE_NAMES
) + f"""
record(longout, "ioc:{SCALAR_NAME}") {{
  field(DESC, "Small scalar for latency measurements")
}}
"""

OTHER_DB_CONTENTS = "".join(
    f"""
record(longout, "ioc2:{name}") {{
  field(DESC, "Scalar on the IOC without array traffic")
}}
"""
    for name in OTHER_NAMES
)

# Which test harness (and so, which gateway implementation) this runs with: the rsrv-based
# gateway's conftest takes iocsh commands for the gateway, the V2 one command-line arguments.
RSRV_GATEWAY = "gateway_commands" in inspect.signature(conftest.custom_environment).parameters
GATEWAY_KIND = "rsrv-based" if RSRV_GATEWAY else "V2 (PCAS-based)"

if RSRV_GATEWAY:
    # The legacy pvlist translation knows only one upstream client, so the second IOC gets
    # its own client and route through iocsh commands instead.
    GATEWAY_OPTIONS: Dict[str, Any] = {
        "gateway_commands": [
            f"gateCreateClient other localhost 0 {OTHER_IOC_PORT}",
            'gateAddPV "other:(.*)" other DEFAULT "ioc2:$1"',
        ]
    }
    OTHER_PVLIST = ""
else:
    # V2 has a single upstream CA client: a second "-cip" replaces the harness's default
    # address list with one that reaches both IOCs.
    GATEWAY_OPTIONS = {
        "gateway_args": [
            "-cip", f"localhost:{config.default_ioc_port} localhost:{OTHER_IOC_PORT}",
        ]
    }
    OTHER_PVLIST = r"other:\(.*\)    ALIAS ioc2:\1"

ACCESS_CONTENTS = """
ASG(DEFAULT) {
   RULE(1,WRITE)
}
"""

PVLIST_CONTENTS = rf"""
gateway:\(.*\)  ALIAS ioc:\1
{OTHER_PVLIST}
ioc:.*          DENY
gwtest:.*       ALLOW
"""


def _image_subscriber(
    gateway_port: int,
    pvnames: List[str],
    ready: "multiprocessing.synchronize.Event",
    stop: "multiprocessing.synchronize.Event",
    results: "multiprocessing.Queue",
) -> None:
    """Subprocess body: monitor the images through the gateway, report what arrived."""
    os.environ["EPICS_CA_AUTO_ADDR_LIST"] = "NO"
    os.environ["EPICS_CA_ADDR_LIST"] = f"localhost:{gateway_port}"
    os.environ["EPICS_CA_MAX_ARRAY_BYTES"] = MAX_ARRAY_BYTES
    epics.ca.initialize_libca()

    lock = threading.Lock()
    frames: Dict[str, int] = {name: 0 for name in pvnames}
    short_frames = 0
    first_frame: Dict[str, float] = {}

    def on_image(pvname: str = "", value=None, **_):
        nonlocal short_frames
        with lock:
            frames[pvname] += 1
            first_frame.setdefault(pvname, time.monotonic())
            if value is None or len(value) != IMAGE_ELEMENTS:
                short_frames += 1
            if len(first_frame) == len(pvnames):
                ready.set()

    # Low-level subscriptions, created once each channel is connected: with
    # epics.PV(..., auto_monitor=True), about one run in five had a PV that connected but
    # never got a single update -- also directly against a plain softIoc, without any gateway.
    chids = {}
    subscriptions = []
    for name in pvnames:
        chid = epics.ca.create_channel(name)
        if epics.ca.connect_channel(chid, timeout=OP_TIMEOUT):
            subscriptions.append(
                epics.ca.create_subscription(
                    chid, mask=epics.dbr.DBE_VALUE, callback=on_image, count=0
                )
            )
        chids[name] = chid

    stop.wait()
    t_end = time.monotonic()
    with lock:
        # Rate counted from each PV's first frame, so connection time isn't held against it.
        rates = {
            name: (frames[name] - 1) / (t_end - first_frame[name])
            if name in first_frame and t_end > first_frame[name]
            else 0.0
            for name in pvnames
        }
        results.put(
            {
                "frames": dict(frames),
                "rates": rates,
                "short_frames": short_frames,
                "connected": {
                    name: epics.ca.isConnected(chid) for name, chid in chids.items()
                },
            }
        )
    for subscription in subscriptions:
        epics.ca.clear_subscription(subscription[2])
    epics.ca.finalize_libca()


@contextlib.contextmanager
def image_subscribers(count: int) -> Generator[List[dict], None, None]:
    """Run ``count`` image subscriber processes; yields a list filled with their reports."""
    ctx = multiprocessing.get_context("spawn")
    stop = ctx.Event()
    results = ctx.Queue()
    readies = [ctx.Event() for _ in range(count)]
    pvnames = ["gateway:" + name for name in IMAGE_NAMES]
    procs = [
        ctx.Process(
            target=_image_subscriber,
            args=(config.default_gateway_port, pvnames, ready, stop, results),
            daemon=True,
        )
        for ready in readies
    ]
    for proc in procs:
        proc.start()
    reports: List[dict] = []

    def shutdown():
        stop.set()
        for _ in procs:
            try:
                reports.append(results.get(timeout=30.0))
            except Exception:
                logger.exception("Image subscriber did not report")
        for proc in procs:
            proc.join(10.0)
            if proc.is_alive():
                proc.kill()

    # Not fatal if some subscriber hasn't got every image by then: an overloaded gateway is a
    # result to report, not a reason to skip measuring. The final reports (per-PV frame counts
    # and connection states) show what was missing, and the rate assertions catch it.
    deadline = time.monotonic() + 30.0
    if not all(ready.wait(max(0.0, deadline - time.monotonic())) for ready in readies):
        logger.warning("Not all image subscribers received all images within 30 s")
    try:
        yield reports
    finally:
        shutdown()


class OpTimeout(Exception):
    """A measured operation did not complete within MEASURE_TIMEOUT."""


class IocWatcher:
    """Direct monitor on the IOC record: tells when a write has actually arrived upstream."""

    def __init__(self, ioc_pvname: str):
        self.cond = threading.Condition()
        self.seen = None
        # pyepics caches channels by name, so this chid may be shared with a ScalarProbe;
        # channels are left to be released by finalize_libca rather than cleared here.
        chid = epics.ca.create_channel(ioc_pvname)
        assert epics.ca.connect_channel(chid, timeout=OP_TIMEOUT), ioc_pvname
        # Keep everything create_subscription returns: libca only holds a raw pointer to
        # the (bound method) callback, which must outlive the subscription.
        self._subscription = epics.ca.create_subscription(
            chid, mask=epics.dbr.DBE_VALUE, callback=self._on_value
        )
        self._next = int(epics.ca.get(chid, timeout=OP_TIMEOUT)) + 1

    def next_value(self) -> int:
        """A value never written before, so every write is a visible change on the IOC."""
        value = self._next
        self._next += 1
        return value

    def close(self):
        epics.ca.clear_subscription(self._subscription[2])

    def _on_value(self, value=None, **_):
        with self.cond:
            self.seen = value
            self.cond.notify_all()


class ScalarProbe:
    """Times read / write / write-complete operations on one scalar PV."""

    def __init__(self, op_pvname: str, watcher: IocWatcher):
        self.chid = epics.ca.create_channel(op_pvname)
        assert epics.ca.connect_channel(self.chid, timeout=OP_TIMEOUT), op_pvname
        self._watcher = watcher

    def read(self) -> float:
        t0 = time.perf_counter()
        value = epics.ca.get(self.chid, timeout=MEASURE_TIMEOUT)
        dt = time.perf_counter() - t0
        if value is None:
            raise OpTimeout("read")
        return dt

    def write(self) -> float:
        value = self._watcher.next_value()
        cond = self._watcher.cond
        with cond:
            t0 = time.perf_counter()
            epics.ca.put(self.chid, value, wait=False)
            ok = cond.wait_for(lambda: self._watcher.seen == value, timeout=MEASURE_TIMEOUT)
            dt = time.perf_counter() - t0
        if not ok:
            raise OpTimeout(f"write of {value} did not arrive at the IOC")
        return dt

    def write_complete(self) -> float:
        value = self._watcher.next_value()
        t0 = time.perf_counter()
        ret = epics.ca.put(self.chid, value, wait=True, timeout=MEASURE_TIMEOUT)
        dt = time.perf_counter() - t0
        # pyepics returns -1 if the completion callback didn't arrive in time.
        if ret == -1:
            raise OpTimeout(f"write-complete of {value}")
        return dt


class MonitorProbe:
    """Times monitor updates: put directly on the IOC, until the subscription delivers it."""

    def __init__(self, sub_pvnames: List[str], put_pvnames: List[str], values: Iterator[int]):
        self._cond = threading.Condition()
        self._seen: Dict[str, Any] = {}
        self._values = values
        self._index = 0
        # Keep everything create_subscription returns (see IocWatcher). Channels may be
        # shared through pyepics' cache, so they're left to finalize_libca.
        self._subscriptions = []
        self._pairs = []
        for sub_pvname, put_pvname in zip(sub_pvnames, put_pvnames):
            sub_chid = epics.ca.create_channel(sub_pvname)
            assert epics.ca.connect_channel(sub_chid, timeout=OP_TIMEOUT), sub_pvname
            put_chid = epics.ca.create_channel(put_pvname)
            assert epics.ca.connect_channel(put_chid, timeout=OP_TIMEOUT), put_pvname
            self._subscriptions.append(
                epics.ca.create_subscription(
                    sub_chid, mask=epics.dbr.DBE_VALUE, callback=self._on_value
                )
            )
            self._pairs.append((sub_pvname, put_chid))
        with self._cond:
            ok = self._cond.wait_for(
                lambda: len(self._seen) == len(self._pairs), timeout=OP_TIMEOUT
            )
        assert ok, "Not all subscriptions delivered an initial value"

    def close(self):
        for subscription in self._subscriptions:
            epics.ca.clear_subscription(subscription[2])

    def _on_value(self, pvname: str = "", value=None, **_):
        with self._cond:
            self._seen[pvname] = value
            self._cond.notify_all()

    def monitor(self) -> float:
        # Round-robin over the subscribed scalars.
        sub_pvname, put_chid = self._pairs[self._index]
        self._index = (self._index + 1) % len(self._pairs)
        value = next(self._values)
        with self._cond:
            t0 = time.perf_counter()
            epics.ca.put(put_chid, value, wait=False)
            ok = self._cond.wait_for(
                lambda: self._seen.get(sub_pvname) == value, timeout=MEASURE_TIMEOUT
            )
            dt = time.perf_counter() - t0
        if not ok:
            raise OpTimeout(f"monitor update {value} for {sub_pvname}")
        return dt


OPERATIONS = ("read", "write", "write-complete", "monitor (other IOC)")


Phase = Tuple[str, Dict[str, List[float]], Dict[str, int]]  # name, timings, timeouts


def measure(name: str, probe: ScalarProbe, monitor: MonitorProbe, samples: int) -> Phase:
    """Interleave the operations so they all see the same load over time.

    A timed-out operation enters the timings as MEASURE_TIMEOUT (a lower bound) and is counted;
    after MAX_PHASE_TIMEOUTS of them, the rest of the phase is skipped.
    """
    ops = {
        "read": probe.read,
        "write": probe.write,
        "write-complete": probe.write_complete,
        "monitor (other IOC)": monitor.monitor,
    }
    timings: Dict[str, List[float]] = {op: [] for op in OPERATIONS}
    timeouts: Dict[str, int] = {op: 0 for op in OPERATIONS}
    for _ in range(samples):
        for op in OPERATIONS:
            try:
                timings[op].append(ops[op]())
            except OpTimeout as ex:
                logger.warning("%s: %s timed out", name, ex)
                timings[op].append(MEASURE_TIMEOUT)
                timeouts[op] += 1
        if sum(timeouts.values()) >= MAX_PHASE_TIMEOUTS:
            logger.warning("%s: too many timeouts, cutting the phase short", name)
            break
    return name, timings, timeouts


def summarize(timings: List[float]) -> Dict[str, float]:
    ordered = sorted(timings)

    def pct(p: float) -> float:
        return ordered[min(len(ordered) - 1, int(round(p / 100.0 * (len(ordered) - 1))))]

    return {
        "min": ordered[0],
        "median": statistics.median(ordered),
        "mean": statistics.fmean(ordered),
        "p95": pct(95),
        "p99": pct(99),
        "max": ordered[-1],
    }


def format_report(phases: List[Phase], loads: Dict[str, str]) -> str:
    cols = ("min", "median", "mean", "p95", "p99", "max")
    lines = [
        f"Scalar latency in ms ({SAMPLES} samples per operation and phase), "
        f"{GATEWAY_KIND} gateway {config.gateway_executable}",
        f"Load: {IMAGE_COUNT} x {IMAGE_WIDTH}x{IMAGE_HEIGHT} USHORT images "
        f"({IMAGE_BYTES / 2**20:.1f} MiB) at {IMAGE_RATE:g} Hz, {SUBSCRIBERS} subscribers",
    ]
    lines += [f"  {key}: {value}" for key, value in loads.items()]
    header = (
        f"{'phase':<17} {'operation':<20}{'n':>5}"
        + "".join(f"{c:>9}" for c in cols)
        + f"{'timeouts':>10}"
    )
    lines += [header, "-" * len(header)]
    for phase, timings, timeouts in phases:
        for op in OPERATIONS:
            stats = summarize(timings[op])
            lines.append(
                f"{phase:<17} {op:<20}{len(timings[op]):>5}"
                + "".join(f"{stats[c] * 1e3:9.2f}" for c in cols)
                + f"{timeouts[op]:>10}"
            )
    lines.append(f"(timed-out operations, > {MEASURE_TIMEOUT:g} s, enter the statistics as "
                 f"{MEASURE_TIMEOUT:g} s)")
    return "\n".join(lines)


def read_gateway_stat(name: str, scale: float = 1.0, fmt: str = "{:.1f}") -> str:
    """A formatted gateway statistic, or "n/a" (e.g. from a gateway too overloaded to answer)."""
    value = epics.caget(f"gwtest:{name}", timeout=MEASURE_TIMEOUT)
    return "n/a" if value is None else fmt.format(value * scale)


def gateway_load_stats() -> Dict[str, str]:
    """The gateway's own view of the load; the two implementations have different statistics."""
    if RSRV_GATEWAY:
        return {
            "gateway downstream during load": (
                f"{read_gateway_stat('downstreamVolumeRate', 2**-20)} MiB/s, "
                f"{read_gateway_stat('downstreamEventRate')} events/s"
            )
        }
    return {
        "gateway rates during load": ", ".join(
            f"{name} {read_gateway_stat(name)} Hz"
            for name in ("clientEventRate", "clientPostRate", "serverEventRate", "serverPostRate")
        )
    }


def gateway_queue_stats() -> Dict[str, str]:
    if not RSRV_GATEWAY:
        return {}  # V2 has no per-client event queue statistics
    return {
        "gateway queue drops": read_gateway_stat("queueDropped", fmt="{:.0f}"),
        "gateway max queue bytes": f"{read_gateway_stat('queueMaxBytes', 2**-20)} MiB",
    }


def test_scalar_performance_under_image_load(monkeypatch, record_property):
    # Everybody involved (IOC, gateway, all clients) needs room for a full image.
    monkeypatch.setenv("EPICS_CA_MAX_ARRAY_BYTES", MAX_ARRAY_BYTES)
    monkeypatch.setenv("IOC_EPICS_CA_MAX_ARRAY_BYTES", MAX_ARRAY_BYTES)

    with contextlib.ExitStack() as env_stack:
        other_db = env_stack.enter_context(tempfile.NamedTemporaryFile(mode="wt", suffix=".db"))
        other_db.write(OTHER_DB_CONTENTS)
        other_db.flush()
        env_stack.enter_context(
            conftest.run_ioc(db_file=other_db.name, ioc_port=OTHER_IOC_PORT)
        )
        env_stack.enter_context(
            conftest.custom_environment(
                access_contents=ACCESS_CONTENTS,
                pvlist_contents=PVLIST_CONTENTS,
                db_contents=DB_CONTENTS,
                **GATEWAY_OPTIONS,
            )
        )
        # custom_environment() only lets this process reach the first IOC and the gateway.
        env_stack.enter_context(
            conftest.context_set_env(
                "EPICS_CA_ADDR_LIST",
                f"{os.environ['EPICS_CA_ADDR_LIST']} localhost:{OTHER_IOC_PORT}",
            )
        )
        conftest.reset_libca()

        # A waveform with no input link keeps its contents, but still posts a full-size
        # monitor every time it's processed: seed each one once, directly on the IOC.
        frame = (np.arange(IMAGE_ELEMENTS) % 65536).astype(np.uint16)
        for name in IMAGE_NAMES:
            assert epics.caput("ioc:" + name, frame, wait=True, timeout=OP_TIMEOUT) == 1

        watcher = IocWatcher("ioc:" + SCALAR_NAME)
        gateway = ScalarProbe("gateway:" + SCALAR_NAME, watcher)
        direct = ScalarProbe("ioc:" + SCALAR_NAME, watcher)
        # Both monitor probes put to the same records: share one sequence of fresh values.
        other_values = itertools.count(1)
        other_put_names = ["ioc2:" + name for name in OTHER_NAMES]
        other_gateway = MonitorProbe(
            ["other:" + name for name in OTHER_NAMES], other_put_names, other_values
        )
        other_direct = None
        phases: List[Phase] = []
        loads: Dict[str, str] = {}
        try:
            measure("warm-up", gateway, other_gateway, 20)
            phases.append(measure("idle", gateway, other_gateway, SAMPLES))

            with image_subscribers(SUBSCRIBERS) as reports:
                time.sleep(2.0)  # let the stream (and the gateway's rate statistics) settle
                phases.append(measure("loaded", gateway, other_gateway, SAMPLES))
                loads.update(gateway_load_stats())
                other_direct = MonitorProbe(other_put_names, other_put_names, other_values)
                phases.append(measure("loaded (direct)", direct, other_direct, SAMPLES))

                shared_frames = 0

                def on_image(**_):
                    nonlocal shared_frames
                    shared_frames += 1

                with contextlib.ExitStack() as stack:
                    for name in IMAGE_NAMES:
                        stack.enter_context(
                            conftest.ca_subscription(
                                "gateway:" + name, on_image, form="native", count=0
                            )
                        )
                    deadline = time.monotonic() + 30.0
                    while shared_frames < IMAGE_COUNT and time.monotonic() < deadline:
                        time.sleep(0.05)
                    phases.append(measure("loaded (shared)", gateway, other_gateway, SAMPLES))
                loads["measuring client images received"] = str(shared_frames)
                loads.update(gateway_queue_stats())
        finally:
            watcher.close()
            other_gateway.close()
            if other_direct is not None:
                other_direct.close()

    for i, report in enumerate(reports):
        loads[f"subscriber {i}"] = ", ".join(
            f"{name.split(':')[-1]} {rate:.1f} Hz"
            + ("" if report["connected"][name] else " (disconnected)")
            for name, rate in report["rates"].items()
        ) + (f", {report['short_frames']} short frames" if report["short_frames"] else "")

    text = format_report(phases, loads)
    print("\n" + text)
    record_property("perf_report", text)

    assert len(reports) == SUBSCRIBERS, "Not all image subscribers reported back"
    assert shared_frames >= IMAGE_COUNT, f"Measuring client got no images\n{text}"
    for i, report in enumerate(reports):
        assert report["short_frames"] == 0, f"Subscriber {i} received truncated images"
        for name, rate in report["rates"].items():
            assert rate >= MIN_RATE_FRACTION * IMAGE_RATE, (
                f"Subscriber {i} only received {name} at {rate:.1f} Hz "
                f"(nominal {IMAGE_RATE:g} Hz): the image load didn't really flow\n{text}"
            )

    for phase, timings, timeouts in phases:
        for op in OPERATIONS:
            assert timeouts[op] == 0, (
                f"{phase} {op}: {timeouts[op]} operations timed out (> {MEASURE_TIMEOUT:g} s)"
                f"\n{text}"
            )
        if phase == "idle":
            continue
        for op in OPERATIONS:
            p95 = summarize(timings[op])["p95"]
            assert p95 < P95_LIMIT, (
                f"{phase} {op}: p95 latency {p95 * 1e3:.1f} ms exceeds "
                f"{P95_LIMIT * 1e3:.0f} ms\n{text}"
            )
