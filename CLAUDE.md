# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Branch context

This branch (`reimplement-using-rsrv`) is an **experimental, in-progress rewrite** of the
CA Gateway. It replaces the traditional PCAS/GDD-based implementation (still used on `master`
and described in `README.md`/`docs/Gateway.html`) with one built directly on EPICS Base's
`rsrv` (Channel Access server) library. It now supports PV name aliasing/rewriting, and serves
`DBR_STS_*`/`DBR_TIME_*`/`DBR_GR_*`/`DBR_CTRL_*` requests with real status/severity/timestamp/
limits/precision/units/enum-strings (see Architecture below) — this was added specifically so
the pre-existing `testTop` pytest suite (adapted to the new config mechanism) could actually
exercise the gateway end-to-end, since virtually every real CA client (`pyepics` included)
requests one of those formats by default. It also now supports real ASG/UAG/HAG
access-security enforcement (via `gateLoadAccess`/`asInitFile`, see Architecture below) and
pvlist-level `DENY`/`DENY FROM <hosts>` route hiding. It now also has gateway statistics and
rate PVs (`gateInitStats`), caPutLog put-logging in all three of the old Gateway's flavors
(traditional-text and JSON to a network log server, plus a local `-putlog`-equivalent file),
and bounded per-downstream-client event queues — all described under Architecture below.

Of the original Gateway's feature set, what remains unimplemented is the `CONTROL_PVS`
write-to-trigger flags (`commandFlag`/`report1Flag`/…/`quitFlag`: the iocsh prompt already
covers that role here), the `heartbeat` PV (dead code even in the old implementation — nothing
ever updated it), and the `RATE_STATS`/`CAS_DIAGNOSTICS` counters that have no rsrv equivalent
(`loopRate`, `cpuFract`, `load`, `serverEventRate`). "Multi-server listening" is **not** a gap:
the old `-sip`/`-sport` options only set `EPICS_CAS_INTF_ADDR_LIST`/`EPICS_CAS_SERVER_PORT`, and
the vendored `caservertask.c` already binds one socket per interface in that list; only the CLI
convenience wrapper is absent, and this gateway deliberately has no CLI at all.

Some commits were produced with AI coding agents; expect rough edges and re-verify behavior
against actual EPICS semantics rather than trusting comments or prior commit messages.

## Architecture

The core idea: run `rsrv` (the same CA server code an IOC uses to serve PVs to clients) as a
standalone server, but redirect all the database-access calls it normally makes (`dbNameToAddr`,
`dbChannel_create/get/put`, `db_add_event`, access-security lookups, etc.) into a custom C++
"data cache" that is itself a CA *client* to upstream IOCs. This avoids reimplementing the CA
server protocol (as the old PCAS-based Gateway had to) by reusing `rsrv` as-is.

Three layers, bottom to top:

1. **Vendored `rsrv` sources** (`src/camessage.c`, `camsgtask.c`, `caserverio.c`,
   `caservertask.c`, `cast_server.c`, `online_notify.c`, `rsrv.h`, `server.h`,
   `rsrvIocRegister.c`). These are copies of EPICS Base's internal `rsrv` module, compiled
   directly into the `gateway` binary (see comment `# Copy of rsrv` in `src/Makefile`) rather
   than linked from Base, so the code can be patched to work outside of a real IOC and across
   multiple Base versions (3.15/7.0 — see `gate_compat.h`). Treat these as close to
   upstream EPICS Base; prefer fixing incompatibilities in the shim/compat layer over editing
   them, so they stay easy to diff against a real Base checkout.

   Base 3.14 is **not, and cannot easily be, supported by this branch**: this vendored `rsrv`
   snapshot is built entirely around the `dbChannel` API (`struct dbChannel`,
   `dbChannel_create`, etc., see layer 2 below), which doesn't exist at all before Base 3.15 —
   3.14's real `rsrv` used the older `dbAddr`-only access API instead. Supporting 3.14 would
   mean vendoring a materially different (older) `rsrv` snapshot and a parallel code path
   through the shim/gateway logic, not a `gate_compat.h`-style compat fix. There is no
   `.ci-local/base-3.14.set` and no `B-3.14` CI matrix entry.

2. **Shim/compat layer** (`src/gateShim.c`, `src/gate_compat.h`, `src/net_convert.h`,
   `src/db_field_log.h`) — the "database hijack" layer. `rsrv` expects to call into a real
   EPICS database (`dbAccess`/`dbEvent`/`dbChannel`/access-security). `gateShim.c` implements
   just enough of that API surface (`dbNameToAddr`, `dbChannel_create/get/put`,
   `db_add_event`/`db_cancel_event`, `asDbGetMemberPvt`, etc.) to satisfy `rsrv`'s linker
   requirements and forward every call into layer 3 through `gate_db_interface.h`.
   `gate_compat.h` reconciles `DBR_*`/`DBF_*` constant differences across supported Base
   versions (there's deliberate `#undef`/`#define` juggling — don't "clean up" these without
   checking the surrounding version guards).

   Two subtleties here, found by actually running the gateway against a real IOC (not just
   getting it to compile) — if you touch `gateShim.c`, keep both in mind:
   - `dbChannelTest()` is a real, un-shimmed Base function (`dbFindRecordPart`/`dbPvdFind`
     against `pdbbase`) called directly by `rsrv`'s **UDP search-reply** path
     (`search_reply_udp`/`search_reply` in `camessage.c`) — it never goes through
     `dbNameToAddr`/`dbChannel_create` at all. Left un-overridden, it segfaults on `pdbbase`
     (our empty `dummy_dbbase`) the moment any real CA client does its normal "who has this
     PV" broadcast, i.e. before it even opens a connection. `gateShim.c` overrides it via
     `gate_channel_exists()` (a thin, side-effect-free wrapper around
     `gate_create_channel()`'s route matching).
   - Every `dbEventCtx`-consuming function (`db_add_extra_labor_event`,
     `db_flush_extra_labor_event`, `db_event_flow_ctrl_mode_on/off`, `db_event_change_priority`)
     is a real, un-shimmed Base function that would dereference a real `event_user` and crash,
     so all of them must be overridden in `gateShim.c`. `db_init_events()` returns our own
     per-client `struct GateEventCtx` (rsrv creates exactly one per TCP client, `client->evuser`
     in `create_tcp_client()`), which holds the extra-labor callback registration *and* that
     client's delivery queue — see layer 3. So `db_start_events()`/`db_close_events()` start and
     stop the queue's reader thread, and `db_event_flow_ctrl_mode_on/off` (CA_PROTO_EVENTS_OFF/
     ON) really do pause/resume delivery rather than being no-ops. `db_flush_extra_labor_event`,
     `db_event_change_priority`, `db_event_enable/disable` and `db_post_single_event` remain
     no-ops. If a future Base version adds another `dbEventCtx`-taking function, it needs the
     same treatment.
   - **`dbFldTypes.h`'s database field-type ordering is not the same across supported Base
     versions**, and this bit us for real: Base 7.0 inserted `DBF_INT64`/`DBF_UINT64` before
     `FLOAT`/`DOUBLE`, so e.g. `DBF_DOUBLE` is `8` on 3.15 but `10` on 7.0.
     `GateFormat.cpp`/`gateShim.c` need Base's *real*, per-version value here (to index
     `dbGetConvertRoutine[][]`/`dbDBRnewToDBRold[]`, both Base-provided), never
     `gate_dbr_to_dbf()`'s own portable canonical numbering (used purely for this code's
     internal switch/dispatch, unrelated to either table) — see `gate_compat.h`'s
     `gate_dbf_to_real_dbf()` and its comment for the translation between the two. Relatedly,
     always get `GETCONVERTFUNC dbGetConvertRoutine[][]`'s declaration from Base's own
     `<dbConvert.h>` (included early in `gate_compat.h`, before the `DBF_*`/`DBR_*`
     `#undef`/`#define` block below shadows the macros `dbConvert.h` sizes its array with) —
     never hand-declare it with a literal dimension: a previous hardcoded `[14][12]` (7.0's
     shape) compiled fine but silently corrupted every value conversion on 3.15 (real shape
     `[12][10]`), since the array's *row stride* differs by version and a wrong stride means
     every 2D index lands on the wrong element — not a compile or link error, memory
     corruption, only caught by actually running the DBE_LOG test against a real 3.15 IOC.

3. **Gateway logic** (`src/GateLogic.cpp` + `src/GateFormat.h`/`.cpp`, declared via
   `src/gate_db_interface.h`, entered from `src/GateMain.cpp`). A C++ engine that:
   - Owns upstream CA client contexts (`GateClient`, one per configured `client`). Every
     `gate_*` entry point that can run on an arbitrary `rsrv` server thread (not just the main
     thread `gate_init()` ran on) calls `ensure_ca_context()` first — CA client contexts are
     per-thread, so a bare `ca_create_channel()`/`ca_array_put()` from an unattached thread
     fails silently rather than crashing.
   - Maintains `GateChannel` objects keyed by the **downstream** requested PV name, each an
     upstream `chid` plus per-event-mask subscriptions (`MaskSub`). On upstream connect, a
     default `DBE_VALUE` subscription is created eagerly (not just lazily per downstream
     mask), so a plain get has cached data even with no prior downstream monitor. Upstream
     subscriptions always request `DBR_TIME_<native type>` (never bare native), which is what
     gives real status/severity/timestamp; a separate one-shot `DBR_CTRL_<native type>` get
     (repeated on `DBE_PROPERTY` events) caches precision/units/limits/enum-strings.
   - **Never delivers an event to a downstream client from an upstream thread.** Each
     downstream client has a `GateClientQueue` (multiple-writer/single-reader), created and torn
     down with its `dbEventCtx` (see layer 2); upstream callbacks only push, and a per-client
     reader thread pops and calls rsrv's `read_reply()`. This matters because `read_reply()`
     ends in `cas_send_bs_msg()` → a **blocking** `send()` (rsrv sets neither `SO_SNDTIMEO` nor
     non-blocking mode), while libca runs a single thread per upstream TCP circuit that both
     reads the wire *and* dispatches callbacks (`tcpRecvThread::run()`). Delivering inline
     therefore let one client that stopped draining its socket stall every other client on that
     PV *and* all traffic from that whole upstream IOC. A real IOC avoids this with `dbEvent.c`'s
     per-client event task; this is the equivalent. Queue entries hold a `shared_ptr<GateData>`,
     so one update fanned out to N subscribers is stored once and reference-counted until the
     last queue drains it. Queues are bounded by element count and/or payload bytes (`0` =
     unlimited; default 2000 / 16 MB) with a `oldest`/`newest` overflow policy, both set live by
     `gateSetQueueLimits`; counters appear as statistics PVs and in `gateQueueReport`. Note
     `gate_cancel_event()` must purge a subscription's queued entries and wait out any in-flight
     delivery before freeing it — rsrv cancels subscriptions from the client's TCP thread
     concurrently with that client's reader thread.
   - Serves gateway statistics PVs under a configurable prefix (`gateInitStats <prefix>
     [as_group]`), comparable to the old Gateway's `STAT_PVS`/`RATE_STATS`. These are
     `GateStatEntry` objects, not `GateChannel`s: they have no upstream `chid`, bypass route/DENY
     matching entirely, are read-only, and compute their value live on each get. Counts
     (`DBF_LONG`): `vctotal` (sum of per-channel downstream claim counts — one client's claim on
     one PV, so it exceeds `active` when clients share a PV), `pvtotal`, `connected`, `active`,
     `inactive`, plus `queueDepth`/`queueBytes`/`queueMaxDepth`/`queueMaxBytes`/`queueDropped`.
     Rates (`DBF_DOUBLE`, refreshed by a lazily-started 2 s `epicsTimer`): `upstreamEventRate`/
     `downstreamEventRate` (Hz) and `upstreamVolumeRate`/`downstreamVolumeRate` (B/s) — one
     upstream tick per event received from an IOC, one downstream tick per event actually posted
     to a client, so they legitimately differ (e.g. by fan-out, or when a client's subscription
     mask differs from the gateway's own eager `DBE_VALUE|DBE_PROPERTY` one, which makes two
     independent upstream subscriptions).
   - caPutLog put-logging, driven by our own `asTrapWriteListener` (requires `TRAPWRITE` on the
     ASG rule). Three independently configurable sinks: `gateLoadPutLogText`/`gateLoadPutLogJson`
     send to a network log server via Base's own `logClient`, and `gateLoadPutLogFile` writes the
     old Gateway's local `-putlog` file format. **None of caPutLog's runtime code can be used
     here**: its own entry points all call `caPutLogAsInit()`, which registers a listener that
     builds a `LOGDATA` via `dbGetField()` against a real record — and `dbGetRset()` returns NULL
     here. That also rules out `caPutLogTaskStart/Send` and `caPutLogDataCalloc/Free`, which look
     harmless but share a free list initialized *only* inside `caPutLogAsInit()` (confirmed with
     gdb: SIGSEGV in `freeListCalloc`). Only caPutLog's plain data definitions (`LOGDATA`/
     `VALUE`) and config enum are used — it is a header-only dependency, no library is linked.
     Consequently there is no burst coalescing, so config levels 1 and 2 are equivalent. The
     whole feature is compiled out unless `CAPUTLOG` is set (see Build).
   - `GateFormat.cpp` hand-assembles `DBR_STS_*/TIME_*/GR_*/CTRL_*` responses from that cached
     data — there's no real record/`rset` behind a channel for `dbAccess`'s normal metadata-
     filling to work against. It always uses the real struct types from `<db_access.h>` (never
     hand-computed offsets) and reuses `dbGetConvertRoutine` only for the raw value transcode.
     Note `dbGetConvertRoutine`'s indices are **not** CA-wire ordered (`db_access.h`:
     STRING=0,SHORT=1,FLOAT=2,ENUM=3,CHAR=4,LONG=5,DOUBLE=6) on both axes — it's declared
     `[DBF_DEVICE+1][DBR_ENUM+1]` using `dbFldTypes.h`'s *database* field-type ordering
     (STRING=0,CHAR=1,UCHAR=2,SHORT=3,...,FLOAT=9,DOUBLE=10,ENUM=11) for **both** dimensions;
     `gate_dbr_to_dbf()` converts CA-wire → database ordering and must be applied to *both* the
     source and the requested type before indexing it.
   - Routes incoming PV name lookups (from `dbNameToAddr`/`dbChannel_create`, i.e. downstream
     CA clients connecting through `rsrv`) to an upstream client using PCRE2 patterns
     (`gate_add_pv_cmd` / `routes`), assigning an access-security group name per route, and
     optionally rewriting the upstream name via an optional `target` (PCRE2 `$1`-style
     back-reference, e.g. pattern `"gateway:(.*)"` + target `"ioc:$1"`) — the `channels` cache
     stays keyed by the downstream name; only the name passed to `ca_create_channel()` changes.
     A `Route` can instead be a DENY route (`gate_add_deny_cmd`, legacy pvlist `DENY`/
     `DENY FROM <hosts>` semantics — distinct from ASG/UAG/HAG, see below): a blanket DENY
     hides a matching name from every client (same as no route at all); `DENY FROM` hides it
     only from clients whose self-reported CA hostname is in the given list. `routes` is
     scanned in full (not first-match) so a DENY route always overrides a matching ALLOW/ALIAS
     route, modeling this codebase's only usage of the legacy `EVALUATION ORDER ALLOW, DENY`
     convention. Search-time existence checks (`gate_channel_exists`, from the anonymous UDP
     search-reply path) have no client identity and only honor blanket DENY; claim-time checks
     (`gate_create_channel_for_client`, called from `dbNameToAddr`/`dbChannel_create` on the
     per-client TCP thread) also honor `DENY FROM`, using the client's self-reported hostname
     recovered from rsrv's own `rsrvCurrentClient` thread-local (`server.h`) — no vendored-file
     changes needed for this. Routes are re-evaluated even for an already-cached channel, so a
     cache hit can't bypass a per-client `DENY FROM` check.
   - Real ASG/UAG/HAG access-security enforcement (read/write rights on an *existing* channel,
     as opposed to the DENY routes above, which control whether it can be claimed at all) is
     just rsrv's own vendored, unmodified `asAddClient`/`asCheckGet`/`asCheckPut`/
     `casAccessRightsCB` machinery — already correctly wired to each `GateChannel`'s
     `ASMEMBERPVT` via `asDbGetMemberPvt()` (`gateShim.c`) — becoming live once an ACF is
     actually loaded. `gate_load_access()` (`GateLogic.cpp`) calls Base's `asInitFile()`,
     exposed as the `gateLoadAccess <file>` iocsh command (there is no JSON-config equivalent;
     it's a separate iocsh command, called before `gateLoadConfig` so `asInitialize` runs
     before any route/channel exists); until it's called, libCom's `asActive` stays false and
     every access check silently allows everything (this was the case for the whole history of
     this branch until now). A channel's ASG membership comes from its route's `as_group`
     (default `"DEFAULT"`).
   - Parses a small JSON config format (via `yajl`, Base's bundled JSON parser — see
     `gate_load_config`) with `clients: [{name, addr_list, auto_addr, port}]` and
     `pvs: [{pattern, action?, client, as_group, target?, hosts?}]` (`action` is `"allow"`
     (default) or `"deny"`; `"deny"` entries take `hosts` — an array, absent/empty = blanket —
     instead of `client`/`as_group`/`target`). This is **not** the same schema as the
     historical `GATEWAY.pvlist`/`GATEWAY.access` files, nor the same as pvAccess gateway JSON
     configs (`dummy.conf` is an example of that *different*, unrelated format).
   - `pvlist_to_json.py` is a migration helper from the old `pvlist` format, but its output
     shape (`servers`/`pvlist` keys) currently does **not** match what `gate_load_config`
     actually parses (`clients`/`pvs`) — treat it as unfinished, not as a working converter.
     (`testTop/pyTestsApp/gateway_tests/conftest.py` has its own, working
     `pvlist_text_to_routes()` translator used by the test harness — see Tests below.)
   - `GateClient` folds the client's `port` into each address in `EPICS_CA_ADDR_LIST`
     (`"addr:port"`) rather than relying on `EPICS_CA_SERVER_PORT` — that env var is
     process-wide and also governs the Gateway's own CAS listening port, so if the upstream
     client just inherited it, and the upstream IOC listens on a different port, the upstream
     channel would silently search on the wrong port and never connect.

`main()` (`GateMain.cpp`) calls `gate_init()`, `rsrvIocRegister()` (registers `rsrv` with the
shim's `dbRegisterServer`), then drives the registered server's `init()`/`run()`, and finally
drops into `iocsh()`. All the `gate*` iocsh commands are registered here — see the command
reference under "Running / manual smoke test" below (plus `rsrv`'s own `casr`). The
`gateLoadPutLog*` commands are registered unconditionally even in a build without caPutLog
support, where they just report that it wasn't built in.

## Build

Standard EPICS "extension"/module build (`configure/RULES` from EPICS Base's build system).

- Point at an EPICS Base checkout by creating `RELEASE.local` (at the repo root, sibling to
  `TOP`) or `configure/RELEASE.local` with `EPICS_BASE = /path/to/base`. Supported Base lines:
  3.15, 7.0 (`gate_compat.h` branches on `EPICS_VERSION_AT_LEAST`) — **not** 3.14, see
  Architecture above.
- Requires `pcre2-8` (`libpcre2-dev` at build time, `libpcre2-8` at runtime) —
  `gateway_SYS_LIBS += pcre2-8` in `src/Makefile`.
- caPutLog support is **optional**, selected purely by whether `CAPUTLOG = /path/to/caPutLog`
  is set in `RELEASE.local`: `src/Makefile` turns that into `-DWITH_CAPUTLOG`, which is what
  `GateLogic.cpp` guards the whole put-logging section on. It's a header-only dependency (no
  library is linked — see Architecture), so only `$(CAPUTLOG)/include` needs to exist. Without
  it the gateway builds fine and the `gateLoadPutLog*` commands report the feature is absent.
  Both configurations are worth building when touching that code; `make -C src CAPUTLOG=`
  overrides an inherited setting for a one-off check.
- Build: `make` from the repo root (or `make -C src` for just the gateway sources).
- The `PROD_IOC` target is `gateway`; the built binary lands at
  `bin/<EPICS_HOST_ARCH>/gateway`.
- CI (`.github/workflows/ci-scripts-build.yml`) uses EPICS `ci-scripts` (`.ci/cue.py`) against
  the module sets in `.ci-local/` (`base-3.15.set`, `base-7.0.set`):
  `python .ci/cue.py prepare && python .ci/cue.py build && python .ci/cue.py test`.

`configure/CONFIG_SITE` still carries flags from the legacy PCAS-based Gateway
(`USE_PCRE`, `USE_DENY_FROM`, `STAT_PVS`, `RATE_STATS`, `CONTROL_PVS`, `HEARTBEAT_PV`,
`CAS_DIAGNOSTICS`, `HANDLE_EXCEPTIONS`). These are inert leftovers, not evidence a feature is
implemented — none of them gate anything in the current `GateLogic.cpp`/`rsrv`-based code
path. In particular, `USE_DENY_FROM` does **not** control the `DENY FROM <hosts>` route
support described above, and `STAT_PVS`/`RATE_STATS` do **not** control the statistics and rate
PVs — those are unconditional runtime features enabled by the `gateInitStats` iocsh command.
`WITH_CAPUTLOG` (from `CAPUTLOG` in `RELEASE.local`, above) is the only compile-time feature
switch this branch actually honors.

## Running / manual smoke test

`test_gateway.sh` at the repo root is an ad hoc smoke test (with a hardcoded
`/home/jules/.cache/base-7.0` path — edit before use) that: starts a `softIoc` with `ioc.cmd`,
starts `gateway` with a `gate.json` config, then `caget`s a PV through the gateway. The gateway
has no CLI args at all — it's driven entirely by iocsh commands over stdin, so anything
scripting it needs to keep stdin open (a blind heredoc/pipe that closes stdin makes `iocsh`
exit, which exits the whole process — see how `testTop/pyTestsApp/gateway_tests/conftest.py`'s
`run_gateway()` handles this). Useful commands:

```
gateCreateClient <name> <addr_list> <auto_addr 0|1> <port>   # define an upstream CA client
gateAddPV <pcre-pattern> <client-name> <as-group> [target]   # route matching PV names to it
gateLoadConfig <file.json>                                    # do both from a JSON file
gateLoadAccess <file.acf>                                     # load an ASG/UAG/HAG access file
gateInitStats <prefix> [as-group]                             # create <prefix>:* statistics PVs
gateLoadPutLogText <addr> <config> <timeout>                  # caPutLog: traditional, to network
gateLoadPutLogJson <addr> <config> <timeout>                  # caPutLog: JSON, to network
gateLoadPutLogFile <filename>                                 # caPutLog: local file (old -putlog)
gateSetQueueLimits <maxElements> <maxBytes> <oldest|newest>   # bound per-client event queues
gateQueueReport <level>                                       # queue totals; >0 = per client
casr <level>                                                  # rsrv's built-in server report
```

Ordering matters for two of these: `gateLoadAccess` must come before `gateLoadConfig` (so
`asInitialize` runs before any route/channel exists) and before `gateInitStats` (so the stats
PVs' ASG membership registers while access security is already active). Put-logging needs
`TRAPWRITE` on the relevant ASG rule or the trap listener is never invoked at all. The
put-log `<config>` is caPutLog's own convention: `-1` disable / `0` on-change / `1` all /
`2` all-no-filter (1 and 2 behave identically here); `<timeout>` is accepted for interface
compatibility but unused, as there is no burst coalescing.

A freshly-created channel has no cached data until its upstream connection completes (async);
a `caget`/`caput` issued immediately after first referencing a brand-new PV name can race that
and return `ECA_GETFAIL` or read back stale data — this is inherent to the eager-subscribe
design, not a bug, and resolves itself on the next request a moment later. Well-behaved CA
client libraries (e.g. `pyepics`'s default auto-monitoring `PV` objects) establish a
subscription and wait for the first update rather than firing a blind synchronous get, so they
don't hit this in practice.

## Tests

`testTop/pyTestsApp` (pytest, run via `make runtests`/`make tapfiles` or directly with
`python3 -m pytest gateway_tests`, after `make testfiles testmodule pvlist.txt` once per
`testTop/README.md`) is the pre-existing test suite from the legacy PCAS-based Gateway,
adapted to run against the `rsrv`-based reimplementation:

- `conftest.py`'s `run_gateway()` launches the gateway with no CLI args (env vars
  `EPICS_CA_SERVER_PORT`/`EPICS_CAS_INTF_ADDR_LIST` set its listening address/port) and drives
  it via `gateCreateClient`/`gateLoadAccess`/`gateLoadConfig` over stdin (`gateLoadAccess` only
  if the `access` file exists and is non-empty, and always before `gateLoadConfig` so
  `asInitialize` runs before any route/channel can be created). `pvlist_text_to_routes()`
  translates the legacy ALIAS/ALLOW/DENY pvlist mini-language (still used by `standard_env`'s
  static `pvlist_bre.txt`/`pvlist_pcre.txt`) into the new JSON route schema, including
  converting BRE `\(..\)`/`\1` and PCRE `(..)`/`\1` group syntax into the PCRE2 `$1`-style
  `target` the reimplementation expects, and (unlike earlier in this branch's history) now
  translates `DENY`/`DENY FROM <hosts>` lines into real deny routes instead of dropping them.
  `testTop/pyTestsApp/access.txt` (`ASG(DEFAULT) { RULE(1,WRITE) }`, loaded for every test via
  `standard_env`/`default_access`) is what keeps every non-permissions test's full read+write
  access unchanged now that AS enforcement is actually active.
- `run_gateway()` also takes `stats_prefix` (default `gwtest`, so `conftest.GatewayStats` and
  the `gwtest:.* ALLOW` line already in `pvlist_bre.txt`/`pvlist_pcre.txt` work), plus
  `put_log_text_addr`/`put_log_json_addr`/`put_log_file` for the caPutLog sinks.
- **The whole suite should pass with nothing skipped**, given `caproto` installed (only
  `test_permissions.py`'s `util.py` needs it) and a caPutLog-enabled build. The
  `caputlog_supported` fixture detects the build variant by looking for the not-built-in stub's
  message inside the gateway binary — rather than re-running `make`, which wouldn't apply to a
  binary installed from elsewhere — and `test_logging.py`/`test_caputlog_network.py` skip on it.
  Previously-skipped files that now run for real: `test_property_cache.py`,
  `test_enum_property_cache.py`'s third test, and `test_logging.py` (11 parametrizations of the
  local put-log file). Two tests in `test_enum_property_cache.py` remain `xfail` for unfixed
  bug #58, unrelated.
- `test_caputlog_network.py` is new (not from the legacy suite): it covers the text and JSON
  network sinks against a local TCP listener, and the on-change filter.
- Everything else (`test_simple.py`, the `test_dbe_*.py` files, `test_subscriptions.py`,
  `test_cs_studio.py`, `test_structures.py`, `test_enum_undefined_timestamp.py`,
  `test_waveform_with_ca_max_array_bytes.py`) exercises the gateway for real and should pass —
  these are exactly what the `DBR_STS_/TIME_/GR_/CTRL_` format-serving work in `GateFormat.cpp`
  and the PV-alias feature in `GateLogic.cpp` exist to support.
- A full run takes ~11 minutes. When it fails in ways that look unrelated to your change,
  suspect the `EPICS_BASE` checkout: an in-development Base was once picked up mid-session with
  a broken rsrv bounds check that made *every* put-with-callback disconnect the client, which
  surfaced here as four unrelated-looking property-cache failures. Reproducing with Base's own
  `caput` against a plain `softIoc` (no gateway involved) is the fastest way to tell the two
  apart.
