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
requests one of those formats by default. Large parts of the original Gateway's feature set
(real ASG/UAG/HAG access-security enforcement, stat/heartbeat/rate PVs, caPutLog, multi-server
listening, etc.) are **still not implemented** — the corresponding `testTop` tests are marked
`skip` with a reason, not adapted. Recent commits were produced with an AI coding agent
(`google-labs-jules[bot]`); expect rough edges and re-verify behavior against actual EPICS
semantics rather than trusting comments or prior commit messages.

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
   multiple Base versions (3.14/3.15/7.0 — see `gate_compat.h`). Treat these as close to
   upstream EPICS Base; prefer fixing incompatibilities in the shim/compat layer over editing
   them, so they stay easy to diff against a real Base checkout.

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
   - Several `dbEventCtx`-consuming functions (`db_add_extra_labor_event`,
     `db_flush_extra_labor_event`, `db_event_flow_ctrl_mode_on/off`, `db_event_change_priority`)
     are real, un-shimmed Base functions too, and `db_init_events()` here returns a fake
     non-NULL handle (`(dbEventCtx)1`) rather than a real one — calling any of the real
     versions against that fake handle segfaults. They're stubbed as no-ops in `gateShim.c`,
     consistent with `db_post_extra_labor()`/`db_close_events()` already being no-ops (nothing
     ever actually queues/delivers the extra-labor callback here). If a future Base version
     adds another function that takes a `dbEventCtx`, it needs the same treatment.

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
   - Parses a small JSON config format (via `yajl`, Base's bundled JSON parser — see
     `gate_load_config`) with `clients: [{name, addr_list, auto_addr, port}]` and
     `pvs: [{pattern, client, as_group, target?}]`. This is **not** the same schema as the
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
drops into `iocsh()`. New iocsh commands registered here: `gateCreateClient`,
`gateAddPV <pattern> <client> <as_group> [target]`, `gateLoadConfig` (plus `rsrv`'s own
`casr`).

## Build

Standard EPICS "extension"/module build (`configure/RULES` from EPICS Base's build system).

- Point at an EPICS Base checkout by creating `RELEASE.local` (at the repo root, sibling to
  `TOP`) or `configure/RELEASE.local` with `EPICS_BASE = /path/to/base`. Supported Base lines:
  3.14, 3.15, 7.0 (`gate_compat.h` branches on `EPICS_VERSION_AT_LEAST`).
- Requires `pcre2-8` (`libpcre2-dev` at build time, `libpcre2-8` at runtime) —
  `gateway_SYS_LIBS += pcre2-8` in `src/Makefile`.
- Build: `make` from the repo root (or `make -C src` for just the gateway sources).
- The `PROD_IOC` target is `gateway`; the built binary lands at
  `bin/<EPICS_HOST_ARCH>/gateway`.
- CI (`.github/workflows/ci-scripts-build.yml`) uses EPICS `ci-scripts` (`.ci/cue.py`) against
  the module sets in `.ci-local/` (`base-3.14.set`, `base-3.15.set`, `base-7.0.set`):
  `python .ci/cue.py prepare && python .ci/cue.py build && python .ci/cue.py test`.

`configure/CONFIG_SITE` still carries flags from the legacy PCAS-based Gateway
(`USE_PCRE`, `USE_DENY_FROM`, `STAT_PVS`, `RATE_STATS`, `CONTROL_PVS`, `HEARTBEAT_PV`,
`CAS_DIAGNOSTICS`, `HANDLE_EXCEPTIONS`). None of those features exist in the current
`GateLogic.cpp`/`rsrv`-based code path — the flags are inert leftovers, not evidence the
feature is implemented.

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
casr <level>                                                  # rsrv's built-in server report
```

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
  it via `gateCreateClient`/`gateLoadConfig` over stdin. `pvlist_text_to_routes()` translates
  the legacy ALIAS/ALLOW/DENY pvlist mini-language (still used by `standard_env`'s static
  `pvlist_bre.txt`/`pvlist_pcre.txt`) into the new JSON route schema, including converting BRE
  `\(..\)`/`\1` and PCRE `(..)`/`\1` group syntax into the PCRE2 `$1`-style `target` the
  reimplementation expects.
- `test_permissions.py`, `test_property_cache.py`, and `test_logging.py` are skipped outright
  (real access-security enforcement / Gateway stats PVs / caPutLog, respectively, aren't
  implemented); one test in `test_enum_property_cache.py` is skipped for the same stats-PV
  reason (its two siblings were already `xfail` for an unrelated bug, unchanged).
- Everything else (`test_simple.py`, the `test_dbe_*.py` files, `test_subscriptions.py`,
  `test_cs_studio.py`, `test_structures.py`, `test_enum_undefined_timestamp.py`,
  `test_waveform_with_ca_max_array_bytes.py`) exercises the gateway for real and should pass —
  these are exactly what the `DBR_STS_/TIME_/GR_/CTRL_` format-serving work in `GateFormat.cpp`
  and the PV-alias feature in `GateLogic.cpp` exist to support.
