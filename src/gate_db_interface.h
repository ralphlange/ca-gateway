#ifndef GATE_DB_INTERFACE_H
#define GATE_DB_INTERFACE_H
#include <stddef.h>
#ifdef __cplusplus
extern "C" {
#endif
void gate_init(void);
void* gate_create_channel(const char* name);
int gate_channel_exists(const char* name);
void gate_wait_channel_ready(void* handle);
int gate_native_ca_type(void* handle);
long gate_native_count(void* handle);
void gate_delete_channel(void* channel);
// pfl mirrors real EPICS Base's db_field_log: NULL for a plain get (uses the eager default
// mask's cached data); for a monitor-triggered call (via gate_event_callback below), the
// GateData* snapshot of the specific event being delivered -- see GateLogic.cpp for why this
// matters (a non-default-mask event, e.g. a bare DBE_ALARM severity transition, never touches
// the default mask's own cache at all).
int gate_get_count(void* channel, int buffer_type, void* pbuffer, long* nRequest, void* pfl);
int gate_put(void* channel, int src_type, const void* psrc, long no_elements);
// Async put-with-completion: `cb` fires once the upstream IOC's own put-notify actually
// completes (status 0) or fails (status -1) -- not merely once the write was queued.
typedef void (gate_put_notify_callback)(void* user_arg, int status);
void gate_put_notify(void* channel, int src_type, const void* psrc, long no_elements,
                      gate_put_notify_callback* cb, void* user_arg);
// Bridges a real struct dbChannel* (as seen by rsrv's write_notify_action, camessage.c)
// to the gate_put_notify() call above; defined in gateShim.c alongside dbChannel_put().
struct dbChannel;
void dbChannel_put_notify(struct dbChannel* chan, int buffer_type, const void* pbuffer,
                           long no_elements, gate_put_notify_callback* cb, void* user_arg);
// Matches rsrv's real EVENTFUNC/read_reply signature (pArg, dbChannel*, eventsRemaining,
// db_field_log*) exactly -- `channel` is really a struct dbChannel*, and the last argument is
// really a GateData* (see GateLogic.cpp's gate_get_count()), not a raw value pointer.
typedef void (gate_event_callback)(void* user_arg, void* channel, int eventsRemaining, void* pfl);
void* gate_add_event(void* channel, gate_event_callback* cb, void* user_arg, void* real_dbchan, unsigned int select);
void gate_cancel_event(void* event_id);
void gate_create_client_cmd(const char* name, const char* addr_list, int auto_addr, int port);
void gate_add_pv_cmd(const char* pattern, const char* client_name, const char* as_group, const char* target);
// Adds a DENY route: hosts_csv empty = blanket deny (hidden from every client); otherwise a
// comma-separated host list -- matched by exact string against the client's self-reported CA
// HOST_NAME (same convention as HAG matching, see gate_asCheckClientIP), denying only those
// hosts while other clients still see whatever an ALLOW/ALIAS route grants for the same name.
void gate_add_deny_cmd(const char* pattern, const char* hosts_csv);
void gate_load_config(const char* filename);
// Loads an ACF (access security) file via asInitFile(); see GateLogic.cpp for details.
void gate_load_access(const char* filename);
void* gate_get_as_member(void* handle);
// Claim-time channel lookup, host-aware: hostname is the requesting client's self-reported
// CA HOST_NAME (NULL if unknown), used to evaluate DENY FROM routes even against an
// already-cached channel. gate_create_channel(name) is gate_create_channel_for_client(name, NULL).
void* gate_create_channel_for_client(const char* name, const char* hostname);
// Registers a set of gateway statistics PVs (<prefix>:vctotal/pvtotal/connected/active/
// inactive) as an iocsh command (gateInitStats); as_group defaults to "DEFAULT" if empty/NULL.
void gate_init_stats_cmd(const char* prefix, const char* as_group);
// Marks `handle` (a real, upstream-backed channel, never a statistics PV) as claimed by one
// more/one fewer downstream dbChannel -- called by gateShim.c's dbChannel_create/
// dbChannelDelete, which pair 1:1 per downstream client channel. Drives the "vctotal"/
// "active"/"inactive" statistics PVs.
void gate_channel_claim(void* handle);
// Recovers the gate_create_channel_for_client()-returned handle from a real struct dbChannel*
// (asTrapWriteMessage::serverSpecific, see GateLogic.cpp's caPutLog trap-write listener);
// implemented in gateShim.c, which alone knows the dbChannelGate layout.
void* gate_handle_from_dbchannel(struct dbChannel* chan);
// Configures caPutLog network put-logging (see GateLogic.cpp for the full design comment on
// why this bypasses caPutLogAsInit()/the caPutLog module's own convenience entry points).
// config: -1 (none/disable) / 0 (on-change) / 1 (log all) / 2 (log all, no same-PV filtering) --
// same convention as caPutLog.h's caPutLogNone/OnChange/All/AllNoFilter. addr is a
// whitespace-separated list of "host[:port]" log-server destinations.
void gate_load_put_log_text_cmd(const char* addr, int config, double timeout);
void gate_load_put_log_json_cmd(const char* addr, int config, double timeout);
// Local put-log file -- a drop-in replacement for the old PCAS-based Gateway's "-putlog
// <file>" (same line format; see GateLogic.cpp's gate_send_put_log_file() for the exact match).
// Always logs every trapped write (no config level -- the old file writer had none).
void gate_load_put_log_file_cmd(const char* filename);
#ifdef __cplusplus
}
#endif
#endif
