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
int gate_get_count(void* channel, int buffer_type, void* pbuffer, long* nRequest);
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
typedef void (gate_event_callback)(void* user_arg, void* channel, int dbrType, int count, void* pbuffer);
void* gate_add_event(void* channel, gate_event_callback* cb, void* user_arg, void* real_dbchan, unsigned int select);
void gate_cancel_event(void* event_id);
void gate_create_client_cmd(const char* name, const char* addr_list, int auto_addr, int port);
void gate_add_pv_cmd(const char* pattern, const char* client_name, const char* as_group, const char* target);
void gate_load_config(const char* filename);
void* gate_get_as_member(void* handle);
#ifdef __cplusplus
}
#endif
#endif
