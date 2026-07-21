#ifndef GATE_DB_INTERFACE_H
#define GATE_DB_INTERFACE_H
#include <stddef.h>
#ifdef __cplusplus
extern "C" {
#endif
void gate_init(void);
void* gate_create_channel(const char* name);
void gate_delete_channel(void* channel);
int gate_get_count(void* channel, int buffer_type, void* pbuffer, long* nRequest);
int gate_put(void* channel, int src_type, const void* psrc, long no_elements);
typedef void (gate_event_callback)(void* user_arg, void* channel, int dbrType, int count, void* pbuffer);
void* gate_add_event(void* channel, gate_event_callback* cb, void* user_arg, unsigned int select);
void gate_cancel_event(void* event_id);
void gate_create_client_cmd(const char* name, const char* addr_list, int auto_addr, int port);
void gate_add_pv_cmd(const char* pattern, const char* client_name, const char* as_group);
void gate_load_config(const char* filename);
void* gate_get_as_member(void* handle);
#ifdef __cplusplus
}
#endif
#endif
