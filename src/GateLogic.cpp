#include "gate_compat.h"
#include "gate_db_interface.h"
#include <string>
#include <map>
#include <vector>
#include <memory>
#include <mutex>
#include <iostream>
#include <fstream>
#include <cstring>
#define PCRE2_CODE_UNIT_WIDTH 8
#include <pcre2.h>
#include <cjson/cJSON.h>

struct GateData {
    int dbrType;
    long count;
    std::vector<char> data;
};

class GateClient {
public:
    GateClient(const std::string& name, const std::string& addr_list, bool auto_addr, int port) : name(name) {
        if (!ca_current_context()) ca_context_create(ca_enable_preemptive_callback);
        if (!addr_list.empty()) setenv("EPICS_CA_ADDR_LIST", addr_list.c_str(), 1);
        setenv("EPICS_CA_AUTO_ADDR_LIST", auto_addr ? "YES" : "NO", 1);
        ctx = ca_current_context();
    }
    ~GateClient() { ca_context_destroy(); }
    ca_client_context* getCtx() { return ctx; }
private:
    std::string name;
    ca_client_context* ctx;
};

struct GateChannel {
    std::string name;
    chid caChid;
    ASMEMBERPVT asMember;

    struct MaskSub {
        evid caEvid;
        std::shared_ptr<GateData> lastData;
        std::mutex dataMutex;
        struct UserSub {
            void (*cb)(void*, void*, int, int, void*);
            void* user_arg;
        };
        std::vector<UserSub*> userSubs;
        std::mutex subsMutex;
    };
    std::map<unsigned int, std::shared_ptr<MaskSub>> maskSubs;
    std::mutex maskMutex;

    GateChannel(const char* name, const char* as_group) : name(name), caChid(NULL), asMember(NULL) {
        ca_create_channel(name, NULL, this, 20, &caChid);
        asAddMember(&asMember, as_group);
    }
};

static std::map<std::string, std::shared_ptr<GateChannel>> channels;
static std::mutex channelsMutex;
static std::map<std::string, std::shared_ptr<GateClient>> clients;

struct Route {
    pcre2_code* re;
    std::string client_name;
    std::string as_group;
};
static std::vector<Route> routes;
static std::mutex routesMutex;

extern "C" {
typedef long (*GETCONVERTFUNC)(const struct dbAddr *paddr, void *pbuffer, long nRequest, long no_elements, long offset);
extern GETCONVERTFUNC dbGetConvertRoutine[14][12];
}

static void ca_event_cb(struct event_handler_args args) {
    GateChannel::MaskSub* msub = (GateChannel::MaskSub*)args.usr;
    if (args.status != ECA_NORMAL) return;
    auto newData = std::make_shared<GateData>();
    newData->dbrType = args.type;
    newData->count = args.count;
    size_t sz = dbr_size_n(args.type, args.count);
    newData->data.assign((char*)args.dbr, (char*)args.dbr + sz);
    {
        std::lock_guard<std::mutex> lock(msub->dataMutex);
        msub->lastData = newData;
    }
    std::lock_guard<std::mutex> lock(msub->subsMutex);
    for (auto usub : msub->userSubs) {
        void* pvalue = (void*)((char*)newData->data.data() + dbr_size[newData->dbrType] - dbr_value_size[newData->dbrType]);
        usub->cb(usub->user_arg, (void*)msub, newData->dbrType, newData->count, pvalue);
    }
}

extern "C" {
void gate_init(void) {
    if (!ca_current_context()) ca_context_create(ca_enable_preemptive_callback);
}

void* gate_create_channel(const char* name) {
    std::lock_guard<std::mutex> lock(channelsMutex);
    auto it = channels.find(name);
    if (it != channels.end()) return (void*)it->second.get();

    std::lock_guard<std::mutex> rlock(routesMutex);
    for (const auto& route : routes) {
        pcre2_match_data* match_data = pcre2_match_data_create_from_pattern(route.re, NULL);
        int rc = pcre2_match(route.re, (PCRE2_SPTR)name, strlen(name), 0, 0, match_data, NULL);
        pcre2_match_data_free(match_data);
        if (rc >= 0) {
            auto gchan = std::make_shared<GateChannel>(name, route.as_group.c_str());
            channels[name] = gchan;
            ca_flush_io();
            return (void*)gchan.get();
        }
    }
    return NULL;
}

void gate_delete_channel(void* channel) {}

void* gate_get_as_member(void* handle) {
    return (void*)((GateChannel*)handle)->asMember;
}

int gate_get_count(void* handle, int buffer_type, void* pbuffer, long* nRequest) {
    GateChannel* gchan = (GateChannel*)handle;
    std::shared_ptr<GateChannel::MaskSub> msub;
    {
        std::lock_guard<std::mutex> lock(gchan->maskMutex);
        if (gchan->maskSubs.empty()) return -1;
        msub = gchan->maskSubs.begin()->second;
    }
    std::lock_guard<std::mutex> lock(msub->dataMutex);
    if (!msub->lastData) return -1;
    struct dbAddr addr;
    memset(&addr, 0, sizeof(addr));
    addr.field_type = (short)gate_dbr_to_dbf(msub->lastData->dbrType);
    addr.pfield = (void*)((char*)msub->lastData->data.data() + dbr_size[msub->lastData->dbrType] - dbr_value_size[msub->lastData->dbrType]);
    addr.no_elements = msub->lastData->count;
    if (addr.field_type <= 13 && buffer_type <= 11) {
        GETCONVERTFUNC convert = dbGetConvertRoutine[addr.field_type][buffer_type];
        if (convert) {
            convert(&addr, pbuffer, *nRequest, addr.no_elements, 0);
            return 0;
        }
    }
    return -1;
}

int gate_put(void* channel, int src_type, const void* psrc, long no_elements) {
    GateChannel* gchan = (GateChannel*)channel;
    return ca_array_put(src_type, no_elements, gchan->caChid, psrc);
}

void* gate_add_event(void* channel, gate_event_callback* cb, void* user_arg, unsigned int select) {
    GateChannel* gchan = (GateChannel*)channel;
    std::lock_guard<std::mutex> lock(gchan->maskMutex);
    if (gchan->maskSubs.find(select) == gchan->maskSubs.end()) {
        auto msub = std::make_shared<GateChannel::MaskSub>();
        gchan->maskSubs[select] = msub;
        ca_create_subscription(ca_field_type(gchan->caChid), ca_element_count(gchan->caChid),
                               gchan->caChid, select, ca_event_cb, msub.get(), &msub->caEvid);
        ca_flush_io();
    }
    auto msub = gchan->maskSubs[select];
    auto* usub = new GateChannel::MaskSub::UserSub{(void (*)(void*, void*, int, int, void*))cb, user_arg};
    {
        std::lock_guard<std::mutex> lock(msub->subsMutex);
        msub->userSubs.push_back(usub);
    }
    return (void*)usub;
}

void gate_cancel_event(void* event_id) {}

void gate_create_client_cmd(const char* name, const char* addr_list, int auto_addr, int port) {
    std::lock_guard<std::mutex> lock(channelsMutex);
    clients[name] = std::make_shared<GateClient>(name, addr_list ? addr_list : "", auto_addr != 0, port);
}

void gate_add_pv_cmd(const char* pattern, const char* client_name, const char* as_group) {
    int error;
    PCRE2_SIZE erroroffset;
    pcre2_code* re = pcre2_compile((PCRE2_SPTR)pattern, PCRE2_ZERO_TERMINATED, 0, &error, &erroroffset, NULL);
    if (re) {
        std::lock_guard<std::mutex> lock(routesMutex);
        routes.push_back({re, client_name, as_group ? as_group : ""});
    }
}

void gate_load_config(const char* filename) {
    std::ifstream ifs(filename);
    if (!ifs.is_open()) return;
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
    cJSON* root = cJSON_Parse(content.c_str());
    if (!root) return;

    cJSON* clients_obj = cJSON_GetObjectItem(root, "clients");
    if (cJSON_IsArray(clients_obj)) {
        cJSON* client_obj;
        cJSON_ArrayForEach(client_obj, clients_obj) {
            cJSON* n = cJSON_GetObjectItem(client_obj, "name");
            cJSON* a = cJSON_GetObjectItem(client_obj, "addr_list");
            cJSON* aa = cJSON_GetObjectItem(client_obj, "auto_addr");
            cJSON* p = cJSON_GetObjectItem(client_obj, "port");
            if (n) gate_create_client_cmd(n->valuestring, a?a->valuestring:"", aa?cJSON_IsTrue(aa):1, p?p->valueint:5064);
        }
    }

    cJSON* pvs_obj = cJSON_GetObjectItem(root, "pvs");
    if (cJSON_IsArray(pvs_obj)) {
        cJSON* pv_obj;
        cJSON_ArrayForEach(pv_obj, pvs_obj) {
            cJSON* pat = cJSON_GetObjectItem(pv_obj, "pattern");
            cJSON* cl = cJSON_GetObjectItem(pv_obj, "client");
            cJSON* gr = cJSON_GetObjectItem(pv_obj, "as_group");
            if (pat && cl) gate_add_pv_cmd(pat->valuestring, cl->valuestring, gr?gr->valuestring:"DEFAULT");
        }
    }
    cJSON_Delete(root);
}
}
