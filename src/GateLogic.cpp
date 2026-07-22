#include "gate_compat.h"
#include "gate_db_interface.h"
#include "GateFormat.h"
#include <string>
#include <map>
#include <vector>
#include <memory>
#include <mutex>
#include <iostream>
#include <fstream>
#include <sstream>
#include <cstring>
#define PCRE2_CODE_UNIT_WIDTH 8
#include <pcre2.h>
#include <yajl_parse.h>

struct GateData {
    int dbrType;
    long count;
    std::vector<char> data;
    short status = 0;
    short severity = 0;
    epicsTimeStamp stamp = {0, 0};
};

// Appends ":port" to each whitespace-separated address that doesn't already specify one.
// EPICS_CA_SERVER_PORT is process-wide and also governs the Gateway's own CAS listening
// port (set via the environment before the process starts) -- if the upstream client just
// inherited it, and the upstream IOC listens on a different port, ca_create_channel() would
// silently search on the wrong port and never connect. Per-address ":port" in
// EPICS_CA_ADDR_LIST overrides the default port for that address regardless.
static std::string with_port(const std::string& addr_list, int port) {
    if (port <= 0) return addr_list;
    std::istringstream iss(addr_list);
    std::string token, result;
    while (iss >> token) {
        if (!result.empty()) result += " ";
        result += token;
        if (token.find(':') == std::string::npos)
            result += ":" + std::to_string(port);
    }
    return result;
}

class GateClient {
public:
    GateClient(const std::string& name, const std::string& addr_list, bool auto_addr, int port) : name(name) {
        if (!ca_current_context()) ca_context_create(ca_enable_preemptive_callback);
        if (!addr_list.empty()) setenv("EPICS_CA_ADDR_LIST", with_port(addr_list, port).c_str(), 1);
        setenv("EPICS_CA_AUTO_ADDR_LIST", auto_addr ? "YES" : "NO", 1);
        ctx = ca_current_context();
    }
    ~GateClient() { ca_context_destroy(); }
    ca_client_context* getCtx() { return ctx; }
private:
    std::string name;
    ca_client_context* ctx;
};

// Forward-declared so GateChannel's constructor can register it as the connection
// callback; defined below (after GateChannel) since it operates on a complete GateChannel.
static void ca_conn_cb(struct connection_handler_args args);

struct GateChannel {
    std::string name;
    chid caChid;
    ASMEMBERPVT asMember;

    struct MaskSub {
        evid caEvid;
        unsigned int mask = 0;
        struct GateChannel* gchan = nullptr;
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

    GateStaticMeta meta;
    std::mutex metaMutex;

    GateChannel(const char* name, const char* upstream_name, const char* as_group) : name(name), caChid(NULL), asMember(NULL) {
        ca_create_channel(upstream_name, ca_conn_cb, this, 20, &caChid);
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
    std::string target; // optional PCRE2 $1-style rewrite of the upstream name; empty = same name
};
static std::vector<Route> routes;
static std::mutex routesMutex;

static void ca_meta_cb(struct event_handler_args args) {
    GateChannel* gchan = (GateChannel*)args.usr;
    if (args.status != ECA_NORMAL) return;
    int dbf = gate_dbr_to_dbf(args.type);
    std::lock_guard<std::mutex> lock(gchan->metaMutex);
    gate_format_extract_meta(dbf, args.dbr, &gchan->meta);
}

static void refresh_static_meta(GateChannel* gchan, chid ch) {
    short native = ca_field_type(ch);
    short ctrl_type = (short)(native + 28); // DBR_CTRL_STRING(28) + native basic type (0..6)
    ca_array_get_callback(ctrl_type, 1, ch, ca_meta_cb, gchan);
    ca_flush_io();
}

// Upstream subscriptions are always created with type (DBR_TIME_STRING + native field type),
// regardless of the event mask or of what format downstream clients eventually request: this
// is what gives gate_get_count() real status/severity/timestamp to work with (see GateFormat.h).
static void ca_event_cb(struct event_handler_args args) {
    GateChannel::MaskSub* msub = (GateChannel::MaskSub*)args.usr;
    if (args.status != ECA_NORMAL) return;
    auto newData = std::make_shared<GateData>();
    newData->dbrType = args.type;
    newData->count = args.count;
    size_t sz = dbr_size_n(args.type, args.count);
    newData->data.assign((char*)args.dbr, (char*)args.dbr + sz);
    int dbf = gate_dbr_to_dbf(args.type);
    gate_format_extract_time(dbf, args.dbr, &newData->status, &newData->severity, &newData->stamp);
    {
        std::lock_guard<std::mutex> lock(msub->dataMutex);
        msub->lastData = newData;
    }
    if (msub->mask & DBE_PROPERTY) {
        // A property change may have altered limits/precision/units/enum strings upstream;
        // refresh the cached static metadata so gate_get_count() serves current GR_/CTRL_ data.
        refresh_static_meta(msub->gchan, msub->gchan->caChid);
    }
    std::lock_guard<std::mutex> lock(msub->subsMutex);
    for (auto usub : msub->userSubs) {
        void* pvalue = (void*)((char*)newData->data.data() + dbr_size[newData->dbrType] - dbr_value_size[newData->dbrType]);
        usub->cb(usub->user_arg, (void*)msub, newData->dbrType, newData->count, pvalue);
    }
}

// Returns the MaskSub for `select`, creating the upstream CA subscription (type
// DBR_TIME_STRING + native) on first use. Used both for downstream-requested masks
// (gate_add_event) and for the eager default subscription created on connect (ca_conn_cb),
// so a plain get always has cached data even with no downstream monitor yet.
static std::shared_ptr<GateChannel::MaskSub> get_or_create_mask_sub(GateChannel* gchan, chid ch, unsigned int select) {
    std::lock_guard<std::mutex> lock(gchan->maskMutex);
    auto it = gchan->maskSubs.find(select);
    if (it != gchan->maskSubs.end()) return it->second;
    auto msub = std::make_shared<GateChannel::MaskSub>();
    msub->mask = select;
    msub->gchan = gchan;
    gchan->maskSubs[select] = msub;
    short native = ca_field_type(ch);
    short time_type = (short)(native + 14); // DBR_TIME_STRING(14) + native basic type (0..6)
    ca_create_subscription(time_type, ca_element_count(ch), ch, select, ca_event_cb, msub.get(), &msub->caEvid);
    ca_flush_io();
    return msub;
}

static void ca_conn_cb(struct connection_handler_args args) {
    if (args.op != CA_OP_CONN_UP) return;
    GateChannel* gchan = (GateChannel*)ca_puser(args.chid);
    get_or_create_mask_sub(gchan, args.chid, DBE_VALUE);
    refresh_static_meta(gchan, args.chid);
}

static ca_client_context* g_ca_ctx = NULL;

// Every entry point below can run on whichever rsrv server thread happens to be handling
// the client request (e.g. "CAS-UDP"/"CAS-TCP"/per-client threads) -- not the main thread
// that gate_init() created the CA client context on. CA contexts are per-thread, so any
// ca_*() call from a thread with no attached context silently fails (upstream channel never
// connects, put/subscribe never happens) rather than crashing. Call this first.
static void ensure_ca_context() {
    if (!ca_current_context() && g_ca_ctx) ca_attach_context(g_ca_ctx);
}

extern "C" {
void gate_init(void) {
    if (!ca_current_context()) ca_context_create(ca_enable_preemptive_callback);
    g_ca_ctx = ca_current_context();
}

void* gate_create_channel(const char* name) {
    ensure_ca_context();

    std::lock_guard<std::mutex> lock(channelsMutex);
    auto it = channels.find(name);
    if (it != channels.end()) return (void*)it->second.get();

    std::lock_guard<std::mutex> rlock(routesMutex);
    for (const auto& route : routes) {
        pcre2_match_data* match_data = pcre2_match_data_create_from_pattern(route.re, NULL);
        int rc = pcre2_match(route.re, (PCRE2_SPTR)name, strlen(name), 0, 0, match_data, NULL);
        pcre2_match_data_free(match_data);
        if (rc >= 0) {
            std::string upstream_name = name;
            if (!route.target.empty()) {
                PCRE2_UCHAR outbuf[512];
                PCRE2_SIZE outlen = sizeof(outbuf) / sizeof(PCRE2_UCHAR);
                int rc2 = pcre2_substitute(route.re, (PCRE2_SPTR)name, strlen(name), 0,
                                           0, NULL, NULL,
                                           (PCRE2_SPTR)route.target.c_str(), route.target.size(),
                                           outbuf, &outlen);
                if (rc2 >= 0) {
                    upstream_name.assign((const char*)outbuf, outlen);
                } else {
                    errlogPrintf("gate_create_channel: pcre2_substitute failed (%d) for '%s' -> '%s', using unmodified name\n",
                                 rc2, name, route.target.c_str());
                }
            }
            auto gchan = std::make_shared<GateChannel>(name, upstream_name.c_str(), route.as_group.c_str());
            channels[name] = gchan;
            ca_flush_io();
            return (void*)gchan.get();
        }
    }
    return NULL;
}

int gate_channel_exists(const char* name) {
    return gate_create_channel(name) != NULL;
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
        // Prefer the eager default (DBE_VALUE) subscription for plain gets, since a
        // DBE_LOG/DBE_ALARM/DBE_PROPERTY-only subscription may have staler data.
        auto it = gchan->maskSubs.find((unsigned int)DBE_VALUE);
        if (it == gchan->maskSubs.end()) it = gchan->maskSubs.begin();
        msub = it->second;
    }
    std::shared_ptr<GateData> data;
    {
        std::lock_guard<std::mutex> lock(msub->dataMutex);
        if (!msub->lastData) return -1;
        data = msub->lastData;
    }
    GateStaticMeta meta;
    {
        std::lock_guard<std::mutex> lock(gchan->metaMutex);
        meta = gchan->meta;
    }
    int dbf = gate_dbr_to_dbf(data->dbrType);
    const void* rawValue = (const void*)((const char*)data->data.data() + dbr_size[data->dbrType] - dbr_value_size[data->dbrType]);
    return gate_format_response(dbf, buffer_type, pbuffer, nRequest, data->count,
                                 data->status, data->severity, data->stamp, rawValue, meta);
}

int gate_put(void* channel, int src_type, const void* psrc, long no_elements) {
    ensure_ca_context();
    GateChannel* gchan = (GateChannel*)channel;
    int rc = ca_array_put(src_type, no_elements, gchan->caChid, psrc);
    ca_flush_io();
    return rc;
}

void* gate_add_event(void* channel, gate_event_callback* cb, void* user_arg, unsigned int select) {
    ensure_ca_context();
    GateChannel* gchan = (GateChannel*)channel;
    auto msub = get_or_create_mask_sub(gchan, gchan->caChid, select);
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
    errlogPrintf("gate_create_client_cmd: client '%s' created (addr_list='%s' auto_addr=%d port=%d)\n",
                 name, addr_list ? addr_list : "", auto_addr, port);
}

void gate_add_pv_cmd(const char* pattern, const char* client_name, const char* as_group, const char* target) {
    int error;
    PCRE2_SIZE erroroffset;
    pcre2_code* re = pcre2_compile((PCRE2_SPTR)pattern, PCRE2_ZERO_TERMINATED, 0, &error, &erroroffset, NULL);
    if (re) {
        std::lock_guard<std::mutex> lock(routesMutex);
        routes.push_back({re, client_name, as_group ? as_group : "", target ? target : ""});
        errlogPrintf("gate_add_pv_cmd: route added: pattern='%s' client='%s' as_group='%s' target='%s'\n",
                     pattern, client_name, as_group ? as_group : "", target ? target : "");
    } else {
        errlogPrintf("gate_add_pv_cmd: failed to compile pattern '%s' (pcre2 error %d)\n", pattern, error);
    }
}

namespace {

struct ConfigParseCtx {
    enum Ctx { CTX_ROOT_OBJ, CTX_CLIENTS_ARRAY, CTX_CLIENT_OBJ, CTX_PVS_ARRAY, CTX_PV_OBJ, CTX_OTHER };
    std::vector<Ctx> stack;
    std::string key;

    std::string name, addr_list;
    bool auto_addr;
    int port;
    bool has_name;

    std::string pattern, client_name, as_group, target;
    bool has_pattern, has_client;

    Ctx top() const { return stack.back(); }
};

int cb_start_map(void* vctx) {
    ConfigParseCtx* ctx = (ConfigParseCtx*)vctx;
    if (ctx->stack.empty()) {
        ctx->stack.push_back(ConfigParseCtx::CTX_ROOT_OBJ);
    } else if (ctx->top() == ConfigParseCtx::CTX_CLIENTS_ARRAY) {
        ctx->stack.push_back(ConfigParseCtx::CTX_CLIENT_OBJ);
        ctx->name.clear(); ctx->addr_list.clear();
        ctx->auto_addr = true; ctx->port = 5064; ctx->has_name = false;
    } else if (ctx->top() == ConfigParseCtx::CTX_PVS_ARRAY) {
        ctx->stack.push_back(ConfigParseCtx::CTX_PV_OBJ);
        ctx->pattern.clear(); ctx->client_name.clear(); ctx->as_group = "DEFAULT"; ctx->target.clear();
        ctx->has_pattern = false; ctx->has_client = false;
    } else {
        ctx->stack.push_back(ConfigParseCtx::CTX_OTHER);
    }
    return 1;
}

int cb_end_map(void* vctx) {
    ConfigParseCtx* ctx = (ConfigParseCtx*)vctx;
    ConfigParseCtx::Ctx t = ctx->top();
    ctx->stack.pop_back();
    if (t == ConfigParseCtx::CTX_CLIENT_OBJ && ctx->has_name) {
        gate_create_client_cmd(ctx->name.c_str(), ctx->addr_list.c_str(), ctx->auto_addr ? 1 : 0, ctx->port);
    } else if (t == ConfigParseCtx::CTX_PV_OBJ && ctx->has_pattern && ctx->has_client) {
        gate_add_pv_cmd(ctx->pattern.c_str(), ctx->client_name.c_str(), ctx->as_group.c_str(), ctx->target.c_str());
    }
    return 1;
}

int cb_start_array(void* vctx) {
    ConfigParseCtx* ctx = (ConfigParseCtx*)vctx;
    if (ctx->top() == ConfigParseCtx::CTX_ROOT_OBJ && ctx->key == "clients") {
        ctx->stack.push_back(ConfigParseCtx::CTX_CLIENTS_ARRAY);
    } else if (ctx->top() == ConfigParseCtx::CTX_ROOT_OBJ && ctx->key == "pvs") {
        ctx->stack.push_back(ConfigParseCtx::CTX_PVS_ARRAY);
    } else {
        ctx->stack.push_back(ConfigParseCtx::CTX_OTHER);
    }
    return 1;
}

int cb_end_array(void* vctx) {
    ConfigParseCtx* ctx = (ConfigParseCtx*)vctx;
    ctx->stack.pop_back();
    return 1;
}

int cb_map_key(void* vctx, const unsigned char* key, size_t len) {
    ConfigParseCtx* ctx = (ConfigParseCtx*)vctx;
    ctx->key.assign((const char*)key, len);
    return 1;
}

int cb_string(void* vctx, const unsigned char* val, size_t len) {
    ConfigParseCtx* ctx = (ConfigParseCtx*)vctx;
    std::string s((const char*)val, len);
    if (ctx->top() == ConfigParseCtx::CTX_CLIENT_OBJ) {
        if (ctx->key == "name") { ctx->name = s; ctx->has_name = true; }
        else if (ctx->key == "addr_list") ctx->addr_list = s;
    } else if (ctx->top() == ConfigParseCtx::CTX_PV_OBJ) {
        if (ctx->key == "pattern") { ctx->pattern = s; ctx->has_pattern = true; }
        else if (ctx->key == "client") { ctx->client_name = s; ctx->has_client = true; }
        else if (ctx->key == "as_group") ctx->as_group = s;
        else if (ctx->key == "target") ctx->target = s;
    }
    return 1;
}

int cb_boolean(void* vctx, int boolVal) {
    ConfigParseCtx* ctx = (ConfigParseCtx*)vctx;
    if (ctx->top() == ConfigParseCtx::CTX_CLIENT_OBJ && ctx->key == "auto_addr") ctx->auto_addr = boolVal != 0;
    return 1;
}

int cb_integer(void* vctx, long long integerVal) {
    ConfigParseCtx* ctx = (ConfigParseCtx*)vctx;
    if (ctx->top() == ConfigParseCtx::CTX_CLIENT_OBJ && ctx->key == "port") ctx->port = (int)integerVal;
    return 1;
}

const yajl_callbacks configCallbacks = {
    NULL,           /* yajl_null */
    cb_boolean,
    cb_integer,
    NULL,           /* yajl_double */
    NULL,           /* yajl_number */
    cb_string,
    cb_start_map,
    cb_map_key,
    cb_end_map,
    cb_start_array,
    cb_end_array
};

} // namespace

void gate_load_config(const char* filename) {
    std::ifstream ifs(filename);
    if (!ifs.is_open()) {
        errlogPrintf("gate_load_config: could not open '%s'\n", filename);
        return;
    }
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    ConfigParseCtx ctx;
    yajl_handle hand = yajl_alloc(&configCallbacks, NULL, &ctx);
    yajl_status status = yajl_parse(hand, (const unsigned char*)content.data(), content.size());
    if (status == yajl_status_ok)
        status = yajl_complete_parse(hand);
    if (status == yajl_status_ok) {
        errlogPrintf("gate_load_config: '%s' loaded successfully\n", filename);
    } else {
        unsigned char* err = yajl_get_error(hand, 1, (const unsigned char*)content.data(), content.size());
        errlogPrintf("gate_load_config: failed to parse '%s': %s\n", filename, err ? (const char*)err : "unknown error");
        if (err) yajl_free_error(hand, err);
    }
    yajl_free(hand);
}
}
