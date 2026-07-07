#include "GateCAClient.h"
#include "GateDbrHelper.h"
#include <iostream>
#include <epicsThread.h>
#include <envDefs.h>

GateCAClient::GateCAClient(const std::string& name, const std::string& addrList) : name(name) {
    if (!addrList.empty()) {
        epicsEnvSet("EPICS_CA_ADDR_LIST", addrList.c_str());
        epicsEnvSet("EPICS_CA_AUTO_ADDR_LIST", "NO");
    }
    ca_context_create(ca_enable_preemptive_callback);
    caContext = ca_current_context();
}
GateCAClient::~GateCAClient() {
    ca_attach_context(caContext);
    for (auto& pair : pvs) {
        ca_clear_channel(pair.second->channel);
    }
    ca_context_destroy();
}
void GateCAClient::connect(const std::string& pvName, unsigned int mask) {
    ca_attach_context(caContext);
    std::lock_guard<std::mutex> lock(mutex);
    SubKey key = {pvName, mask};
    if (pvs.count(key)) return;
    auto info = std::make_shared<PVInfo>();
    info->name = pvName;
    info->mask = mask;
    info->client = this;
    int status = ca_create_channel(pvName.c_str(), connectionCallback, info.get(), 20, &info->channel);
    if (status == ECA_NORMAL) {
        pvs[key] = info;
        channelsByName[pvName] = info->channel;
        ca_flush_io();
    }
}
void GateCAClient::put(const std::string& pvName, int type, const void* value, long count) {
    ca_attach_context(caContext);
    std::lock_guard<std::mutex> lock(mutex);
    if (channelsByName.count(pvName)) {
        ca_array_put(type, count, channelsByName[pvName], value);
        ca_flush_io();
    }
}
void GateCAClient::connectionCallback(struct connection_handler_args args) {
    PVInfo* info = (PVInfo*)ca_puser(args.chid);
    if (args.op == CA_OP_CONN_UP) {
        ca_create_subscription(dbf_type_to_DBR_TIME(ca_field_type(args.chid)), 0, args.chid, info->mask,
                               eventCallback, info, &info->event);
        ca_flush_io();
    }
}
void GateCAClient::eventCallback(struct event_handler_args args) {
    PVInfo* info = (PVInfo*)args.usr;
    auto cacheEntry = GateCache::instance().findOrCreate(info->name);
    auto newData = std::make_shared<GateData>();
    newData->dbrType = args.type;
    newData->count = args.count;
    size_t size = dbr_size_n(args.type, args.count);
    newData->data.assign((char*)args.dbr, (char*)args.dbr + size);
    extract_metadata(args.type, args.dbr, newData->timestamp, newData->status, newData->severity);
    cacheEntry->update(newData);
}
GateCAClientManager& GateCAClientManager::instance() {
    static GateCAClientManager manager;
    return manager;
}
std::shared_ptr<GateCAClient> GateCAClientManager::createClient(const std::string& name, const std::string& addrList) {
    std::lock_guard<std::mutex> lock(mutex);
    auto client = std::make_shared<GateCAClient>(name, addrList);
    clients[name] = client;
    return client;
}
std::shared_ptr<GateCAClient> GateCAClientManager::getClient(const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex);
    return clients.count(name) ? clients[name] : nullptr;
}
