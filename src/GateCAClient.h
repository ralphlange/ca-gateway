#ifndef GATE_CA_CLIENT_H
#define GATE_CA_CLIENT_H
#include <string>
#include <map>
#include <memory>
#include <mutex>
#include <cadef.h>
#include "GateCache.h"
class GateCAClient {
public:
    GateCAClient(const std::string& name, const std::string& addrList = "");
    ~GateCAClient();
    void connect(const std::string& pvName, unsigned int mask);
    void put(const std::string& pvName, int type, const void* value, long count);
private:
    static void connectionCallback(struct connection_handler_args args);
    static void eventCallback(struct event_handler_args args);
    std::string name;
    struct ca_client_context *caContext;
    std::mutex mutex;
    struct PVInfo {
        chid channel;
        evid event;
        std::string name;
        unsigned int mask;
        GateCAClient* client;
    };
    struct SubKey {
        std::string name;
        unsigned int mask;
        bool operator<(const SubKey& o) const {
            if (name != o.name) return name < o.name;
            return mask < o.mask;
        }
    };
    std::map<SubKey, std::shared_ptr<PVInfo>> pvs;
    std::map<std::string, chid> channelsByName;
};
class GateCAClientManager {
public:
    static GateCAClientManager& instance();
    std::shared_ptr<GateCAClient> createClient(const std::string& name, const std::string& addrList = "");
    std::shared_ptr<GateCAClient> getClient(const std::string& name);
private:
    GateCAClientManager() = default;
    std::map<std::string, std::shared_ptr<GateCAClient>> clients;
    std::mutex mutex;
};
#endif
