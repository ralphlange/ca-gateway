#ifndef GATE_CACHE_H
#define GATE_CACHE_H
#include <string>
#include <map>
#include <vector>
#include <memory>
#include <mutex>
#include "db_access.h"
#include <epicsTime.h>

struct GateData {
    int dbrType;
    long count;
    std::vector<char> data;
    epicsTimeStamp timestamp;
    short status;
    short severity;
};

typedef void (GateEventFunc)(void *user_arg, struct dbChannel *chan,
                             int dbrType, int count, void *pbuffer);

struct GateSubscription {
    GateEventFunc *callback;
    void *user_arg;
    struct dbChannel *chan;
    unsigned int mask;
};

class GateCacheEntry {
public:
    GateCacheEntry(const std::string& name) : name(name) {}
    void update(std::shared_ptr<GateData> newData);
    std::shared_ptr<GateData> getData();
    void addSubscription(GateSubscription* sub);
    void removeSubscription(GateSubscription* sub);
private:
    std::string name;
    std::shared_ptr<GateData> data;
    std::vector<GateSubscription*> subscriptions;
    std::mutex mutex;
};

class GateCache {
public:
    static GateCache& instance();
    std::shared_ptr<GateCacheEntry> findOrCreate(const std::string& name);
    void report(int level);
private:
    std::map<std::string, std::shared_ptr<GateCacheEntry>> entries;
    std::mutex mutex;
};
#endif
