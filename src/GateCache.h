#ifndef GATE_CACHE_H
#define GATE_CACHE_H
#include <string>
#include <memory>
#include <mutex>
#include <map>
#include <vector>
#include <list>
#include <epicsTime.h>

struct dbChannel;
struct db_field_log;

struct GateData {
    int dbrType;
    long count;
    std::vector<char> data;
    epicsTimeStamp timestamp;
    short status;
    short severity;
};
typedef std::shared_ptr<const GateData> GateDataPtr;
typedef void (GateEventFunc)(void *user_arg, struct dbChannel *chan,
    int eventsRemaining, struct db_field_log *pfl);
struct GateSubscription {
    GateEventFunc* callback;
    void* user_arg;
    struct dbChannel* chan;
    unsigned int mask;
};
class GateCacheEntry {
public:
    GateCacheEntry(const std::string& name);
    void update(GateDataPtr newData);
    GateDataPtr getData() const;
    const std::string& getName() const { return name; }
    void addSubscription(GateSubscription* sub);
    void removeSubscription(GateSubscription* sub);
private:
    std::string name;
    GateDataPtr currentData;
    std::list<GateSubscription*> subscriptions;
    mutable std::mutex mutex;
};
class GateCache {
public:
    static GateCache& instance();
    std::shared_ptr<GateCacheEntry> findOrCreate(const std::string& name);
    std::shared_ptr<GateCacheEntry> find(const std::string& name);
private:
    GateCache() = default;
    std::map<std::string, std::shared_ptr<GateCacheEntry>> entries;
    std::mutex mutex;
};
#endif
