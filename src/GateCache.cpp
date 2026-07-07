#include "GateCache.h"
GateCacheEntry::GateCacheEntry(const std::string& name) : name(name) {}
void GateCacheEntry::update(GateDataPtr newData) {
    std::lock_guard<std::mutex> lock(mutex);
    currentData = newData;
    for (auto sub : subscriptions) {
        if (sub->callback) {
            sub->callback(sub->user_arg, sub->chan, 0, nullptr);
        }
    }
}
GateDataPtr GateCacheEntry::getData() const {
    std::lock_guard<std::mutex> lock(mutex);
    return currentData;
}
void GateCacheEntry::addSubscription(GateSubscription* sub) {
    std::lock_guard<std::mutex> lock(mutex);
    subscriptions.push_back(sub);
}
void GateCacheEntry::removeSubscription(GateSubscription* sub) {
    std::lock_guard<std::mutex> lock(mutex);
    subscriptions.remove(sub);
}
GateCache& GateCache::instance() {
    static GateCache cache;
    return cache;
}
std::shared_ptr<GateCacheEntry> GateCache::findOrCreate(const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex);
    auto it = entries.find(name);
    if (it == entries.end()) {
        auto entry = std::make_shared<GateCacheEntry>(name);
        entries[name] = entry;
        return entry;
    }
    return it->second;
}
std::shared_ptr<GateCacheEntry> GateCache::find(const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex);
    auto it = entries.find(name);
    if (it == entries.end()) {
        return nullptr;
    }
    return it->second;
}
