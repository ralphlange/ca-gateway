#include "GateCache.h"
#include <cstring>
#include <algorithm>
#include <iostream>

GateCache& GateCache::instance() {
    static GateCache inst;
    return inst;
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

void GateCache::report(int level) {
    std::lock_guard<std::mutex> lock(mutex);
    std::cout << "Gateway Cache Report: " << entries.size() << " PVs cached." << std::endl;
}

void GateCacheEntry::update(std::shared_ptr<GateData> newData) {
    std::lock_guard<std::mutex> lock(mutex);
    data = newData;

    for (auto sub : subscriptions) {
        if (sub->callback) {
            sub->callback(sub->user_arg, sub->chan, data->dbrType, data->count, data->data.data());
        }
    }
}

std::shared_ptr<GateData> GateCacheEntry::getData() {
    std::lock_guard<std::mutex> lock(mutex);
    return data;
}

void GateCacheEntry::addSubscription(GateSubscription* sub) {
    std::lock_guard<std::mutex> lock(mutex);
    subscriptions.push_back(sub);
}

void GateCacheEntry::removeSubscription(GateSubscription* sub) {
    std::lock_guard<std::mutex> lock(mutex);
    subscriptions.erase(std::remove(subscriptions.begin(), subscriptions.end(), sub), subscriptions.end());
}
