#ifndef GATE_DB_CHANNEL_H
#define GATE_DB_CHANNEL_H
#include "dbChannel.h"
#include "GateCache.h"
#include <memory>

struct dbChannelGate {
    dbChannel chan;
    std::shared_ptr<GateCacheEntry> cacheEntry;
};
#endif
