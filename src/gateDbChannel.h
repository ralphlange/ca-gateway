#ifndef GATE_DB_CHANNEL_H
#define GATE_DB_CHANNEL_H
#include "dbChannel.h"
#include "GateCache.h"
struct dbChannelGate : public dbChannel {
    std::shared_ptr<GateCacheEntry> cacheEntry;
};
#endif
