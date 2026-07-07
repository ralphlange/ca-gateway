#include "GateConfig.h"
GateConfig& GateConfig::instance() {
    static GateConfig config;
    return config;
}
void GateConfig::addPVPattern(const std::string& pattern, const std::string& clientName) {
    std::lock_guard<std::mutex> lock(mutex);
    entries.push_back({pattern, std::regex(pattern), clientName});
}
bool GateConfig::isAllowed(const std::string& pvName, std::string& clientName) {
    std::lock_guard<std::mutex> lock(mutex);
    for (const auto& entry : entries) {
        if (std::regex_match(pvName, entry.pattern)) {
            clientName = entry.clientName;
            return true;
        }
    }
    return false;
}
