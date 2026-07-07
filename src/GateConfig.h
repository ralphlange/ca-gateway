#ifndef GATE_CONFIG_H
#define GATE_CONFIG_H
#include <string>
#include <vector>
#include <regex>
#include <mutex>

class GateConfig {
public:
    static GateConfig& instance();
    void addPVPattern(const std::string& pattern, const std::string& clientName);
    bool isAllowed(const std::string& pvName, std::string& clientName);
private:
    GateConfig() = default;
    struct Entry {
        std::string patternStr;
        std::regex pattern;
        std::string clientName;
    };
    std::vector<Entry> entries;
    std::mutex mutex;
};
#endif
