#ifndef GATE_CONFIG_H
#define GATE_CONFIG_H
#include <string>
#include <vector>
#include <mutex>
#include <map>

class GateConfig {
public:
    static GateConfig& instance();
    bool isAllowed(const std::string& pvName, const std::string& user, const std::string& host, std::string& clientName);
    void load(const std::string& filename);
    void reload();
    void report(int level);
    void addPVPattern(const std::string& pattern, const std::string& clientName);
private:
    std::string filename;
    std::map<std::string, std::string> pvPatterns;
    std::mutex mutex;
};
#endif
