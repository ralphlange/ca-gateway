#include "GateConfig.h"
#include <iostream>
#include <regex>

GateConfig& GateConfig::instance() {
    static GateConfig inst;
    return inst;
}

bool GateConfig::isAllowed(const std::string& pvName, const std::string& user, const std::string& host, std::string& clientName) {
    std::lock_guard<std::mutex> lock(mutex);
    for (auto const& [pattern, client] : pvPatterns) {
        try {
            if (std::regex_match(pvName, std::regex(pattern))) {
                clientName = client;
                return true;
            }
        } catch (...) {}
    }
    clientName = "default";
    return true;
}

void GateConfig::addPVPattern(const std::string& pattern, const std::string& clientName) {
    std::lock_guard<std::mutex> lock(mutex);
    pvPatterns[pattern] = clientName;
}

void GateConfig::load(const std::string& filename) {
    std::lock_guard<std::mutex> lock(mutex);
    this->filename = filename;
}

void GateConfig::reload() {
    std::lock_guard<std::mutex> lock(mutex);
}

void GateConfig::report(int level) {
    std::lock_guard<std::mutex> lock(mutex);
    std::cout << "Gateway Config Report" << std::endl;
    for (auto const& [pattern, client] : pvPatterns) {
        std::cout << "  Pattern: " << pattern << " => Client: " << client << std::endl;
    }
}
