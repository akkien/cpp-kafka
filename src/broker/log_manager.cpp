#include "broker/log_manager.h"

#include <fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstring>
#include <filesystem>
#include <iostream>
#include <stdexcept>

namespace fs = std::filesystem;
namespace kafka {

LogManager& LogManager::instance() {
    static LogManager inst;
    return inst;
}

LogManager::LogManager() {
    fs::create_directories(data_dir_);
    try {
        for (const auto& entry : fs::directory_iterator(data_dir_)) {
            get_or_create(entry.path().stem().string());
        }
    } catch (const fs::filesystem_error& e) {
        std::cerr << "Error: " << e.what() << "\n";
    }
}

LogManager::~LogManager() {}

// ---------------------------------------------------------------------------
bool LogManager::topic_exists(const std::string& topic) {
    auto it = topics_.find(topic);
    return it != topics_.end();
}

TopicState& LogManager::get_or_create(const std::string& topic) {
    // Must be called with mu_ held.
    auto it = topics_.find(topic);
    if (it != topics_.end())
        return *(it->second);  // dereference to borrow object

    std::unique_ptr<TopicState> state = std::make_unique<TopicState>(data_dir_, topic);
    auto [inserted, _]                = topics_.emplace(topic, std::move(state));
    return *(inserted->second);
}

// ---------------------------------------------------------------------------
uint64_t LogManager::append(const std::string& topic, Batch& batch) {
    TopicState* state_ptr = nullptr;
    {
        std::lock_guard lock(mu_);
        state_ptr = &get_or_create(topic);
    }
    return state_ptr->append(batch);
}

// TODO: handle max_bytes
bool LogManager::send(int const& client_fd, const std::string& topic, uint64_t offset, uint32_t max_bytes) {
    TopicState* state_ptr = nullptr;

    {
        std::lock_guard lock(mu_);
        if (!topic_exists(topic)) {
            std::cout << "[LogManager::send] topic not found: " << topic << std::endl;
            off_t amt_to_send = 0;
            ::send(client_fd, &amt_to_send, sizeof(amt_to_send), 0);
            return {};
        }

        state_ptr = &get_or_create(topic);
    }

    std::cout << "[LogManager::send] topic: " << topic << ", offset: " << offset << ", max_bytes: " << max_bytes
              << std::endl;

    return state_ptr->send(client_fd, offset, max_bytes);
}

// ---------------------------------------------------------------------------
std::vector<std::string> LogManager::list_topics() const {
    std::lock_guard          lock(mu_);
    std::vector<std::string> names;
    names.reserve(topics_.size());
    for (const auto& [name, _] : topics_) {
        names.push_back(name);
    }
    return names;
}

}  // namespace kafka
