#include "broker/log_manager.h"

#include <fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstring>
#include <filesystem>
#include <iostream>
#include <stdexcept>

#include "common/message.h"
#include "common/serialize.h"

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
bool LogManager::send(int const& client_fd, int32_t correlation_id, const std::string& topic, uint64_t offset,
                      uint32_t max_bytes) {
    TopicState* state_ptr = nullptr;

    {
        std::lock_guard lock(mu_);
        if (!topic_exists(topic)) {
            std::cout << "[LogManager::send] topic not found: " << topic << std::endl;

            // Send FetchResponse with error code UNKNOWN_TOPIC_OR_PARTITION (3)
            FetchResponse res;
            res.correlation_id   = correlation_id;
            res.throttle_time_ms = 0;
            TopicFetchResponse t_res;
            t_res.name = topic;
            PartitionFetchResponse p_res;
            p_res.partition_index    = 0;
            p_res.error_code         = 3;  // UNKNOWN_TOPIC_OR_PARTITION
            p_res.high_watermark     = 0;
            p_res.last_stable_offset = 0;
            p_res.log_start_offset   = 0;
            t_res.partitions.push_back(p_res);
            res.topics.push_back(t_res);

            std::string header_buf = serialize_fetch_response_header(res);
            int32_t     total_size = static_cast<int32_t>(header_buf.size() + 4);  // +4 for record_set_size

            std::string final_header;
            encode_int32(final_header, total_size);
            final_header += header_buf;
            encode_int32(final_header, 0);  // record_set_size = 0

            ::send(client_fd, final_header.data(), final_header.size(), 0);
            return false;
        }

        state_ptr = &get_or_create(topic);
    }

    std::cout << "[LogManager::send] topic: " << topic << ", offset: " << offset << ", max_bytes: " << max_bytes
              << std::endl;

    return state_ptr->send(client_fd, correlation_id, offset, max_bytes);
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
