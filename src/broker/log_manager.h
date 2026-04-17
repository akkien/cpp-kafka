#pragma once

#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/types.h"

namespace kafka {

/// Manages append-only log files — one per topic.
class LogManager {
public:
    /// Get the singleton instance.
    static LogManager& instance();

    /// Append a message payload to the given topic.
    /// Returns the byte offset where the message was written.
    uint64_t append(const std::string& topic, const std::string& payload);

    /// Read messages starting at `offset` up to `max_bytes` worth of data.
    std::vector<Message> read(const std::string& topic, uint64_t offset, uint32_t max_bytes);

    /// Return the list of known topic names.
    std::vector<std::string> list_topics() const;

private:
    LogManager();
    ~LogManager();
    LogManager(const LogManager&) = delete;
    LogManager& operator=(const LogManager&) = delete;

    /// Lazily open (or create) the log file for a topic.
    TopicState& get_or_create(const std::string& topic);

    std::string                                data_dir_{"data"};
    mutable std::mutex                         mu_;
    std::unordered_map<std::string, TopicState> topics_;
};

}  // namespace kafka
