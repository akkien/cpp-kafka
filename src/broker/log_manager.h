#pragma once

#include <iostream>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/batch.h"
#include "common/types.h"

const int INDEX_INTERVAL = 50;  // 4096 in real kafka

namespace kafka {

struct IndexEntry {
    /// @dev real msg_offset use uint32_t because the value is relatively to the first message offset of the file
    /// we save the absolute offset to keep it simple at the moment
    int64_t  msg_offset;
    uint64_t byte_offset;
};

/// Runtime bookkeeping for one topic.
struct TopicState {
    int                     log_fd{-1};          // File descriptor for the .log file
    uint64_t                next_log_offset{0};  // Next available byte offset
    int                     idx_fd{-1};          // File descriptor for the .idx file
    uint64_t                next_idx_offset{0};  // Next available byte offset of index file
    std::vector<IndexEntry> indexes;
    uint64_t                next_msg_offset{0};
    uint16_t                bytes_since_last_index_entry{0};
};

inline std::ostream& operator<<(std::ostream& os, const TopicState& s) {
    return os << "{\n"
              << "  log_fd: " << s.log_fd << ",\n"
              << "  next_log_offset: " << s.next_log_offset << ",\n"
              << "  idx_fd: " << s.idx_fd << ",\n"
              << "  next_idx_offset: " << s.next_idx_offset << ",\n"
              << "  next_msg_offset: " << s.next_msg_offset << ",\n"
              << "  bytes_since_last_index_entry: " << s.bytes_since_last_index_entry << "\n"
              << "}\n";
}

/// Manages append-only log files — one per topic.
class LogManager {
public:
    /// Get the singleton instance.
    static LogManager& instance();

    /// Append a message payload to the given topic.
    /// Returns the byte offset where the message was written.
    uint64_t append(const std::string& topic, Batch& batch);

    bool send(int const& client_fd, const std::string& topic, uint64_t offset, uint32_t max_bytes);
    /// Return the list of known topic names.
    std::vector<std::string> list_topics() const;

private:
    LogManager();
    ~LogManager();
    LogManager(const LogManager&)            = delete;
    LogManager& operator=(const LogManager&) = delete;

    void  open_topic_logs(const std::string& topic, TopicState& state);
    void  open_topic_index(const std::string& topic, TopicState& state);
    Batch find_last_batch(const TopicState& state);
    /// Lazily open (or create) the log file for a topic.
    TopicState& get_or_create(const std::string& topic);

    std::string                                 data_dir_{"data"};
    mutable std::mutex                          mu_;
    std::unordered_map<std::string, TopicState> topics_;
};

}  // namespace kafka
