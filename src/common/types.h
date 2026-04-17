#pragma once

#include <cstdint>
#include <string>

namespace kafka {

/// On-disk and in-memory message representation.
struct Message {
    uint64_t    offset;   // Byte offset in the log file
    uint32_t    size;     // Payload size in bytes
    std::string payload;  // Message data
};

/// Runtime bookkeeping for one topic.
struct TopicState {
    int      fd{-1};           // File descriptor for the .log file
    uint64_t next_offset{0};   // Next available byte offset
};

/// Default listen port (matches Apache Kafka convention).
constexpr uint16_t kDefaultPort = 9092;

/// Size of the fixed header written before each message payload.
///   offset (8 bytes) + size (4 bytes) = 12 bytes
constexpr size_t kMessageHeaderSize = sizeof(uint64_t) + sizeof(uint32_t);

}  // namespace kafka
