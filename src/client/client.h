#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "client/connection.h"
#include "common/types.h"

namespace kafka {

/// High-level client for talking to the Mini Kafka broker.
class Client {
public:
    Client(const std::string& host, uint16_t port);
    ~Client() = default;

    /// Connect to the broker. Returns false on failure.
    bool connect();

    /// Produce a message to a topic. Returns the offset on success, -1 on error.
    int64_t produce(const std::string& topic, const std::string& payload);

    /// Consume messages from a topic starting at the given offset.
    std::vector<Message> consume(const std::string& topic,
                                  uint64_t offset,
                                  uint32_t max_bytes = 4096);

    /// List all known topics on the broker.
    std::vector<std::string> list_topics();

private:
    std::string host_;
    uint16_t    port_;
    Connection  conn_;
};

}  // namespace kafka
