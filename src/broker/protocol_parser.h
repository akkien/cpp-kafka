#pragma once

#include <cstdint>
#include <string>
#include <variant>

namespace kafka {

// ─── Parsed request types ────────────────────────────────────────────

struct ProduceRequest {
    std::string topic;
    uint32_t    payload_len{0};
    std::string payload;
};

struct ConsumeRequest {
    std::string topic;
    uint64_t    offset{0};
    uint32_t    max_bytes{0};
};

struct ListTopicsRequest {};

struct BadRequest {
    std::string reason;
};

using Request = std::variant<ProduceRequest, ConsumeRequest, ListTopicsRequest, BadRequest>;

// ─── Parser ──────────────────────────────────────────────────────────

/// Stateless helper that turns a raw text line into a typed request.
class ProtocolParser {
public:
    /// Parse the command line (everything up to the first '\n').
    /// For PRODUCE requests the caller must still supply the payload body
    /// separately via `attach_payload()`.
    static Request parse(const std::string& line);

    /// Attach the raw payload bytes to an already-parsed ProduceRequest.
    static void attach_payload(ProduceRequest& req, const std::string& payload);
};

}  // namespace kafka
