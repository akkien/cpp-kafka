#include <cstdint>
#include <string>
#include <variant>

#include "common/batch.h"

enum class ReqType : uint16_t {
    PRODUCE = 0,
    CONSUME = 1,
};

struct ProduceRequest {
    ReqType     api_key;
    std::string topic;
    Batch       batch;
};

struct ConsumeRequest {
    ReqType     api_key;
    std::string topic;
    uint64_t    offset;
    uint32_t    max_bytes;
};

using Request = std::variant<ProduceRequest, ConsumeRequest>;

std::string serialize_produce_request(const ProduceRequest& req);
std::string serialize_consume_request(const ConsumeRequest& req);

bool parse_produce_request(const char* data, size_t len, ProduceRequest& req);
bool parse_consume_request(const char* data, size_t len, ConsumeRequest& req);