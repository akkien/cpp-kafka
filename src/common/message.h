#pragma once

#include <cstdint>
#include <string>
#include <variant>

#include "common/batch.h"

enum class ReqType : int16_t {
    PRODUCE = 0,
    CONSUME = 1,
};

struct ConsumeRequest {
    ReqType     api_key;
    std::string topic;
    uint64_t    offset;
    uint32_t    max_bytes;
};

struct RequestHeader {
    int16_t     api_key;
    int16_t     api_version;
    int32_t     correlation_id;
    std::string client_id;  // nullable string
};

struct PartitionProduceData {
    int32_t partition_index;
    int32_t records_size;
    Batch   records;
};

struct TopicProduceData {
    std::string                       name;
    std::vector<PartitionProduceData> partitions;
};

struct ProduceRequest {
    RequestHeader                 header;
    std::string                   transactional_id;  // nullable string
    int16_t                       acks;
    int32_t                       timeout_ms;
    std::vector<TopicProduceData> topics;
};

using Request = std::variant<ProduceRequest, ConsumeRequest>;

std::string serialize_produce_request(const ProduceRequest& req);
std::string serialize_consume_request(const ConsumeRequest& req);

bool parse_produce_request(const char* data, size_t len, ProduceRequest& req);
bool parse_consume_request(const char* data, size_t len, ConsumeRequest& req);