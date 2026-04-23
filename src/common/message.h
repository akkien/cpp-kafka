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

struct PartitionProduceResponse {
    int32_t  partition_index;
    int16_t  error_code;
    int64_t  base_offset;
    int64_t  log_append_time;
    int64_t  log_start_offset;
};

struct TopicProduceResponse {
    std::string                         name;
    std::vector<PartitionProduceResponse> partitions;
};

struct ProduceResponse {
    int32_t                         correlation_id;
    std::vector<TopicProduceResponse> topics;
    int32_t                         throttle_time_ms;
};

using Request = std::variant<ProduceRequest, ConsumeRequest>;

std::string serialize_produce_request(const ProduceRequest& req);
std::string serialize_consume_request(const ConsumeRequest& req);
std::string serialize_produce_response(const ProduceResponse& res);

bool parse_produce_request(const char* data, size_t len, ProduceRequest& req);
bool parse_consume_request(const char* data, size_t len, ConsumeRequest& req);
bool parse_produce_response(const char* data, size_t len, ProduceResponse& res);