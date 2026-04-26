#include "common/message.h"

#include <arpa/inet.h>

#include <iostream>

#include "common/serialize.h"

std::string serialize_produce_request(const ProduceRequest& req) {
    std::string body;

    // header
    encode_int16(body, req.header.api_key);
    encode_int16(body, req.header.api_version);
    encode_int32(body, req.header.correlation_id);
    encode_nullable_string(body, req.header.client_id);

    // body
    encode_nullable_string(body, req.transactional_id);
    encode_int16(body, req.acks);
    encode_int32(body, req.timeout_ms);

    // topics array
    encode_int32(body, static_cast<int32_t>(req.topics.size()));
    for (const auto& topic : req.topics) {
        encode_string(body, topic.name);

        // partitions array
        encode_int32(body, static_cast<int32_t>(topic.partitions.size()));
        for (const auto& part : topic.partitions) {
            encode_int32(body, part.partition_index);
            
            std::string batch_data = serialize_batch(part.records);
            encode_int32(body, static_cast<int32_t>(batch_data.size()));
            body += batch_data;
        }
    }

    // Prepend 4-byte total size
    std::string final_buf;
    encode_int32(final_buf, static_cast<int32_t>(body.size()));
    final_buf += body;

    return final_buf;
}

std::string serialize_consume_request(const ConsumeRequest& req) {
    std::string body;
    encode_int16(body, req.header.api_key);
    encode_int16(body, req.header.api_version);
    encode_int32(body, req.header.correlation_id);
    encode_nullable_string(body, req.header.client_id);

    encode_int32(body, req.replica_id);
    encode_int32(body, req.max_wait_time);
    encode_int32(body, req.min_bytes);

    encode_int32(body, static_cast<int32_t>(req.topics.size()));
    for (const auto& topic : req.topics) {
        encode_string(body, topic.name);
        encode_int32(body, static_cast<int32_t>(topic.partitions.size()));
        for (const auto& part : topic.partitions) {
            encode_int32(body, part.partition_index);
            encode_int64(body, part.fetch_offset);
            encode_int32(body, part.max_bytes);
        }
    }

    std::string final_buf;
    encode_int32(final_buf, static_cast<int32_t>(body.size()));
    final_buf += body;
    return final_buf;
}

bool parse_produce_request(const char* data, size_t len, ProduceRequest& req) {
    size_t pos = 0;
    
    // Read header
    int read = decode_int16(data + pos, len - pos, req.header.api_key);
    if (read < 0) return false; pos += read;
    
    read = decode_int16(data + pos, len - pos, req.header.api_version);
    if (read < 0) return false; pos += read;
    
    read = decode_int32(data + pos, len - pos, req.header.correlation_id);
    if (read < 0) return false; pos += read;
    
    read = decode_nullable_string(data + pos, len - pos, req.header.client_id);
    if (read < 0) return false; pos += read;

    // Read body
    read = decode_nullable_string(data + pos, len - pos, req.transactional_id);
    if (read < 0) return false; pos += read;
    
    read = decode_int16(data + pos, len - pos, req.acks);
    if (read < 0) return false; pos += read;
    
    read = decode_int32(data + pos, len - pos, req.timeout_ms);
    if (read < 0) return false; pos += read;

    // Read topics array
    int32_t topics_count;
    read = decode_int32(data + pos, len - pos, topics_count);
    if (read < 0) return false; pos += read;
    
    for (int32_t i = 0; i < topics_count; ++i) {
        TopicProduceData topic;
        read = decode_string(data + pos, len - pos, topic.name);
        if (read < 0) return false; pos += read;
        
        int32_t parts_count;
        read = decode_int32(data + pos, len - pos, parts_count);
        if (read < 0) return false; pos += read;
        
        for (int32_t j = 0; j < parts_count; ++j) {
            PartitionProduceData part;
            read = decode_int32(data + pos, len - pos, part.partition_index);
            if (read < 0) return false; pos += read;
            
            read = decode_int32(data + pos, len - pos, part.records_size);
            if (read < 0) return false; pos += read;
            
            if (pos + part.records_size > len) return false;
            if (!deserialize_batch(data + pos, part.records_size, part.records)) return false;
            pos += part.records_size;
            
            topic.partitions.push_back(std::move(part));
        }
        req.topics.push_back(std::move(topic));
    }

    return true;
}

bool parse_consume_request(const char* data, size_t len, ConsumeRequest& req) {
    size_t pos = 0;
    int    read;

    read = decode_int16(data + pos, len - pos, req.header.api_key);
    if (read < 0) return false; pos += read;
    read = decode_int16(data + pos, len - pos, req.header.api_version);
    if (read < 0) return false; pos += read;
    read = decode_int32(data + pos, len - pos, req.header.correlation_id);
    if (read < 0) return false; pos += read;
    read = decode_nullable_string(data + pos, len - pos, req.header.client_id);
    if (read < 0) return false; pos += read;

    read = decode_int32(data + pos, len - pos, req.replica_id);
    if (read < 0) return false; pos += read;
    read = decode_int32(data + pos, len - pos, req.max_wait_time);
    if (read < 0) return false; pos += read;
    read = decode_int32(data + pos, len - pos, req.min_bytes);
    if (read < 0) return false; pos += read;

    int32_t topic_count;
    read = decode_int32(data + pos, len - pos, topic_count);
    if (read < 0) return false; pos += read;

    for (int i = 0; i < topic_count; ++i) {
        TopicConsumeData topic;
        read = decode_string(data + pos, len - pos, topic.name);
        if (read < 0) return false; pos += read;

        int32_t part_count;
        read = decode_int32(data + pos, len - pos, part_count);
        if (read < 0) return false; pos += read;

        for (int j = 0; j < part_count; ++j) {
            PartitionConsumeData part;
            read = decode_int32(data + pos, len - pos, part.partition_index);
            if (read < 0) return false; pos += read;
            read = decode_int64(data + pos, len - pos, part.fetch_offset);
            if (read < 0) return false; pos += read;
            read = decode_int32(data + pos, len - pos, part.max_bytes);
            if (read < 0) return false; pos += read;
            topic.partitions.push_back(std::move(part));
        }
        req.topics.push_back(std::move(topic));
    }

    return true;
}

std::string serialize_produce_response(const ProduceResponse& res) {
    std::string body;
    encode_int32(body, res.correlation_id);

    // Topics array
    encode_int32(body, static_cast<int32_t>(res.topics.size()));
    for (const auto& topic : res.topics) {
        encode_string(body, topic.name);

        // Partitions array
        encode_int32(body, static_cast<int32_t>(topic.partitions.size()));
        for (const auto& part : topic.partitions) {
            encode_int32(body, part.partition_index);
            encode_int16(body, part.error_code);
            encode_int64(body, part.base_offset);
            encode_int64(body, part.log_append_time);
            encode_int64(body, part.log_start_offset);
        }
    }
    encode_int32(body, res.throttle_time_ms);

    // Prepend size
    std::string final_buf;
    encode_int32(final_buf, static_cast<int32_t>(body.size()));
    final_buf += body;
    return final_buf;
}

bool parse_produce_response(const char* data, size_t len, ProduceResponse& res) {
    size_t pos = 0;
    int    read;

    read = decode_int32(data + pos, len - pos, res.correlation_id);
    if (read < 0) return false; pos += read;

    int32_t topic_count;
    read = decode_int32(data + pos, len - pos, topic_count);
    if (read < 0) return false; pos += read;

    for (int i = 0; i < topic_count; ++i) {
        TopicProduceResponse topic;
        read = decode_string(data + pos, len - pos, topic.name);
        if (read < 0) return false; pos += read;

        int32_t part_count;
        read = decode_int32(data + pos, len - pos, part_count);
        if (read < 0) return false; pos += read;

        for (int j = 0; j < part_count; ++j) {
            PartitionProduceResponse part;
            read = decode_int32(data + pos, len - pos, part.partition_index);
            if (read < 0) return false; pos += read;
            read = decode_int16(data + pos, len - pos, part.error_code);
            if (read < 0) return false; pos += read;
            read = decode_int64(data + pos, len - pos, part.base_offset);
            if (read < 0) return false; pos += read;
            read = decode_int64(data + pos, len - pos, part.log_append_time);
            if (read < 0) return false; pos += read;
            read = decode_int64(data + pos, len - pos, part.log_start_offset);
            if (read < 0) return false; pos += read;
            topic.partitions.push_back(part);
        }
        res.topics.push_back(std::move(topic));
    }

    read = decode_int32(data + pos, len - pos, res.throttle_time_ms);
    if (read < 0) return false; pos += read;

    return true;
}

std::string serialize_fetch_response_header(const FetchResponse& res) {
    std::string body;
    encode_int32(body, res.correlation_id);
    encode_int32(body, res.throttle_time_ms);

    encode_int32(body, static_cast<int32_t>(res.topics.size()));
    for (const auto& topic : res.topics) {
        encode_string(body, topic.name);
        encode_int32(body, static_cast<int32_t>(topic.partitions.size()));
        for (const auto& part : topic.partitions) {
            encode_int32(body, part.partition_index);
            encode_int16(body, part.error_code);
            encode_int64(body, part.high_watermark);
            encode_int64(body, part.last_stable_offset);
            encode_int64(body, part.log_start_offset);
            encode_int32(body, 0); // Placeholder for aborted_transactions count
            
            // Note: RecordSet size and data will be handled by the caller
            // because they might use sendfile.
        }
    }
    
    // We don't prepend the total size here because we don't know the RecordSet size yet.
    return body;
}

bool parse_fetch_response(const char* data, size_t len, FetchResponse& res, std::string& record_set) {
    size_t pos = 0;
    int    read;

    read = decode_int32(data + pos, len - pos, res.correlation_id);
    if (read < 0) return false; pos += read;
    read = decode_int32(data + pos, len - pos, res.throttle_time_ms);
    if (read < 0) return false; pos += read;

    int32_t topic_count;
    read = decode_int32(data + pos, len - pos, topic_count);
    if (read < 0) return false; pos += read;

    for (int i = 0; i < topic_count; ++i) {
        TopicFetchResponse topic;
        read = decode_string(data + pos, len - pos, topic.name);
        if (read < 0) return false; pos += read;

        int32_t part_count;
        read = decode_int32(data + pos, len - pos, part_count);
        if (read < 0) return false; pos += read;

        for (int j = 0; j < part_count; ++j) {
            PartitionFetchResponse part;
            read = decode_int32(data + pos, len - pos, part.partition_index);
            if (read < 0) return false; pos += read;
            read = decode_int16(data + pos, len - pos, part.error_code);
            if (read < 0) return false; pos += read;
            read = decode_int64(data + pos, len - pos, part.high_watermark);
            if (read < 0) return false; pos += read;
            read = decode_int64(data + pos, len - pos, part.last_stable_offset);
            if (read < 0) return false; pos += read;
            read = decode_int64(data + pos, len - pos, part.log_start_offset);
            if (read < 0) return false; pos += read;
            
            int32_t aborted_count;
            read = decode_int32(data + pos, len - pos, aborted_count);
            if (read < 0) return false; pos += read;
            // Skip aborted transactions (we don't support them)
            
            int32_t record_set_size;
            read = decode_int32(data + pos, len - pos, record_set_size);
            if (read < 0) return false; pos += read;
            
            if (pos + record_set_size > len) return false;
            record_set.assign(data + pos, record_set_size);
            pos += record_set_size;

            topic.partitions.push_back(part);
        }
        res.topics.push_back(std::move(topic));
    }

    return true;
}
// ---------------------------------------------------------------------------
// Helpers for new APIs
// ---------------------------------------------------------------------------

int16_t peek_api_key(const char* data, size_t len) {
    int16_t key = -1;
    if (len < 2) return key;
    decode_int16(data, len, key);
    return key;
}

bool parse_api_versions_request(const char* data, size_t len, ApiVersionsRequest& req) {
    size_t pos = 0;
    int    rd;

    rd = decode_int16(data + pos, len - pos, req.header.api_key);
    if (rd < 0) return false; pos += rd;
    rd = decode_int16(data + pos, len - pos, req.header.api_version);
    if (rd < 0) return false; pos += rd;
    rd = decode_int32(data + pos, len - pos, req.header.correlation_id);
    if (rd < 0) return false; pos += rd;
    rd = decode_nullable_string(data + pos, len - pos, req.header.client_id);
    if (rd < 0) return false; pos += rd;
    // v0-v2: no body fields
    (void)pos;
    return true;
}

bool parse_metadata_request(const char* data, size_t len, MetadataRequest& req) {
    size_t pos = 0;
    int    rd;

    rd = decode_int16(data + pos, len - pos, req.header.api_key);
    if (rd < 0) return false; pos += rd;
    rd = decode_int16(data + pos, len - pos, req.header.api_version);
    if (rd < 0) return false; pos += rd;
    rd = decode_int32(data + pos, len - pos, req.header.correlation_id);
    if (rd < 0) return false; pos += rd;
    rd = decode_nullable_string(data + pos, len - pos, req.header.client_id);
    if (rd < 0) return false; pos += rd;

    // topics array — null array (count = -1) means "all topics" in Metadata v0-v5
    int32_t topic_count;
    rd = decode_int32(data + pos, len - pos, topic_count);
    if (rd < 0) return false; pos += rd;

    for (int32_t i = 0; i < topic_count; ++i) {
        std::string name;
        rd = decode_string(data + pos, len - pos, name);
        if (rd < 0) return false; pos += rd;
        req.topics.push_back(std::move(name));
    }
    (void)pos;
    return true;
}

// ---------------------------------------------------------------------------
// serialize_api_versions_response
// Format (v0-v2, non-flexible):
//   [4B correlation_id] [2B error_code] [4B array_len]
//     per entry: [2B api_key] [2B min_ver] [2B max_ver]
//   [4B throttle_time_ms]
// Prepend 4-byte total size.
// ---------------------------------------------------------------------------
std::string serialize_api_versions_response(const ApiVersionsResponse& res) {
    std::string body;
    encode_int32(body, res.correlation_id);
    encode_int16(body, res.error_code);

    encode_int32(body, static_cast<int32_t>(res.api_versions.size()));
    for (const auto& entry : res.api_versions) {
        encode_int16(body, entry.api_key);
        encode_int16(body, entry.min_version);
        encode_int16(body, entry.max_version);
    }
    encode_int32(body, res.throttle_time_ms);

    std::string out;
    encode_int32(out, static_cast<int32_t>(body.size()));
    out += body;
    return out;
}

// ---------------------------------------------------------------------------
// serialize_metadata_response
// Format (v0-v4, non-flexible):
//   [4B correlation_id]
//   [4B broker_count]
//     broker: [4B node_id] [2B+str host] [4B port] [2B rack=null=-1]
//   [4B controller_id]      <- v1+
//   [4B topic_count]
//     topic: [2B err] [2B+str name] [1B is_internal] [4B part_count]
//       part: [2B err] [4B index] [4B leader] [4B replica_count] [4B*N] [4B isr_count] [4B*N]
// Prepend 4-byte total size.
// ---------------------------------------------------------------------------
std::string serialize_metadata_response(const MetadataResponse& res) {
    std::string body;
    encode_int32(body, res.correlation_id);

    // Brokers
    encode_int32(body, static_cast<int32_t>(res.brokers.size()));
    for (const auto& b : res.brokers) {
        encode_int32(body, b.node_id);
        encode_string(body, b.host);
        encode_int32(body, b.port);
        encode_int16(body, -1);  // rack = null
    }

    // controller_id (v1+)
    encode_int32(body, res.controller_id);

    // Topics
    encode_int32(body, static_cast<int32_t>(res.topics.size()));
    for (const auto& t : res.topics) {
        encode_int16(body, t.error_code);
        encode_string(body, t.name);
        body.push_back(0);  // is_internal = false (1 byte)

        encode_int32(body, static_cast<int32_t>(t.partitions.size()));
        for (const auto& p : t.partitions) {
            encode_int16(body, p.error_code);
            encode_int32(body, p.partition_index);
            encode_int32(body, p.leader_id);

            // replicas array = [leader_id]
            encode_int32(body, 1);
            encode_int32(body, p.leader_id);

            // isr array = [leader_id]
            encode_int32(body, 1);
            encode_int32(body, p.leader_id);
        }
    }

    std::string out;
    encode_int32(out, static_cast<int32_t>(body.size()));
    out += body;
    return out;
}
