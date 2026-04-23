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


