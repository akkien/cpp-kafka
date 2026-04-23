#include "client/client.h"
#include "common/serialize.h"

#include <unistd.h>

#include <iostream>
#include <sstream>

namespace kafka {

Client::Client(const std::string& host, uint16_t port) : host_(host), port_(port) {}

bool Client::connect() {
    return conn_.connect(host_, port_);
}

int64_t Client::produce(const std::string& topic, const std::string& key, const std::vector<std::string>& messages) {
    ProduceRequest produce_request;
    produce_request.header.api_key        = static_cast<int16_t>(ReqType::PRODUCE);
    produce_request.header.api_version    = 3;
    produce_request.header.correlation_id = 1;
    produce_request.header.client_id      = "mini-kafka-client";
    produce_request.transactional_id      = "";
    produce_request.acks                  = 1;
    produce_request.timeout_ms            = 1000;

    int64_t timestamp = std::chrono::system_clock::now().time_since_epoch().count();
    Batch   batch;
    batch.first_timestamp = timestamp;
    batch.max_timestamp   = timestamp;
    int64_t offset_delta  = 0;
    for (const auto& msg : messages) {
        Record rec;
        rec.key             = key;
        rec.value           = msg;
        rec.timestamp_delta = 0;
        rec.offset_delta    = offset_delta++;
        rec.length          = static_cast<int32_t>(msg.size());
        batch.records.push_back(std::move(rec));
    }

    batch.records_count     = messages.size();
    batch.last_offset_delta = offset_delta - 1;

    PartitionProduceData part;
    part.partition_index = 0;
    part.records         = batch;

    TopicProduceData tpd;
    tpd.name = topic;
    tpd.partitions.push_back(std::move(part));

    produce_request.topics.push_back(std::move(tpd));

    std::cout << "before serialize" << std::endl;
    if (!conn_.send(serialize_produce_request(produce_request))) {
        std::cout << "after serialize" << std::endl;
        return -1;
    }

    std::string response_size_buf;
    if (!conn_.read_bytes(response_size_buf, 4))
        return -1;
    
    int32_t response_size;
    decode_int32(response_size_buf.data(), 4, response_size);

    std::string response_body;
    if (!conn_.read_bytes(response_body, response_size))
        return -1;

    ProduceResponse res;
    if (!parse_produce_response(response_body.data(), response_body.size(), res))
        return -1;

    // Return the offset from the first partition of the first topic for simplicity
    if (!res.topics.empty() && !res.topics[0].partitions.empty()) {
        return res.topics[0].partitions[0].base_offset;
    }
    return 0;
}

/// @dev max_bytes in kafka means whenever the data is more than that, stop reading.
/// max_bytes is not upper bound of data size.
std::vector<Batch> Client::consume(const std::string& topic, uint64_t& offset, uint32_t max_bytes) {
    ConsumeRequest consume_request;
    consume_request.header.api_key        = static_cast<int16_t>(ReqType::CONSUME);
    consume_request.header.api_version    = 3;
    consume_request.header.correlation_id = 2;
    consume_request.header.client_id      = "mini-kafka-client";
    
    consume_request.replica_id    = -1;
    consume_request.max_wait_time = 500;
    consume_request.min_bytes     = 1;

    PartitionConsumeData p_data;
    p_data.partition_index = 0;
    p_data.fetch_offset    = static_cast<int64_t>(offset);
    p_data.max_bytes       = static_cast<int32_t>(max_bytes);

    TopicConsumeData t_data;
    t_data.name = topic;
    t_data.partitions.push_back(p_data);

    consume_request.topics.push_back(t_data);

    if (!conn_.send(serialize_consume_request(consume_request)))
        return {};

    std::string response_size_buf;
    if (!conn_.read_bytes(response_size_buf, 4))
        return {};
    
    int32_t response_size;
    decode_int32(response_size_buf.data(), 4, response_size);

    std::string response_body;
    if (!conn_.read_bytes(response_body, response_size))
        return {};

    FetchResponse res;
    std::string   record_set;
    if (!parse_fetch_response(response_body.data(), response_body.size(), res, record_set))
        return {};

    std::vector<Batch> batches;
    const char*        data = record_set.data();
    size_t             len  = record_set.size();
    size_t             pos  = 0;

    while (pos < len) {
        Batch batch;
        // Each batch in the record_set is a full RecordBatch as stored on disk
        // which we can deserialize using deserialize_batch.
        // However, deserialize_batch needs to know how many bytes to read.
        // In Kafka, the RecordBatch starts with:
        // [BaseOffset: 8] [BatchLength: 4] ...
        // The BatchLength is the size of the REST of the batch (after the BatchLength field).
        
        if (pos + 12 > len) break;
        
        int32_t batch_length;
        decode_int32(data + pos + 8, 4, batch_length);
        
        size_t full_batch_size = 12 + batch_length;
        if (pos + full_batch_size > len) break;
        
        if (deserialize_batch(data + pos, full_batch_size, batch)) {
            batches.push_back(std::move(batch));
            offset = batches.back().base_offset + batches.back().records.size();
        }
        pos += full_batch_size;
    }

    return batches;
}

std::vector<std::string> Client::list_topics() {
    if (!conn_.send("LIST_TOPICS\n"))
        return {};

    std::vector<std::string> topics;
    std::string              line;
    while (conn_.read_line(line)) {
        if (line == "END")
            break;
        topics.push_back(line);
    }
    return topics;
}

}  // namespace kafka
