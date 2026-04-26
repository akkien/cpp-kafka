#include <gtest/gtest.h>
#include "common/message.h"
#include "common/serialize.h"

TEST(MessageTest, ProduceRequestSerializeDeserialize) {
    ProduceRequest req;
    req.header.api_key = static_cast<int16_t>(ReqType::PRODUCE);
    req.header.api_version = 3;
    req.header.correlation_id = 42;
    req.header.client_id = "test-client";
    req.transactional_id = "";
    req.acks = 1;
    req.timeout_ms = 500;
    
    Record rec;
    rec.length = 5;
    rec.value = "hello";
    
    Batch batch;
    batch.records.push_back(std::move(rec));
    
    PartitionProduceData part;
    part.partition_index = 0;
    part.records = std::move(batch);
    
    TopicProduceData topic;
    topic.name = "test-topic";
    topic.partitions.push_back(std::move(part));
    
    req.topics.push_back(std::move(topic));
    
    std::string serialized = serialize_produce_request(req);
    
    // Total size header is 4 bytes
    EXPECT_GT(serialized.size(), 4);
    
    ProduceRequest decoded;
    // Skip the 4 byte header for parsing
    bool success = parse_produce_request(serialized.data() + 4, serialized.size() - 4, decoded);
    EXPECT_TRUE(success);
    
    EXPECT_EQ(decoded.header.api_key, static_cast<int16_t>(ReqType::PRODUCE));
    EXPECT_EQ(decoded.header.client_id, "test-client");
    EXPECT_EQ(decoded.timeout_ms, 500);
    ASSERT_EQ(decoded.topics.size(), 1);
    EXPECT_EQ(decoded.topics[0].name, "test-topic");
    ASSERT_EQ(decoded.topics[0].partitions.size(), 1);
    EXPECT_EQ(decoded.topics[0].partitions[0].partition_index, 0);
    ASSERT_EQ(decoded.topics[0].partitions[0].records.records.size(), 1);
    EXPECT_EQ(decoded.topics[0].partitions[0].records.records[0].value, "hello");
}

TEST(MessageTest, ConsumeRequestSerializeDeserialize) {
    ConsumeRequest req;
    req.header.api_key = static_cast<int16_t>(ReqType::FETCH);
    req.header.api_version = 3;
    req.header.correlation_id = 123;
    req.header.client_id = "test-client";
    
    req.replica_id = -1;
    req.max_wait_time = 1000;
    req.min_bytes = 1;
    
    PartitionConsumeData p_data;
    p_data.partition_index = 0;
    p_data.fetch_offset = 12345;
    p_data.max_bytes = 4096;
    
    TopicConsumeData t_data;
    t_data.name = "test-topic";
    t_data.partitions.push_back(p_data);
    
    req.topics.push_back(t_data);
    
    std::string serialized = serialize_consume_request(req);
    EXPECT_GT(serialized.size(), 4);
    
    ConsumeRequest decoded;
    bool success = parse_consume_request(serialized.data() + 4, serialized.size() - 4, decoded);
    EXPECT_TRUE(success);
    
    EXPECT_EQ(decoded.header.api_key, static_cast<int16_t>(ReqType::FETCH));
    EXPECT_EQ(decoded.header.client_id, "test-client");
    ASSERT_EQ(decoded.topics.size(), 1);
    EXPECT_EQ(decoded.topics[0].name, "test-topic");
    ASSERT_EQ(decoded.topics[0].partitions.size(), 1);
    EXPECT_EQ(decoded.topics[0].partitions[0].fetch_offset, 12345);
}


TEST(MessageTest, ProduceResponseSerializeDeserialize) {
    ProduceResponse res;
    res.correlation_id = 123;
    res.throttle_time_ms = 10;
    
    PartitionProduceResponse p_res;
    p_res.partition_index = 0;
    p_res.error_code = 0;
    p_res.base_offset = 1000;
    p_res.log_append_time = 123456789;
    p_res.log_start_offset = 500;
    
    TopicProduceResponse t_res;
    t_res.name = "test-topic";
    t_res.partitions.push_back(p_res);
    
    res.topics.push_back(t_res);
    
    std::string serialized = serialize_produce_response(res);
    EXPECT_GT(serialized.size(), 4);
    
    ProduceResponse decoded;
    bool success = parse_produce_response(serialized.data() + 4, serialized.size() - 4, decoded);
    EXPECT_TRUE(success);
    
    EXPECT_EQ(decoded.correlation_id, 123);
    EXPECT_EQ(decoded.throttle_time_ms, 10);
    ASSERT_EQ(decoded.topics.size(), 1);
    EXPECT_EQ(decoded.topics[0].name, "test-topic");
    ASSERT_EQ(decoded.topics[0].partitions.size(), 1);
    EXPECT_EQ(decoded.topics[0].partitions[0].base_offset, 1000);
}

TEST(MessageTest, FetchResponseSerializeDeserialize) {
    FetchResponse res;
    res.correlation_id = 456;
    res.throttle_time_ms = 0;
    
    PartitionFetchResponse p_res;
    p_res.partition_index = 0;
    p_res.error_code = 0;
    p_res.high_watermark = 1000;
    p_res.last_stable_offset = 1000;
    p_res.log_start_offset = 0;
    
    TopicFetchResponse t_res;
    t_res.name = "test-topic";
    t_res.partitions.push_back(p_res);
    res.topics.push_back(t_res);
    
    std::string header = serialize_fetch_response_header(res);
    
    // Manual construction of the full response for testing
    std::string full_response;
    // We simulate what TopicState::send does
    std::string record_set = "mock-record-set";
    int32_t record_set_size = static_cast<int32_t>(record_set.size());
    
    int32_t total_size = static_cast<int32_t>(header.size() + 4 + record_set_size);
    encode_int32(full_response, total_size);
    full_response += header;
    encode_int32(full_response, record_set_size);
    full_response += record_set;
    
    FetchResponse decoded;
    std::string decoded_record_set;
    bool success = parse_fetch_response(full_response.data() + 4, full_response.size() - 4, decoded, decoded_record_set);
    EXPECT_TRUE(success);
    EXPECT_EQ(decoded.correlation_id, 456);
    EXPECT_EQ(decoded_record_set, "mock-record-set");
}


