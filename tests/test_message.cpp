#include <gtest/gtest.h>
#include "common/message.h"

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
    req.api_key = ReqType::CONSUME;
    req.topic = "test-topic";
    req.offset = 12345;
    req.max_bytes = 4096;
    
    std::string serialized = serialize_consume_request(req);
    EXPECT_GT(serialized.size(), 4);
    
    ConsumeRequest decoded;
    // Skip the 4 byte header for parsing
    bool success = parse_consume_request(serialized.data() + 4, serialized.size() - 4, decoded);
    EXPECT_TRUE(success);
    
    EXPECT_EQ(decoded.api_key, ReqType::CONSUME);
    EXPECT_EQ(decoded.topic, "test-topic");
    EXPECT_EQ(decoded.offset, 12345);
    EXPECT_EQ(decoded.max_bytes, 4096);
}
