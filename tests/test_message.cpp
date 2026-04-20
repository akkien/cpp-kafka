#include <gtest/gtest.h>
#include "common/message.h"

TEST(MessageTest, ProduceRequestSerializeDeserialize) {
    ProduceRequest req;
    req.api_key = ReqType::PRODUCE;
    req.topic = "test-topic";
    
    Record rec;
    rec.length = 5;
    rec.value = "hello";
    req.batch.records.push_back(std::move(rec));
    
    std::string serialized = serialize_produce_request(req);
    
    // Total size header is 4 bytes
    EXPECT_GT(serialized.size(), 4);
    
    ProduceRequest decoded;
    // Skip the 4 byte header for parsing
    bool success = parse_produce_request(serialized.data() + 4, serialized.size() - 4, decoded);
    EXPECT_TRUE(success);
    
    EXPECT_EQ(decoded.api_key, ReqType::PRODUCE);
    EXPECT_EQ(decoded.topic, "test-topic");
    ASSERT_EQ(decoded.batch.records.size(), 1);
    EXPECT_EQ(decoded.batch.records[0].value, "hello");
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
