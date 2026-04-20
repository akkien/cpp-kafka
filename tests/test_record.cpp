#include <gtest/gtest.h>
#include "common/record.h"
#include <string>

TEST(RecordTest, SerializeDeserializeBasic) {
    Record rec;
    rec.length = 100;
    rec.attributes = 0x01;
    rec.timestamp_delta = 1000;
    rec.offset_delta = 5;
    rec.key = "test-key";
    rec.value = "test-value";

    std::string serialized = serialize_record(rec);
    EXPECT_GT(serialized.size(), 0);

    Record decoded;
    size_t consumed = deserialize_record(serialized.data(), serialized.size(), decoded);
    EXPECT_GT(consumed, 0);

    EXPECT_EQ(decoded.length, rec.length);
    EXPECT_EQ(decoded.attributes, rec.attributes);
    EXPECT_EQ(decoded.timestamp_delta, rec.timestamp_delta);
    EXPECT_EQ(decoded.offset_delta, rec.offset_delta);
    EXPECT_EQ(decoded.key, rec.key);
    EXPECT_EQ(decoded.value, rec.value);
    EXPECT_EQ(decoded.headers.size(), 0);
}

TEST(RecordTest, SerializeDeserializeWithHeaders) {
    Record rec;
    rec.length = 100;
    rec.attributes = 0;
    rec.timestamp_delta = 0;
    rec.offset_delta = 0;
    rec.key = "key";
    rec.value = "val";
    
    Header h1{"h1", "v1"};
    Header h2{"h2", "v2"};
    rec.headers.push_back(h1);
    rec.headers.push_back(h2);

    std::string serialized = serialize_record(rec);
    Record decoded;
    size_t consumed = deserialize_record(serialized.data(), serialized.size(), decoded);
    EXPECT_GT(consumed, 0);

    ASSERT_EQ(decoded.headers.size(), 2);
    EXPECT_EQ(decoded.headers[0].key, "h1");
    EXPECT_EQ(decoded.headers[0].value, "v1");
    EXPECT_EQ(decoded.headers[1].key, "h2");
    EXPECT_EQ(decoded.headers[1].value, "v2");
}

TEST(RecordTest, DeserializeInvalidData) {
    Record decoded;
    std::string invalid_data = "abc"; // Too short, not valid varints
    size_t consumed = deserialize_record(invalid_data.data(), invalid_data.size(), decoded);
    EXPECT_EQ(consumed, 0);
}
