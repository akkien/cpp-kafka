#include <gtest/gtest.h>
#include "common/batch.h"

TEST(BatchTest, SerializeDeserialize) {
    Batch batch;
    
    Record r1;
    r1.length = 10;
    r1.value = "val1";
    r1.key = "key1";
    
    Record r2;
    r2.length = 10;
    r2.value = "val2";
    r2.key = "key2";
    
    batch.records.push_back(std::move(r1));
    batch.records.push_back(std::move(r2));
    
    std::string serialized = serialize_batch(batch);
    EXPECT_GT(serialized.size(), 0);
    
    Batch decoded;
    bool success = deserialize_batch(serialized.data(), serialized.size(), decoded);
    EXPECT_TRUE(success);
    
    ASSERT_EQ(decoded.records.size(), 2);
    EXPECT_EQ(decoded.records[0].value, "val1");
    EXPECT_EQ(decoded.records[1].value, "val2");
}

TEST(BatchTest, EmptyBatch) {
    Batch batch;
    std::string serialized = serialize_batch(batch);
    EXPECT_EQ(serialized.size(), 0);
    
    Batch decoded;
    bool success = deserialize_batch(serialized.data(), serialized.size(), decoded);
    EXPECT_TRUE(success);
    EXPECT_EQ(decoded.records.size(), 0);
}
