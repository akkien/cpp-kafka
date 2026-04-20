#include <gtest/gtest.h>
#include "common/serialize.h"
#include <string>

// Test Unsigned Varint
TEST(SerializeTest, EncodeDecodeVarint) {
    std::string buf;
    
    // Test small number (1 byte)
    encode_varint(buf, 127);
    uint64_t val = 0;
    int bytes = decode_varint(buf.data(), buf.size(), val);
    EXPECT_EQ(val, 127);
    EXPECT_EQ(bytes, 1);
    
    // Test medium number (2 bytes)
    buf.clear();
    encode_varint(buf, 300);
    bytes = decode_varint(buf.data(), buf.size(), val);
    EXPECT_EQ(val, 300);
    EXPECT_EQ(bytes, 2);

    // Test large number (multiple bytes)
    buf.clear();
    uint64_t large_num = 1234567890123456789ULL;
    encode_varint(buf, large_num);
    bytes = decode_varint(buf.data(), buf.size(), val);
    EXPECT_EQ(val, large_num);
    EXPECT_GT(bytes, 2);
}

// Test Decode Error Handling (Truncated Buffer)
TEST(SerializeTest, DecodeVarintIncomplete) {
    std::string buf;
    encode_varint(buf, 300); // 300 takes 2 bytes
    
    uint64_t val = 0;
    // Pass only 1 byte (length is artificially shortened)
    int bytes = decode_varint(buf.data(), 1, val);
    
    // Should return -1 for incomplete buffer
    EXPECT_EQ(bytes, -1);
}
