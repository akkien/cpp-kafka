#include <string>

void encode_varint(std::string& buf, uint64_t value);
int  decode_varint(const char* buf, size_t n, uint64_t& value);

void encode_int16(std::string& buf, int16_t value);
int  decode_int16(const char* buf, size_t n, int16_t& value);

void encode_int32(std::string& buf, int32_t value);
int  decode_int32(const char* buf, size_t n, int32_t& value);

void encode_int64(std::string& buf, int64_t value);
int  decode_int64(const char* buf, size_t n, int64_t& value);

void encode_string(std::string& buf, const std::string& value);
int  decode_string(const char* buf, size_t n, std::string& value);

void encode_nullable_string(std::string& buf, const std::string& value);
int  decode_nullable_string(const char* buf, size_t n, std::string& value);

// Kafka standard big-endian encoding utilities are now used via specific encode/decode functions.