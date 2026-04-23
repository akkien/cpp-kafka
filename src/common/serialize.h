#include <string>

void encode_varint(std::string& buf, uint64_t value);
int  decode_varint(const char* buf, size_t n, uint64_t& value);

void encode_int16(std::string& buf, int16_t value);
int  decode_int16(const char* buf, size_t n, int16_t& value);

void encode_int32(std::string& buf, int32_t value);
int  decode_int32(const char* buf, size_t n, int32_t& value);

void encode_string(std::string& buf, const std::string& value);
int  decode_string(const char* buf, size_t n, std::string& value);

void encode_nullable_string(std::string& buf, const std::string& value);
int  decode_nullable_string(const char* buf, size_t n, std::string& value);

template <typename T>
void append_fixed(std::string& buf, T val) {
    buf.append(reinterpret_cast<const char*>(&val), sizeof(val));
}

template <typename T>
size_t read_fixed(const char* data, T& val) {
    std::memcpy(&val, data, sizeof(T));
    return sizeof(T);
}