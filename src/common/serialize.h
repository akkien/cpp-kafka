#include <string>

void encode_varint(std::string& buf, uint64_t value);
int  decode_varint(const char* buf, size_t n, uint64_t& value);

template <typename T>
void append_fixed(std::string& buf, T val) {
    buf.append(reinterpret_cast<const char*>(&val), sizeof(val));
}

template <typename T>
size_t read_fixed(const char* data, T& val) {
    std::memcpy(&val, data, sizeof(T));
    return sizeof(T);
}