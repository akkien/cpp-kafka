#include "common/serialize.h"

#include <string>

void encode_varint(std::string& buf, uint64_t value) {
    while (value >= 0x80) {
        buf += static_cast<char>((value & 0x7F) | 0x80);
        value >>= 7;
    }
    buf += static_cast<char>(value);
}

int decode_varint(const char* buf, size_t n, uint64_t& value) {
    value        = 0;
    size_t bytes = 0;
    for (size_t i = 0; i < n; ++i) {
        uint64_t byte = static_cast<uint64_t>(buf[i]);
        value |= (byte & 0x7F) << (bytes * 7);
        bytes++;
        if ((byte & 0x80) == 0)
            return bytes;
    }
    return -1;
}