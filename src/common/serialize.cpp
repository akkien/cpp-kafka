#include "common/serialize.h"

#include <arpa/inet.h>

#include <cstring>
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

void encode_int16(std::string& buf, int16_t value) {
    int16_t net_val = htons(value);
    buf.append(reinterpret_cast<const char*>(&net_val), sizeof(net_val));
}

int decode_int16(const char* buf, size_t n, int16_t& value) {
    if (n < sizeof(int16_t))
        return -1;
    int16_t net_val;
    std::memcpy(&net_val, buf, sizeof(int16_t));
    value = ntohs(net_val);
    return sizeof(int16_t);
}

void encode_int32(std::string& buf, int32_t value) {
    int32_t net_val = htonl(value);
    buf.append(reinterpret_cast<const char*>(&net_val), sizeof(net_val));
}

int decode_int32(const char* buf, size_t n, int32_t& value) {
    if (n < sizeof(int32_t))
        return -1;
    int32_t net_val;
    std::memcpy(&net_val, buf, sizeof(int32_t));
    value = ntohl(net_val);
    return sizeof(int32_t);
}

void encode_int64(std::string& buf, int64_t value) {
    uint64_t u = static_cast<uint64_t>(value);
    for (int i = 7; i >= 0; --i) {
        buf += static_cast<char>((u >> (i * 8)) & 0xFF);
    }
}

int decode_int64(const char* buf, size_t n, int64_t& value) {
    if (n < 8)
        return -1;
    uint64_t u = 0;
    for (int i = 0; i < 8; ++i) {
        u = (u << 8) | static_cast<uint8_t>(buf[i]);
    }
    value = static_cast<int64_t>(u);
    return 8;
}

void encode_string(std::string& buf, const std::string& value) {
    encode_int16(buf, static_cast<int16_t>(value.size()));
    buf.append(value);
}

int decode_string(const char* buf, size_t n, std::string& value) {
    int16_t len;
    int     read = decode_int16(buf, n, len);
    if (read < 0)
        return -1;
    if (len < 0)
        return -1;  // Standard string cannot be negative, only nullable string
    if (n - read < static_cast<size_t>(len))
        return -1;
    value.assign(buf + read, len);
    return read + len;
}

void encode_nullable_string(std::string& buf, const std::string& value) {
    if (value.empty()) {
        encode_int16(buf, -1);
    } else {
        encode_int16(buf, static_cast<int16_t>(value.size()));
        buf.append(value);
    }
}

int decode_nullable_string(const char* buf, size_t n, std::string& value) {
    int16_t len;
    int     read = decode_int16(buf, n, len);
    if (read < 0)
        return -1;
    if (len == -1) {
        value.clear();
        return read;
    }
    if (len < 0 || n - read < static_cast<size_t>(len))
        return -1;
    value.assign(buf + read, len);
    return read + len;
}
