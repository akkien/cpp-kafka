#include "common/record.h"

#include <string>
#include <vector>

#include "common/serialize.h"

// Serialize Record → bytes
std::string serialize_record(const Record& rec) {
    std::string body;
    body += rec.attributes;
    encode_zigzag(body, rec.timestamp_delta);
    encode_zigzag(body, rec.offset_delta);
    
    // key (nullable bytes)
    if (rec.key.empty()) {
        encode_zigzag(body, -1);
    } else {
        encode_zigzag(body, static_cast<int64_t>(rec.key.size()));
        body += rec.key;
    }
    
    // value (nullable bytes)
    if (rec.value.empty()) {
        encode_zigzag(body, -1);
    } else {
        encode_zigzag(body, static_cast<int64_t>(rec.value.size()));
        body += rec.value;
    }
    
    // headers count
    encode_varint(body, static_cast<uint64_t>(rec.headers.size()));
    for (const auto& h : rec.headers) {
        encode_zigzag(body, static_cast<int64_t>(h.key.size()));
        body += h.key;
        encode_zigzag(body, static_cast<int64_t>(h.value.size()));
        body += h.value;
    }
    
    std::string out;
    encode_zigzag(out, static_cast<int64_t>(body.size()));
    out += body;
    return out;
}

size_t deserialize_record(const char* data, size_t len, Record& rec) {
    size_t   pos = 0;
    uint64_t val = 0;
    int64_t  sval = 0;
    int bytes = 0;

    // length (varint, signed zigzag)
    bytes = decode_zigzag(data + pos, len - pos, sval);
    if (bytes <= 0) return 0;
    pos += bytes;
    rec.length = static_cast<int32_t>(sval);

    if (pos >= len) return 0;
    // attributes (1 byte)
    rec.attributes = static_cast<int8_t>(data[pos++]);

    // timestamp_delta (zigzag varint)
    bytes = decode_zigzag(data + pos, len - pos, sval);
    if (bytes <= 0) return 0;
    pos += bytes;
    rec.timestamp_delta = sval;

    // offset_delta (zigzag varint)
    bytes = decode_zigzag(data + pos, len - pos, sval);
    if (bytes <= 0) return 0;
    pos += bytes;
    rec.offset_delta = sval;

    // key_length (zigzag varint, -1 = null)
    bytes = decode_zigzag(data + pos, len - pos, sval);
    if (bytes <= 0) return 0;
    pos += bytes;
    if (sval >= 0) {
        // non-null key
        if (pos + static_cast<size_t>(sval) > len) return 0;
        rec.key.assign(data + pos, static_cast<size_t>(sval));
        pos += static_cast<size_t>(sval);
    }
    // sval == -1 → null key, rec.key stays empty

    // value_length (zigzag varint, -1 = null)
    bytes = decode_zigzag(data + pos, len - pos, sval);
    if (bytes <= 0) return 0;
    pos += bytes;
    if (sval >= 0) {
        if (pos + static_cast<size_t>(sval) > len) return 0;
        rec.value.assign(data + pos, static_cast<size_t>(sval));
        pos += static_cast<size_t>(sval);
    }

    // headers count (unsigned varint)
    bytes = decode_varint(data + pos, len - pos, val);
    if (bytes <= 0) return 0;
    pos += bytes;
    size_t header_count = val;
    for (size_t i = 0; i < header_count; ++i) {
        Header h;
        // header key (zigzag for nullable)
        bytes = decode_zigzag(data + pos, len - pos, sval);
        if (bytes <= 0) return 0;
        pos += bytes;
        if (sval >= 0) {
            if (pos + static_cast<size_t>(sval) > len) return 0;
            h.key.assign(data + pos, static_cast<size_t>(sval));
            pos += static_cast<size_t>(sval);
        }
        // header value (zigzag for nullable)
        bytes = decode_zigzag(data + pos, len - pos, sval);
        if (bytes <= 0) return 0;
        pos += bytes;
        if (sval >= 0) {
            if (pos + static_cast<size_t>(sval) > len) return 0;
            h.value.assign(data + pos, static_cast<size_t>(sval));
            pos += static_cast<size_t>(sval);
        }
        rec.headers.push_back(std::move(h));
    }

    return pos;
}

