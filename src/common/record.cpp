#include "common/record.h"

#include <string>
#include <vector>

#include "common/serialize.h"

// Serialize Record → bytes
std::string serialize_record(const Record& rec) {
    std::string buf;
    encode_varint(buf, rec.length);           // varint
    buf += rec.attributes;                    // 1 byte cố định
    encode_varint(buf, rec.timestamp_delta);  // varint
    encode_varint(buf, rec.offset_delta);     // varint
    encode_varint(buf, rec.key.size());       // varint (key length)
    buf += rec.key;                           // raw bytes
    encode_varint(buf, rec.value.size());     // varint (value length)
    buf += rec.value;                         // raw bytes
    encode_varint(buf, rec.headers.size());   // varint (header count)
    for (const auto& h : rec.headers) {
        encode_varint(buf, h.key.size());
        buf += h.key;
        encode_varint(buf, h.value.size());
        buf += h.value;
    }
    return buf;
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

