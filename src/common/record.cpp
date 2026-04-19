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

Record deserialize_record(const char* data, size_t len) {
    Record   rec;
    size_t   pos = 0;
    uint64_t val = 0;

    // length (varint)
    pos += decode_varint(data + pos, len - pos, val);
    rec.length = static_cast<int32_t>(val);

    // attributes (1 byte)
    rec.attributes = static_cast<int8_t>(data[pos++]);

    // timestamp_delta (varint)
    pos += decode_varint(data + pos, len - pos, val);
    rec.timestamp_delta = static_cast<int64_t>(val);

    // offset_delta (varint)
    pos += decode_varint(data + pos, len - pos, val);
    rec.offset_delta = static_cast<int64_t>(val);

    // key
    pos += decode_varint(data + pos, len - pos, val);
    rec.key.assign(data + pos, val);
    pos += val;

    // value
    pos += decode_varint(data + pos, len - pos, val);
    rec.value.assign(data + pos, val);
    pos += val;

    // headers
    pos += decode_varint(data + pos, len - pos, val);
    size_t header_count = val;
    for (size_t i = 0; i < header_count; ++i) {
        Header h;
        pos += decode_varint(data + pos, len - pos, val);
        h.key.assign(data + pos, val);
        pos += val;
        pos += decode_varint(data + pos, len - pos, val);
        h.value.assign(data + pos, val);
        pos += val;
        rec.headers.push_back(std::move(h));
    }

    return rec;
}
