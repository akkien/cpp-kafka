#include "common/message.h"

#include <arpa/inet.h>

#include <iostream>

#include "common/serialize.h"

std::string serialize_produce_request(const ProduceRequest& req) {
    std::string body;

    // api_key (2 bytes, big-endian)
    uint16_t api_key_net = htons(static_cast<uint16_t>(req.api_key));
    body.append(reinterpret_cast<const char*>(&api_key_net), sizeof(api_key_net));

    // topic
    encode_varint(body, req.topic.size());
    body += req.topic;

    // record
    body += serialize_record(req.record);

    // Prepend 4-byte total size
    std::string final_buf;
    uint32_t    size_net = htonl(static_cast<uint32_t>(body.size()));
    final_buf.append(reinterpret_cast<const char*>(&size_net), sizeof(size_net));
    final_buf += body;

    return final_buf;
}

std::string serialize_consume_request(const ConsumeRequest& req) {
    std::string body;

    // api_key (2 bytes)
    uint16_t api_key_net = htons(static_cast<uint16_t>(req.api_key));
    body.append(reinterpret_cast<const char*>(&api_key_net), sizeof(api_key_net));

    // topic
    encode_varint(body, req.topic.size());
    body += req.topic;

    // offset
    encode_varint(body, req.offset);

    // max_bytes
    encode_varint(body, req.max_bytes);

    // Prepend 4-byte total size
    std::string final_buf;
    uint32_t    size_net = htonl(static_cast<uint32_t>(body.size()));
    final_buf.append(reinterpret_cast<const char*>(&size_net), sizeof(size_net));
    final_buf += body;

    return final_buf;
}

bool parse_produce_request(const char* data, size_t len, ProduceRequest& req) {
    req.api_key  = ReqType::PRODUCE;
    size_t   pos = 2;  // skip api_key
    uint64_t val = 0;

    // topic length
    int bytes = decode_varint(data + pos, len - pos, val);
    if (bytes < 0)
        return false;
    pos += bytes;

    // topic string
    if (pos + val > len)
        return false;
    req.topic.assign(data + pos, val);
    pos += val;

    // record
    req.record = deserialize_record(data + pos, len - pos);
    return true;
}

bool parse_consume_request(const char* data, size_t len, ConsumeRequest& req) {
    req.api_key  = ReqType::CONSUME;
    size_t   pos = 2;  // skip api_key
    uint64_t val = 0;

    // topic length
    int bytes = decode_varint(data + pos, len - pos, val);
    if (bytes < 0)
        return false;
    pos += bytes;

    // topic string
    if (pos + val > len)
        return false;
    req.topic.assign(data + pos, val);
    pos += val;

    pos += decode_varint(data + pos, len - pos, req.offset);

    decode_varint(data + pos, len - pos, val);
    req.max_bytes = static_cast<uint32_t>(val);
    return true;
}
