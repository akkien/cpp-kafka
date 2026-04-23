#include "common/batch.h"

#include "common/serialize.h"
#include "crc32c/crc32c.h"

std::string serialize_batch(const Batch& batch) {
    std::string buf;
    buf.reserve(1024);

    encode_int64(buf, batch.base_offset);
    encode_int32(buf, batch.batch_length);
    encode_int32(buf, batch.partition_epoch);
    buf += static_cast<char>(batch.magic);
    int32_t placeholder_crc = 0;
    encode_int32(buf, placeholder_crc);
    encode_int16(buf, batch.attributes);
    encode_int32(buf, batch.last_offset_delta);
    encode_int64(buf, batch.first_timestamp);
    encode_int64(buf, batch.max_timestamp);
    encode_int64(buf, batch.producer_id);
    encode_int16(buf, batch.producer_epoch);
    encode_int32(buf, batch.base_sequence);
    encode_int32(buf, batch.records_count);

    for (const auto& rec : batch.records) {
        buf += serialize_record(rec);
    }

    int32_t  batch_length = static_cast<int32_t>(buf.size() - INT64_T_SIZE - INT32_T_SIZE);
    uint32_t net_len      = htonl(static_cast<uint32_t>(batch_length));
    std::memcpy(buf.data() + INT64_T_SIZE, &net_len, INT32_T_SIZE);

    calculate_crc(buf);
    return buf;
}

bool deserialize_batch(const char* data, size_t len, Batch& batch) {
    // 61 is the length of the batch header
    if (len < 61)
        return false;

    size_t pos  = 0;
    int    read = 0;

    read = decode_int64(data + pos, len - pos, batch.base_offset);
    if (read < 0)
        return false;
    pos += read;
    read = decode_int32(data + pos, len - pos, batch.batch_length);
    if (read < 0)
        return false;
    pos += read;
    read = decode_int32(data + pos, len - pos, batch.partition_epoch);
    if (read < 0)
        return false;
    pos += read;

    batch.magic = static_cast<int8_t>(data[pos++]);

    read = decode_int32(data + pos, len - pos, batch.crc);
    if (read < 0)
        return false;
    pos += read;
    read = decode_int16(data + pos, len - pos, batch.attributes);
    if (read < 0)
        return false;
    pos += read;
    read = decode_int32(data + pos, len - pos, batch.last_offset_delta);
    if (read < 0)
        return false;
    pos += read;
    read = decode_int64(data + pos, len - pos, batch.first_timestamp);
    if (read < 0)
        return false;
    pos += read;
    read = decode_int64(data + pos, len - pos, batch.max_timestamp);
    if (read < 0)
        return false;
    pos += read;
    read = decode_int64(data + pos, len - pos, batch.producer_id);
    if (read < 0)
        return false;
    pos += read;
    read = decode_int16(data + pos, len - pos, batch.producer_epoch);
    if (read < 0)
        return false;
    pos += read;
    read = decode_int32(data + pos, len - pos, batch.base_sequence);
    if (read < 0)
        return false;
    pos += read;
    read = decode_int32(data + pos, len - pos, batch.records_count);
    if (read < 0)
        return false;
    pos += read;

    while (pos < len) {
        Record rec;
        size_t consumed = deserialize_record(data + pos, len - pos, rec);
        if (consumed == 0) {
            return false;
        }
        batch.records.push_back(std::move(rec));
        pos += consumed;
    }
    return true;
}

bool calculate_crc(std::string& batch_buf) {
    if (batch_buf.size() < BYTE_AFTER_CRC)
        return false;

    uint32_t crc     = crc32c::Crc32c(batch_buf.c_str() + BYTE_AFTER_CRC, batch_buf.size() - BYTE_AFTER_CRC);
    uint32_t net_crc = htonl(crc);
    std::memcpy(batch_buf.data() + CRC_START_BYTE, &net_crc, INT32_T_SIZE);
    return true;
}