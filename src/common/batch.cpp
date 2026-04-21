#include "common/batch.h"

#include "common/serialize.h"
#include "crc32c/crc32c.h"

std::string serialize_batch(const Batch& batch) {
    std::string buf;
    buf.reserve(1024);

    append_fixed(buf, batch.base_offset);
    append_fixed(buf, batch.batch_length);
    append_fixed(buf, batch.partition_epoch);
    append_fixed(buf, batch.magic);

    int32_t placeholder_crc = 0;
    append_fixed(buf, placeholder_crc);

    append_fixed(buf, batch.attributes);
    append_fixed(buf, batch.last_offset_delta);
    append_fixed(buf, batch.first_timestamp);
    append_fixed(buf, batch.max_timestamp);
    append_fixed(buf, batch.producer_id);
    append_fixed(buf, batch.producer_epoch);
    append_fixed(buf, batch.base_sequence);
    append_fixed(buf, batch.records_count);

    for (const auto& rec : batch.records) {
        buf += serialize_record(rec);
    }

    int32_t batch_length = static_cast<int32_t>(buf.size() - INT64_T_SIZE - INT32_T_SIZE);
    std::memcpy(buf.data() + INT64_T_SIZE, &batch_length, INT32_T_SIZE);

    calculate_crc(buf);
    return buf;
}

bool deserialize_batch(const char* data, size_t len, Batch& batch) {
    // 61 is the length of the batch header
    if (len < 61)
        return false;

    size_t pos = 0;
    pos += read_fixed(data + pos, batch.base_offset);
    pos += read_fixed(data + pos, batch.batch_length);
    pos += read_fixed(data + pos, batch.partition_epoch);
    pos += read_fixed(data + pos, batch.magic);
    pos += read_fixed(data + pos, batch.crc);
    pos += read_fixed(data + pos, batch.attributes);
    pos += read_fixed(data + pos, batch.last_offset_delta);
    pos += read_fixed(data + pos, batch.first_timestamp);
    pos += read_fixed(data + pos, batch.max_timestamp);
    pos += read_fixed(data + pos, batch.producer_id);
    pos += read_fixed(data + pos, batch.producer_epoch);
    pos += read_fixed(data + pos, batch.base_sequence);
    pos += read_fixed(data + pos, batch.records_count);

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

    uint32_t crc = crc32c::Crc32c(batch_buf.c_str() + BYTE_AFTER_CRC, batch_buf.size() - BYTE_AFTER_CRC);
    std::memcpy(batch_buf.data() + CRC_START_BYTE, &crc, INT32_T_SIZE);
    return true;
}