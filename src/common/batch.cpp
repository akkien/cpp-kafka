#include "common/batch.h"

std::string serialize_batch(const Batch& batch) {
    std::string buf;
    for (const auto& rec : batch.records) {
        buf += serialize_record(rec);
    }
    return buf;
}

/**
 * deserialize_batch
 * @param data: data to deserialize
 * @param len: length of data
 * @param batch: batch to deserialize
 * @return true if successful, false otherwise
 */
bool deserialize_batch(const char* data, size_t len, Batch& batch) {
    size_t pos = 0;
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
