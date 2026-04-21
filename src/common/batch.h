#include <string>
#include <vector>

#include "common/record.h"

const int INT32_T_SIZE   = sizeof(int32_t);
const int INT64_T_SIZE   = sizeof(int64_t);
const int INT16_T_SIZE   = sizeof(int16_t);
const int INT8_T_SIZE    = sizeof(int8_t);
const int CRC_START_BYTE = INT64_T_SIZE + INT32_T_SIZE + INT32_T_SIZE + INT8_T_SIZE;
const int BYTE_AFTER_CRC = CRC_START_BYTE + INT32_T_SIZE;

struct Batch {
    int64_t base_offset =
        0;  // The starting offset for the first record in this batch. All records use a delta relative to this.
    int32_t batch_length = 0;  // Total size of the batch in bytes (excluding base_offset and batch_length themselves).
    int32_t partition_epoch = 0;  // Used to fence stale leaders (part of replication protocol).
    int8_t  magic           = 2;  // Magic byte indicating the message format version (e.g., 2 for Kafka 0.11+).
    int32_t crc = 0;  // CRC32C checksum covering the rest of the batch (from attributes to the end) for data integrity.
    int16_t attributes = 0;  // 16-bit flags indicating compression type, timestamp type, transactional status, etc.
    int32_t last_offset_delta =
        0;  // Difference between the last record's offset and the base_offset (effectively: num_records - 1).
    int64_t first_timestamp = 0;  // Timestamp of the first record in the batch.
    int64_t max_timestamp = 0;  // Maximum timestamp of any record in the batch (used by broker for retention policies).
    int64_t producer_id   = 0;  // Identifier of the producer, used for Exactly-Once Semantics (Idempotent Producer).
    int16_t producer_epoch = 0;  // Epoch of the producer, used to fence zombie producers in Exactly-Once Semantics.
    int32_t base_sequence  = 0;  // Sequence number of the first record, used for deduplication in Idempotent Producers.
    int32_t records_count  = 0;  // Total number of records contained in this batch.
    std::vector<Record> records;  // The actual variable-length records.
};

std::string serialize_batch(const Batch& batch);
bool        deserialize_batch(const char* data, size_t len, Batch& batch);
bool        calculate_crc(std::string& batch_buf);