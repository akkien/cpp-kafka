#include <string>

struct Header {
    std::string key;
    std::string value;
};

struct Record {
    int32_t     length          = 0;  // Total size of the record in bytes (excluding this length field).
    int8_t      attributes      = 0;  // Unused in modern Kafka (currently 0), reserved for future flags.
    int64_t     timestamp_delta = 0;  // Difference between this record's timestamp and the batch's first_timestamp.
    int64_t     offset_delta    = 0;  // Difference between this record's sequence index and the batch's base_offset.
    std::string key;                  // Optional key for partitioning and log compaction (empty if null).
    std::string value;                // The actual message payload (empty if null).
    std::vector<Header> headers;      // Key-value pairs for metadata (like HTTP headers).
};

std::string serialize_record(const Record& rec);
size_t      deserialize_record(const char* data, size_t len, Record& rec);