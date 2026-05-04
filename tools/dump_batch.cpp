// Standalone diagnostic: serialize a batch containing "goose" and dump bytes
#include <cstdio>
#include <cstring>
#include <chrono>
#include <string>

// Include our own headers
#include "common/batch.h"
#include "common/record.h"
#include "common/serialize.h"

static void hex_dump(const char* label, const std::string& data) {
    printf("\n=== %s (%zu bytes) ===\n", label, data.size());
    for (size_t i = 0; i < data.size(); ++i) {
        if (i % 16 == 0) printf("%04zx: ", i);
        printf("%02x ", (unsigned char)data[i]);
        if ((i + 1) % 16 == 0) printf("\n");
    }
    printf("\n");
}

int main() {
    // Build a record matching what kafkajs would produce for {value: 'goose'}
    Record rec;
    rec.attributes      = 0;
    rec.timestamp_delta = 0;
    rec.offset_delta    = 0;
    rec.key             = "";       // null key  
    rec.value           = "goose";
    rec.length          = 0;       // will be set by serialize

    std::string rec_bytes = serialize_record(rec);
    hex_dump("Record bytes", rec_bytes);

    // Build a batch
    Batch batch;
    batch.base_offset      = 0;
    batch.partition_epoch  = 0;
    batch.magic            = 2;
    batch.attributes       = 0;
    batch.last_offset_delta = 0;
    auto now = std::chrono::system_clock::now().time_since_epoch();
    batch.first_timestamp  = std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
    batch.max_timestamp    = batch.first_timestamp;
    batch.producer_id      = -1;
    batch.producer_epoch   = -1;
    batch.base_sequence    = -1;
    batch.records_count    = 1;
    batch.records.push_back(rec);

    std::string batch_bytes = serialize_batch(batch);
    hex_dump("Full Batch (as stored in log)", batch_bytes);

    // Show the FetchResponse layout for context
    printf("\n=== FetchResponse v3 layout ===\n");
    printf("  [4B] message_size (outer frame)\n");
    printf("  [4B] correlation_id\n");
    printf("  [4B] throttle_time_ms\n");
    printf("  [4B] topics_count\n");
    printf("  [2B+N] topic_name\n");
    printf("  [4B] partitions_count\n");
    printf("  [4B] partition_index\n");
    printf("  [2B] error_code\n");
    printf("  [8B] high_watermark\n");
    printf("  [4B] record_set_size (bytes following)\n");
    printf("  [...] record batch data\n");

    printf("\n=== Batch header layout (first 61 bytes) ===\n");
    printf("  [8B] base_offset\n");
    printf("  [4B] batch_length (size of everything after this field)\n");
    printf("  [4B] partition_epoch (partitionLeaderEpoch)\n");
    printf("  [1B] magic (should be 0x02)\n");
    printf("  [4B] crc\n");
    printf("  [2B] attributes\n");
    printf("  [4B] lastOffsetDelta\n");
    printf("  [8B] firstTimestamp\n");
    printf("  [8B] maxTimestamp\n");
    printf("  [8B] producerId\n");
    printf("  [2B] producerEpoch\n");
    printf("  [4B] baseSequence\n");
    printf("  [4B] records_count\n");
    printf("  [...] records\n");

    printf("\nTotal batch size: %zu bytes\n", batch_bytes.size());
    printf("Expected header: 61 bytes\n");
    printf("Record payload: %zu bytes\n", batch_bytes.size() - 61);

    // Verify batch_length field
    // batch_length = total_size - 8 (base_offset) - 4 (batch_length itself)
    int32_t stored_batch_length;
    memcpy(&stored_batch_length, batch_bytes.data() + 8, 4);
    stored_batch_length = ntohl(stored_batch_length);
    printf("\nbatch_length field in batch: %d\n", stored_batch_length);
    printf("Expected (total - 12): %zu\n", batch_bytes.size() - 12);

    return 0;
}
