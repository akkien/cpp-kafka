#include <string>
#include <vector>

#include "common/record.h"

struct Batch {
    std::vector<Record> records;
};

std::string serialize_batch(const Batch& batch);
bool        deserialize_batch(const char* data, size_t len, Batch& batch);