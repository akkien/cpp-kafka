#include <string>

struct Header {
    std::string key;
    std::string value;
};

struct Record {
    int32_t             length;
    int8_t              attributes;
    int64_t             timestamp_delta;
    int64_t             offset_delta;
    std::string         key;
    std::string         value;
    std::vector<Header> headers;
};

std::string serialize_record(const Record& rec);
bool        deserialize_record(const char* data, size_t len, Record& rec);