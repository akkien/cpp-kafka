#include <string>

struct Header {
    std::string key;
    std::string value;
};

struct Record {
    int32_t             length          = 0;
    int8_t              attributes      = 0;
    int64_t             timestamp_delta = 0;
    int64_t             offset_delta    = 0;
    std::string         key;
    std::string         value;
    std::vector<Header> headers;
};

std::string serialize_record(const Record& rec);
size_t      deserialize_record(const char* data, size_t len, Record& rec);