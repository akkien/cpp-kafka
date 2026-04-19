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
