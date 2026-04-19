#include <string>

void encode_varint(std::string& buf, uint64_t value);
int  decode_varint(const char* buf, size_t n, uint64_t& value);
