#pragma once

#include <string>

namespace kafka {

struct ConnectionSession {
    int fd;
    std::string read_buffer;
};

}  // namespace kafka
