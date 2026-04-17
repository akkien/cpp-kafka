#pragma once

#include <cstdint>
#include <string>

namespace kafka {

/// Low-level TCP connection to the broker.
class Connection {
public:
    Connection() = default;
    ~Connection();

    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    bool connect(const std::string& host, uint16_t port);
    void close();
    bool send(const std::string& data);
    bool read_line(std::string& out);
    bool read_bytes(std::string& out, size_t n);
    [[nodiscard]] bool is_connected() const { return fd_ >= 0; }

private:
    int fd_{-1};
};

}  // namespace kafka
