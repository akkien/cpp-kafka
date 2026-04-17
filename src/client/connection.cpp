#include "client/connection.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>

namespace kafka {

Connection::~Connection() { close(); }

bool Connection::connect(const std::string& host, uint16_t port) {
    fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ < 0) return false;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(port);
    ::inet_pton(AF_INET, host.c_str(), &addr.sin_addr);

    if (::connect(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd_);
        fd_ = -1;
        return false;
    }
    return true;
}

void Connection::close() {
    if (fd_ >= 0) { ::close(fd_); fd_ = -1; }
}

bool Connection::send(const std::string& data) {
    size_t total = 0;
    while (total < data.size()) {
        ssize_t n = ::send(fd_, data.data() + total, data.size() - total, 0);
        if (n <= 0) return false;
        total += static_cast<size_t>(n);
    }
    return true;
}

bool Connection::read_line(std::string& out) {
    out.clear();
    char c;
    while (true) {
        ssize_t n = ::recv(fd_, &c, 1, 0);
        if (n <= 0) return false;
        if (c == '\n') return true;
        out += c;
    }
}

bool Connection::read_bytes(std::string& out, size_t n) {
    out.resize(n);
    size_t total = 0;
    while (total < n) {
        ssize_t r = ::recv(fd_, out.data() + total, n - total, 0);
        if (r <= 0) return false;
        total += static_cast<size_t>(r);
    }
    return true;
}

}  // namespace kafka
