#pragma once

#include <atomic>
#include <cstdint>
#include <string>

namespace kafka {

/// TCP server that listens for producer / consumer connections.
class Server {
public:
    explicit Server(uint16_t port = kDefaultPort);
    ~Server();

    /// Start accepting connections (blocks the calling thread).
    void run();

    /// Signal the server to shut down gracefully.
    void shutdown();

private:
    static constexpr uint16_t kDefaultPort = 9092;
    static constexpr int      kBacklog     = 128;

    uint16_t         port_;
    int              listen_fd_{-1};
    std::atomic<bool> running_{false};

    /// Set up the listening socket.
    void bind_and_listen();

    /// Accept loop — spawns a handler per connection.
    void accept_loop();
};

}  // namespace kafka
