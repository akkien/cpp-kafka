#pragma once

#include <atomic>
#include <cstdint>
#include <string>

namespace kafka {

/// TCP server that listens for producer / consumer connections.
class Server {
public:
    /// @dev 'explicit' prevents implicit conversions and copy constructions.
    /// ex: Server s = 9092; is illegal.
    explicit Server(uint16_t port = kDefaultPort);
    ~Server();

    /// Start accepting connections (blocks the calling thread).
    void run();

    /// Signal the server to shut down gracefully.
    void shutdown();

private:
    /// @dev constexpr: assign value at compile time.
    static constexpr uint16_t kDefaultPort = 9092;
    static constexpr int      kBacklog     = 128;

    uint16_t port_;
    /// @dev default member initializer (C++11), -1 is default value for file descriptor
    int               listen_fd_{-1};
    std::atomic<bool> running_{false};

    /// Set up the listening socket.
    void bind_and_listen();

    /// Accept loop — spawns a handler per connection.
    void accept_loop();
};

}  // namespace kafka
