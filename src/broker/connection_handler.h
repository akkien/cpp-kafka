#pragma once

#include <string>

namespace kafka {

/// Handles one TCP client connection (runs on its own thread).
class ConnectionHandler {
public:
    explicit ConnectionHandler(int client_fd);
    ~ConnectionHandler();

    /// Read requests in a loop until the client disconnects.
    void run();

private:
    int client_fd_;

    /// Read one line (terminated by '\n') from the socket.
    bool read_line(std::string& out);

    /// Read exactly `n` bytes from the socket.
    bool read_bytes(std::string& out, size_t n);

    /// Write a string to the socket.
    bool send_response(const std::string& data);

    // ─── Command handlers ────────────────────────────────────────
    void handle_produce(const std::string& line);
    void handle_consume(const std::string& line);
    void handle_list_topics();
};

}  // namespace kafka
