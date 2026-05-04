#pragma once

#include <string>
#include <variant>

#include "broker/thread_pool.h"
#include "common/message.h"

namespace kafka {

/// Handles one TCP client connection (runs on its own thread).
class ConnectionHandler {
public:
    explicit ConnectionHandler(int client_fd, ThreadPool& thread_pool);
    ~ConnectionHandler();

    /// Read requests in a loop until the client disconnects.
    void run();

private:
    int         client_fd_;
    ThreadPool& thread_pool_;

    bool read_message(ReqType& req_type, Request& req);
    void send_unsupported(const char* buf, size_t len);
};

}  // namespace kafka
