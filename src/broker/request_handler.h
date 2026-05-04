#pragma once

#include <string>
#include <variant>

#include "common/message.h"

namespace kafka {

class RequestHandler {
public:
    explicit RequestHandler(int client_fd);
    ~RequestHandler();

    void handle_request(ReqType& req_type, Request& req);

private:
    int client_fd_;

    bool send_response(const std::string& data);

    // ─── Command handlers ────────────────────────────────────────────
    void handle_produce(Request& req);
    void handle_consume(Request& req);
    void handle_api_versions(Request& req);
    void handle_metadata(Request& req);
    void handle_find_coordinator(Request& req);
    void handle_join_group(Request& req);
    void handle_sync_group(Request& req);
    void handle_heartbeat(Request& req);
    void handle_offset_fetch(Request& req);
    void handle_offset_commit(Request& req);
    void handle_list_offsets(Request& req);

    /// Send a generic error response for unsupported APIs.
    void handle_list_topics();
};

}  // namespace kafka
