#include "broker/connection_handler.h"

#include <sys/socket.h>
#include <unistd.h>

#include <iostream>
#include <variant>

#include "broker/log_manager.h"
#include "broker/protocol_parser.h"

namespace kafka {

ConnectionHandler::ConnectionHandler(int client_fd) : client_fd_(client_fd) {}

ConnectionHandler::~ConnectionHandler() {
    if (client_fd_ >= 0)
        ::close(client_fd_);
}

// ---------------------------------------------------------------------------
void ConnectionHandler::run() {
    std::string line;
    while (read_line(line)) {
        Request req = ProtocolParser::parse(line);

        std::visit(
            [this, &line](auto&& r) {
                using T = std::decay_t<decltype(r)>;

                if constexpr (std::is_same_v<T, ProduceRequest>) {
                    handle_produce(line);
                } else if constexpr (std::is_same_v<T, ConsumeRequest>) {
                    handle_consume(line);
                } else if constexpr (std::is_same_v<T, ListTopicsRequest>) {
                    handle_list_topics();
                } else {
                    // BadRequest
                    send_response("ERROR " + r.reason + "\n");
                }
            },
            req);
    }
}

// ---------------------------------------------------------------------------
void ConnectionHandler::handle_produce(const std::string& line) {
    auto  req_var = ProtocolParser::parse(line);
    auto* req     = std::get_if<ProduceRequest>(&req_var);
    if (!req) {
        send_response("ERROR parse\n");
        return;
    }

    // Read the payload body
    std::string payload;
    if (!read_bytes(payload, req->payload_len)) {
        send_response("ERROR incomplete payload\n");
        return;
    }
    ProtocolParser::attach_payload(*req, payload);

    uint64_t offset = LogManager::instance().append(req->topic, req->payload);
    send_response("OK " + std::to_string(offset) + "\n");
}

void ConnectionHandler::handle_consume(const std::string& line) {
    auto  req_var = ProtocolParser::parse(line);
    auto* req     = std::get_if<ConsumeRequest>(&req_var);
    if (!req) {
        send_response("ERROR parse\n");
        return;
    }

    auto messages = LogManager::instance().read(req->topic, req->offset, req->max_bytes);
    for (const auto& msg : messages) {
        send_response("MESSAGE " + std::to_string(msg.offset) + " " + std::to_string(msg.size) + "\n");
        send_response(msg.payload);
    }
    send_response("END\n");
}

void ConnectionHandler::handle_list_topics() {
    auto topics = LogManager::instance().list_topics();
    for (const auto& t : topics) {
        send_response(t + "\n");
    }
    send_response("END\n");
}

// ─── Low-level I/O helpers ───────────────────────────────────────────
// TODO: readline using buffer
bool ConnectionHandler::read_line(std::string& out) {
    out.clear();
    char c;
    while (true) {
        ssize_t n = ::recv(client_fd_, &c, 1, 0);
        if (n <= 0)
            return false;
        if (c == '\n')
            return true;
        out += c;
    }
}

bool ConnectionHandler::read_bytes(std::string& out, size_t n) {
    out.resize(n);
    size_t total = 0;
    while (total < n) {
        ssize_t r = ::recv(client_fd_, out.data() + total, n - total, 0);
        if (r <= 0)
            return false;
        total += static_cast<size_t>(r);
    }
    return true;
}

bool ConnectionHandler::send_response(const std::string& data) {
    size_t total = 0;
    while (total < data.size()) {
        ssize_t n = ::send(client_fd_, data.data() + total, data.size() - total, 0);
        if (n <= 0)
            return false;
        total += static_cast<size_t>(n);
    }
    return true;
}

}  // namespace kafka
