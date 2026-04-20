#include "broker/connection_handler.h"

#include <sys/socket.h>
#include <unistd.h>

#include <iostream>
#include <variant>

#include "broker/log_manager.h"

namespace kafka {

ConnectionHandler::ConnectionHandler(int client_fd) : client_fd_(client_fd) {}

ConnectionHandler::~ConnectionHandler() {
    if (client_fd_ >= 0)
        ::close(client_fd_);
}

// ---------------------------------------------------------------------------
void ConnectionHandler::run() {
    std::string line;
    Request     req;
    ReqType     req_type;
    while (read_message(req_type, req)) {
        switch (req_type) {
            case ReqType::PRODUCE:
                std::cout << "[broker] produce request received" << std::endl;
                handle_produce(req);
                break;
            case ReqType::CONSUME:
                handle_consume(req);
                break;
            default:
                send_response("ERROR unknown request type\n");
                break;
        }
    }
}

// ---------------------------------------------------------------------------
void ConnectionHandler::handle_produce(Request& req) {
    auto&    pr     = std::get<ProduceRequest>(req);
    uint64_t offset = LogManager::instance().append(pr.topic, serialize_batch(pr.batch));
    send_response("OK " + std::to_string(offset) + "\n");
}

void ConnectionHandler::handle_consume(Request& req) {
    auto& cr = std::get<ConsumeRequest>(req);

    LogManager::instance().send(client_fd_, cr.topic, cr.offset, cr.max_bytes);
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

bool ConnectionHandler::read_message(ReqType& req_type, std::variant<ProduceRequest, ConsumeRequest>& req) {
    char req_size_buf[4];
    int  bytes_received = ::recv(client_fd_, req_size_buf, sizeof(req_size_buf), MSG_WAITALL);
    if (bytes_received <= 0) {
        return false;
    }
    uint32_t req_size_val = ntohl(*reinterpret_cast<uint32_t*>(req_size_buf));

    std::string req_buf(req_size_val, '\0');
    bytes_received = ::recv(client_fd_, req_buf.data(), req_size_val, MSG_WAITALL);
    if (bytes_received <= 0) {
        return false;
    }
    uint16_t api_key = ntohs(*reinterpret_cast<uint16_t*>(req_buf.data()));
    req_type         = static_cast<ReqType>(api_key);
    if (req_type == ReqType::PRODUCE) {
        ProduceRequest pr;
        if (!parse_produce_request(req_buf.data(), req_buf.size(), pr))
            return false;
        req = std::move(pr);
    } else if (req_type == ReqType::CONSUME) {
        ConsumeRequest cr;
        if (!parse_consume_request(req_buf.data(), req_buf.size(), cr))
            return false;
        req = std::move(cr);
    }

    return true;
}

}  // namespace kafka
