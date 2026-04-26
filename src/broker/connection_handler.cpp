#include "broker/connection_handler.h"

#include <sys/socket.h>
#include <unistd.h>

#include <iostream>
#include <variant>

#include "broker/log_manager.h"

namespace kafka {

static constexpr int32_t BROKER_NODE_ID = 1;
static constexpr int32_t BROKER_PORT    = 9092;
static const std::string BROKER_HOST    = "127.0.0.1";

ConnectionHandler::ConnectionHandler(int client_fd) : client_fd_(client_fd) {}

ConnectionHandler::~ConnectionHandler() {
    if (client_fd_ >= 0)
        ::close(client_fd_);
}

// ---------------------------------------------------------------------------
void ConnectionHandler::run() {
    Request req;
    ReqType req_type;
    while (read_message(req_type, req)) {
        switch (req_type) {
            case ReqType::PRODUCE:
                std::cout << "[broker] PRODUCE request received\n";
                handle_produce(req);
                break;
            case ReqType::FETCH:
                std::cout << "[broker] FETCH request received\n";
                handle_consume(req);
                break;
            case ReqType::API_VERSIONS:
                std::cout << "[broker] API_VERSIONS request received\n";
                handle_api_versions(req);
                break;
            case ReqType::METADATA:
                std::cout << "[broker] METADATA request received\n";
                handle_metadata(req);
                break;
            default:
                std::cerr << "[broker] unknown api_key, skipping\n";
                break;
        }
    }
}

// ---------------------------------------------------------------------------
void ConnectionHandler::handle_produce(Request& req) {
    auto& pr = std::get<ProduceRequest>(req);
    ProduceResponse res;
    res.correlation_id   = pr.header.correlation_id;
    res.throttle_time_ms = 0;

    for (auto& topic : pr.topics) {
        TopicProduceResponse topic_res;
        topic_res.name = topic.name;
        for (auto& part : topic.partitions) {
            PartitionProduceResponse part_res;
            part_res.partition_index = part.partition_index;
            part_res.error_code      = 0;

            part_res.base_offset =
                LogManager::instance().append(topic.name, part.records);
            std::cout << "[broker] append ok offset=" << part_res.base_offset << "\n";
            part_res.log_append_time =
                std::chrono::system_clock::now().time_since_epoch().count() / 1000000;
            part_res.log_start_offset = 0;

            topic_res.partitions.push_back(part_res);
        }
        res.topics.push_back(topic_res);
    }

    std::string buf = serialize_produce_response(res);
    ::send(client_fd_, buf.data(), buf.size(), 0);
}

// ---------------------------------------------------------------------------
void ConnectionHandler::handle_consume(Request& req) {
    auto& cr = std::get<ConsumeRequest>(req);

    if (!cr.topics.empty() && !cr.topics[0].partitions.empty()) {
        const auto& topic = cr.topics[0];
        const auto& part  = topic.partitions[0];
        LogManager::instance().send(client_fd_, cr.header.correlation_id,
                                    topic.name, part.fetch_offset, part.max_bytes);
    }
}

// ---------------------------------------------------------------------------
void ConnectionHandler::handle_api_versions(Request& req) {
    auto& avr = std::get<ApiVersionsRequest>(req);

    ApiVersionsResponse res;
    res.correlation_id   = avr.header.correlation_id;
    res.error_code       = 0;
    res.throttle_time_ms = 0;

    // Advertise only the versions we actually implement (non-flexible)
    res.api_versions = {
        {0,  0, 3},   // Produce
        {1,  0, 3},   // Fetch
        {3,  0, 5},   // Metadata
        {18, 0, 3},   // ApiVersions
    };

    std::string buf = serialize_api_versions_response(res);
    ::send(client_fd_, buf.data(), buf.size(), 0);
}

// ---------------------------------------------------------------------------
void ConnectionHandler::handle_metadata(Request& req) {
    auto& mr = std::get<MetadataRequest>(req);

    MetadataResponse res;
    res.correlation_id = mr.header.correlation_id;
    res.controller_id  = BROKER_NODE_ID;

    // Single-broker cluster
    res.brokers.push_back({BROKER_NODE_ID, BROKER_HOST, BROKER_PORT});

    // Determine which topics to describe
    std::vector<std::string> requested = mr.topics;
    if (requested.empty()) {
        // Client wants ALL topics
        requested = LogManager::instance().list_topics();
    }

    for (const auto& tname : requested) {
        TopicMetadata tm;
        tm.name       = tname;
        tm.error_code = 0;

        // Single partition 0, led by our only broker
        PartitionMetadata pm;
        pm.error_code      = 0;
        pm.partition_index = 0;
        pm.leader_id       = BROKER_NODE_ID;
        tm.partitions.push_back(pm);

        res.topics.push_back(tm);
    }

    std::string buf = serialize_metadata_response(res);
    ::send(client_fd_, buf.data(), buf.size(), 0);
}

// ---------------------------------------------------------------------------
void ConnectionHandler::handle_list_topics() {
    auto topics = LogManager::instance().list_topics();
    for (const auto& t : topics) {
        send_response(t + "\n");
    }
    send_response("END\n");
}

// ─── Low-level I/O helpers ───────────────────────────────────────────
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

// ---------------------------------------------------------------------------
// read_message: reads [4B size][body], peeks api_key, parses into Request variant
// ---------------------------------------------------------------------------
bool ConnectionHandler::read_message(ReqType& req_type, Request& req) {
    // 1. Read 4-byte frame size
    char     size_buf[4];
    ssize_t  n = ::recv(client_fd_, size_buf, sizeof(size_buf), MSG_WAITALL);
    if (n <= 0)
        return false;

    uint32_t req_size = ntohl(*reinterpret_cast<uint32_t*>(size_buf));
    if (req_size == 0 || req_size > 64 * 1024 * 1024)
        return false;

    // 2. Read body
    std::string buf(req_size, '\0');
    n = ::recv(client_fd_, buf.data(), req_size, MSG_WAITALL);
    if (n <= 0)
        return false;

    // 3. Dispatch on api_key (first 2 bytes of body)
    int16_t api_key = peek_api_key(buf.data(), buf.size());
    std::cout << "[broker] api_key=" << api_key << " size=" << req_size << "\n";
    req_type = static_cast<ReqType>(api_key);

    switch (req_type) {
        case ReqType::PRODUCE: {
            ProduceRequest pr;
            if (!parse_produce_request(buf.data(), buf.size(), pr))
                return false;
            req = std::move(pr);
            break;
        }
        case ReqType::FETCH: {
            ConsumeRequest cr;
            if (!parse_consume_request(buf.data(), buf.size(), cr))
                return false;
            req = std::move(cr);
            break;
        }
        case ReqType::API_VERSIONS: {
            ApiVersionsRequest avr;
            if (!parse_api_versions_request(buf.data(), buf.size(), avr))
                return false;
            req = std::move(avr);
            break;
        }
        case ReqType::METADATA: {
            MetadataRequest mr;
            if (!parse_metadata_request(buf.data(), buf.size(), mr))
                return false;
            req = std::move(mr);
            break;
        }
        default:
            std::cerr << "[broker] unknown api_key=" << api_key << ", skipping\n";
            return true;  // keep connection alive, skip unknown request
    }

    return true;
}

}  // namespace kafka
