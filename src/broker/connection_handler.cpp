#include "broker/connection_handler.h"

#include <sys/socket.h>
#include <unistd.h>

#include <iostream>
#include <variant>

#include "broker/log_manager.h"

namespace kafka {

ConnectionHandler::ConnectionHandler(int client_fd, ThreadPool& thread_pool)
    : client_fd_(client_fd), thread_pool_(thread_pool) {}

ConnectionHandler::~ConnectionHandler() {
    if (client_fd_ >= 0)
        ::close(client_fd_);
}

// ---------------------------------------------------------------------------
void ConnectionHandler::run() {
    Request req;
    ReqType req_type;
    while (read_message(req_type, req)) {
        RequestItem req_item{client_fd_, req_type, req};
        thread_pool_.enqueue(req_item);
    }
}

// ---------------------------------------------------------------------------
// read_message: reads [4B size][body], peeks api_key, parses into Request variant
// ---------------------------------------------------------------------------
bool ConnectionHandler::read_message(ReqType& req_type, Request& req) {
    // 1. Read 4-byte frame size
    char    size_buf[4];
    ssize_t n = ::recv(client_fd_, size_buf, sizeof(size_buf), MSG_WAITALL);
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
            if (!parse_produce_request(buf.data(), buf.size(), pr)) {
                std::cerr << "[broker] PARSE FAILED: ProduceRequest (size=" << req_size << ")\n";
                return false;
            }
            req = std::move(pr);
            break;
        }
        case ReqType::FETCH: {
            ConsumeRequest cr;
            if (!parse_consume_request(buf.data(), buf.size(), cr)) {
                std::cerr << "[broker] PARSE FAILED: ConsumeRequest (size=" << req_size << ")\n";
                return false;
            }
            req = std::move(cr);
            break;
        }
        case ReqType::LIST_OFFSETS: {
            ListOffsetsRequest lor;
            if (!parse_list_offsets_request(buf.data(), buf.size(), lor)) {
                std::cerr << "[broker] PARSE FAILED: ListOffsetsRequest (size=" << req_size << ")\n";
                return false;
            }
            req = std::move(lor);
            break;
        }
        case ReqType::API_VERSIONS: {
            ApiVersionsRequest avr;
            if (!parse_api_versions_request(buf.data(), buf.size(), avr)) {
                std::cerr << "[broker] PARSE FAILED: ApiVersionsRequest (size=" << req_size << ")\n";
                return false;
            }
            req = std::move(avr);
            break;
        }
        case ReqType::METADATA: {
            MetadataRequest mr;
            if (!parse_metadata_request(buf.data(), buf.size(), mr)) {
                std::cerr << "[broker] PARSE FAILED: MetadataRequest (size=" << req_size << ")\n";
                return false;
            }
            req = std::move(mr);
            break;
        }
        case ReqType::FIND_COORDINATOR: {
            FindCoordinatorRequest fcr;
            if (!parse_find_coordinator_request(buf.data(), buf.size(), fcr)) {
                std::cerr << "[broker] PARSE FAILED: FindCoordinatorRequest (size=" << req_size << ")\n";
                return false;
            }
            req = std::move(fcr);
            break;
        }
        case ReqType::JOIN_GROUP: {
            JoinGroupRequest jgr;
            if (!parse_join_group_request(buf.data(), buf.size(), jgr)) {
                std::cerr << "[broker] PARSE FAILED: JoinGroupRequest (size=" << req_size << ")\n";
                return false;
            }
            req = std::move(jgr);
            break;
        }
        case ReqType::SYNC_GROUP: {
            SyncGroupRequest sgr;
            if (!parse_sync_group_request(buf.data(), buf.size(), sgr)) {
                std::cerr << "[broker] PARSE FAILED: SyncGroupRequest (size=" << req_size << ")\n";
                return false;
            }
            req = std::move(sgr);
            break;
        }
        case ReqType::HEARTBEAT: {
            HeartbeatRequest hbr;
            if (!parse_heartbeat_request(buf.data(), buf.size(), hbr)) {
                std::cerr << "[broker] PARSE FAILED: HeartbeatRequest (size=" << req_size << ")\n";
                return false;
            }
            req = std::move(hbr);
            break;
        }
        case ReqType::OFFSET_FETCH: {
            OffsetFetchRequest ofr;
            if (!parse_offset_fetch_request(buf.data(), buf.size(), ofr)) {
                std::cerr << "[broker] PARSE FAILED: OffsetFetchRequest (size=" << req_size << ")\n";
                return false;
            }
            req = std::move(ofr);
            break;
        }
        case ReqType::OFFSET_COMMIT: {
            OffsetCommitRequest ocr;
            if (!parse_offset_commit_request(buf.data(), buf.size(), ocr)) {
                std::cerr << "[broker] PARSE FAILED: OffsetCommitRequest (size=" << req_size << ")\n";
                return false;
            }
            req = std::move(ocr);
            break;
        }
        default:
            std::cerr << "[broker] unknown api_key=" << api_key << ", sending error\n";
            send_unsupported(buf.data(), buf.size());
            req_type = static_cast<ReqType>(-1);  // mark as unknown
            return true;                          // keep connection alive
    }

    return true;
}

// ---------------------------------------------------------------------------
void ConnectionHandler::send_unsupported(const char* buf, size_t len) {
    int32_t corr = peek_correlation_id(buf, len);
    // error_code 35 = UNSUPPORTED_VERSION — safe generic error
    std::string resp = serialize_error_response(corr, 35);
    ::send(client_fd_, resp.data(), resp.size(), 0);
}

}  // namespace kafka
