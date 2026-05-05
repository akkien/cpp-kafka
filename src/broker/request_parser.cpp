#include "broker/request_parser.h"
#include <iostream>

namespace kafka {

bool parse_request_body(int16_t api_key, const std::string& body, ReqType& req_type, Request& req) {
    req_type = static_cast<ReqType>(api_key);

    switch (req_type) {
        case ReqType::PRODUCE: {
            ProduceRequest pr;
            if (!parse_produce_request(body.data(), body.size(), pr)) {
                std::cerr << "[broker] PARSE FAILED: ProduceRequest\n";
                return false;
            }
            req = std::move(pr);
            break;
        }
        case ReqType::FETCH: {
            ConsumeRequest cr;
            if (!parse_consume_request(body.data(), body.size(), cr)) {
                std::cerr << "[broker] PARSE FAILED: ConsumeRequest\n";
                return false;
            }
            req = std::move(cr);
            break;
        }
        case ReqType::LIST_OFFSETS: {
            ListOffsetsRequest lor;
            if (!parse_list_offsets_request(body.data(), body.size(), lor)) {
                std::cerr << "[broker] PARSE FAILED: ListOffsetsRequest\n";
                return false;
            }
            req = std::move(lor);
            break;
        }
        case ReqType::API_VERSIONS: {
            ApiVersionsRequest avr;
            if (!parse_api_versions_request(body.data(), body.size(), avr)) {
                std::cerr << "[broker] PARSE FAILED: ApiVersionsRequest\n";
                return false;
            }
            req = std::move(avr);
            break;
        }
        case ReqType::METADATA: {
            MetadataRequest mr;
            if (!parse_metadata_request(body.data(), body.size(), mr)) {
                std::cerr << "[broker] PARSE FAILED: MetadataRequest\n";
                return false;
            }
            req = std::move(mr);
            break;
        }
        case ReqType::FIND_COORDINATOR: {
            FindCoordinatorRequest fcr;
            if (!parse_find_coordinator_request(body.data(), body.size(), fcr)) {
                std::cerr << "[broker] PARSE FAILED: FindCoordinatorRequest\n";
                return false;
            }
            req = std::move(fcr);
            break;
        }
        case ReqType::JOIN_GROUP: {
            JoinGroupRequest jgr;
            if (!parse_join_group_request(body.data(), body.size(), jgr)) {
                std::cerr << "[broker] PARSE FAILED: JoinGroupRequest\n";
                return false;
            }
            req = std::move(jgr);
            break;
        }
        case ReqType::SYNC_GROUP: {
            SyncGroupRequest sgr;
            if (!parse_sync_group_request(body.data(), body.size(), sgr)) {
                std::cerr << "[broker] PARSE FAILED: SyncGroupRequest\n";
                return false;
            }
            req = std::move(sgr);
            break;
        }
        case ReqType::HEARTBEAT: {
            HeartbeatRequest hbr;
            if (!parse_heartbeat_request(body.data(), body.size(), hbr)) {
                std::cerr << "[broker] PARSE FAILED: HeartbeatRequest\n";
                return false;
            }
            req = std::move(hbr);
            break;
        }
        case ReqType::OFFSET_FETCH: {
            OffsetFetchRequest ofr;
            if (!parse_offset_fetch_request(body.data(), body.size(), ofr)) {
                std::cerr << "[broker] PARSE FAILED: OffsetFetchRequest\n";
                return false;
            }
            req = std::move(ofr);
            break;
        }
        case ReqType::OFFSET_COMMIT: {
            OffsetCommitRequest ocr;
            if (!parse_offset_commit_request(body.data(), body.size(), ocr)) {
                std::cerr << "[broker] PARSE FAILED: OffsetCommitRequest\n";
                return false;
            }
            req = std::move(ocr);
            break;
        }
        default:
            std::cerr << "[broker] unknown api_key=" << api_key << "\n";
            req_type = static_cast<ReqType>(-1);
            return true; // Return true to keep connection alive, error handled later
    }
    return true;
}

std::string serialize_unsupported_error(const std::string& body) {
    if (body.empty()) return "";
    int32_t corr = peek_correlation_id(body.data(), body.size());
    // error_code 35 = UNSUPPORTED_VERSION
    return serialize_error_response(corr, 35);
}

} // namespace kafka
