#include "broker/request_handler.h"

#include <sys/socket.h>
#include <unistd.h>

#include <iostream>
#include <variant>

#include "broker/log_manager.h"

namespace kafka {

static constexpr int32_t BROKER_NODE_ID = 1;
static constexpr int32_t BROKER_PORT    = 9092;
static const std::string BROKER_HOST    = "127.0.0.1";

RequestHandler::RequestHandler(int client_fd) : client_fd_(client_fd) {}

RequestHandler::~RequestHandler() {}

// ---------------------------------------------------------------------------

void RequestHandler::handle_request(ReqType& req_type, Request& req) {
    switch (req_type) {
        case ReqType::PRODUCE:
            std::cout << "[broker] PRODUCE request received\n";
            handle_produce(req);
            break;
        case ReqType::FETCH:
            std::cout << "[broker] FETCH request received\n";
            handle_consume(req);
            break;
        case ReqType::LIST_OFFSETS:
            std::cout << "[broker] LIST_OFFSETS request received\n";
            handle_list_offsets(req);
            break;
        case ReqType::API_VERSIONS:
            std::cout << "[broker] API_VERSIONS request received\n";
            handle_api_versions(req);
            break;
        case ReqType::METADATA:
            std::cout << "[broker] METADATA request received\n";
            handle_metadata(req);
            break;
        case ReqType::FIND_COORDINATOR:
            std::cout << "[broker] FIND_COORDINATOR request received\n";
            handle_find_coordinator(req);
            break;
        case ReqType::JOIN_GROUP:
            std::cout << "[broker] JOIN_GROUP request received\n";
            handle_join_group(req);
            break;
        case ReqType::SYNC_GROUP:
            std::cout << "[broker] SYNC_GROUP request received\n";
            handle_sync_group(req);
            break;
        case ReqType::HEARTBEAT:
            std::cout << "[broker] HEARTBEAT request received\n";
            handle_heartbeat(req);
            break;
        case ReqType::OFFSET_FETCH:
            std::cout << "[broker] OFFSET_FETCH request received\n";
            handle_offset_fetch(req);
            break;
        case ReqType::OFFSET_COMMIT:
            std::cout << "[broker] OFFSET_COMMIT request received\n";
            handle_offset_commit(req);
            break;
        default:
            std::cerr << "[broker] unknown api_key, skipping\n";
            break;
    }
}

// ---------------------------------------------------------------------------
void RequestHandler::handle_produce(Request& req) {
    auto&           pr = std::get<ProduceRequest>(req);
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

            part_res.base_offset = LogManager::instance().append(topic.name, part.records);
            std::cout << "[broker] append ok offset=" << part_res.base_offset << "\n";
            part_res.log_append_time  = std::chrono::system_clock::now().time_since_epoch().count() / 1000000;
            part_res.log_start_offset = 0;

            topic_res.partitions.push_back(part_res);
        }
        res.topics.push_back(topic_res);
    }

    std::string buf = serialize_produce_response(res);
    ::send(client_fd_, buf.data(), buf.size(), 0);
}

// ---------------------------------------------------------------------------
void RequestHandler::handle_consume(Request& req) {
    auto& cr = std::get<ConsumeRequest>(req);

    if (!cr.topics.empty() && !cr.topics[0].partitions.empty()) {
        const auto& topic = cr.topics[0];
        const auto& part  = topic.partitions[0];
        LogManager::instance().send(client_fd_, cr.header.api_version, cr.header.correlation_id, topic.name,
                                    part.fetch_offset, part.max_bytes);
    }
}

// ---------------------------------------------------------------------------
void RequestHandler::handle_api_versions(Request& req) {
    auto& avr = std::get<ApiVersionsRequest>(req);

    ApiVersionsResponse res;
    res.correlation_id   = avr.header.correlation_id;
    res.error_code       = 0;
    res.throttle_time_ms = 0;

    // Advertise only the versions we actually implement (non-flexible)
    res.api_versions = {
        {0, 0, 3},   // Produce
        {1, 0, 4},   // Fetch (v4 so kafkajs uses RecordBatchDecoder for magic byte 2)
        {2, 0, 2},   // ListOffsets
        {3, 0, 5},   // Metadata
        {8, 0, 2},   // OffsetCommit
        {9, 0, 2},   // OffsetFetch
        {10, 0, 2},  // FindCoordinator
        {11, 0, 2},  // JoinGroup
        {12, 0, 1},  // Heartbeat
        {14, 0, 1},  // SyncGroup
        {18, 0, 3},  // ApiVersions
    };

    std::string buf = serialize_api_versions_response(res);
    ::send(client_fd_, buf.data(), buf.size(), 0);
}

// ---------------------------------------------------------------------------
void RequestHandler::handle_metadata(Request& req) {
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
void RequestHandler::handle_find_coordinator(Request& req) {
    auto&                   fcr = std::get<FindCoordinatorRequest>(req);
    FindCoordinatorResponse res;
    res.correlation_id   = fcr.header.correlation_id;
    res.throttle_time_ms = 0;
    res.error_code       = 0;
    res.node_id          = BROKER_NODE_ID;
    res.host             = BROKER_HOST;
    res.port             = BROKER_PORT;
    std::string buf      = serialize_find_coordinator_response(res);
    ::send(client_fd_, buf.data(), buf.size(), 0);
}

// ---------------------------------------------------------------------------
void RequestHandler::handle_list_topics() {
    auto topics = LogManager::instance().list_topics();
    for (const auto& t : topics) {
        send_response(t + "\n");
    }
    send_response("END\n");
}

bool RequestHandler::send_response(const std::string& data) {
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
void RequestHandler::handle_join_group(Request& req) {
    auto&             jgr = std::get<JoinGroupRequest>(req);
    JoinGroupResponse res;
    res.correlation_id   = jgr.header.correlation_id;
    res.throttle_time_ms = 0;
    res.error_code       = 0;
    res.generation_id    = 1;
    res.protocol_name    = jgr.first_protocol_name.empty() ? "consumer" : jgr.first_protocol_name;
    res.leader           = "kafkajs-dummy-member";
    res.member_id        = "kafkajs-dummy-member";
    res.member_metadata  = jgr.first_protocol_metadata;

    std::string buf = serialize_join_group_response(res);
    ::send(client_fd_, buf.data(), buf.size(), 0);
}

// ---------------------------------------------------------------------------
void RequestHandler::handle_sync_group(Request& req) {
    auto&             sgr = std::get<SyncGroupRequest>(req);
    SyncGroupResponse res;
    res.correlation_id   = sgr.header.correlation_id;
    res.throttle_time_ms = 0;
    res.error_code       = 0;
    res.assignment       = sgr.group_assignment;

    std::string buf = serialize_sync_group_response(res);
    ::send(client_fd_, buf.data(), buf.size(), 0);
}

// ---------------------------------------------------------------------------
void RequestHandler::handle_heartbeat(Request& req) {
    auto&             hbr = std::get<HeartbeatRequest>(req);
    HeartbeatResponse res;
    res.correlation_id   = hbr.header.correlation_id;
    res.throttle_time_ms = 0;
    res.error_code       = 0;

    std::string buf = serialize_heartbeat_response(res);
    ::send(client_fd_, buf.data(), buf.size(), 0);
}

// ---------------------------------------------------------------------------
void RequestHandler::handle_offset_fetch(Request& req) {
    auto&               ofr = std::get<OffsetFetchRequest>(req);
    OffsetFetchResponse res;
    res.correlation_id   = ofr.header.correlation_id;
    res.throttle_time_ms = 0;
    res.topics           = ofr.topics;  // Send back the same topics

    std::string buf = serialize_offset_fetch_response(res);
    ::send(client_fd_, buf.data(), buf.size(), 0);
}

// ---------------------------------------------------------------------------
void RequestHandler::handle_offset_commit(Request& req) {
    auto&                ocr = std::get<OffsetCommitRequest>(req);
    OffsetCommitResponse res;
    res.correlation_id   = ocr.header.correlation_id;
    res.throttle_time_ms = 0;
    res.topics           = ocr.topics;  // Send back the same topics

    std::string buf = serialize_offset_commit_response(res);
    ::send(client_fd_, buf.data(), buf.size(), 0);
}

// ---------------------------------------------------------------------------
void RequestHandler::handle_list_offsets(Request& req) {
    auto&               lor = std::get<ListOffsetsRequest>(req);
    ListOffsetsResponse res;
    res.correlation_id   = lor.header.correlation_id;
    res.throttle_time_ms = 0;

    for (const auto& topic : lor.topics) {
        ListOffsetsTopicResponse tr;
        tr.topic = topic;

        ListOffsetsPartitionResponse pr;
        pr.partition  = 0;
        pr.error_code = 0;
        pr.timestamp  = -1;  // -1 means we don't return timestamp
        // For offset, we should return the current log end offset or 0.
        // Let's just return 0 for the mock.
        pr.offset = 0;

        tr.partitions.push_back(pr);
        res.topics.push_back(tr);
    }

    std::string buf = serialize_list_offsets_response(res);
    ::send(client_fd_, buf.data(), buf.size(), 0);
}

}  // namespace kafka
