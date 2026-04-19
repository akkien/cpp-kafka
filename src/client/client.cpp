#include "client/client.h"

#include <sstream>

namespace kafka {

Client::Client(const std::string& host, uint16_t port) : host_(host), port_(port) {}

bool Client::connect() {
    return conn_.connect(host_, port_);
}

int64_t Client::produce(const std::string& topic, const std::string& payload) {
    ProduceRequest produce_request;
    produce_request.api_key       = ReqType::PRODUCE;
    produce_request.topic         = topic;
    produce_request.record.value  = payload;
    produce_request.record.length = static_cast<int32_t>(payload.size());

    if (!conn_.send(serialize_produce_request(produce_request)))
        return -1;

    std::string response;
    if (!conn_.read_line(response))
        return -1;

    // Expected: "OK <offset>"
    if (response.substr(0, 3) != "OK ")
        return -1;
    return std::stoll(response.substr(3));
}

std::vector<Message> Client::consume(const std::string& topic, uint64_t offset, uint32_t max_bytes) {
    ConsumeRequest consume_request;
    consume_request.api_key   = ReqType::CONSUME;
    consume_request.topic     = topic;
    consume_request.offset    = offset;
    consume_request.max_bytes = max_bytes;

    if (!conn_.send(serialize_consume_request(consume_request)))
        return {};

    std::vector<Message> messages;
    std::string          line;
    while (conn_.read_line(line)) {
        if (line == "END")
            break;

        // Expected: "MESSAGE <offset> <len>"
        std::istringstream iss(line);
        std::string        tag;
        Message            msg;
        iss >> tag >> msg.offset >> msg.size;

        if (tag != "MESSAGE" || iss.fail())
            break;

        if (!conn_.read_bytes(msg.payload, msg.size))
            break;
        messages.push_back(std::move(msg));
    }
    return messages;
}

std::vector<std::string> Client::list_topics() {
    if (!conn_.send("LIST_TOPICS\n"))
        return {};

    std::vector<std::string> topics;
    std::string              line;
    while (conn_.read_line(line)) {
        if (line == "END")
            break;
        topics.push_back(line);
    }
    return topics;
}

}  // namespace kafka
