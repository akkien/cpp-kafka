#include "broker/protocol_parser.h"

#include <sstream>

namespace kafka {

Request ProtocolParser::parse(const std::string& line) {
    std::istringstream iss(line);
    std::string command;
    iss >> command;

    if (command == "PRODUCE") {
        ProduceRequest req;
        iss >> req.topic >> req.payload_len;
        if (req.topic.empty() || iss.fail()) {
            return BadRequest{"malformed PRODUCE"};
        }
        return req;
    }

    if (command == "CONSUME") {
        ConsumeRequest req;
        iss >> req.topic >> req.offset >> req.max_bytes;
        if (req.topic.empty() || iss.fail()) {
            return BadRequest{"malformed CONSUME"};
        }
        return req;
    }

    if (command == "LIST_TOPICS") {
        return ListTopicsRequest{};
    }

    return BadRequest{"unknown command: " + command};
}

void ProtocolParser::attach_payload(ProduceRequest& req, const std::string& payload) {
    req.payload = payload;
}

}  // namespace kafka
