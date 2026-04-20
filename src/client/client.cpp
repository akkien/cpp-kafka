#include "client/client.h"

#include <unistd.h>

#include <iostream>
#include <sstream>

namespace kafka {

Client::Client(const std::string& host, uint16_t port) : host_(host), port_(port) {}

bool Client::connect() {
    return conn_.connect(host_, port_);
}

int64_t Client::produce(const std::string& topic, const std::string& payload) {
    ProduceRequest produce_request;
    produce_request.api_key = ReqType::PRODUCE;
    produce_request.topic   = topic;

    Record rec;
    rec.value  = payload;
    rec.length = static_cast<int32_t>(payload.size());
    produce_request.batch.records.push_back(std::move(rec));

    std::cout << "before serialize" << std::endl;
    if (!conn_.send(serialize_produce_request(produce_request))) {
        std::cout << "after serialize" << std::endl;
        return -1;
    }

    std::string response;
    if (!conn_.read_line(response))
        return -1;

    // Expected: "OK <offset>"
    if (response.substr(0, 3) != "OK ")
        return -1;
    return std::stoll(response.substr(3));
}

/// @dev max_bytes in kafka means whenever the data is more than that, stop reading.
/// max_bytes is not upper bound of data size.
std::vector<Batch> Client::consume(const std::string& topic, uint64_t offset, uint32_t max_bytes) {
    ConsumeRequest consume_request;
    consume_request.api_key   = ReqType::CONSUME;
    consume_request.topic     = topic;
    consume_request.offset    = offset;
    consume_request.max_bytes = max_bytes;

    if (!conn_.send(serialize_consume_request(consume_request)))
        return {};

    std::vector<Batch> batches;
    std::string        line;
    Batch              batch;
    size_t             byte_read = 0;
    while (byte_read < max_bytes) {
        conn_.read_bytes(line, 8);  // offset
        conn_.read_bytes(line, 4);  // size
        uint32_t size = ntohl(*reinterpret_cast<uint32_t*>(line.data()));
        conn_.read_bytes(line, size);
        deserialize_batch(line.data(), size, batch);
        batches.push_back(std::move(batch));
        byte_read += 12 + size;
    }

    return batches;
}

// while (conn_.read_bytes(line, max_bytes)) {
//     if (line == "END")
//         break;

//     // Expected: "MESSAGE <offset> <len>"
//     std::istringstream iss(line);
//     std::string        tag;
//     Message            msg;
//     iss >> tag >> msg.offset >> msg.size;

//     if (tag != "MESSAGE" || iss.fail())
//         break;

//     if (!conn_.read_bytes(msg.payload, msg.size))
//         break;
//     messages.push_back(std::move(msg));
// }
// return messages;
// }

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
