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

    // TODO: multiple records in one batch
    produce_request.batch.records.push_back(std::move(rec));
    produce_request.batch.records_count     = produce_request.batch.records.size();
    produce_request.batch.last_offset_delta = produce_request.batch.records_count - 1;

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
std::vector<Batch> Client::consume(const std::string& topic, uint64_t& offset, uint32_t max_bytes) {
    ConsumeRequest consume_request;
    consume_request.api_key   = ReqType::CONSUME;
    consume_request.topic     = topic;
    consume_request.offset    = offset;
    consume_request.max_bytes = max_bytes;

    if (!conn_.send(serialize_consume_request(consume_request)))
        return {};

    std::string amt_to_receive_buf;
    uint64_t    amt_to_receive;
    if (!conn_.read_bytes(amt_to_receive_buf, 8))
        return {};
    amt_to_receive = *reinterpret_cast<uint64_t*>(amt_to_receive_buf.data());

    std::vector<Batch> batches;
    std::string        batch_offset_buf;
    std::string        size_buf;
    std::string        payload_buf;
    Batch              batch;
    size_t             bytes_read = 0;
    while (amt_to_receive != 0 && bytes_read != amt_to_receive) {
        bool susscess = conn_.read_bytes(batch_offset_buf, 8);  // offset
        if (!susscess) {
            std::cout << "Error: Failed to read offset" << std::endl;
            break;
        }
        /// @dev batch_offset_buf.data() return to pointer to memory location, batch_offset_buf is string class, not
        /// data itself reinterpret_cast<uint64_t*> cast to pointer to uint64_t
        /// * before reinterpret_cast dereference the pointer to get the value
        uint64_t batch_offset = *reinterpret_cast<uint64_t*>(batch_offset_buf.data());
        std::cout << "batch_offset = " << batch_offset << std::endl;

        susscess = conn_.read_bytes(size_buf, 4);  // size
        if (!susscess) {
            std::cout << "Error: Failed to read size" << std::endl;
            break;
        }
        /// @dev memcpy is safer way to convert bytes to type than reinterpret_cast
        /// because it handles endianness, alignment, and other issues
        /// we use both ways to learn
        uint32_t size;
        std::memcpy(&size, size_buf.data(), sizeof(uint32_t));
        std::cout << "size = " << size << std::endl;
        // TODO: size can be >= max_bytes, we should not return here.
        if (size > max_bytes) {
            std::cout << "Error: Size exceeds max_bytes" << std::endl;
            break;
        }

        susscess = conn_.read_bytes(payload_buf, size);
        if (!susscess) {
            std::cout << "Error: Failed to read payload" << std::endl;
            break;
        }
        deserialize_batch(payload_buf.data(), size, batch);
        batches.push_back(std::move(batch));
        bytes_read += 12 + size;
        std::cout << "bytes_read: " << bytes_read << std::endl;
    }
    offset += amt_to_receive;

    return batches;
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
