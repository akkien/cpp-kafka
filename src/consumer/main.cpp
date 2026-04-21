#include <unistd.h>

#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>

#include "client/client.h"
#include "common/types.h"

static void usage(const char* prog) {
    std::cerr << "Usage: " << prog << " [--host HOST] [--port PORT] <topic> <offset> [max_bytes]\n";
}

int main(int argc, char* argv[]) {
    std::string host = "127.0.0.1";
    uint16_t    port = kafka::kDefaultPort;
    std::string topic;
    uint64_t    offset      = 0;
    uint32_t    max_bytes   = 4096;
    bool        have_offset = false;

    int i = 1;
    while (i < argc) {
        std::string arg = argv[i];
        if (arg == "--host" && i + 1 < argc) {
            host = argv[++i];
        } else if (arg == "--port" && i + 1 < argc) {
            port = static_cast<uint16_t>(std::atoi(argv[++i]));
        } else if (topic.empty()) {
            topic = arg;
        } else if (!have_offset) {
            offset      = std::stoull(arg);
            have_offset = true;
        } else {
            max_bytes = static_cast<uint32_t>(std::stoul(arg));
        }
        ++i;
    }

    if (topic.empty() || !have_offset) {
        usage(argv[0]);
        return 1;
    }

    kafka::Client client(host, port);
    if (!client.connect()) {
        std::cerr << "[consumer] failed to connect to " << host << ":" << port << "\n";
        return 1;
    }

    while (true) {
        auto start_offset = offset;
        auto batches      = client.consume(topic, offset, max_bytes);
        for (const auto& batch : batches) {
            // process each message
            for (const auto& rec : batch.records) {
                int rec_offset = batch.base_offset + rec.offset_delta;
                if (rec_offset < start_offset) {
                    continue;
                }
                std::cout << "offset = " << rec_offset << " value = " << rec.value << "\n";
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    return 0;
}
