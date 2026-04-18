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

    // auto messages = client.consume(topic, offset, max_bytes);
    // for (const auto& msg : messages) {
    //     std::cout << "[offset=" << msg.offset << " size=" << msg.size << "] "
    //               << msg.payload << "\n";
    // }
    while (true) {
        auto messages = client.consume(topic, offset, max_bytes);
        for (const auto& msg : messages) {
            // process each message
            std::cout << "[offset=" << msg.offset << " size=" << msg.size << "] " << msg.payload << "\n";

            // advance offset past this message
            // 8 bytes (offset) + 4 bytes (size) + payload size
            offset = msg.offset + 12 + msg.size;

            if (messages.empty()) {
                std::cout << "[consumer] no messages\n";
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    return 0;
}
