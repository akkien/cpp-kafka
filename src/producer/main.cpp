#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>

#include "client/client.h"
#include "common/types.h"

static void usage(const char* prog) {
    std::cerr << "Usage: " << prog << " [--host HOST] [--port PORT] <topic> <message>\n";
}

int main(int argc, char* argv[]) {
    std::string              host = "127.0.0.1";
    uint16_t                 port = kafka::kDefaultPort;
    std::string              key  = "";
    std::string              topic;
    std::string              raw_messages;
    std::vector<std::string> messages;

    // Simple arg parsing
    int i = 1;
    while (i < argc) {
        std::string arg = argv[i];
        if (arg == "--host" && i + 1 < argc) {
            host = argv[++i];
        } else if (arg == "--key" && i + 1 < argc) {
            key = argv[++i];
        } else if (arg == "--port" && i + 1 < argc) {
            port = static_cast<uint16_t>(std::atoi(argv[++i]));
        } else if (topic.empty()) {
            topic = arg;
        } else {
            raw_messages = arg;
        }
        ++i;
    }

    if (topic.empty() || raw_messages.empty()) {
        usage(argv[0]);
        return 1;
    }
    std::stringstream ss(raw_messages);
    std::string       msg;
    while (std::getline(ss, msg, ':')) {
        messages.push_back(msg);
    }

    kafka::Client client(host, port);
    if (!client.connect()) {
        std::cerr << "[producer] failed to connect to " << host << ":" << port << "\n";
        return 1;
    }

    int64_t offset = client.produce(topic, key, messages);
    if (offset < 0) {
        std::cerr << "[producer] produce failed\n";
        return 1;
    }

    std::cout << "[producer] ok offset=" << offset << "\n";
    return 0;
}
