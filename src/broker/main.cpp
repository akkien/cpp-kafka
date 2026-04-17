#include <cstdlib>
#include <iostream>
// @dev include path is /src (see CMakeLists.txt), so /src is the base path for all includes
#include "broker/server.h"
#include "common/types.h"

int main(int argc, char* argv[]) {
    uint16_t port = kafka::kDefaultPort;
    if (argc > 1) {
        port = static_cast<uint16_t>(std::atoi(argv[1]));
    }

    try {
        kafka::Server server(port);
        server.run();
    } catch (const std::exception& e) {
        std::cerr << "[broker] fatal: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
