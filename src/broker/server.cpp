#include "broker/server.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <iostream>
#include <stdexcept>
#include <thread>

#include "broker/connection_handler.h"

namespace kafka {

Server::Server(uint16_t port) : port_(port) {}

Server::~Server() {
    shutdown();
}

void Server::run() {
    bind_and_listen();
    running_ = true;
    std::cout << "[broker] listening on port " << port_ << "\n";
    accept_loop();
}

void Server::shutdown() {
    running_ = false;
    if (listen_fd_ >= 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
}

void Server::bind_and_listen() {
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        throw std::runtime_error("socket(): " + std::string(std::strerror(errno)));
    }

    int opt = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(port_);

    if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        throw std::runtime_error("bind(): " + std::string(std::strerror(errno)));
    }

    if (::listen(listen_fd_, kBacklog) < 0) {
        throw std::runtime_error("listen(): " + std::string(std::strerror(errno)));
    }
}

void Server::accept_loop() {
    while (running_) {
        sockaddr_in client_addr{};
        socklen_t   len = sizeof(client_addr);
        int client_fd = ::accept(listen_fd_, reinterpret_cast<sockaddr*>(&client_addr), &len);
        if (client_fd < 0) {
            if (!running_) break;
            std::cerr << "[broker] accept error: " << std::strerror(errno) << "\n";
            continue;
        }

        // TODO(phase 4): replace with thread-pool dispatch
        std::thread([client_fd]() {
            ConnectionHandler handler(client_fd);
            handler.run();
        }).detach();
    }
}

}  // namespace kafka
