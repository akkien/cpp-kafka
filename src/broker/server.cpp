#include "broker/server.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/event.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <iostream>
#include <stdexcept>
#include <thread>

#include "broker/connection_handler.h"
#include "broker/log_manager.h"

namespace kafka {

const int num_thread = std::thread::hardware_concurrency();

/// @dev initializer list is more efficient than assignment in the body
/// because it avoids default-constructing then assigning.
/// especially constant members could only be initialized in initializer list.
Server::Server(uint16_t port) : port_(port), thread_pool_(num_thread) {}

Server::~Server() {
    shutdown();
}

void Server::run() {
    LogManager::instance();  // init log manager
    bind_and_listen();
    running_ = true;
    std::cout << "[broker] listening on port " << port_ << "\n";
    accept_loop();
}

void Server::shutdown() {
    running_ = false;
    if (server_fd_ >= 0) {
        /// @dev by default, compiler seek for kafka namespace first => close == kafka::close
        /// so we need to use ::close to refer to global namespace.
        ::close(server_fd_);
        server_fd_ = -1;
    }
}

static void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

/// TODO: non-blocking broker
void Server::bind_and_listen() {
    server_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ < 0) {
        throw std::runtime_error("socket(): " + std::string(std::strerror(errno)));
    }

    int opt = 1;
    ::setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(port_);

    if (::bind(server_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        throw std::runtime_error("bind(): " + std::string(std::strerror(errno)));
    }

    set_nonblocking(server_fd_);

    if (::listen(server_fd_, kBacklog) < 0) {
        throw std::runtime_error("listen(): " + std::string(std::strerror(errno)));
    }

    kq_ = kqueue();
    // Register server_fd to watch for read events (new incoming connections).
    // EV_SET fills a kevent struct:
    //   ident  = fd to watch
    //   filter = EVFILT_READ: fire when fd is readable
    //   flags  = EV_ADD: add this event to the queue
    struct kevent accept_event_trigger;
    EV_SET(&accept_event_trigger, server_fd_, EVFILT_READ, EV_ADD, 0, 0, nullptr);
    kevent(kq_, &accept_event_trigger, 1, nullptr, 0, nullptr);
}

void Server::accept_loop() {
    struct kevent events[64];

    while (running_) {
        int no_event = kevent(kq_, nullptr, 0, events, 64, nullptr);  // process 64 events each time
        for (int i = 0; i < no_event; ++i) {
            int fd = (int)events[i].ident;
            // event came from server_fd → new client connection is ready to be accepted
            if (fd == server_fd_) {
                int client = accept(server_fd_, nullptr, nullptr);
                if (client < 0)
                    continue;

                set_nonblocking(client);

                // Register the new client fd to watch for incoming data
                struct kevent client_event_trigger;
                EV_SET(&client_event_trigger, client, EVFILT_READ, EV_ADD, 0, 0, nullptr);
                kevent(kq_, &client_event_trigger, 1, nullptr, 0, nullptr);
            } else {
                ConnectionHandler handler(fd, thread_pool_);
                handler.run();
            }
        }
    }
}

}  // namespace kafka
