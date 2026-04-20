#include "broker/log_manager.h"

#include <fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstring>
#include <filesystem>
#include <iostream>
#include <stdexcept>

namespace fs = std::filesystem;
namespace kafka {

LogManager& LogManager::instance() {
    static LogManager inst;
    return inst;
}

LogManager::LogManager() {
    // TODO: init once and pass to every handler, not create every time
    fs::create_directories(data_dir_);
    try {
        for (const auto& entry : fs::directory_iterator(data_dir_)) {
            get_or_create(entry.path().stem().string());
        }
    } catch (const fs::filesystem_error& e) {
        std::cerr << "Error: " << e.what() << "\n";
    }
}

LogManager::~LogManager() {
    for (auto& [name, state] : topics_) {
        if (state.fd >= 0)
            ::close(state.fd);
    }
}

// ---------------------------------------------------------------------------
TopicState& LogManager::get_or_create(const std::string& topic) {
    // Must be called with mu_ held.
    auto it = topics_.find(topic);
    if (it != topics_.end())
        return it->second;

    std::string path = data_dir_ + "/" + topic + ".log";
    int         fd   = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
        throw std::runtime_error("open(" + path + "): " + std::strerror(errno));
    }

    // Seek to end to determine next_offset.
    auto       end = ::lseek(fd, 0, SEEK_END);
    TopicState state;
    state.fd          = fd;
    state.next_offset = (end < 0) ? 0 : static_cast<uint64_t>(end);

    auto [inserted, _] = topics_.emplace(topic, state);
    return inserted->second;
}

// ---------------------------------------------------------------------------
uint64_t LogManager::append(const std::string& topic, const std::string& payload) {
    std::lock_guard lock(mu_);
    auto&           state = get_or_create(topic);

    uint64_t offset = state.next_offset;
    uint32_t size   = static_cast<uint32_t>(payload.size());

    // Write header: offset (8 B) + size (4 B)
    ::lseek(state.fd, static_cast<off_t>(offset), SEEK_SET);
    ::write(state.fd, &offset, sizeof(offset));
    ::write(state.fd, &size, sizeof(size));
    ::write(state.fd, payload.data(), size);

    state.next_offset = offset + kMessageHeaderSize + size;
    return offset;
}

// ---------------------------------------------------------------------------
std::vector<Message> LogManager::read(const std::string& topic, uint64_t offset, uint32_t max_bytes) {
    std::lock_guard lock(mu_);
    auto            it = topics_.find(topic);
    if (it == topics_.end()) {
        // Auto-create empty topic (consistent with error-handling strategy).
        get_or_create(topic);
        return {};
    }

    auto&                state = it->second;
    std::vector<Message> messages;
    uint32_t             bytes_read = 0;

    ::lseek(state.fd, static_cast<off_t>(offset), SEEK_SET);

    while (bytes_read < max_bytes) {
        Message msg;
        // Read header
        ssize_t n = ::read(state.fd, &msg.offset, sizeof(msg.offset));
        if (n <= 0)
            break;
        n = ::read(state.fd, &msg.size, sizeof(msg.size));
        if (n <= 0)
            break;

        // Read payload
        msg.payload.resize(msg.size);
        n = ::read(state.fd, msg.payload.data(), msg.size);
        if (n <= 0)
            break;

        bytes_read += kMessageHeaderSize + msg.size;
        messages.push_back(std::move(msg));
    }

    return messages;
}

// TODO: handle max_bytes
bool LogManager::send(int const& client_fd, const std::string& topic, uint64_t offset, uint32_t max_bytes) {
    std::lock_guard lock(mu_);
    auto            it = topics_.find(topic);
    if (it == topics_.end()) {
        std::cout << "[LogManager::send] topic not found: " << topic << std::endl;
        // Auto-create empty topic (consistent with error-handling strategy).
        get_or_create(topic);
        return {};
    }
    std::cout << "[LogManager::send] topic: " << topic << ", offset: " << offset << ", max_bytes: " << max_bytes
              << std::endl;
    auto& state = it->second;
    off_t end   = ::lseek(state.fd, 0, SEEK_END);
    std::cout << "[LogManager::send] end: " << end << std::endl;
    off_t amt_to_send = end - offset;
    if (amt_to_send <= 0) {
        amt_to_send = 0;
        ::send(client_fd, &amt_to_send, sizeof(amt_to_send), 0);
        return false;
    }
    ::send(client_fd, &amt_to_send, sizeof(amt_to_send), 0);
    std::cout << "[LogManager::send] amt_to_send: " << amt_to_send << std::endl;
    int res = ::sendfile(state.fd, client_fd, offset, &amt_to_send, NULL, 0);
    return res == 0;
}

// ---------------------------------------------------------------------------
std::vector<std::string> LogManager::list_topics() const {
    std::lock_guard          lock(mu_);
    std::vector<std::string> names;
    names.reserve(topics_.size());
    for (const auto& [name, _] : topics_) {
        names.push_back(name);
    }
    return names;
}

}  // namespace kafka
