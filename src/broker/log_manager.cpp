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
        if (state.log_fd >= 0)
            ::close(state.log_fd);
        if (state.idx_fd >= 0)
            ::close(state.idx_fd);
    }
}

// ---------------------------------------------------------------------------
TopicState& LogManager::get_or_create(const std::string& topic) {
    // Must be called with mu_ held.
    auto it = topics_.find(topic);
    if (it != topics_.end())
        return it->second;

    TopicState state;
    open_topic_logs(topic, state);
    open_topic_index(topic, state);

    if (state.next_log_offset == 0) {
        state.indexes.push_back({0, 0});
        state.bytes_since_last_index_entry = 0;
    } else {
        state.bytes_since_last_index_entry =
            state.next_log_offset - state.indexes[state.indexes.size() - 1].byte_offset;
    }

    auto [inserted, _] = topics_.emplace(topic, state);
    return inserted->second;
}

// ---------------------------------------------------------------------------
uint64_t LogManager::append(const std::string& topic, Batch& batch) {
    std::string payload = serialize_batch(batch);

    std::lock_guard lock(mu_);
    auto&           state = get_or_create(topic);

    uint64_t offset = state.next_log_offset;
    uint32_t size   = static_cast<uint32_t>(payload.size());

    // Write header: offset (8 B) + size (4 B)
    ::lseek(state.log_fd, static_cast<off_t>(offset), SEEK_SET);
    ::write(state.log_fd, &offset, sizeof(offset));
    ::write(state.log_fd, &size, sizeof(size));
    ::write(state.log_fd, payload.data(), size);

    state.bytes_since_last_index_entry += kMessageHeaderSize + size;
    if (state.bytes_since_last_index_entry > INDEX_INTERVAL) {
        IndexEntry entry = {batch.base_offset, state.next_log_offset};
        ::lseek(state.idx_fd, static_cast<off_t>(state.next_idx_offset), SEEK_SET);
        ::write(state.idx_fd, &entry, sizeof(IndexEntry));
        state.indexes.push_back(entry);
        state.bytes_since_last_index_entry = 0;
        state.next_idx_offset += sizeof(IndexEntry);
    }
    state.next_log_offset = offset + kMessageHeaderSize + size;
    return offset;
}

// TODO: handle max_bytes
bool LogManager::send(int const& client_fd, const std::string& topic, uint64_t offset, uint32_t max_bytes) {
    std::lock_guard lock(mu_);
    auto            it = topics_.find(topic);
    if (it == topics_.end()) {
        std::cout << "[LogManager::send] topic not found: " << topic << std::endl;
        // Auto-create empty topic (consistent with error-handling strategy).
        get_or_create(topic);
        off_t amt_to_send = 0;
        ::send(client_fd, &amt_to_send, sizeof(amt_to_send), 0);
        return {};
    }
    std::cout << "[LogManager::send] topic: " << topic << ", offset: " << offset << ", max_bytes: " << max_bytes
              << std::endl;
    auto& state = it->second;
    off_t end   = ::lseek(state.log_fd, 0, SEEK_END);
    std::cout << "[LogManager::send] end: " << end << std::endl;
    off_t amt_to_send = end - offset;
    if (amt_to_send <= 0) {
        amt_to_send = 0;
        ::send(client_fd, &amt_to_send, sizeof(amt_to_send), 0);
        return false;
    }
    ::send(client_fd, &amt_to_send, sizeof(amt_to_send), 0);
    std::cout << "[LogManager::send] amt_to_send: " << amt_to_send << std::endl;
    int res = ::sendfile(state.log_fd, client_fd, offset, &amt_to_send, NULL, 0);
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

// ---------------------------------------------------------------------------
void LogManager::open_topic_logs(const std::string& topic, TopicState& state) {
    // open or create log file
    std::string path = data_dir_ + "/" + topic + ".log";
    int         fd   = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
        throw std::runtime_error("open(" + path + "): " + std::strerror(errno));
    }

    auto end              = ::lseek(fd, 0, SEEK_END);
    state.log_fd          = fd;
    state.next_log_offset = (end < 0) ? 0 : static_cast<uint64_t>(end);
}

void LogManager::open_topic_index(const std::string& topic, TopicState& state) {
    // open or create index file
    std::string idx_path = data_dir_ + "/" + topic + ".index";
    int         idx_fd   = ::open(idx_path.c_str(), O_RDWR | O_CREAT, 0644);
    if (idx_fd < 0) {
        throw std::runtime_error("open(" + idx_path + "): " + std::strerror(errno));
    }

    auto idx_end          = ::lseek(idx_fd, 0, SEEK_END);
    state.idx_fd          = idx_fd;
    state.next_idx_offset = (idx_end < 0) ? 0 : static_cast<uint64_t>(idx_end);

    // TODO: real kafka use mmap() instead of read()
    // load indexes
    // SEEK_SET: offset from the beginning of the file
    ::lseek(state.idx_fd, 0, SEEK_SET);
    size_t num_entries = state.next_idx_offset / sizeof(IndexEntry);
    if (num_entries > 0) {
        state.indexes.resize(num_entries);
        ::read(state.idx_fd, state.indexes.data(), state.next_idx_offset);
    }
}

}  // namespace kafka
