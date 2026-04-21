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
        state.next_msg_offset              = 0;
    } else {
        auto last_batch_position = state.indexes.back().byte_offset;

        state.bytes_since_last_index_entry = state.next_log_offset - last_batch_position;

        Batch last_batch      = find_last_batch(state);
        state.next_msg_offset = last_batch.base_offset + last_batch.records_count;
    }
    std::cout << "Initial State: " << state << std::endl;
    std::cout << "Indexes: " << std::endl;
    for (auto& [msg_offset, byte_position] : state.indexes) {
        std::cout << "msg_offset: " << msg_offset << ", byte_position: " << byte_position << std::endl;
    }
    auto [inserted, _] = topics_.emplace(topic, state);
    return inserted->second;
}

// ---------------------------------------------------------------------------
uint64_t LogManager::append(const std::string& topic, Batch& batch) {
    std::lock_guard lock(mu_);
    auto&           state = get_or_create(topic);

    batch.base_offset = state.next_msg_offset;
    state.next_msg_offset += batch.records_count;

    std::string payload = serialize_batch(batch);

    uint64_t cur_offset = state.next_log_offset;
    uint32_t size       = static_cast<uint32_t>(payload.size());

    // Write header: offset (8 B) + size (4 B)
    ::lseek(state.log_fd, static_cast<off_t>(cur_offset), SEEK_SET);
    ::write(state.log_fd, &cur_offset, sizeof(cur_offset));
    ::write(state.log_fd, &size, sizeof(size));
    ::write(state.log_fd, payload.data(), size);

    state.bytes_since_last_index_entry += kMessageHeaderSize + size;
    if (state.bytes_since_last_index_entry > INDEX_INTERVAL) {
        IndexEntry entry = {batch.base_offset, cur_offset};
        ::lseek(state.idx_fd, static_cast<off_t>(state.next_idx_offset), SEEK_SET);
        ::write(state.idx_fd, &entry, sizeof(IndexEntry));
        state.indexes.push_back(entry);
        state.bytes_since_last_index_entry = kMessageHeaderSize + size;
        state.next_idx_offset += sizeof(IndexEntry);
    }
    state.next_log_offset = cur_offset + kMessageHeaderSize + size;
    std::cout << "State after append: " << state << std::endl;
    return cur_offset;
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
    auto& state        = it->second;
    int   index_idx    = find_index_by_msg_offset(state.indexes, offset);
    auto  batch_offset = state.indexes[index_idx].byte_offset;

    off_t end = ::lseek(state.log_fd, 0, SEEK_END);
    std::cout << "[LogManager::send] end: " << end << std::endl;
    off_t amt_to_send = end - batch_offset;
    if (amt_to_send <= 0) {
        amt_to_send = 0;
        ::send(client_fd, &amt_to_send, sizeof(amt_to_send), 0);
        return false;
    }
    ::send(client_fd, &amt_to_send, sizeof(amt_to_send), 0);
    std::cout << "[LogManager::send] amt_to_send: " << amt_to_send << std::endl;
    int res = ::sendfile(state.log_fd, client_fd, batch_offset, &amt_to_send, NULL, 0);
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

Batch LogManager::find_last_batch(const TopicState& state) {
    auto last_batch_position = state.indexes.back().byte_offset;

    ::lseek(state.log_fd, static_cast<off_t>(last_batch_position + 8), SEEK_SET);
    uint32_t batch_size;
    ::read(state.log_fd, &batch_size, sizeof(batch_size));
    // keep reading until we reach last batch
    while (last_batch_position + 8 + 4 + batch_size < state.next_log_offset) {
        last_batch_position += 8 + 4 + batch_size;
        ::lseek(state.log_fd, static_cast<off_t>(last_batch_position + 8), SEEK_SET);
        ::read(state.log_fd, &batch_size, sizeof(batch_size));
    }

    std::vector<char> batch_buf(batch_size);
    Batch             last_batch;
    ::lseek(state.log_fd, static_cast<off_t>(last_batch_position + 8 + 4), SEEK_SET);
    ::read(state.log_fd, batch_buf.data(), batch_size);
    deserialize_batch(batch_buf.data(), batch_size, last_batch);
    return last_batch;
}

int find_index_by_msg_offset(const std::vector<IndexEntry>& indexes, int64_t msg_offset) {
    if (indexes.empty())
        return 0;

    int l   = 0;
    int r   = indexes.size() - 1;
    int ans = 0;

    while (l <= r) {
        int mid = l + (r - l) / 2;
        if (indexes[mid].msg_offset <= msg_offset) {
            ans = mid;
            l   = mid + 1;
        } else {
            r = mid - 1;
        }
    }
    return ans;
}

}  // namespace kafka
