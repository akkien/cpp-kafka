#include "broker/topic_state.h"

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <iostream>
#include <mutex>

namespace kafka {

TopicState::TopicState(const std::string& data_dir, const std::string& name) : data_dir_(data_dir), topic_name_(name) {
    open_log_file();
    open_index_file();

    if (next_log_offset_ == 0) {
        indexes_.push_back({0, 0});
        bytes_since_last_index_ = 0;
        next_msg_offset_        = 0;
    } else {
        auto last_batch_position = indexes_.back().byte_offset;

        bytes_since_last_index_ = next_log_offset_ - last_batch_position;

        Batch last_batch = find_last_batch();
        next_msg_offset_ = last_batch.base_offset + last_batch.records_count;
    }
    std::cout << "Initial State: " << *this << std::endl;
    std::cout << "Indexes: " << std::endl;
    for (auto& [msg_offset, byte_position] : indexes_) {
        std::cout << "msg_offset: " << msg_offset << ", byte_position: " << byte_position << std::endl;
    }
}

TopicState::~TopicState() {
    if (log_fd_ >= 0) {
        ::close(log_fd_);
    }
    if (idx_fd_ >= 0) {
        ::close(idx_fd_);
    }
}

uint64_t TopicState::append(Batch& batch) {
    std::unique_lock lock(mu_);

    batch.base_offset = next_msg_offset_;
    next_msg_offset_ += batch.records_count;

    std::string payload = serialize_batch(batch);

    uint64_t cur_offset = next_log_offset_;
    uint32_t size       = static_cast<uint32_t>(payload.size());

    // Write header: offset (8 B) + size (4 B)
    ::lseek(log_fd_, static_cast<off_t>(cur_offset), SEEK_SET);
    ::write(log_fd_, &cur_offset, sizeof(cur_offset));
    ::write(log_fd_, &size, sizeof(size));
    ::write(log_fd_, payload.data(), size);

    bytes_since_last_index_ += kMessageHeaderSize + size;
    if (bytes_since_last_index_ > INDEX_INTERVAL) {
        IndexEntry entry = {batch.base_offset, cur_offset};
        ::lseek(idx_fd_, static_cast<off_t>(next_idx_offset_), SEEK_SET);
        ::write(idx_fd_, &entry, sizeof(IndexEntry));
        indexes_.push_back(entry);
        bytes_since_last_index_ = kMessageHeaderSize + size;
        next_idx_offset_ += sizeof(IndexEntry);
    }
    next_log_offset_ = cur_offset + kMessageHeaderSize + size;
    std::cout << "State after append: " << *this << std::endl;
    return cur_offset;
}

// TODO: handle max_bytes
// TODO: can remove lock here? this is just read
bool TopicState::send(int const& client_fd, uint64_t offset, uint32_t max_bytes) {
    std::shared_lock lock(mu_);

    // If offset is greater than or equal to the last record's offset, return 0 bytes
    if (offset >= next_msg_offset_) {
        off_t amt_to_send = 0;
        ::send(client_fd, &amt_to_send, sizeof(amt_to_send), 0);
        return true;
    }

    int  index_idx    = find_index_by_msg_offset(indexes_, offset);
    auto batch_offset = indexes_[index_idx].byte_offset;

    off_t end = next_log_offset_;
    std::cout << "[LogManager::send] end: " << end << std::endl;
    off_t amt_to_send = end - batch_offset;
    if (amt_to_send <= 0) {
        amt_to_send = 0;
        ::send(client_fd, &amt_to_send, sizeof(amt_to_send), 0);
        return false;
    }
    ::send(client_fd, &amt_to_send, sizeof(amt_to_send), 0);
    std::cout << "[LogManager::send] amt_to_send: " << amt_to_send << std::endl;
    int res = ::sendfile(log_fd_, client_fd, batch_offset, &amt_to_send, NULL, 0);
    return res == 0;
}

void TopicState::open_log_file() {
    // open or create log file
    std::string path = data_dir_ + "/" + topic_name_ + ".log";
    int         fd   = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
        throw std::runtime_error("open(" + path + "): " + std::strerror(errno));
    }

    auto end         = ::lseek(fd, 0, SEEK_END);
    log_fd_          = fd;
    next_log_offset_ = (end < 0) ? 0 : static_cast<uint64_t>(end);
}

void TopicState::open_index_file() {
    // open or create index file
    std::string idx_path = data_dir_ + "/" + topic_name_ + ".index";
    int         idx_fd   = ::open(idx_path.c_str(), O_RDWR | O_CREAT, 0644);
    if (idx_fd < 0) {
        throw std::runtime_error("open(" + idx_path + "): " + std::strerror(errno));
    }

    auto idx_end     = ::lseek(idx_fd, 0, SEEK_END);
    idx_fd_          = idx_fd;
    next_idx_offset_ = (idx_end < 0) ? 0 : static_cast<uint64_t>(idx_end);

    // TODO: real kafka use mmap() instead of read()
    // load indexes
    ::lseek(idx_fd_, 0, SEEK_SET);
    size_t num_entries = next_idx_offset_ / sizeof(IndexEntry);
    if (num_entries > 0) {
        indexes_.resize(num_entries);
        ::read(idx_fd_, indexes_.data(), next_idx_offset_);
    }
}

Batch TopicState::find_last_batch() {
    auto last_batch_position = indexes_.back().byte_offset;

    ::lseek(log_fd_, static_cast<off_t>(last_batch_position + 8), SEEK_SET);
    uint32_t batch_size;
    ::read(log_fd_, &batch_size, sizeof(batch_size));
    // keep reading until we reach last batch
    while (last_batch_position + 8 + 4 + batch_size < next_log_offset_) {
        last_batch_position += 8 + 4 + batch_size;
        ::lseek(log_fd_, static_cast<off_t>(last_batch_position + 8), SEEK_SET);
        ::read(log_fd_, &batch_size, sizeof(batch_size));
    }

    std::vector<char> batch_buf(batch_size);
    Batch             last_batch;
    ::lseek(log_fd_, static_cast<off_t>(last_batch_position + 8 + 4), SEEK_SET);
    ::read(log_fd_, batch_buf.data(), batch_size);
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

std::ostream& operator<<(std::ostream& os, const TopicState& s) {
    return os << "{\n"
              << "  topic_name: " << s.topic_name_ << ",\n"
              << "  log_fd: " << s.log_fd_ << ",\n"
              << "  next_log_offset: " << s.next_log_offset_ << ",\n"
              << "  idx_fd: " << s.idx_fd_ << ",\n"
              << "  next_idx_offset: " << s.next_idx_offset_ << ",\n"
              << "  next_msg_offset: " << s.next_msg_offset_ << ",\n"
              << "  bytes_since_last_index: " << s.bytes_since_last_index_ << "\n"
              << "}\n";
}

}  // namespace kafka