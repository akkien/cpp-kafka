#include "broker/topic_state.h"
#include "common/message.h"
#include "common/serialize.h"

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
    uint32_t    size    = static_cast<uint32_t>(payload.size());

    uint64_t cur_offset = next_log_offset_;
    ::lseek(log_fd_, static_cast<off_t>(cur_offset), SEEK_SET);
    ::write(log_fd_, payload.data(), size);

    bytes_since_last_index_ += size;
    if (bytes_since_last_index_ > INDEX_INTERVAL) {
        IndexEntry entry = {batch.base_offset, cur_offset};
        ::lseek(idx_fd_, static_cast<off_t>(next_idx_offset_), SEEK_SET);
        ::write(idx_fd_, &entry, sizeof(IndexEntry));
        indexes_.push_back(entry);
        bytes_since_last_index_ = size;
        next_idx_offset_ += sizeof(IndexEntry);
    }
    next_log_offset_ = cur_offset + size;
    std::cout << "State after append: " << *this << std::endl;
    return cur_offset;
}

// TODO: handle max_bytes
bool TopicState::send(int const& client_fd, int32_t correlation_id, uint64_t offset, [[maybe_unused]] uint32_t max_bytes) {
    std::shared_lock lock(mu_);

    off_t batch_offset = 0;
    off_t amt_to_send = 0;
    int16_t error_code = 0;

    if (offset >= next_msg_offset_) {
        error_code = 0; // Success but empty
        amt_to_send = 0;
    } else {
        int index_idx = find_index_by_msg_offset(indexes_, offset);
        batch_offset = indexes_[index_idx].byte_offset;
        amt_to_send = next_log_offset_ - batch_offset;
        if (amt_to_send < 0) amt_to_send = 0;
    }

    // Build FetchResponse header
    FetchResponse res;
    res.correlation_id = correlation_id;
    res.throttle_time_ms = 0;

    TopicFetchResponse t_res;
    t_res.name = topic_name_;
    
    PartitionFetchResponse p_res;
    p_res.partition_index = 0;
    p_res.error_code = error_code;
    p_res.high_watermark = static_cast<int64_t>(next_msg_offset_);
    p_res.last_stable_offset = static_cast<int64_t>(next_msg_offset_);
    p_res.log_start_offset = 0;
    
    t_res.partitions.push_back(p_res);
    res.topics.push_back(t_res);

    std::string header_buf = serialize_fetch_response_header(res);
    
    // Total size = header_buf + 4 (record_set_size) + amt_to_send
    int32_t total_size = static_cast<int32_t>(header_buf.size() + 4 + amt_to_send);
    
    std::string final_header;
    encode_int32(final_header, total_size);
    final_header += header_buf;
    encode_int32(final_header, static_cast<int32_t>(amt_to_send));

    // 1. Send header
    if (::send(client_fd, final_header.data(), final_header.size(), 0) < 0) return false;

    // 2. Send file data if any
    if (amt_to_send > 0) {
        off_t sent = amt_to_send;
        int res_sf = ::sendfile(log_fd_, client_fd, batch_offset, &sent, NULL, 0);
        return res_sf == 0;
    }

    return true;
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