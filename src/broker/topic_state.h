#include <mutex>
#include <shared_mutex>
#include <vector>

#include "common/batch.h"
#include "common/types.h"

namespace kafka {

struct IndexEntry {
    /// @dev real msg_offset use uint32_t because the value is relatively to the first message offset of the file
    /// we save the absolute offset to keep it simple at the moment
    int64_t  msg_offset;
    uint64_t byte_offset;
};

int find_index_by_msg_offset(const std::vector<IndexEntry>& indexes, int64_t msg_offset);

/// @dev we cannot use singleton for TopicState, because we have multiple topics
/// to keep only one instance for each topic, we use unique_ptr in LogManager
class TopicState {
public:
    TopicState(const std::string& data_dir, const std::string& topic_name);
    ~TopicState();
    // @dev prevent copy constructor and assignment operator
    TopicState(const TopicState&)            = delete;
    TopicState& operator=(const TopicState&) = delete;

    uint64_t append(Batch& batch);
    bool     send(int const& client_fd, uint64_t offset, uint32_t max_bytes);

    friend std::ostream& operator<<(std::ostream& os, const TopicState& s);

private:
    std::string               data_dir_;
    std::string               topic_name_;
    mutable std::shared_mutex mu_;
    int                       log_fd_{-1};          // File descriptor for the .log file
    uint64_t                  next_log_offset_{0};  // Next available byte offset
    int                       idx_fd_{-1};          // File descriptor for the .idx file
    uint64_t                  next_idx_offset_{0};  // Next available byte offset of index file
    std::vector<IndexEntry>   indexes_;
    uint64_t                  next_msg_offset_{0};
    uint16_t                  bytes_since_last_index_{0};

    void  open_log_file();
    void  open_index_file();
    Batch find_last_batch();
};

}  // namespace kafka