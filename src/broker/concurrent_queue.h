#pragma once

#include <condition_variable>
#include <mutex>
#include <optional>
#include <queue>

namespace kafka {

/**
 * @brief thread-safe queue for multiple producer and multiple consumer
 */
template <typename T>
class ConcurrentQueue {
public:
    ConcurrentQueue() = default;

    ConcurrentQueue(const ConcurrentQueue&)            = delete;
    ConcurrentQueue& operator=(const ConcurrentQueue&) = delete;

    /**
     * @brief push item into queue. notify one waiting thread
     */
    void push(T value) {
        {
            std::lock_guard<std::mutex> lock(mu_);
            queue_.push(std::move(value));
        }
        cv_.notify_one();
    }

    /**
     * @brief pop item from queue. blocks if queue is empty
     */
    T pop() {
        std::unique_lock<std::mutex> lock(mu_);
        cv_.wait(lock, [this] { return !queue_.empty(); });

        T value = std::move(queue_.front());
        queue_.pop();
        return value;
    }

    /**
     * @brief try pop item from queue. returns empty if queue is empty
     */
    std::optional<T> try_pop() {
        std::lock_guard<std::mutex> lock(mu_);
        if (queue_.empty()) {
            return std::nullopt;
        }

        T value = std::move(queue_.front());
        queue_.pop();
        return value;
    }

    bool empty() const {
        std::lock_guard<std::mutex> lock(mu_);
        return queue_.empty();
    }

    size_t size() const {
        std::lock_guard<std::mutex> lock(mu_);
        return queue_.size();
    }

private:
    mutable std::mutex      mu_;
    std::queue<T>           queue_;
    std::condition_variable cv_;
};

}  // namespace kafka
