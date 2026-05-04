#pragma once

#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include "broker/concurrent_queue.h"
#include "broker/types.h"

namespace kafka {
class ThreadPool {
public:
    ThreadPool(size_t num_threads);
    void enqueue(RequestItem req_item);
    ~ThreadPool();

private:
    std::vector<std::thread>            workers;
    kafka::ConcurrentQueue<RequestItem> queue_;
};
}  // namespace kafka
