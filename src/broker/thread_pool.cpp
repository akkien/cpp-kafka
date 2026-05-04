#include "broker/thread_pool.h"

#include <functional>
#include <thread>

#include "broker/request_handler.h"

namespace kafka {

ThreadPool::ThreadPool(size_t num_threads) {
    for (size_t i = 0; i < num_threads; ++i) {
        workers.emplace_back([this]() {
            while (true) {
                RequestItem req_item = queue_.pop();

                if (req_item.client_fd == -1) {
                    break;
                }

                RequestHandler handler(req_item.client_fd);
                handler.handle_request(req_item.req_type, req_item.request);
            }
        });
    }
}

void ThreadPool::enqueue(RequestItem req_item) {
    queue_.push(std::move(req_item));
}

ThreadPool::~ThreadPool() {
    for (size_t i = 0; i < workers.size(); ++i) {
        enqueue({-1, {}, {}});
    }

    for (std::thread &worker : workers) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

}  // namespace kafka