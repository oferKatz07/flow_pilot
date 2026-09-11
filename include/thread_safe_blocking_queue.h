// thread_safe_blocking_queue.h

#pragma once

#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <queue>


template<typename T>
class ThreadSafeBlockingQueue {
public:
    explicit ThreadSafeBlockingQueue(std::size_t capacity)
        : capacity_(capacity) {}

    ThreadSafeBlockingQueue(const ThreadSafeBlockingQueue&) = delete; // Don't allow for a copy constructor
    ThreadSafeBlockingQueue& operator=(const ThreadSafeBlockingQueue&) = delete; // Don't allow assignment operator

    // Stop accepting new items, allow draining
    void close() {
        {
            std::lock_guard lock(mtx_);
            closed_ = true;
        }
        not_empty_.notify_all();
        not_full_.notify_all();
    }

    bool push(T value) {
        std::unique_lock lock(mtx_);
        not_full_.wait(lock, [this]() {
            return closed_ || queue_.size() < capacity_;
        });

        if (closed_) {
            return false;
        }

        queue_.push(std::move(value));
        not_empty_.notify_one();
        return true;
    }

    bool pop(T& out) {
        std::unique_lock lock(mtx_);
        not_empty_.wait(lock, [this]() {
            return closed_ || !queue_.empty();
        });

        if (queue_.empty()) {
            return false;
        }

        out = std::move(queue_.front());
        queue_.pop();
        not_full_.notify_one();
        return true;
    }

    std::size_t capacity() const {
        return capacity_;
    }

    bool closed() const {
        std::lock_guard lock(mtx_);
        return closed_;
    }

private:
    const std::size_t capacity_;
    std::queue<T> queue_;
    mutable std::mutex mtx_;
    std::condition_variable not_empty_;
    std::condition_variable not_full_;
    bool closed_{false};
};
