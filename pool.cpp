#include "pool.hpp"
#include <mutex>
#include <stdexcept>

ThreadPool::ThreadPool(size_t threads):
  stopped_(false),
  paused_(false),
  cancel_pending_(false),
  active_tasks_(0)
{
  if (threads == 0) {
    threads = 1;
  }

  workers_stop_.resize(threads, false);

  for (size_t i = 0; i < threads; ++i) {
    threads_.emplace_back(&ThreadPool::execute, this, i);
  }
}
void ThreadPool::stop(bool cancel_pending)
{
  checkNotWorkerThread();
  {
    std::lock_guard< std::mutex > lock(mutex_);
    if (stopped_ && threads_.empty()) {
      return;
    }

    stopped_ = true;
    paused_ = false;
    cancel_pending_ = cancel_pending;

    if (cancel_pending_) {
      std::queue< Task > stub;
      stub.swap(tasks_);
    }
  }

  condition_.notify_all();

  for (size_t i = 0; i < threads_.size(); ++i) {
    if (threads_[i].joinable()) {
      threads_[i].join();
    }
  }

  {
    std::lock_guard< std::mutex > lock(mutex_);
    threads_.clear();
    workers_stop_.clear();
    if (tasks_.empty() && active_tasks_ == 0) {
      idle_condition_.notify_all();
    }
  }
}
void ThreadPool::checkNotWorkerThread() const
{
  std::thread::id current = std::this_thread::get_id();
  for (const auto &thread : threads_) {
    if (thread.joinable() && thread.get_id() == current) {
      throw std::logic_error("This operation cannot be called from a pool task");
    }
  }
}
