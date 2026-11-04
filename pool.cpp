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

void ThreadPool::growTo(size_t count)
{
  std::lock_guard< std::mutex > lock(mutex_);
  if (stopped_) {
    throw std::runtime_error("Pool is stopped");
  }
  checkNotWorkerThread();

  if (count <= threads_.size()) {
    return;
  }

  size_t old_size = threads_.size();
  workers_stop_.resize(count, false);
  threads_.reserve(count);

  for (size_t i = old_size; i < count; ++i) {
    threads_.emplace_back(&ThreadPool::execute, this, i);
  }
}

void ThreadPool::shrinkTo(size_t count)
{
  checkNotWorkerThread();

  if (count == 0) {
    count = 1;
  }

  size_t old_size = 0;
  {
    std::lock_guard< std::mutex > lock(mutex_);
    if (stopped_) {
      throw std::runtime_error("Pool is stopped");
    }
    old_size = threads_.size();

    if (count >= old_size) {
      return;
    }

    for (size_t i = count; i < old_size; ++i) {
      workers_stop_[i] = true;
    }
  }

  condition_.notify_all();

  for (size_t i = count; i < old_size; ++i) {
    if (threads_[i].joinable()) {
      threads_[i].join();
    }
  }

  {
    std::lock_guard< std::mutex > lock(mutex_);
    threads_.resize(count);
    workers_stop_.resize(count);
  }
}

void ThreadPool::resize(size_t count)
{
  if (count == 0) {
    count = 1;
  }

  if (count > size()) {
    growTo(count);
  } else {
    shrinkTo(count);
  }
}

size_t ThreadPool::size() const
{
  std::lock_guard< std::mutex > lock(mutex_);
  return threads_.size();
}

ThreadPool::~ThreadPool() noexcept
{
  try {
    stop();
  } catch (...) {
  }
}

void ThreadPool::execute(size_t id)
{
  bool finished = false;

  while (!finished) {
    Task task;

    {
      std::unique_lock< std::mutex > lock(mutex_);

      condition_.wait(lock,
          [this, id]()
          {
            return stopped_ || workers_stop_[id] || (!paused_ && !tasks_.empty());
          });

      finished = workers_stop_[id] || (stopped_ && (cancel_pending_ || tasks_.empty()));

      if (!finished) {
        task = std::move(tasks_.front());
        tasks_.pop();
        ++active_tasks_;
      }
    }

    if (finished) {
      break;
    }

    try {
      task();
    } catch (...) {
    }

    {
      std::lock_guard< std::mutex > lock(mutex_);
      --active_tasks_;

      if (tasks_.empty() && active_tasks_ == 0) {
        idle_condition_.notify_all();
      }
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
