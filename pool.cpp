#include "pool.hpp"

#include <algorithm>
#include <stdexcept>

ThreadPool::ThreadPool(size_t threads)
{
  if (threads == 0) {
    threads = 1;
  }

  workers_.reserve(threads);
  threads_.reserve(threads);

  for (size_t i = 0; i < threads; ++i) {
    workers_.emplace_back(std::make_unique< Worker >());
    threads_.emplace_back(&ThreadPool::workerLoop, this, i, workers_[i].get());
  }
}

ThreadPool::~ThreadPool() noexcept
{
  try {
    stop();
  } catch (...) {
  }
}

size_t ThreadPool::pickWorkerLocked() const
{
  size_t best = 0;
  bool found = false;
  for (size_t i = 0; i < workers_.size(); ++i) {
    if (workers_[i]->stop) {
      continue;
    }
    if (!found) {
      best = i;
      found = true;
      continue;
    }
    size_t load = workers_[i]->task_count.load(std::memory_order_relaxed);
    size_t best_load = workers_[best]->task_count.load(std::memory_order_relaxed);
    if (load < best_load) {
      best = i;
    }
  }
  return found ? best : 0;
}

bool ThreadPool::tryPopLocal(Worker* self, std::vector< Task >& batch)
{
  std::lock_guard< std::mutex > lk(self->mtx);
  if (self->tasks.empty()) {
    return false;
  }

  size_t n = std::min(kBatchSize, self->tasks.size());
  for (size_t i = 0; i < n; ++i) {
    batch.push_back(std::move(self->tasks.back()));
    self->tasks.pop_back();
  }

  active_tasks_.fetch_add(n, std::memory_order_relaxed);
  self->task_count.fetch_sub(n, std::memory_order_relaxed);
  total_pending_tasks_.fetch_sub(n, std::memory_order_relaxed);
  return true;
}

bool ThreadPool::trySteal(size_t id, std::vector< Task >& batch)
{
  std::lock_guard< std::mutex > g(state_mutex_);
  const size_t n = workers_.size();
  if (n <= 1) {
    return false;
  }

  for (size_t k = 0; k < n - 1; ++k) {
    size_t victim_id = (id + 1 + k) % n;
    if (victim_id == id) {
      continue;
    }
    Worker& victim = *workers_[victim_id];
    std::lock_guard< std::mutex > lk(victim.mtx);
    if (victim.tasks.empty()) {
      continue;
    }

    size_t to_steal = std::max< size_t >(1, std::min(kBatchSize, victim.tasks.size() / 2));
    to_steal = std::min(to_steal, victim.tasks.size());

    for (size_t j = 0; j < to_steal; ++j) {
      batch.push_back(std::move(victim.tasks.front()));
      victim.tasks.pop_front();
    }
    active_tasks_.fetch_add(to_steal, std::memory_order_relaxed);
    victim.task_count.fetch_sub(to_steal, std::memory_order_relaxed);
    total_pending_tasks_.fetch_sub(to_steal, std::memory_order_relaxed);
    return true;
  }
  return false;
}

void ThreadPool::executeBatch(std::vector< Task >& batch)
{
  const size_t n = batch.size();
  for (auto& t : batch) {
    try {
      t();
    } catch (...) {
    }
  }
  batch.clear();

  active_tasks_.fetch_sub(n, std::memory_order_relaxed);

  bool signal_idle = false;
  {
    std::lock_guard< std::mutex > g(state_mutex_);
    signal_idle = (total_pending_tasks_.load(std::memory_order_relaxed) == 0
        && active_tasks_.load(std::memory_order_relaxed) == 0);
  }
  if (signal_idle) {
    idle_condition_.notify_all();
  }
}

void ThreadPool::workerLoop(size_t id, Worker* self)
{
  std::vector< Task > batch;
  batch.reserve(kBatchSize);

  while (true) {
    {
      std::lock_guard< std::mutex > g(state_mutex_);
      if (self->stop) {
        break;
      }
      if (stopped_ && (cancel_pending_ || total_pending_tasks_.load(std::memory_order_relaxed) == 0)) {
        break;
      }
    }

    if (paused_.load(std::memory_order_acquire)) {
      std::unique_lock< std::mutex > lk(state_mutex_);
      condition_.wait(lk,
          [&]()
          {
            return !paused_.load() || stopped_ || self->stop;
          });
      continue;
    }

    batch.clear();
    if (tryPopLocal(self, batch) || trySteal(id, batch)) {
      executeBatch(batch);
      continue;
    }

    std::unique_lock< std::mutex > lk(state_mutex_);
    condition_.wait(lk,
        [&]()
        {
          if (stopped_ || self->stop) {
            return true;
          }
          if (paused_.load()) {
            return false;
          }
          return total_pending_tasks_.load(std::memory_order_relaxed) > 0;
        });
  }
}

void ThreadPool::waitIdle()
{
  checkNotWorkerThread();
  std::unique_lock< std::mutex > lock(state_mutex_);
  idle_condition_.wait(lock,
      [this]()
      {
        return total_pending_tasks_.load(std::memory_order_relaxed) == 0
            && active_tasks_.load(std::memory_order_relaxed) == 0;
      });
}

void ThreadPool::pause()
{
  checkNotWorkerThread();
  std::lock_guard< std::mutex > lock(state_mutex_);
  if (!stopped_) {
    paused_.store(true, std::memory_order_release);
  }
}

void ThreadPool::resume()
{
  {
    std::lock_guard< std::mutex > lock(state_mutex_);
    paused_.store(false, std::memory_order_release);
  }
  condition_.notify_all();
}

void ThreadPool::stop(bool cancel_pending)
{
  checkNotWorkerThread();
  std::lock_guard< std::mutex > mgmt(mgmt_mutex_);
  {
    std::lock_guard< std::mutex > lock(state_mutex_);
    if (stopped_ && threads_.empty()) {
      return;
    }
    stopped_ = true;
    paused_.store(false, std::memory_order_release);
    cancel_pending_ = cancel_pending;

    if (cancel_pending_) {
      for (auto& w : workers_) {
        std::lock_guard< std::mutex > wl(w->mtx);
        w->tasks.clear();
        w->task_count.store(0, std::memory_order_relaxed);
      }
      total_pending_tasks_.store(0, std::memory_order_relaxed);
    }
  }

  condition_.notify_all();

  for (auto& t : threads_) {
    if (t.joinable()) {
      t.join();
    }
  }

  {
    std::lock_guard< std::mutex > lock(state_mutex_);
    threads_.clear();
    workers_.clear();
  }
  idle_condition_.notify_all();
}

void ThreadPool::growTo(size_t count)
{
  checkNotWorkerThread();
  std::lock_guard< std::mutex > mgmt(mgmt_mutex_);

  std::lock_guard< std::mutex > g(state_mutex_);
  if (stopped_) {
    throw std::runtime_error("Pool is stopped");
  }
  if (count <= workers_.size()) {
    return;
  }

  size_t old_size = workers_.size();
  workers_.reserve(count);
  threads_.reserve(count);

  for (size_t i = old_size; i < count; ++i) {
    workers_.emplace_back(std::make_unique< Worker >());
    threads_.emplace_back(&ThreadPool::workerLoop, this, i, workers_[i].get());
  }
}

void ThreadPool::shrinkTo(size_t count)
{
  checkNotWorkerThread();
  if (count == 0) {
    count = 1;
  }

  std::lock_guard< std::mutex > mgmt(mgmt_mutex_);

  std::vector< std::thread > to_join;

  {
    std::lock_guard< std::mutex > lock(state_mutex_);
    if (stopped_) {
      throw std::runtime_error("Pool is stopped");
    }
    size_t old_size = workers_.size();
    if (count >= old_size) {
      return;
    }

    std::vector< Task > orphaned;
    for (size_t i = count; i < old_size; ++i) {
      Worker& w = *workers_[i];
      std::lock_guard< std::mutex > wl(w.mtx);
      while (!w.tasks.empty()) {
        orphaned.push_back(std::move(w.tasks.front()));
        w.tasks.pop_front();
      }
      w.task_count.store(0, std::memory_order_relaxed);
    }
    for (size_t j = 0; j < orphaned.size(); ++j) {
      size_t target = j % count;
      Worker& w = *workers_[target];
      std::lock_guard< std::mutex > wl(w.mtx);
      w.tasks.push_back(std::move(orphaned[j]));
      w.task_count.fetch_add(1, std::memory_order_relaxed);
    }

    for (size_t i = count; i < old_size; ++i) {
      workers_[i]->stop = true;
    }

    for (size_t i = count; i < old_size; ++i) {
      to_join.push_back(std::move(threads_[i]));
    }
    threads_.resize(count);
  }

  condition_.notify_all();

  for (auto& t : to_join) {
    if (t.joinable()) {
      t.join();
    }
  }

  {
    std::lock_guard< std::mutex > lock(state_mutex_);
    workers_.resize(count);
  }
  condition_.notify_all();
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
  std::lock_guard< std::mutex > lock(state_mutex_);
  return workers_.size();
}

void ThreadPool::checkNotWorkerThread() const
{
  std::thread::id current = std::this_thread::get_id();
  std::lock_guard< std::mutex > g(state_mutex_);
  for (const auto& t : threads_) {
    if (t.joinable() && t.get_id() == current) {
      throw std::logic_error("This operation cannot be called from a pool task");
    }
  }
}
