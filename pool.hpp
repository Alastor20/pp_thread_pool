#ifndef POOL_HPP
#define POOL_HPP

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

class ThreadPool {
public:
  using Task = std::function< void(void) >;

  explicit ThreadPool(size_t threads);
  ~ThreadPool() noexcept;

  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;

  template< class F, class... Args >
  auto submit(F&& func, Args&&... args)
      -> std::future< std::invoke_result_t< std::decay_t< F >, std::decay_t< Args >... > >
  {
    using Result = std::invoke_result_t< std::decay_t< F >, std::decay_t< Args >... >;

    auto bound = [f = std::forward< F >(func), a = std::make_tuple(std::forward< Args >(args)...)]() mutable -> Result
    {
      return std::apply(std::move(f), std::move(a));
    };

    auto task = std::make_shared< std::packaged_task< Result() > >(std::move(bound));
    std::future< Result > result = task->get_future();

    {
      std::lock_guard< std::mutex > lock(state_mutex_);
      if (stopped_) {
        throw std::runtime_error("Thread pool is stopped");
      }
      size_t target = pickWorkerLocked();
      Worker& w = *workers_[target];
      {
        std::lock_guard< std::mutex > wl(w.mtx);
        w.tasks.push_back(
            [task]()
            {
              (*task)();
            });
        w.task_count.fetch_add(1, std::memory_order_relaxed);
      }
      total_pending_tasks_.fetch_add(1, std::memory_order_relaxed);
    }
    condition_.notify_one();
    return result;
  }

  void waitIdle();
  void pause();
  void resume();
  void stop(bool cancel_pending = false);
  void growTo(size_t count);
  void shrinkTo(size_t count);
  void resize(size_t count);
  size_t size() const;

private:
  static constexpr size_t kBatchSize = 8;

  struct Worker {
    std::deque< Task > tasks;
    std::mutex mtx;
    std::atomic< size_t > task_count{0};
    bool stop = false;

    Worker() = default;
    Worker(const Worker&) = delete;
    Worker& operator=(const Worker&) = delete;
  };

  void workerLoop(size_t id, Worker* self);
  bool tryPopLocal(Worker* self, std::vector< Task >& batch);
  bool trySteal(size_t id, std::vector< Task >& batch);
  void executeBatch(std::vector< Task >& batch);
  size_t pickWorkerLocked() const;
  void checkNotWorkerThread() const;

  std::vector< std::unique_ptr< Worker > > workers_;
  std::vector< std::thread > threads_;

  mutable std::mutex state_mutex_;
  mutable std::mutex mgmt_mutex_;
  std::condition_variable condition_;
  std::condition_variable idle_condition_;

  bool stopped_ = false;
  std::atomic< bool > paused_{false};
  bool cancel_pending_ = false;

  std::atomic< size_t > active_tasks_{0};
  std::atomic< size_t > total_pending_tasks_{0};
};

#endif
