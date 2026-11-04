#ifndef POOL_HPP
#define POOL_HPP

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

class ThreadPool {
public:
  using Task = std::function< void(void) >;
  explicit ThreadPool(size_t threads);

  template< class F, class... Args >
  auto submit(F&& func, Args&&... args) -> std::future< std::invoke_result_t< F, Args... > >
  {
    using Result = std::invoke_result_t< F, Args... >;

    auto bound = std::bind(std::forward< F >(func), std::forward< Args >(args)...);

    auto task = std::make_shared< std::packaged_task< Result() > >(std::move(bound));
    std::future< Result > result = task->get_future();

    {
      std::lock_guard< std::mutex > lock(mutex_);
      if (stopped_) {
        throw std::runtime_error("Thread pool is stopped");
      }
      tasks_.push(
          [task]()
          {
            (*task)();
          });
      condition_.notify_one();
    }

    return result;
  }
  void stop(bool cancel_pending = false);
  void growTo(size_t count);
  void shrinkTo(size_t count);
  void resize(size_t count);
  size_t size() const;

  ~ThreadPool() noexcept;

private:
  void execute(size_t id);
  void checkNotWorkerThread() const;

  std::queue< Task > tasks_;
  mutable std::mutex mutex_;
  std::condition_variable condition_;
  std::condition_variable idle_condition_;
  bool stopped_;
  bool paused_;
  bool cancel_pending_;
  size_t active_tasks_;
  std::vector< std::thread > threads_;
  std::vector< bool > workers_stop_;
};

#endif
