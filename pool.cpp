#include "pool.hpp"
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
