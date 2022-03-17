#ifndef TEST_THREAD_POOL_H
#define TEST_THREAD_POOL_H

#include <algorithm>
#include <atomic>
#include <cassert>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <vector>

namespace tsdb {
namespace base {

class ThreadPool {
 public:
  explicit ThreadPool(std::size_t threads =
                          (std::max)(2u, std::thread::hardware_concurrency()));
  template <class F, class... Args>
  auto run(F&& f, Args&&... args)
      -> std::future<typename std::result_of<F(Args...)>::type>;
  void wait_until_empty();
  void wait_until_nothing_in_flight();
  void set_queue_size_limit(std::size_t limit);
  void set_pool_size(std::size_t limit);
  ~ThreadPool();

 private:
  void start_worker(std::size_t worker_number,
                    std::unique_lock<std::mutex> const& lock);

  // need to keep track of threads so we can join them
  std::vector<std::thread> workers;
  // target pool size
  std::size_t pool_size;
  // the task queue
  std::queue<std::function<void()> > tasks;
  // queue length limit
  std::size_t max_queue_size = 100000;
  // stop signal
  bool stop = false;

  // synchronization
  std::mutex queue_mutex;
  std::condition_variable condition_producers;
  std::condition_variable condition_consumers;

  std::mutex in_flight_mutex;
  std::condition_variable in_flight_condition;
  std::atomic<std::size_t> in_flight;

  struct handle_in_flight_decrement {
    ThreadPool& tp;

    handle_in_flight_decrement(ThreadPool& tp_) : tp(tp_) {}

    ~handle_in_flight_decrement() {
      std::size_t prev = std::atomic_fetch_sub_explicit(
          &tp.in_flight, std::size_t(1), std::memory_order_acq_rel);
      if (prev == 1) {
        std::unique_lock<std::mutex> guard(tp.in_flight_mutex);
        tp.in_flight_condition.notify_all();
      }
    }
  };
};

// the constructor just launches some amount of workers
inline ThreadPool::ThreadPool(std::size_t threads)
    : pool_size(threads), in_flight(0) {
  std::unique_lock<std::mutex> lock(this->queue_mutex);
  for (std::size_t i = 0; i != threads; ++i) start_worker(i, lock);
}

// add new work item to the pool
template <class F, class... Args>
auto ThreadPool::run(F&& f, Args&&... args)
    -> std::future<typename std::result_of<F(Args...)>::type> {
  using return_type = typename std::result_of<F(Args...)>::type;

  auto task = std::make_shared<std::packaged_task<return_type()> >(
      std::bind(std::forward<F>(f), std::forward<Args>(args)...));

  std::future<return_type> res = task->get_future();

  std::unique_lock<std::mutex> lock(queue_mutex);
  if (tasks.size() >= max_queue_size)
    // wait for the queue to empty or be stopped
    condition_producers.wait(
        lock, [this] { return tasks.size() < max_queue_size || stop; });

  // don't allow enqueueing after stopping the pool
  if (stop) throw std::runtime_error("enqueue on stopped ThreadPool");

  tasks.emplace([task]() { (*task)(); });
  std::atomic_fetch_add_explicit(&in_flight, std::size_t(1),
                                 std::memory_order_relaxed);
  condition_consumers.notify_one();

  return res;
}

// the destructor joins all threads
inline ThreadPool::~ThreadPool() {
  std::unique_lock<std::mutex> lock(queue_mutex);
  stop = true;
  pool_size = 0;
  condition_consumers.notify_all();
  condition_producers.notify_all();
  condition_consumers.wait(lock, [this] { return this->workers.empty(); });
  assert(in_flight == 0);
}

inline void ThreadPool::wait_until_empty() {
  std::unique_lock<std::mutex> lock(this->queue_mutex);
  this->condition_producers.wait(lock, [this] { return this->tasks.empty(); });
}

inline void ThreadPool::wait_until_nothing_in_flight() {
  std::unique_lock<std::mutex> lock(this->in_flight_mutex);
  this->in_flight_condition.wait(lock, [this] { return this->in_flight == 0; });
}

inline void ThreadPool::set_queue_size_limit(std::size_t limit) {
  std::unique_lock<std::mutex> lock(this->queue_mutex);

  if (stop) return;

  std::size_t const old_limit = max_queue_size;
  max_queue_size = (std::max)(limit, std::size_t(1));
  if (old_limit < max_queue_size) condition_producers.notify_all();
}

inline void ThreadPool::set_pool_size(std::size_t limit) {
  if (limit < 1) limit = 1;

  std::unique_lock<std::mutex> lock(this->queue_mutex);

  if (stop) return;

  std::size_t const old_size = pool_size;
  assert(this->workers.size() >= old_size);

  pool_size = limit;
  if (pool_size > old_size) {
    // create new worker threads
    // it is possible that some of these are still running because
    // they have not stopped yet after a pool size reduction, such
    // workers will just keep running
    for (std::size_t i = old_size; i != pool_size; ++i) start_worker(i, lock);
  } else if (pool_size < old_size)
    // notify all worker threads to start downsizing
    this->condition_consumers.notify_all();
}

inline void ThreadPool::start_worker(std::size_t worker_number,
                                     std::unique_lock<std::mutex> const& lock) {
  assert(lock.owns_lock() && lock.mutex() == &this->queue_mutex);
  assert(worker_number <= this->workers.size());

  auto worker_func = [this, worker_number] {
    for (;;) {
      std::function<void()> task;
      bool notify;

      {
        std::unique_lock<std::mutex> tmp_lock(this->queue_mutex);
        this->condition_consumers.wait(tmp_lock, [this, worker_number] {
          return this->stop || !this->tasks.empty() ||
                 pool_size < worker_number + 1;
        });

        // deal with downsizing of thread pool or shutdown
        if ((this->stop && this->tasks.empty()) ||
            (!this->stop && pool_size < worker_number + 1)) {
          // detach this worker, effectively marking it stopped
          this->workers[worker_number].detach();
          // downsize the workers vector as much as possible
          while (this->workers.size() > pool_size &&
                 !this->workers.back().joinable())
            this->workers.pop_back();
          // if this is was last worker, notify the destructor
          if (this->workers.empty()) this->condition_consumers.notify_all();
          return;
        } else if (!this->tasks.empty()) {
          task = std::move(this->tasks.front());
          this->tasks.pop();
          notify =
              this->tasks.size() + 1 == max_queue_size || this->tasks.empty();
        } else
          continue;
      }

      handle_in_flight_decrement guard(*this);

      if (notify) {
        std::unique_lock<std::mutex> tmp_lock(this->queue_mutex);
        condition_producers.notify_all();
      }

      task();
    }
  };

  if (worker_number < this->workers.size()) {
    std::thread& worker = this->workers[worker_number];
    // start only if not already running
    if (!worker.joinable()) {
      worker = std::thread(worker_func);
    }
  } else
    this->workers.push_back(std::thread(worker_func));
}

}  // namespace base.
}  // namespace tsdb.

#endif