#pragma once

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <random>
#include <stdexcept>
#include <thread>
#include <vector>

class WaitGroup {
 public:
  WaitGroup() : counter(0) {}

  void Add(int incr = 1) { counter += incr; }

  void Done() {
    counter--;
    if (counter <= 0) cond.notify_all();
  }

  void Wait() {
    std::unique_lock<std::mutex> lock(mutex);
    cond.wait(lock, [&] { return counter <= 0; });
  }

 private:
  std::mutex mutex;
  std::atomic<int> counter;
  std::condition_variable cond;
};

class RandomNumber {
 public:
  RandomNumber()
      : seed(std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::high_resolution_clock::now().time_since_epoch())
                 .count()) {}

  double range(double start, double end) {
    std::uniform_real_distribution<double> f(start, end);
    return f(seed);
  }

  int range(int start, int end) {
    std::uniform_int_distribution<int> f(start, end);
    return f(seed);
  }

  std::function<int()> int_generator(int start, int end) {
    return bind(std::uniform_int_distribution<int>(start, end), seed);
  }

  std::function<double()> double_generator(double start, double end) {
    return bind(std::uniform_real_distribution<double>(start, end), seed);
  }

 private:
  std::mt19937 seed;
};

class Timer {
 public:
  typedef std::chrono::high_resolution_clock Time;
  typedef std::chrono::milliseconds ms;
  typedef std::chrono::nanoseconds ns;
  typedef std::chrono::duration<float> fsec;

  Timer() = default;
  int stop() {
    return std::chrono::duration_cast<ms>(Time::now() - last_stop_).count();
  }
  uint64_t stop_nano() {
    return std::chrono::duration_cast<ns>(Time::now() - last_stop_).count();
  }
  void start() {
    start_time_ = Time::now();
    last_stop_ = Time::now();
  }
  void resume() { last_stop_ = Time::now(); }
  int since_start() {
    return std::chrono::duration_cast<ms>(Time::now() - start_time_).count();
  }
  uint64_t since_start_nano() {
    return std::chrono::duration_cast<ns>(Time::now() - start_time_).count();
  }

 private:
  std::chrono::time_point<std::chrono::high_resolution_clock> start_time_;
  std::chrono::time_point<std::chrono::high_resolution_clock> last_stop_;
};

class ThreadPool {
 public:
  ThreadPool(size_t);
  template <class F, class... Args>
  auto enqueue(F&& f, Args&&... args)
      -> std::future<typename std::result_of<F(Args...)>::type>;

  void wait_barrier();
  ~ThreadPool();

 private:
  // need to keep track of threads so we can join them
  std::vector<std::thread> workers;
  // the task queue
  std::queue<std::function<void()> > tasks;

  int jobs_in_progress;

  // synchronization
  std::mutex queue_mutex;
  std::condition_variable condition;

  std::mutex barrier_mutex;
  std::condition_variable idle_condition;
  bool stop;
};

// the constructor just launches some amount of workers
inline ThreadPool::ThreadPool(size_t threads)
    : jobs_in_progress(0), stop(false) {
  for (size_t i = 0; i < threads; ++i)
    workers.emplace_back([this, i] {
      for (;;) {
        std::function<void()> task;

        {
          std::unique_lock<std::mutex> lock(this->queue_mutex);
          this->condition.wait(
              lock, [this] { return this->stop || !this->tasks.empty(); });
          if (this->stop && this->tasks.empty()) return;
          task = std::move(this->tasks.front());
          this->tasks.pop();
        }
        task();
        {
          std::unique_lock<std::mutex> lock(this->barrier_mutex);
          this->jobs_in_progress--;
        }
        this->idle_condition.notify_one();
      }
    });
}

inline void ThreadPool::wait_barrier() {
  std::unique_lock<std::mutex> lock(this->barrier_mutex);
  idle_condition.wait(lock, [this] { return this->jobs_in_progress == 0; });
}

// add new work item to the pool
template <class F, class... Args>
auto ThreadPool::enqueue(F&& f, Args&&... args)
    -> std::future<typename std::result_of<F(Args...)>::type> {
  using return_type = typename std::result_of<F(Args...)>::type;

  auto task = std::make_shared<std::packaged_task<return_type()> >(
      std::bind(std::forward<F>(f), std::forward<Args>(args)...));

  std::future<return_type> res = task->get_future();
  {
    std::unique_lock<std::mutex> lock(queue_mutex);

    // don't allow enqueueing after stopping the pool
    if (stop) throw std::runtime_error("enqueue on stopped ThreadPool");

    tasks.emplace([task]() { (*task)(); });
  }
  {
    std::unique_lock<std::mutex> lock(this->barrier_mutex);
    this->jobs_in_progress++;
  }
  condition.notify_one();
  return res;
}

// the destructor joins all threads
inline ThreadPool::~ThreadPool() {
  {
    std::unique_lock<std::mutex> lock(queue_mutex);
    stop = true;
  }
  condition.notify_all();
  wait_barrier();
  for (std::thread& worker : workers) worker.join();
}
