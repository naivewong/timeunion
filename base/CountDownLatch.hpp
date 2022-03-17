#ifndef COUNTDOWNLATCH_H
#define COUNTDOWNLATCH_H
#include <boost/noncopyable.hpp>

#include "base/Condition.hpp"
#include "base/Mutex.hpp"

namespace tsdb {
namespace base {

class CountDownLatch : boost::noncopyable {
 public:
  explicit CountDownLatch(int count)
      : mutex_(), condition_(mutex_), count_(count) {}

  void wait() {
    MutexLockGuard lock(mutex_);
    if (count_ > 0) {
      condition_.wait();
    }
  }

  void countDown() {
    MutexLockGuard lock(mutex_);
    --count_;
    if (count_ == 0) {
      condition_.notifyAll();
    }
  }

  int getCount() const {
    MutexLockGuard lock(mutex_);
    return count_;
  }

 private:
  mutable MutexLock mutex_;
  Condition condition_;
  int count_;
};

}  // namespace base
}  // namespace tsdb

#endif