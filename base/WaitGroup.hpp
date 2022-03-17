#ifndef WAITGROUP_H
#define WAITGROUP_H

#include <boost/noncopyable.hpp>

#include "base/Condition.hpp"

namespace tsdb {
namespace base {

class WaitGroup : boost::noncopyable {
 private:
  mutable MutexLock mutex_;
  Condition condition_;
  int count_;

 public:
  explicit WaitGroup() : mutex_(), condition_(mutex_), count_(0) {}

  void add(int i) {
    MutexLockGuard lock(mutex_);
    count_ += i;
  }

  void done() {
    MutexLockGuard lock(mutex_);
    --count_;
    if (count_ <= 0) condition_.notifyAll();
  }

  void wait() {
    MutexLockGuard lock(mutex_);
    if (count_ > 0) condition_.wait();
  }
};

}  // namespace base
}  // namespace tsdb

#endif