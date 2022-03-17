#ifndef CONDITION_H
#define CONDITION_H

#include <errno.h>
#include <pthread.h>

#include <boost/noncopyable.hpp>

#include "base/Mutex.hpp"

namespace tsdb {
namespace base {

class Condition : boost::noncopyable {
 private:
  MutexLock &mutex_;
  pthread_cond_t pcond_;

 public:
  explicit Condition(MutexLock &mutex) : mutex_(mutex) {
    pthread_cond_init(&pcond_, NULL);
  }

  ~Condition() { pthread_cond_destroy(&pcond_); }

  void wait() { pthread_cond_wait(&pcond_, mutex_.getMutex()); }

  void waitForSeconds(double seconds) {
    struct timespec abstime;
    // FIXME: use CLOCK_MONOTONIC or CLOCK_MONOTONIC_RAW to prevent time rewind.
    clock_gettime(CLOCK_REALTIME, &abstime);

    const int64_t kNanoSecondsPerSecond = 1000000000;
    int64_t nanoseconds = static_cast<int64_t>(seconds * kNanoSecondsPerSecond);

    abstime.tv_sec += static_cast<time_t>((abstime.tv_nsec + nanoseconds) /
                                          kNanoSecondsPerSecond);
    abstime.tv_nsec = static_cast<long>((abstime.tv_nsec + nanoseconds) %
                                        kNanoSecondsPerSecond);

    // MutexLock::UnassignGuard ug(mutex_);
    pthread_cond_timedwait(&pcond_, mutex_.getMutex(), &abstime);
  }

  void notify() { pthread_cond_signal(&pcond_); }

  void notifyAll() { pthread_cond_broadcast(&pcond_); }
};

}  // namespace base
}  // namespace tsdb

#endif