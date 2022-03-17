#ifndef BASE_CHANNEL_H
#define BASE_CHANNEL_H

#include <stdlib.h> /* srand, rand */
#include <time.h>   /* time */

#include <deque>
#include <initializer_list>
#include <memory>

#include "base/Condition.hpp"
#include "base/Mutex.hpp"

namespace tsdb {
namespace base {

// unbuffered channel.
template <typename T>
class Channel {
 private:
  std::deque<T> buf_;
  mutable base::MutexLock mutex_;
  Condition condition_;

 public:
  Channel() : mutex_(), condition_(mutex_) {}

  void send(const T &v) {
    base::MutexLockGuard lock(mutex_);
    if (buf_.empty()) buf_.push_back(v);
  }

  T recv() {
    base::MutexLockGuard lock(mutex_);
    if (buf_.empty())
      return T();
    else {
      T r = buf_.front();
      buf_.pop_front();
      return r;
    }
  }

  bool empty() {
    base::MutexLockGuard lock(mutex_);
    return buf_.empty();
  }

  void flush() {
    base::MutexLockGuard lock(mutex_);
    buf_.clear();
  }

  void wait_for_seconds(double seconds) {
    base::MutexLockGuard lock(mutex_);
    condition_.waitForSeconds(seconds);
  }

  void notify() {
    base::MutexLockGuard lock(mutex_);
    condition_.notifyAll();
  }
};

template <typename T>
int channel_select(const std::deque<std::shared_ptr<Channel<T>>> &chans) {
  std::deque<char> d;
  for (int i = 0; i < chans.size(); i++) {
    if (!chans[i]->empty()) d.push_back(i);
  }
  if (d.empty()) return -1;
  // Seed the random number generator
  srand(time(0));
  return d[rand() % d.size()];
}

template <typename T>
int channel_select(
    const std::initializer_list<std::shared_ptr<Channel<T>>> &chans) {
  std::deque<char> d;
  int i = 0;
  for (auto it = chans.begin(); it != chans.end(); ++it) {
    if (!(*it)->empty()) d.push_back(i);
    ++i;
  }
  if (d.empty()) return -1;
  // Seed the random number generator
  srand(time(0));
  return d[rand() % d.size()];
}

}  // namespace base
}  // namespace tsdb

#endif