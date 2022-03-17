#ifndef MEMTONBSTONES_H
#define MEMTONBSTONES_H

#include <unordered_map>

#include "base/Mutex.hpp"
#include "tombstone/Interval.hpp"
#include "tombstone/TombstoneReaderInterface.hpp"

namespace tsdb {

namespace tombstone {

class MemTombstones : public TombstoneReaderInterface {
 public:
  std::unordered_map<uint64_t, Intervals> interval_groups;
  mutable base::RWMutexLock mutex_;

  // NOTICE, may throw std::out_of_range.
  const Intervals &get(uint64_t ref) const {
    base::RWLockGuard mutex(mutex_, false);
    return interval_groups.at(ref);
  }

  void iter(const IterFunc &f) const {
    base::RWLockGuard mutex(mutex_, false);
    for (auto const &pair : interval_groups) f(pair.first, pair.second);
  }
  error::Error iter(const ErrIterFunc &f) const {
    base::RWLockGuard mutex(mutex_, false);
    for (auto const &pair : interval_groups) {
      error::Error err = f(pair.first, pair.second);
      if (err) return error::wrap(err, "MemTombstones::iter");
    }
    return error::Error();
  }

  uint64_t total() const {
    base::RWLockGuard mutex(mutex_, false);
    uint64_t r = 0;
    for (auto const &pair : interval_groups) r += pair.second.size();
    return r;
  }

  void add_interval(uint64_t ref, const Interval &itvl) {
    base::RWLockGuard mutex(mutex_, true);
    itvls_add(interval_groups[ref], itvl);
  }
};

}  // namespace tombstone

}  // namespace tsdb

#endif