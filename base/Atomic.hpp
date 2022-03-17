#ifndef ATOMIC_H
#define ATOMIC_H

#include <stdint.h>

#include <boost/noncopyable.hpp>

namespace tsdb {
namespace base {

class AtomicInt : boost::noncopyable {
 private:
  volatile int value_;

 public:
  AtomicInt() : value_(0) {}

  bool cas(int old_v, int new_v) {
    return __sync_bool_compare_and_swap(&value_, old_v, new_v);
  }

  int get() { return __sync_val_compare_and_swap(&value_, 0, 0); }

  int getAndAdd(int x) { return __sync_fetch_and_add(&value_, x); }

  int addAndGet(int x) { return getAndAdd(x) + x; }

  int incrementAndGet() { return addAndGet(1); }

  int decrementAndGet() { return addAndGet(-1); }

  void add(int x) { getAndAdd(x); }

  void increment() { incrementAndGet(); }

  void decrement() { decrementAndGet(); }

  int getAndSet(int x) { return __sync_lock_test_and_set(&value_, x); }
};

class AtomicInt64 : boost::noncopyable {
 private:
  volatile int64_t value_;

 public:
  AtomicInt64() : value_(0) {}

  bool cas(int64_t old_v, int64_t new_v) {
    return __sync_bool_compare_and_swap(&value_, old_v, new_v);
  }

  int64_t get() { return __sync_val_compare_and_swap(&value_, 0, 0); }

  int64_t getAndAdd(int64_t x) { return __sync_fetch_and_add(&value_, x); }

  int64_t addAndGet(int64_t x) { return getAndAdd(x) + x; }

  int64_t incrementAndGet() { return addAndGet(1); }

  int64_t decrementAndGet() { return addAndGet(-1); }

  void add(int64_t x) { getAndAdd(x); }

  void increment() { incrementAndGet(); }

  void decrement() { decrementAndGet(); }

  int64_t getAndSet(int64_t x) { return __sync_lock_test_and_set(&value_, x); }
};

class AtomicUInt64 : boost::noncopyable {
 private:
  volatile uint64_t value_;

 public:
  AtomicUInt64() : value_(0) {}
  AtomicUInt64(uint64_t v) : value_(v) {}

  bool cas(uint64_t old_v, uint64_t new_v) {
    return __sync_bool_compare_and_swap(&value_, old_v, new_v);
  }

  uint64_t get() { return __sync_val_compare_and_swap(&value_, 0, 0); }

  uint64_t getAndAdd(uint64_t x) { return __sync_fetch_and_add(&value_, x); }

  uint64_t addAndGet(uint64_t x) { return getAndAdd(x) + x; }

  uint64_t incrementAndGet() { return addAndGet(1); }

  uint64_t decrementAndGet() { return addAndGet(-1); }

  void add(uint64_t x) { getAndAdd(x); }

  void increment() { incrementAndGet(); }

  void decrement() { decrementAndGet(); }

  uint64_t getAndSet(uint64_t x) {
    return __sync_lock_test_and_set(&value_, x);
  }
};

}  // namespace base
}  // namespace tsdb

#endif