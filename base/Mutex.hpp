#ifndef MUTEX_H
#define MUTEX_H

#include <assert.h>
#include <pthread.h>

#include <boost/noncopyable.hpp>
// #include<unistd.h>
namespace tsdb {
namespace base {

class MutexLock : boost::noncopyable {
 private:
  pthread_mutex_t mutex_;
  // pid_t holder_;
  // pthread_t holder_;

 public:
  MutexLock() { pthread_mutex_init(&mutex_, NULL); }

  ~MutexLock() {
    // assert(holder_ == 0);
    pthread_mutex_destroy(&mutex_);
  }

  void lock() {
    pthread_mutex_lock(&mutex_);
    // holder_ = pthread_self();
  }

  void unlock() {
    // holder_ = 0;
    pthread_mutex_unlock(&mutex_);
  }

  pthread_mutex_t *getMutex() { return &mutex_; }
};

class PadMutexLock : boost::noncopyable {
 public:
  MutexLock mutex_;
  char padding_[44];
};

class RWMutexLock : boost::noncopyable {
 private:
  pthread_rwlock_t rwlock_;

 public:
  RWMutexLock() { pthread_rwlock_init(&rwlock_, NULL); }

  ~RWMutexLock() { pthread_rwlock_destroy(&rwlock_); }

  void read_lock() { pthread_rwlock_rdlock(&rwlock_); }

  void write_lock() { pthread_rwlock_wrlock(&rwlock_); }

  void unlock() { pthread_rwlock_unlock(&rwlock_); }
};

class PadRWMutexLock : boost::noncopyable {
 private:
  pthread_rwlock_t rwlock_;
  char padding_[static_cast<int>((sizeof(pthread_rwlock_t) / 64 + 1) * 64 -
                                 sizeof(pthread_rwlock_t))];

 public:
  PadRWMutexLock() { pthread_rwlock_init(&rwlock_, NULL); }

  ~PadRWMutexLock() { pthread_rwlock_destroy(&rwlock_); }

  void read_lock() { pthread_rwlock_rdlock(&rwlock_); }

  void write_lock() { pthread_rwlock_wrlock(&rwlock_); }

  void unlock() { pthread_rwlock_unlock(&rwlock_); }
};

class MutexLockGuard : boost::noncopyable {
 private:
  MutexLock &mutex_;

 public:
  explicit MutexLockGuard(MutexLock &mutex) : mutex_(mutex) { mutex_.lock(); }

  explicit MutexLockGuard(PadMutexLock &mutex) : mutex_(mutex.mutex_) {
    mutex_.lock();
  }

  ~MutexLockGuard() { mutex_.unlock(); }
};

// class RawMutexLockGuard: boost::noncopyable{
//     private:
//         pthread_mutex_t & mutex_;

//     public:
//         explicit MutexLockGuard(pthread_mutex_t & mutex): mutex_(mutex){
//             mutex_.lock();
//         }

//         ~MutexLockGuard(){
//             mutex_.unlock();
//         }
// };

class RWLockGuard : boost::noncopyable {
 private:
  RWMutexLock &lock_;

 public:
  // 0 --> Read, 1 --> Write
  explicit RWLockGuard(RWMutexLock &lock, bool mode) : lock_(lock) {
    if (mode == 0)
      lock_.read_lock();
    else
      lock_.write_lock();
  }

  ~RWLockGuard() { lock_.unlock(); }
};

class PadRWLockGuard : boost::noncopyable {
 private:
  PadRWMutexLock &lock_;

 public:
  // 0 --> Read, 1 --> Write
  explicit PadRWLockGuard(PadRWMutexLock &lock, bool mode) : lock_(lock) {
    if (mode == 0)
      lock_.read_lock();
    else
      lock_.write_lock();
  }

  ~PadRWLockGuard() { lock_.unlock(); }
};

// Prevent misuse like:
// MutexLockGuard(mutex_);
// A tempory object doesn't hold the lock for long!
#define MutexLockGuard(x) error "Missing guard object name"

#define RWLockGuard(x, y) error "Missing guard object name"

}  // namespace base
}  // namespace tsdb

#endif