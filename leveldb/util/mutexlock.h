// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_MUTEXLOCK_H_
#define STORAGE_LEVELDB_UTIL_MUTEXLOCK_H_

#include "port/port.h"
#include "port/thread_annotations.h"

namespace leveldb {

// Helper class that locks a mutex on construction and unlocks the mutex when
// the destructor of the MutexLock object is invoked.
//
// Typical usage:
//
//   void MyClass::MyMethod() {
//     MutexLock l(&mu_);       // mu_ is an instance variable
//     ... some complex code, possibly with multiple return paths ...
//   }

class SCOPED_LOCKABLE MutexLock {
 public:
  explicit MutexLock(port::Mutex* mu) EXCLUSIVE_LOCK_FUNCTION(mu) : mu_(mu) {
    this->mu_->Lock();
  }
  ~MutexLock() UNLOCK_FUNCTION() { this->mu_->Unlock(); }

  MutexLock(const MutexLock&) = delete;
  MutexLock& operator=(const MutexLock&) = delete;

 private:
  port::Mutex* const mu_;
};

//
// Acquire a ReadLock on the specified RWMutex.
// The Lock will be automatically released then the
// object goes out of scope.
//
class ReadLock {
 public:
  explicit ReadLock(port::RWMutex* mu) : mu_(mu) { this->mu_->ReadLock(); }
  // No copying allowed
  ReadLock(const ReadLock&) = delete;
  void operator=(const ReadLock&) = delete;

  ~ReadLock() { this->mu_->ReadUnlock(); }

 private:
  port::RWMutex* const mu_;
};

//
// Automatically unlock a locked mutex when the object is destroyed
//
class ReadUnlock {
 public:
  explicit ReadUnlock(port::RWMutex* mu) : mu_(mu) { mu->AssertHeld(); }
  // No copying allowed
  ReadUnlock(const ReadUnlock&) = delete;
  ReadUnlock& operator=(const ReadUnlock&) = delete;

  ~ReadUnlock() { mu_->ReadUnlock(); }

 private:
  port::RWMutex* const mu_;
};

//
// Acquire a WriteLock on the specified RWMutex.
// The Lock will be automatically released then the
// object goes out of scope.
//
class WriteLock {
 public:
  explicit WriteLock(port::RWMutex* mu) : mu_(mu) { this->mu_->WriteLock(); }
  // No copying allowed
  WriteLock(const WriteLock&) = delete;
  void operator=(const WriteLock&) = delete;

  ~WriteLock() { this->mu_->WriteUnlock(); }

 private:
  port::RWMutex* const mu_;
};

//
// SpinMutex has very low overhead for low-contention cases.  Method names
// are chosen so you can use std::unique_lock or std::lock_guard with it.
//
// class SpinMutex {
//  public:
//   SpinMutex() : locked_(false) {}

//   bool try_lock() {
//     auto currently_locked = locked_.load(std::memory_order_relaxed);
//     return !currently_locked &&
//            locked_.compare_exchange_weak(currently_locked, true,
//                                          std::memory_order_acquire,
//                                          std::memory_order_relaxed);
//   }

//   void lock() {
//     for (size_t tries = 0;; ++tries) {
//       if (try_lock()) {
//         // success
//         break;
//       }
//       port::AsmVolatilePause();
//       if (tries > 100) {
//         std::this_thread::yield();
//       }
//     }
//   }

//   void unlock() { locked_.store(false, std::memory_order_release); }

//  private:
//   std::atomic<bool> locked_;
// };

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_MUTEXLOCK_H_
