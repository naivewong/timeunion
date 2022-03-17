#ifndef BASE_FLOCK_H
#define BASE_FLOCK_H

#include <sys/file.h>

#include <cstdio>
#include <iostream>

#include "base/Error.hpp"

namespace tsdb {
namespace base {

// Currently support Unix.
class FLock {
 private:
  FILE *f;
  bool released;
  error::Error err_;

  FLock(const FLock &) = delete;             // non construction-copyable
  FLock &operator=(const FLock &) = delete;  // non copyable

 public:
  FLock() : f(nullptr), released(false) {}

  FLock(const std::string &filename) : f(nullptr), released(false) {
    if ((f = fopen(filename.c_str(), "wb")) == nullptr) {
      err_.set("cannot open lock file " + filename);
      return;
    }
    if (flock(fileno(f), LOCK_EX) != 0) {
      err_.set("cannot hold lock");
    }
  }

  void lock(const std::string &filename) {
    released = false;
    if ((f = fopen(filename.c_str(), "wb")) == nullptr) {
      err_.set("cannot open lock file " + filename);
      return;
    }
    if (flock(fileno(f), LOCK_EX) != 0) {
      err_.set("cannot hold lock");
    }
  }

  void release() {
    if (!released && f != nullptr) {
      flock(fileno(f), LOCK_UN);
      fclose(f);
      released = true;
    }
  }

  error::Error error() { return err_; }

  ~FLock() { release(); }
};

}  // namespace base
}  // namespace tsdb

#endif