#ifndef TSDB_ERROR_H
#define TSDB_ERROR_H

#include <deque>
#include <string>

#include "base/Mutex.hpp"

namespace tsdb {
namespace error {

class Error {
 private:
  std::string err_;

 public:
  Error() = default;
  Error(const std::string &err_) : err_(err_) {}
  Error(const error::Error &err) : err_(err.error()) {}

  // TODO(alec), optimize.
  void wrap(const std::string &msg) { err_ = msg + ": " + err_; }

  void unwrap() {
    int n = err_.find(":");
    if (n != std::string::npos) err_ = err_.substr(n + 2);
  }

  void set(const std::string &s) { err_ = s; }

  void set(const Error &e) { err_ = e.error(); }

  const std::string &error() const { return err_; }

  operator bool() const { return !err_.empty(); }

  bool operator==(const std::string &err) { return err_ == err; }
  bool operator==(const Error &err) { return err_ == err.error(); }

  bool operator!=(const std::string &err) { return err_ != err; }
  bool operator!=(const Error &err) { return err_ != err.error(); }
};

inline Error unwrap(const Error &e) {
  int n = e.error().find(":");
  if (n == std::string::npos)
    return e.error();
  else
    return e.error().substr(n + 2);
}

inline Error wrap(const Error &e, const std::string &msg) {
  std::string r;
  r.reserve(r.length() + msg.length() + 3);
  r += msg;
  r += ": ";
  r += e.error();
  return r;
}

// Thread safe multiple errors.
class MultiError {
 private:
  std::string errs;
  mutable base::MutexLock mutex_;

 public:
  MultiError() = default;
  MultiError(const std::deque<Error> &errs) {
    for (auto const &err : errs) this->errs += err.error() + "\n";
  }
  MultiError(const std::deque<std::string> &errs) {
    for (auto const &err : errs) this->errs += err + "\n";
  }

  void add(const std::string &err) {
    base::MutexLockGuard lock(mutex_);
    errs += err + "\n";
  }

  void add(const Error &err) {
    base::MutexLockGuard lock(mutex_);
    errs += err.error() + "\n";
  }

  const std::string &error() const {
    base::MutexLockGuard lock(mutex_);
    return errs;
  }

  operator bool() const {
    base::MutexLockGuard lock(mutex_);
    return !errs.empty();
  }

  bool operator==(const std::string &err) { return error() == err; }
  bool operator==(const MultiError &err) { return error() == err.error(); }

  bool operator!=(const std::string &err) { return error() != err; }
  bool operator!=(const MultiError &err) { return error() != err.error(); }
};

}  // namespace error
}  // namespace tsdb

#endif