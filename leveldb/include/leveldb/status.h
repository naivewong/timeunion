// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Status encapsulates the result of an operation.  It may indicate success,
// or it may indicate an error with an associated error message.
//
// Multiple threads can invoke const methods on a Status without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Status must use
// external synchronization.

#ifndef STORAGE_LEVELDB_INCLUDE_STATUS_H_
#define STORAGE_LEVELDB_INCLUDE_STATUS_H_

#include <algorithm>
#include <string>

#include "leveldb/export.h"
#include "leveldb/slice.h"

namespace leveldb {

class LEVELDB_EXPORT Status {
 private:
  enum Code {
    kOk = 0,
    kNotFound = 1,
    kCorruption = 2,
    kNotSupported = 3,
    kInvalidArgument = 4,
    kIOError = 5,
    kMergeInProgress = 6,
    kIncomplete = 7,
    kShutdownInProgress = 8,
    kTimedOut = 9,
    kAborted = 10,
    kBusy = 11,
    kExpired = 12,
    kTryAgain = 13,
    kCompactionTooLarge = 14,
    kColumnFamilyDropped = 15,
    kMaxCode
  };

  Status(Code code, const Slice& msg, const Slice& msg2);
  static const char* CopyState(const char* s);

  // OK status has a null state_.  Otherwise, state_ is a new[] array
  // of the following form:
  //    state_[0..3] == length of message
  //    state_[4]    == code
  //    state_[5..]  == message
  const char* state_;

 public:
  // Create a success status.
  Status() noexcept : state_(nullptr) {}
  ~Status() { delete[] state_; }

  Status(const Status& rhs);
  Status& operator=(const Status& rhs);

  Status(Status&& rhs) noexcept : state_(rhs.state_) { rhs.state_ = nullptr; }
  Status& operator=(Status&& rhs) noexcept;

  // Return a success status.
  static Status OK() { return Status(); }

  // Return error status of an appropriate type.
  static Status NotFound(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kNotFound, msg, msg2);
  }
  static Status NotFound() { return Status(kNotFound, "kNone", Slice()); }

  static Status Corruption(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kCorruption, msg, msg2);
  }
  static Status Corruption() { return Status(kCorruption, "kNone", Slice()); }

  static Status NotSupported(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kNotSupported, msg, msg2);
  }
  static Status NotSupported() {
    return Status(kNotSupported, "kNone", Slice());
  }

  static Status InvalidArgument(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kInvalidArgument, msg, msg2);
  }
  static Status InvalidArgument() {
    return Status(kInvalidArgument, "kNone", Slice());
  }

  static Status IOError(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kIOError, msg, msg2);
  }
  static Status IOError() { return Status(kIOError, "kNone", Slice()); }

  static Status Busy(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kBusy, msg, msg2);
  }
  static Status Busy() { return Status(kBusy, "kNone", Slice()); }

  static Status TimedOut(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kTimedOut, msg, msg2);
  }
  static Status TimedOut() { return Status(kTimedOut, "kNone", Slice()); }

  static Status NoSpace(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kIOError, msg, msg2);
  }
  static Status NoSpace() { return Status(kIOError, "kNoSpace", Slice()); }

  static Status PathNotFound(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kIOError, msg, msg2);
  }
  static Status PathNotFound() {
    return Status(kIOError, "kPathNotFound", Slice());
  }

  // Returns true iff the status indicates success.
  bool ok() const { return (state_ == nullptr); }

  // Returns true iff the status indicates a NotFound error.
  bool IsNotFound() const { return code() == kNotFound; }

  // Returns true iff the status indicates a Corruption error.
  bool IsCorruption() const { return code() == kCorruption; }

  // Returns true iff the status indicates an IOError.
  bool IsIOError() const { return code() == kIOError; }

  // Returns true iff the status indicates a NotSupportedError.
  bool IsNotSupportedError() const { return code() == kNotSupported; }

  // Returns true iff the status indicates an InvalidArgument.
  bool IsInvalidArgument() const { return code() == kInvalidArgument; }

  // Return a string representation of this status suitable for printing.
  // Returns the string "OK" for success.
  std::string ToString() const;

  Code code() const {
    return (state_ == nullptr) ? kOk : static_cast<Code>(state_[4]);
  }
};

inline Status::Status(const Status& rhs) {
  state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_);
}
inline Status& Status::operator=(const Status& rhs) {
  // The following condition catches both aliasing (when this == &rhs),
  // and the common case where both rhs and *this are ok.
  if (state_ != rhs.state_) {
    delete[] state_;
    state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_);
  }
  return *this;
}
inline Status& Status::operator=(Status&& rhs) noexcept {
  std::swap(state_, rhs.state_);
  return *this;
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_STATUS_H_
