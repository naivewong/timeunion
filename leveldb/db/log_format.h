// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Log format information shared by reader and writer.
// See ../doc/log_format.md for more detail.

#ifndef STORAGE_LEVELDB_DB_LOG_FORMAT_H_
#define STORAGE_LEVELDB_DB_LOG_FORMAT_H_

#include "label/Label.hpp"

namespace leveldb {
namespace log {

enum RecordType {
  // Zero is reserved for preallocated files
  kZeroType = 0,

  kFullType = 1,

  // For fragments
  kFirstType = 2,
  kMiddleType = 3,
  kLastType = 4
};
static const int kMaxRecordType = kLastType;

static const int kBlockSize = 32768;

// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
static const int kHeaderSize = 4 + 2 + 1;

enum LogRecordType {
  kSeries = 0,
  kSample = 1,
  kSamples = 1,
  kFlush = 3,
  kGroup = 4,
  kGroupSample = 5
};

class RefSeries {
 public:
  uint64_t ref;
  ::tsdb::label::Labels lset;

  RefSeries() = default;
  RefSeries(uint64_t ref, const ::tsdb::label::Labels& lset)
      : ref(ref), lset(lset) {}
};

class RefGroup {
 public:
  uint64_t ref;
  ::tsdb::label::Labels group_lset;
  std::vector<::tsdb::label::Labels> individual_lsets;

  RefGroup() = default;
  RefGroup(uint64_t ref, const ::tsdb::label::Labels& lset,
           const std::vector<::tsdb::label::Labels>& individual_lsets)
      : ref(ref), group_lset(lset), individual_lsets(individual_lsets) {}
  RefGroup(uint64_t ref, const ::tsdb::label::Labels& lset)
      : ref(ref), group_lset(lset) {}
};

class RefSample {
 public:
  uint64_t ref;
  int64_t t;
  double v;
  int64_t txn;

  RefSample() = default;
  RefSample(uint64_t ref, int64_t t, double v, int64_t txn)
      : ref(ref), t(t), v(v), txn(txn) {}

  bool operator==(const RefSample& r) {
    return ref == r.ref && t == r.t && v == r.v && txn == r.txn;
  }
};

class RefGroupSample {
 public:
  uint64_t ref;
  // If this is empty, it represents a full insertion.
  // First bit of 32-bit size set to 1.
  std::vector<::tsdb::label::Labels> individual_lsets;
  std::vector<int> slots;
  int64_t t;
  std::vector<double> v;
  int64_t txn;

  RefGroupSample() = default;
  RefGroupSample(uint64_t ref,
                 const std::vector<::tsdb::label::Labels>& individual_lsets,
                 int64_t t, const std::vector<double>& v, int64_t txn)
      : ref(ref), individual_lsets(individual_lsets), t(t), v(v), txn(txn) {}
  RefGroupSample(uint64_t ref, const std::vector<int>& slots, int64_t t,
                 const std::vector<double>& v, int64_t txn)
      : ref(ref), slots(slots), t(t), v(v), txn(txn) {}
  RefGroupSample(uint64_t ref, int64_t t, const std::vector<double>& v,
                 int64_t txn)
      : ref(ref), slots(slots), t(t), v(v), txn(txn) {}
};

class RefFlush {
 public:
  uint64_t ref;
  int64_t txn;

  RefFlush() = default;
  RefFlush(uint64_t ref, int64_t txn) : ref(ref), txn(txn) {}
};

// class RefSamples {
// public:
//   uint64_t ref;
// }

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_FORMAT_H_
