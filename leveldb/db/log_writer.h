// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_LOG_WRITER_H_
#define STORAGE_LEVELDB_DB_LOG_WRITER_H_

#include "db/log_format.h"
#include <cstdint>
#include <vector>

#include "leveldb/slice.h"
#include "leveldb/status.h"

namespace tsdb {
namespace tsdbutil {
class RefSample;
class MMapRefSample;
class MMapRefGroupSample;
}  // namespace tsdbutil
}  // namespace tsdb

namespace leveldb {

class WritableFile;

namespace log {

// ┌────────────────────────────────────────────┐
// │ type = 1 <1b>                              │
// ├────────────────────────────────────────────┤
// │ ┌─────────┬──────────────────────────────┐ │
// │ │ id <8b> │ n = len(labels) <uvarint>    │ │
// │ ├─────────┴────────────┬─────────────────┤ │
// │ │ len(str_1) <uvarint> │ str_1 <bytes>   │ │
// │ ├──────────────────────┴─────────────────┤ │
// │ │  ...                                   │ │
// │ ├───────────────────────┬────────────────┤ │
// │ │ len(str_2n) <uvarint> │ str_2n <bytes> │ │
// │ └───────────────────────┴────────────────┘ │
// │                  . . .                     │
// └────────────────────────────────────────────┘
//
// Series appends the encoded series to b and returns the resulting slice.
// RefSeries is the series labels with the series ID.
std::string series(const std::vector<RefSeries>& series);
void series(const std::vector<RefSeries>& series, std::string* buf);

// ┌──────────────────────────────────────────────────────────────────────────┐
// │ type = 2 <1b>                                                            │
// ├──────────────────────────────────────────────────────────────────────────┤
// │ ┌───────────────────┬──────────────────────────┬────────────┬──────────┐ │
// │ │ id <8b>           │ timestamp <8b>           │ value <8b> │ txn <8b> │ │
// │ └───────────────────┴──────────────────────────┴────────────┴──────────┘ │
// │ ┌───────────────────┬──────────────────────────┬────────────┬──────────┐ │
// │ │ id_delta <varint> │ timestamp_delta <varint> │ value <8b> │ txn <8b> │ │
// │ └───────────────────┴──────────────────────────┴────────────┴──────────┘ │
// │                              . . .                                       │
// └──────────────────────────────────────────────────────────────────────────┘
std::string samples(const std::vector<RefSample>& samples);
std::string samples(const std::vector<::tsdb::tsdbutil::RefSample>& samples);
std::string samples(
    const std::vector<::tsdb::tsdbutil::MMapRefSample>& samples);
void samples(const std::vector<RefSample>& samples, std::string* buf);
void samples(const std::vector<::tsdb::tsdbutil::RefSample>& samples,
             std::string* buf);
void samples(const std::vector<::tsdb::tsdbutil::MMapRefSample>& samples,
             std::string* buf);

// ┌────────────────────────────────────────────┐
// │ type = 1 <1b>                              │
// ├────────────────────────────────────────────┤
// │ ┌─────────┬──────────────────────────────┐ │
// │ │ id <8b> │ n = len(labels) <uvarint>    │ │
// │ ├─────────┴────────────┬─────────────────┤ │
// │ │ len(str_1) <uvarint> │ str_1 <bytes>   │ │
// │ ├──────────────────────┴─────────────────┤ │
// │ │  ...                                   │ │
// │ ├───────────────┬────────────────────────┤ │
// │ │ #TS <uvarint> │ #TS1 labels <uvarint>  │ │
// │ ├───────────────┴──────┬─────────────────┤ │
// │ │ len(str_1) <uvarint> │ str_1 <bytes>   │ │
// │ ├──────────────────────┴─────────────────┤ │
// │ ├───────────────────────┬────────────────┤ │
// │ │ len(str_2n) <uvarint> │ str_2n <bytes> │ │
// │ └───────────────────────┴────────────────┘ │
// │                  . . .                     │
// └────────────────────────────────────────────┘
std::string group(const RefGroup& g);
void group(const RefGroup& g, std::string* buf);

std::string group_sample(const ::tsdb::tsdbutil::MMapRefGroupSample& g);
std::string group_sample(const RefGroupSample& g);
void group_sample(const ::tsdb::tsdbutil::MMapRefGroupSample& g,
                  std::string* buf);
void group_sample(const RefGroupSample& g, std::string* buf);

std::string flush(const RefFlush& f);
void flush(const RefFlush& f, std::string* buf);

class Writer {
 public:
  // Create a writer that will append data to "*dest".
  // "*dest" must be initially empty.
  // "*dest" must remain live while this Writer is in use.
  explicit Writer(WritableFile* dest);

  // Create a writer that will append data to "*dest".
  // "*dest" must have initial length "dest_length".
  // "*dest" must remain live while this Writer is in use.
  Writer(WritableFile* dest, uint64_t dest_length);

  Writer(const Writer&) = delete;
  Writer& operator=(const Writer&) = delete;

  ~Writer();

  Status AddRecord(const Slice& slice);

  WritableFile* file() { return dest_; }

  int64_t write_off() {
    return kBlockSize * (int64_t)(num_block_) + (int64_t)(block_offset_);
  }

 private:
  Status EmitPhysicalRecord(RecordType type, const char* ptr, size_t length);

  WritableFile* dest_;
  int block_offset_;  // Current offset in block
  int num_block_;

  // crc32c values for all supported record types.  These are
  // pre-computed to reduce the overhead of computing the crc of the
  // record type stored in the header.
  uint32_t type_crc_[kMaxRecordType + 1];
};

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_WRITER_H_
