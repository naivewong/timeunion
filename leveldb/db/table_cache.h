// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Thread-safe (provides internal synchronization)

#ifndef STORAGE_LEVELDB_DB_TABLE_CACHE_H_
#define STORAGE_LEVELDB_DB_TABLE_CACHE_H_

#include "db/dbformat.h"
#include <cstdint>
#include <string>

#include "leveldb/cache.h"
#include "leveldb/table.h"

#include "port/port.h"

namespace leveldb {

class Env;

class TableCache {
 public:
  TableCache(const std::string& dbname, const Options& options, int entries);
  ~TableCache();

  // Return an iterator for the specified file number (the corresponding
  // file length must be exactly "file_size" bytes).  If "tableptr" is
  // non-null, also sets "*tableptr" to point to the Table object
  // underlying the returned iterator, or to nullptr if no Table object
  // underlies the returned iterator.  The returned "*tableptr" object is owned
  // by the cache and should not be deleted, and is valid for as long as the
  // returned iterator is live.
  Iterator* NewIterator(const ReadOptions& options, uint64_t file_number,
                        uint64_t file_size, Table** tableptr = nullptr);

  // If a seek to internal key "k" in specified file finds an entry,
  // call (*handle_result)(arg, found_key, found_value).
  Status Get(const ReadOptions& options, uint64_t file_number,
             uint64_t file_size, const Slice& k, void* arg,
             void (*handle_result)(void*, const Slice&, const Slice&));

  // Evict any entry for the specified file number
  void Evict(uint64_t file_number);

  void* Value(Cache::Handle* handle) { return cache_->Value(handle); }
  Status FindIndex(const std::string& name, Cache::Handle** h);
  // Status FindPartitionIndex2(const std::string& dir, int64_t time, int idx,
  // Cache::Handle** h);
  Status FindCloudTimeseriesFile(const std::string& dir, uint64_t tsid,
                                 int patch, Cache::Handle** h);

  void ReleaseHandle(Cache::Handle* handle);

 private:
  Status FindTable(uint64_t file_number, uint64_t file_size, Cache::Handle**);

  Env* const env_;
  const std::string dbname_;
  const Options& options_;
  Cache* cache_;

  EnvOptions env_options_;  // used in S3 files.
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_TABLE_CACHE_H_
