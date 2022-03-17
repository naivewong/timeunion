// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "db/index_reader.h"
#include "db/partition_index.h"

#include "leveldb/env.h"
#include "leveldb/table.h"

#include "util/coding.h"

namespace leveldb {

struct TableAndFile {
  RandomAccessFile* file;
  Table* table;
};

static void DeleteEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->table;
  delete tf->file;
  delete tf;
}

static void DeleteIndexEntry(const Slice& key, void* value) {
  IndexReader* r = reinterpret_cast<IndexReader*>(value);
  delete r;
}

static void DeleteCloudTimeseriesFileEntry(const Slice& key, void* value) {
  CloudTimeseriesFile* r = reinterpret_cast<CloudTimeseriesFile*>(value);
  delete r;
}

// static void DeletePartitionIndex2Entry(const Slice& key, void* value) {
//   PartitionDiskIndex2SubReader* r =
//   reinterpret_cast<PartitionDiskIndex2SubReader*>(value); delete r;
// }

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

TableCache::TableCache(const std::string& dbname, const Options& options,
                       int entries)
    : env_(options.env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)),
      env_options_(options) {}

TableCache::~TableCache() { delete cache_; }

Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle) {
  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);
  if (*handle == nullptr) {
    std::string fname = TableFileName(dbname_, file_number);
    RandomAccessFile* file = nullptr;
    Table* table = nullptr;
    s = env_->NewRandomAccessFile(fname, &file, env_options_);
    if (!s.ok()) {
      std::string old_fname = SSTTableFileName(dbname_, file_number);
      if (env_->NewRandomAccessFile(old_fname, &file, env_options_).ok()) {
        s = Status::OK();
      }
    }
    if (s.ok()) {
      s = Table::Open(options_, file, file_size, &table);
    }

    if (!s.ok()) {
      assert(table == nullptr);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      TableAndFile* tf = new TableAndFile;
      tf->file = file;
      tf->table = table;
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
    }
  }
  return s;
}

Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number, uint64_t file_size,
                                  Table** tableptr) {
  if (tableptr != nullptr) {
    *tableptr = nullptr;
  }

  Cache::Handle* handle = nullptr;
  Status s = FindTable(file_number, file_size, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  Iterator* result = table->NewIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != nullptr) {
    *tableptr = table;
  }
  return result;
}

Status TableCache::Get(const ReadOptions& options, uint64_t file_number,
                       uint64_t file_size, const Slice& k, void* arg,
                       void (*handle_result)(void*, const Slice&,
                                             const Slice&)) {
  Cache::Handle* handle = nullptr;
  Status s = FindTable(file_number, file_size, &handle);
  if (s.ok()) {
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    s = t->InternalGet(options, k, arg, handle_result);
    cache_->Release(handle);
  }
  return s;
}

void TableCache::ReleaseHandle(Cache::Handle* handle) {
  cache_->Release(handle);
}

Status TableCache::FindIndex(const std::string& name, Cache::Handle** handle) {
  Status s;
  *handle = cache_->Lookup(name);
  if (*handle == nullptr) {
    RandomAccessFile* file = nullptr;
    s = env_->NewRandomAccessFile(name, &file);

    if (!s.ok()) {
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      IndexReader* r = new IndexReader(file);
      // TODO(Alec), the weight of the entry.
      *handle = cache_->Insert(name, r, 1, &DeleteIndexEntry);
    }
  }
  return s;
}

// Status TableCache::FindPartitionIndex2(const std::string& dir, int64_t time,
// int idx, Cache::Handle** handle) {
//   Status s;
//   std::string name = dir + "/" + std::to_string(time) + "_index2_" +
//   std::to_string(idx); *handle = cache_->Lookup(name); if (*handle ==
//   nullptr) {
//     RandomAccessFile* file = nullptr;
//     s = env_->NewRandomAccessFile(name, &file);

//     if (!s.ok()) {
//       delete file;
//       // We do not cache error results so that if the error is transient,
//       // or somebody repairs the file, we recover automatically.
//     } else {
//       PartitionDiskIndex2SubReader* r = new
//       PartitionDiskIndex2SubReader(file);
//       // TODO(Alec), the weight of the entry.
//       *handle = cache_->Insert(name, r, 1, &DeletePartitionIndex2Entry);
//     }
//   }
//   return s;
// }

Status TableCache::FindCloudTimeseriesFile(const std::string& dir,
                                           uint64_t tsid, int patch,
                                           Cache::Handle** handle) {
  Status s;
  std::string name = dir + "/" + std::to_string(tsid);
  if (patch != -1) name = name + "-patch" + std::to_string(patch);
  *handle = cache_->Lookup(name);
  if (*handle == nullptr) {
    RandomAccessFile* file = nullptr;
    s = env_->NewRandomAccessFile(name, &file);

    if (!s.ok()) {
      delete file;
    } else {
      CloudTimeseriesFile* r = new CloudTimeseriesFile(file, tsid);
      // TODO(Alec), the weight of the entry.
      *handle = cache_->Insert(name, r, 1, &DeleteCloudTimeseriesFileEntry);
    }
  }
  return s;
}

void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

}  // namespace leveldb
