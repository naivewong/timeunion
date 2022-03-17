// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <map>
#include <memory>
#include <string>

#include "leveldb/db.h"

#ifdef _WIN32
// Windows API macro interference
#undef DeleteFile
#endif

namespace leveldb {

// This class contains APIs to stack rocksdb wrappers.Eg. Stack TTL over base d
class StackableDB : public DB {
 public:
  // StackableDB take sole ownership of the underlying db.
  explicit StackableDB(DB* db) : db_(db) {}

  // StackableDB take shared ownership of the underlying db.
  explicit StackableDB(std::shared_ptr<DB> db)
      : db_(db.get()), shared_db_ptr_(db) {}

  ~StackableDB() {
    if (shared_db_ptr_ == nullptr) {
      delete db_;
    } else {
      assert(shared_db_ptr_.get() == db_);
    }
    db_ = nullptr;
  }

  virtual Status Close() override { return db_->Close(); }

  virtual DB* GetBaseDB() { return db_; }

  using DB::Put;
  virtual Status Put(const WriteOptions& options, const Slice& key,
                     const Slice& value) override {
    return db_->Put(options, key, value);
  }

  using DB::Get;
  virtual Status Get(const ReadOptions& options, const Slice& key,
                     std::string* value) override {
    return db_->Get(options, key, value);
  }

  using DB::Delete;
  virtual Status Delete(const WriteOptions& options,
                        const Slice& key) override {
    return db_->Delete(options, key);
  }

  virtual Status Write(const WriteOptions& opts, WriteBatch* updates) override {
    return db_->Write(opts, updates);
  }

  using DB::NewIterator;
  virtual Iterator* NewIterator(const ReadOptions& opts) override {
    return db_->NewIterator(opts);
  }

  virtual const Snapshot* GetSnapshot() override { return db_->GetSnapshot(); }

  virtual void ReleaseSnapshot(const Snapshot* snapshot) override {
    return db_->ReleaseSnapshot(snapshot);
  }

  using DB::GetProperty;
  virtual bool GetProperty(const Slice& property, std::string* value) override {
    return db_->GetProperty(property, value);
  }

  using DB::GetApproximateSizes;
  virtual void GetApproximateSizes(const Range* range, int n,
                                   uint64_t* sizes) override {
    db_->GetApproximateSizes(range, n, sizes);
  }

  using DB::CompactRange;
  virtual void CompactRange(const Slice* begin, const Slice* end) override {
    return db_->CompactRange(begin, end);
  }

  virtual DBQuerier* Querier(::tsdb::head::HeadType* head, int64_t mint,
                             int64_t maxt) override {
    return db_->Querier(head, mint, maxt);
  }

  virtual void SetHead(::tsdb::head::MMapHeadWithTrie* head) override {
    db_->SetHead(head);
  }

  virtual int64_t PartitionLength() override { return db_->PartitionLength(); }

  virtual Cache* BlockCache() override { return db_->BlockCache(); }

  // virtual const std::string& GetName() const { return db_->GetName(); }

  // virtual Status GetLiveFiles(std::vector<std::string>& vec, uint64_t* mfs,
  //                             bool flush_memtable = true) override {
  //   return db_->GetLiveFiles(vec, mfs, flush_memtable);
  // }

  // virtual Status DeleteFile(std::string name) override {
  //   return db_->DeleteFile(name);
  // }

 protected:
  DB* db_;
  std::shared_ptr<DB> shared_db_ptr_;
};

}  //  namespace leveldb
