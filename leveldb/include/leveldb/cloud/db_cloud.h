//  Copyright (c) 2017-present, Rockset

#pragma once
#ifndef ROCKSDB_LITE

#include <string>
#include <vector>

#include "leveldb/cloud/cloud_env_options.h"
#include "leveldb/db.h"
#include "leveldb/utilities/stackable_db.h"

namespace leveldb {

//
// Database with Cloud support.
//
// Important: The caller is responsible for ensuring that only one database at
// a time is running with the same cloud destination bucket and path. Running
// two databases concurrently with the same destination path will lead to
// corruption if it lasts for more than couple of minutes.
class DBCloud : public StackableDB {
 public:
  static Status Open(const Options& options, const std::string& name,
                     DBCloud** dbptr);

  virtual void PrintLevel(bool hex = false, bool print_stats = false) override {
    db_->PrintLevel(hex, print_stats);
  }

  virtual ~DBCloud() {}

 protected:
  explicit DBCloud(DB* db) : StackableDB(db) {}
};

}  // namespace leveldb
#endif  // ROCKSDB_LITE
