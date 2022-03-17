// Copyright (c) 2017 Rockset

#pragma once

#include <deque>
#include <string>
#include <vector>

#include "leveldb/cloud/db_cloud.h"
#include "leveldb/db.h"
#include "leveldb/env.h"

namespace leveldb {

//
// All writes to this DB can be configured to be persisted
// in cloud storage.
//
class DBCloudImpl : public DBCloud {
  friend DBCloud;

 public:
  virtual ~DBCloudImpl();

 protected:
  // The CloudEnv used by this open instance.
  CloudEnv* cenv_;

 private:
  // Maximum manifest file size
  static const uint64_t max_manifest_file_size = 4 * 1024L * 1024L;

  explicit DBCloudImpl(DB* db);
};
}  // namespace leveldb
