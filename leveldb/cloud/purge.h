// Copyright (c) 2017 Rockset

#pragma once

#include <deque>
#include <string>
#include <vector>

#include "leveldb/db.h"
#include "leveldb/env.h"

#include "cloud/cloud_env_impl.h"

namespace leveldb {

//
// Purges all unneeded files in a storage bucket
//
class Purge {
 public:
  // The bucket name is specified when the CloudEnv was created.
  Purge(CloudEnvImpl* env, std::shared_ptr<Logger> info_log);

  virtual ~Purge();

  // Remove any un-needed files from the storage bucket
  virtual Status PurgeObsoleteFiles();

  // Remove any dbids that do not have backing files in S3
  virtual Status PurgeObsoleteDbid();

 private:
  CloudEnvImpl* cenv_;
  std::shared_ptr<Logger> info_log_;
};
}  // namespace leveldb
