// Copyright (c) 2017 Rockset

#pragma once

#include <set>
#include <string>
#include <vector>

#include "leveldb/db.h"
#include "leveldb/env.h"

#include "cloud/cloud_env_impl.h"

namespace leveldb {

//
// Operates on MANIFEST files stored in the cloud bucket
//
class ManifestReader {
 public:
  ManifestReader(std::shared_ptr<Logger> info_log, CloudEnv* cenv,
                 const std::string& bucket_prefix);

  virtual ~ManifestReader();

  // Retrieve all live files referred to by this bucket path
  Status GetLiveFiles(const std::string bucket_path, std::set<uint64_t>* list);

  static Status GetMaxFileNumberFromManifest(Env* env, const std::string& fname,
                                             uint64_t* maxFileNumber);

 private:
  std::shared_ptr<Logger> info_log_;
  CloudEnv* cenv_;
  std::string bucket_prefix_;
};
}  // namespace leveldb
