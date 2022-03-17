// Copyright (c) 2017 Rockset.

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "cloud/db_cloud_impl.h"

#include "db/file_util.h"
#include <inttypes.h>

#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/options.h"
#include "leveldb/status.h"
#include "leveldb/table.h"

#include "util/xxhash.h"

#include "cloud/aws/aws_env.h"
#include "cloud/filename.h"
#include "cloud/manifest_reader.h"

namespace leveldb {

DBCloudImpl::DBCloudImpl(DB* db) : DBCloud(db), cenv_(nullptr) {}

DBCloudImpl::~DBCloudImpl() {}

Status DBCloud::Open(const Options& options, const std::string& name,
                     DBCloud** dbptr) {
  Status st;
  DB* db = nullptr;
  st = DB::Open(options, name, &db);
  if (st.ok()) {
    DBCloudImpl* cloud = new DBCloudImpl(db);
    *dbptr = cloud;
  }
  Log(InfoLogLevel::INFO_LEVEL, options.info_log,
      "Opened cloud db with local dir %s. %s", name.c_str(),
      st.ToString().c_str());

  return st;
}

}  // namespace leveldb
