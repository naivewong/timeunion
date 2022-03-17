//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include "db/filename.h"
#include <string>

#include "leveldb/env.h"
#include "leveldb/status.h"

namespace leveldb {
// use_fsync maps to options.use_fsync, which determines the way that
// the file is synced after copying.
extern Status CopyFile(Env* env, const std::string& source,
                       const std::string& destination, uint64_t size);

extern Status CreateFile(Env* env, const std::string& destination,
                         const std::string& contents);

}  // namespace leveldb
