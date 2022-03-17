// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/options.h"

#include "leveldb/comparator.h"
#include "leveldb/env.h"

namespace leveldb {

// NOTE(Alec), default 30 minutes.
int64_t PARTITION_LENGTH = 1800000;

Options::Options() : comparator(BytewiseComparator()), env(Env::Default()) {}

}  // namespace leveldb
