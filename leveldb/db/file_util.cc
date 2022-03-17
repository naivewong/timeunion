//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "db/file_util.h"

#include <algorithm>
#include <string>

#include "leveldb/env.h"

namespace leveldb {

// Utility function to copy a file up to a specified length
Status CopyFile(Env* env, const std::string& source,
                const std::string& destination, uint64_t size) {
  const EnvOptions soptions;
  Status s;
  SequentialFile* srcfile;
  WritableFile* destfile;

  {
    s = env->NewSequentialFile(source, &srcfile, soptions);
    if (!s.ok()) {
      delete srcfile;
      return s;
    }
    s = env->NewWritableFile(destination, &destfile, soptions);
    if (!s.ok()) {
      delete srcfile;
      delete destfile;
      return s;
    }

    if (size == 0) {
      // default argument means copy everything
      s = env->GetFileSize(source, &size);
      if (!s.ok()) {
        delete srcfile;
        delete destfile;
        return s;
      }
    }
  }

  char buffer[4096];
  Slice slice;
  while (size > 0) {
    size_t bytes_to_read = std::min(sizeof(buffer), static_cast<size_t>(size));
    s = srcfile->Read(bytes_to_read, &slice, buffer);
    if (!s.ok()) {
      delete srcfile;
      delete destfile;
      return s;
    }
    if (slice.size() == 0) {
      delete srcfile;
      delete destfile;
      return Status::Corruption("file too small");
    }
    s = destfile->Append(slice);
    if (!s.ok()) {
      delete srcfile;
      delete destfile;
      return s;
    }
    size -= slice.size();
  }
  s = destfile->Sync();
  delete srcfile;
  delete destfile;
  return s;
}

// Utility function to create a file with the provided contents
Status CreateFile(Env* env, const std::string& destination,
                  const std::string& contents) {
  const EnvOptions soptions;
  Status s;

  WritableFile* destfile;
  s = env->NewWritableFile(destination, &destfile, soptions);
  if (!s.ok()) {
    return s;
  }

  s = destfile->Append(Slice(contents));
  if (!s.ok()) {
    return s;
  }
  s = destfile->Sync();
  delete destfile;
  return s;
}

}  // namespace leveldb
