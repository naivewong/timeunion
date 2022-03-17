// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_VERSION_EDIT_H_

#include "db/dbformat.h"
#include <set>
#include <utility>
#include <vector>

namespace leveldb {

class VersionSet;

struct FileMetaData {
  FileMetaData()
      : refs(0),
        allowed_seeks(1 << 30),
        file_size(0),
        start_id(0),
        end_id(0),
        num_patches(0) {}

  int refs;
  int allowed_seeks;  // Seeks allowed until compaction
  uint64_t number;
  uint64_t file_size;    // File size in bytes
  InternalKey smallest;  // Smallest internal key served by table
  InternalKey largest;   // Largest internal key served by table

  int64_t time_boundary;
  int64_t time_interval;

  // For level 2 SST.
  uint64_t start_id;
  uint64_t end_id;
  uint16_t num_patches;
};

class VersionEdit {
 private:
  typedef std::set<std::pair<int, uint64_t>> DeletedFileSet;

 public:
  VersionEdit() { Clear(); }
  ~VersionEdit() = default;

  void Clear();

  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }
  void SetLogNumber(uint64_t num) {
    has_log_number_ = true;
    log_number_ = num;
  }
  void SetPrevLogNumber(uint64_t num) {
    has_prev_log_number_ = true;
    prev_log_number_ = num;
  }
  void SetNextFile(uint64_t num) {
    has_next_file_number_ = true;
    next_file_number_ = num;
  }
  void SetLastSequence(SequenceNumber seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }
  void SetCompactPointer(int level, const InternalKey& key) {
    compact_pointers_.push_back(std::make_pair(level, key));
  }

  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  void AddFile(int level, uint64_t file, uint64_t file_size,
               const InternalKey& smallest, const InternalKey& largest,
               int64_t starting_time, int64_t interval) {
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    f.time_boundary = starting_time;
    f.time_interval = interval;
    new_files_.push_back(std::make_pair(level, f));
  }

  void AddFile(int level, uint64_t file, uint64_t file_size,
               const InternalKey& smallest, const InternalKey& largest,
               int64_t starting_time, int64_t interval, uint64_t sid,
               uint64_t eid, uint16_t patches) {
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    f.time_boundary = starting_time;
    f.time_interval = interval;
    f.start_id = sid;
    f.end_id = eid;
    f.num_patches = patches;
    new_files_.push_back(std::make_pair(level, f));
  }

  // Delete the specified "file" from the specified "level".
  void RemoveFile(int level, uint64_t file) {
    deleted_files_.insert(std::make_pair(level, file));
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);

  std::string DebugString() const;

  const std::vector<std::pair<int, FileMetaData>>& GetNewFiles() {
    return new_files_;
  }

  const DeletedFileSet& GetDeletedFiles() { return deleted_files_; }

  bool GetNextFileNumber(uint64_t* result) const {
    if (has_next_file_number_) {
      *result = next_file_number_;
    }
    return has_next_file_number_;
  }

 private:
  friend class VersionSet;

  std::string comparator_;
  uint64_t log_number_;
  uint64_t prev_log_number_;
  uint64_t next_file_number_;
  SequenceNumber last_sequence_;
  bool has_comparator_;
  bool has_log_number_;
  bool has_prev_log_number_;
  bool has_next_file_number_;
  bool has_last_sequence_;

  std::vector<std::pair<int, InternalKey>> compact_pointers_;
  DeletedFileSet deleted_files_;
  std::vector<std::pair<int, FileMetaData>> new_files_;
  std::vector<std::pair<int, int64_t>> time_partition_indexes_;
  std::set<int64_t> time_partitions_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_EDIT_H_
