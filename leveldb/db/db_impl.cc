// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include "db/builder.h"
#include "db/db_iter.h"
#include "db/db_querier.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/partition_index.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <limits>
#include <set>
#include <string>
#include <vector>

#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"

#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"

#include "chunk/XORChunk.hpp"

namespace leveldb {

const int kNumNonTableCacheFiles = 10;

void encodeKey(std::string* str, const std::string& metric, uint64_t tsid,
               int64_t t) {
  // str->push_back(static_cast<char>(metric.size()));
  // str->append(metric.data(), metric.size());
  PutFixed64BE(str, tsid);
  PutFixed64BE(str, t);
}

void decodeKey(const Slice& s, std::string* metric, uint64_t* tsid,
               int64_t* t) {
  // uint8_t ms = static_cast<uint8_t>(s.data()[0]);
  // if (metric)
  //   metric->append(s.data() + 1, ms);
  if (tsid) *tsid = DecodeFixed64BE(s.data());
  if (t) *t = DecodeFixed64BE(s.data() + 8);
}

ValueType logType(const Slice& s) { return ValueType(s.data()[0]); }

Status decodeLogKey(const Slice& s, Slice* key) {
  Slice input = s;
  char tag = input[0];
  input.remove_prefix(1);
  switch (tag) {
    case kTypeValue:
      if (GetLengthPrefixedSlice(&input, key)) {
      } else {
        return Status::Corruption("bad Put");
      }
      break;
    case kTypeDeletion:
      if (!GetLengthPrefixedSlice(&input, key)) {
        return Status::Corruption("bad Delete");
      }
      break;
    default:
      return Status::Corruption("unknown tag");
  }
  return Status::OK();
}

Status decodeLogKV(const Slice& s, Slice* key, Slice* value) {
  Slice input = s;
  char tag = input[0];
  input.remove_prefix(1);
  switch (tag) {
    case kTypeValue:
      if (GetLengthPrefixedSlice(&input, key) &&
          GetLengthPrefixedSlice(&input, value)) {
      } else {
        return Status::Corruption("bad Put");
      }
      break;
    case kTypeDeletion:
      if (!GetLengthPrefixedSlice(&input, key)) {
        return Status::Corruption("bad Delete");
      }
      break;
    default:
      return Status::Corruption("unknown tag");
  }
  return Status::OK();
}

Status decodeLogIndex(const Slice& s, uint64_t* tsid, int64_t* time_boundary,
                      ::tsdb::label::Labels& lset) {
  Slice input = s;
  int idx = 0;
  while (input.size() > 0) {
    char tag = input[0];
    input.remove_prefix(1);
    if (tag != kTypeIndex) return Status::Corruption("not index type");
    Slice key, value;
    if (GetLengthPrefixedSlice(&input, &key) &&
        GetLengthPrefixedSlice(&input, &value)) {
      if (idx == 0) {
        if (key.compare("tsid") != 0)
          return Status::Corruption("first index tag is not tsid");
        *tsid = std::stoull(std::string(value.data(), value.size()));
      } else if (idx == 1) {
        if (key.compare("tbound") != 0)
          return Status::Corruption("second index tag is not tbound");
        *time_boundary = std::stoll(std::string(value.data(), value.size()));
      } else {
        lset.emplace_back(std::string(key.data(), key.size()),
                          std::string(value.data(), value.size()));
      }
      idx++;
    } else {
      return Status::Corruption("Bad PutIndex");
    }
  }
  return Status::OK();
}

// Information kept for every waiting writer
struct DBImpl::Writer {
  explicit Writer(port::Mutex* mu)
      : batch(nullptr), sync(false), done(false), cv(mu) {}

  Status status;
  WriteBatch* batch;
  bool sync;
  bool done;
  port::CondVar cv;
};

struct DBImpl::CompactionState {
  // Files produced by compaction
  struct Output {
    Output()
        : number(0),
          file_size(0),
          time_boundary(0),
          time_interval(0),
          start_id(0),
          end_id(0),
          patch(0) {}
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;

    int64_t time_boundary;
    int64_t time_interval;

    uint64_t start_id;
    uint64_t end_id;
    uint16_t patch;
  };

  // Output* current_output() { return &outputs[outputs.size() - 1]; }

  bool partition_file_opened(int64_t t) {
    for (size_t i = 0; i < outputs.size(); i++) {
      if (t >= outputs[i][0].time_boundary &&
          t < outputs[i][0].time_boundary + outputs[i][0].time_interval &&
          open_files_[i])
        return true;
    }
    return false;
  }

  void close_partition_file(int64_t t) {
    for (size_t i = 0; i < outputs.size(); i++) {
      if (t >= outputs[i][0].time_boundary &&
          t < outputs[i][0].time_boundary + outputs[i][0].time_interval) {
        open_files_[i] = false;
        set_smallest_key_[i] = false;
        break;
      }
    }
  }

  int get_partition_idx(int64_t t) {
    for (size_t i = 0; i < outputs.size(); i++) {
      if (t >= outputs[i][0].time_boundary &&
          t < outputs[i][0].time_boundary + outputs[i][0].time_interval)
        return i;
    }
    return -1;
  }

  int insert(const Output& o, WritableFile* f, TableBuilder* b) {
    size_t i = 0;
    for (; i < outputs.size(); i++) {
      if (outputs[i][0].time_boundary >= o.time_boundary) break;
    }
    if (i >= outputs.size()) {
      outputs.push_back(std::vector<Output>({o}));
      outfiles.push_back(std::vector<WritableFile*>({f}));
      builders.push_back(std::vector<TableBuilder*>({b}));
      open_files_.push_back(true);
      set_smallest_key_.push_back(false);
      end_ids_.push_back(0);
    } else if (outputs[i][0].time_boundary == o.time_boundary) {
      outputs[i].push_back(o);
      outfiles[i].push_back(f);
      builders[i].push_back(b);
      open_files_[i] = true;
      set_smallest_key_[i] = false;
      end_ids_[i] = 0;
    } else {
      outputs.insert(outputs.begin() + i, std::vector<Output>({o}));
      outfiles.insert(outfiles.begin() + i, std::vector<WritableFile*>({f}));
      builders.insert(builders.begin() + i, std::vector<TableBuilder*>({b}));
      open_files_.insert(open_files_.begin() + i, true);
      set_smallest_key_.insert(set_smallest_key_.begin() + i, false);
      end_ids_.insert(end_ids_.begin() + i, 0);
    }
    return i;
  }

  bool l2_partition_file_opened(int64_t t, uint64_t tsid) {
    for (size_t i = 0; i < l2_outputs.size(); i++) {
      if (t >= l2_outputs[i][0][0].time_boundary &&
          t < l2_outputs[i][0][0].time_boundary +
                  l2_outputs[i][0][0].time_interval) {
        for (size_t j = 0; j < l2_outputs[i].size(); j++) {
          if (tsid >= l2_outputs[i][j][0].start_id) {
            if ((j + 1 < l2_outputs[i].size() &&
                 tsid < l2_outputs[i][j + 1][0].start_id) ||
                j == l2_outputs[i].size() - 1 ||
                tsid <= l2_outputs[i][j][0].end_id)
              return l2_open_files_[i][j];
          }
        }
      }
    }
    return false;
  }

  void l2_close_partition_file(int64_t t, uint64_t tsid) {
    for (size_t i = 0; i < l2_outputs.size(); i++) {
      if (t >= l2_outputs[i][0][0].time_boundary &&
          t < l2_outputs[i][0][0].time_boundary +
                  l2_outputs[i][0][0].time_interval) {
        for (size_t j = 0; j < l2_outputs[i].size(); j++) {
          if (tsid >= l2_outputs[i][j][0].start_id) {
            if ((j + 1 < l2_outputs[i].size() &&
                 tsid < l2_outputs[i][j + 1][0].start_id) ||
                j == l2_outputs[i].size() - 1 ||
                tsid <= l2_outputs[i][j][0].end_id) {
              l2_open_files_[i][j] = false;
              l2_set_smallest_key_[i][j] = false;
              return;
            }
          }
        }
      }
    }
  }

  void l2_get_partition_idx(int64_t t, uint64_t tsid, int* idx1, int* idx2) {
    for (size_t i = 0; i < l2_outputs.size(); i++) {
      if (t >= l2_outputs[i][0][0].time_boundary &&
          t < l2_outputs[i][0][0].time_boundary +
                  l2_outputs[i][0][0].time_interval) {
        for (size_t j = 0; j < l2_outputs[i].size(); j++) {
          if (tsid >= l2_outputs[i][j][0].start_id) {
            if ((j + 1 < l2_outputs[i].size() &&
                 tsid < l2_outputs[i][j + 1][0].start_id) ||
                j == l2_outputs[i].size() - 1 ||
                tsid <= l2_outputs[i][j][0].end_id) {
              *idx1 = i;
              *idx2 = j;
              return;
            }
          }
        }
      }
    }
    *idx1 = -1;
    *idx2 = -1;
  }

  void l2_insert(const Output& o, WritableFile* f, TableBuilder* b) {
    size_t i = 0;
    for (; i < l2_outputs.size(); i++) {
      if (l2_outputs[i][0][0].time_boundary >= o.time_boundary) break;
    }
    if (i >= l2_outputs.size()) {
      l2_outputs.push_back(std::vector<std::vector<Output>>({{o}}));
      l2_outfiles.push_back(std::vector<std::vector<WritableFile*>>({{f}}));
      l2_builders.push_back(std::vector<std::vector<TableBuilder*>>({{b}}));
      l2_open_files_.push_back(std::vector<bool>({true}));
      l2_set_smallest_key_.push_back(std::vector<bool>({false}));
      l2_end_ids_.push_back(std::vector<uint64_t>({0}));
    } else if (l2_outputs[i][0][0].time_boundary == o.time_boundary) {
      size_t j = 0;
      for (; j < l2_outputs[i].size(); j++) {
        if (l2_outputs[i][j][0].start_id >= o.start_id) break;
      }
      if (j >= l2_outputs[i].size()) {
        l2_outputs[i].push_back(std::vector<Output>({o}));
        l2_outfiles[i].push_back(std::vector<WritableFile*>({f}));
        l2_builders[i].push_back(std::vector<TableBuilder*>({b}));
        l2_open_files_[i].push_back(true);
        l2_set_smallest_key_[i].push_back(false);
        l2_end_ids_[i].push_back(0);
      } else if (l2_outputs[i][j][0].start_id == o.start_id) {
        l2_outputs[i][j].push_back(o);
        l2_outfiles[i][j].push_back(f);
        l2_builders[i][j].push_back(b);
        l2_open_files_[i][j] = true;
        l2_set_smallest_key_[i][j] = false;
        l2_end_ids_[i][j] = 0;
      } else {
        l2_outputs[i].insert(l2_outputs[i].begin() + j,
                             std::vector<Output>({o}));
        l2_outfiles[i].insert(l2_outfiles[i].begin() + j,
                              std::vector<WritableFile*>({f}));
        l2_builders[i].insert(l2_builders[i].begin() + j,
                              std::vector<TableBuilder*>({b}));
        l2_open_files_[i].insert(l2_open_files_[i].begin() + j, true);
        l2_set_smallest_key_[i].insert(l2_set_smallest_key_[i].begin() + j,
                                       false);
        l2_end_ids_[i].insert(l2_end_ids_[i].begin() + j, 0);
      }
    } else {
      l2_outputs.insert(l2_outputs.begin() + i,
                        std::vector<std::vector<Output>>({{o}}));
      l2_outfiles.insert(l2_outfiles.begin() + i,
                         std::vector<std::vector<WritableFile*>>({{f}}));
      l2_builders.insert(l2_builders.begin() + i,
                         std::vector<std::vector<TableBuilder*>>({{b}}));
      l2_open_files_.insert(l2_open_files_.begin() + i,
                            std::vector<bool>({true}));
      l2_set_smallest_key_.insert(l2_set_smallest_key_.begin() + i,
                                  std::vector<bool>({false}));
      l2_end_ids_.insert(l2_end_ids_.begin() + i, std::vector<uint64_t>({0}));
    }
  }

  explicit CompactionState(Compaction* c)
      : compaction(c),
        smallest_snapshot(0),
        // outfile(nullptr),
        // builder(nullptr),
        total_bytes(0) {}

  Compaction* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;

  std::vector<std::vector<Output>> outputs;

  // State kept for output being generated
  // WritableFile* outfile;
  // TableBuilder* builder;
  std::vector<std::vector<WritableFile*>> outfiles;
  std::vector<std::vector<TableBuilder*>> builders;

  std::vector<bool> open_files_;
  std::vector<bool> set_smallest_key_;
  std::vector<uint64_t> end_ids_;

  // First dimension: time partition.
  // Second dimension: tsid.
  std::vector<std::vector<std::vector<Output>>> l2_outputs;
  std::vector<std::vector<std::vector<WritableFile*>>> l2_outfiles;
  std::vector<std::vector<std::vector<TableBuilder*>>> l2_builders;
  std::vector<std::vector<bool>> l2_open_files_;
  std::vector<std::vector<bool>> l2_set_smallest_key_;
  std::vector<std::vector<uint64_t>> l2_end_ids_;

  uint64_t total_bytes;
};

// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != nullptr) ? ipolicy : nullptr;
  ClipToRange(&result.max_open_files, 64 + kNumNonTableCacheFiles, 50000);
  ClipToRange(&result.write_buffer_size, 64 << 10, 1 << 30);
  ClipToRange(&result.max_file_size, 1 << 16, 1 << 30);
  ClipToRange(&result.block_size, 1 << 10, 4 << 20);
  if (result.info_log == nullptr) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = nullptr;
    }
  }
  if (result.block_cache == nullptr) {
    result.block_cache = NewLRUCache(8 << 20);
  }
  return result;
}

static int TableCacheSize(const Options& sanitized_options) {
  // Reserve ten files or so for other uses and give the rest to TableCache.
  return sanitized_options.max_open_files - kNumNonTableCacheFiles;
}

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      seq_asce_internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      table_cache_(new TableCache(dbname_, options_, TableCacheSize(options_))),
      db_lock_(nullptr),
      shutting_down_(false),
      background_work_finished_signal_(&mutex_),
      mem_(nullptr),
      // imm_(nullptr),
      has_imm_(false),
      logfile_(nullptr),
      logfile_number_(0),
      log_(nullptr),
      seed_(0),
      tmp_batch_(new WriteBatch),
      background_compaction_scheduled_(false),
      manual_compaction_(nullptr),
      purge_compaction_(nullptr),
      versions_(new VersionSet(dbname_, &options_, table_cache_,
                               &internal_comparator_,
                               &seq_asce_internal_comparator_)),
      head_(nullptr) {}

DBImpl::~DBImpl() {
  // Wait for background work to finish.
  mutex_.Lock();
  shutting_down_.store(true, std::memory_order_release);
  while (background_compaction_scheduled_) {
    background_work_finished_signal_.Wait();
  }
  mutex_.Unlock();

  if (db_lock_ != nullptr) {
    env_->UnlockFile(db_lock_);
  }

  delete versions_;
  if (mem_ != nullptr) mem_->Unref();
  // if (imm_ != nullptr) imm_->Unref();
  for (auto* m : imms_) m->Unref();
  delete tmp_batch_;
  if (options_.use_log) {
    delete log_;
    delete logfile_;
  }
  delete table_cache_;

  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    delete options_.block_cache;
  }
}

Status DBImpl::NewDB() {
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

  const std::string manifest = DescriptorFileName(dbname_, 1);
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file);
  if (!s.ok()) {
    return s;
  }
  {
    log::Writer log(file);
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1);
  } else {
    env_->RemoveFile(manifest);
  }
  return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

void DBImpl::RemoveObsoleteFiles() {
  mutex_.AssertHeld();

  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // Make a set of all of the live files
  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFiles(&live);

  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames);  // Ignoring errors on purpose
  uint64_t number;
  FileType type;
  std::vector<std::string> files_to_delete;
  for (std::string& filename : filenames) {
    if (ParseFileName(filename, &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
          keep = ((number >= versions_->LogNumber()) ||
                  (number == versions_->PrevLogNumber()));
          break;
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
          keep = (live.find(number) != live.end());
          break;
        case kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) {
        files_to_delete.push_back(std::move(filename));
        if (type == kTableFile) {
          table_cache_->Evict(number);
        }
        Log(options_.info_log, "Delete type=%s #%lld\n",
            FileTypeName(type).c_str(),
            static_cast<unsigned long long>(number));
      }
    }
  }

  // While deleting all files unblock other threads. All files being deleted
  // have unique names which will not collide with newly created files and
  // are therefore safe to delete while allowing other threads to proceed.
  mutex_.Unlock();
  for (const std::string& filename : files_to_delete) {
    env_->RemoveFile(dbname_ + "/" + filename);
  }
  mutex_.Lock();
}

Status DBImpl::Recover(VersionEdit* edit, bool* save_manifest) {
  mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  env_->CreateDir(dbname_);
  assert(db_lock_ == nullptr);
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
  if (!s.ok()) {
    return s;
  }

  if (!env_->FileExists(CurrentFileName(dbname_))) {
    if (options_.create_if_missing) {
      Log(options_.info_log, "Creating DB %s since it was missing.",
          dbname_.c_str());
      s = NewDB();
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(dbname_,
                                     "exists (error_if_exists is true)");
    }
  }

  s = versions_->Recover(save_manifest);
  if (!s.ok()) {
    return s;
  }
  SequenceNumber max_sequence(0);

  // Recover from all newer log files than the ones named in the
  // descriptor (new log files may have been added by the previous
  // incarnation without registering them in the descriptor).
  //
  // Note that PrevLogNumber() is no longer used, but we pay
  // attention to it in case we are recovering a database
  // produced by an older version of leveldb.
  const uint64_t min_log = versions_->LogNumber();
  const uint64_t prev_log = versions_->PrevLogNumber();
  std::vector<std::string> filenames;
  s = env_->GetChildren(dbname_, &filenames);
  if (!s.ok()) {
    return s;
  }
  std::set<uint64_t> expected;
  versions_->AddLiveFiles(&expected);
  uint64_t number;
  FileType type;
  std::vector<uint64_t> logs;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      expected.erase(number);
      if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
        logs.push_back(number);
    }
  }
  if (!expected.empty()) {
    char buf[50];
    std::snprintf(buf, sizeof(buf), "%d missing files; e.g.",
                  static_cast<int>(expected.size()));
    return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
  }

  // Recover in the order in which the logs were generated
  std::sort(logs.begin(), logs.end());
  for (size_t i = 0; i < logs.size(); i++) {
    s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit,
                       &max_sequence);
    if (!s.ok()) {
      return s;
    }

    // The previous incarnation may not have written any MANIFEST
    // records after allocating this log number.  So we manually
    // update the file number allocation counter in VersionSet.
    versions_->MarkFileNumberUsed(logs[i]);
  }

  if (versions_->LastSequence() < max_sequence) {
    versions_->SetLastSequence(max_sequence);
  }

  return Status::OK();
}

Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log,
                              bool* save_manifest, VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // null if options_.paranoid_checks==false
    void Corruption(size_t bytes, const Status& s) override {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == nullptr ? "(ignoring error) " : ""), fname,
          static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != nullptr && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : nullptr);
  // We intentionally make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true /*checksum*/, 0 /*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long)log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  int compactions = 0;
  MemTable* mem = nullptr;

  int64_t boundary = -1;
  int64_t decode_t;
  Slice key;

  while (reader.ReadRecord(&record, &scratch) && status.ok()) {
    if (record.size() < 12) {
      reporter.Corruption(record.size(),
                          Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);

    if (mem == nullptr) {
      mem = new MemTable(internal_comparator_);
      mem->Ref();
    }
    status = WriteBatchInternal::InsertInto(&batch, mem);
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq = WriteBatchInternal::Sequence(&batch) +
                                    WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
      compactions++;
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, nullptr);
      mem->Unref();
      mem = nullptr;
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
    }
  }

  delete file;

  // See if we should keep reusing the last log file.
  if (status.ok() && options_.reuse_logs && last_log && compactions == 0) {
    assert(logfile_ == nullptr);
    assert(log_ == nullptr);
    assert(mem_ == nullptr);
    uint64_t lfile_size;
    if (env_->GetFileSize(fname, &lfile_size).ok() &&
        env_->NewAppendableFile(fname, &logfile_).ok()) {
      Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
      log_ = new log::Writer(logfile_, lfile_size);
      logfile_number_ = log_number;
      if (mem != nullptr) {
        mem_ = mem;
        mem = nullptr;
      } else {
        // mem can be nullptr if lognum exists but was empty.
        mem_ = new MemTable(internal_comparator_);
        mem_->Ref();
      }
    }
  }

  if (mem != nullptr) {
    // mem did not get reused; compact it.
    if (status.ok()) {
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, nullptr);
    }
    mem->Unref();
  }

  return status;
}

Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base) {
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  std::vector<FileMetaData> metas;
  Iterator* iter = mem->NewIterator();
  Log(options_.info_log, "Level-0 table x: started");

  Status s;
  {
    mutex_.Unlock();
    s = BuildTables(dbname_, env_, options_, table_cache_, iter, &metas,
                    versions_, &pending_outputs_, PARTITION_LENGTH, head_);
    mutex_.Lock();
  }

  for (auto& meta : metas)
    Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
        (unsigned long long)meta.number, (unsigned long long)meta.file_size,
        s.ToString().c_str());
  delete iter;

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok()) {
    std::cout << "--------------------------------" << std::endl;
    uint64_t tmp_duration = env_->NowMicros() - start_micros;
    for (auto& meta : metas) {
      if (meta.file_size > 0) {
        // const Slice min_user_key = meta.smallest.user_key();
        // const Slice max_user_key = meta.largest.user_key();
        // if (base != nullptr) {
        //   level = base->PickLevelForMemTableOutput(min_user_key,
        //   max_user_key);
        // }
        edit->AddFile(level, meta.number, meta.file_size, meta.smallest,
                      meta.largest, meta.time_boundary, meta.time_interval);
        LOG_DEBUG << "time_boundary:" << meta.time_boundary
                  << " interval:" << meta.time_interval << " fd:" << meta.number
                  << " duration:" << tmp_duration << "us";
      }
    }
    std::cout << "--------------------------------" << std::endl;
  }

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  for (auto& meta : metas) stats.bytes_written += meta.file_size;
  stats_[level].Add(stats);
  return s;
}

void DBImpl::CompactMemTable() {
  mutex_.AssertHeld();
  // assert(imm_ != nullptr);
  assert(!imms_.empty());
  MemTable* imm = imms_[0];
  imms_.erase(imms_.begin());

  // Save the contents of the memtable as a new Table
  VersionEdit edit;
  Version* base = versions_->current();
  base->Ref();
  Status s = WriteLevel0Table(imm, &edit, base);
  base->Unref();

  if (s.ok() && shutting_down_.load(std::memory_order_acquire)) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    s = versions_->LogAndApply(&edit, &mutex_);
  }

  if (s.ok()) {
    // Commit to the new state
    imm->Unref();
    imm = nullptr;
    if (imms_.empty()) has_imm_.store(false, std::memory_order_release);
    RemoveObsoleteFiles();
  } else {
    RecordBackgroundError(s);
  }
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_);
    Version* base = versions_->current();
    for (int level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }
  TEST_CompactMemTable();  // TODO(sanjay): Skip if memtable does not overlap
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
}

void DBImpl::TEST_CompactRange(int level, const Slice* begin,
                               const Slice* end) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.level = level;
  manual.done = false;
  if (begin == nullptr) {
    manual.begin = nullptr;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == nullptr) {
    manual.end = nullptr;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done && !shutting_down_.load(std::memory_order_acquire) &&
         bg_error_.ok()) {
    if (manual_compaction_ == nullptr) {  // Idle
      manual_compaction_ = &manual;
      MaybeScheduleCompaction();
    } else {  // Running either my compaction or another compaction.
      background_work_finished_signal_.Wait();
    }
  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compaction since we aborted early for some reason.
    manual_compaction_ = nullptr;
  }
}

Status DBImpl::TEST_CompactMemTable() {
  // nullptr batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), nullptr);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    // while (imm_ != nullptr && bg_error_.ok()) {
    //   background_work_finished_signal_.Wait();
    // }
    // if (imm_ != nullptr) {
    //   s = bg_error_;
    // }
    while (!imms_.empty() && bg_error_.ok()) {
      background_work_finished_signal_.Wait();
    }
    if (!imms_.empty()) {
      s = bg_error_;
    }
  }
  return s;
}

void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    background_work_finished_signal_.SignalAll();
  }
}

void DBImpl::MaybeScheduleCompaction() {
  mutex_.AssertHeld();
  if (background_compaction_scheduled_) {
    // Already scheduled
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
    // } else if (imm_ == nullptr && manual_compaction_ == nullptr &&
  } else if (imms_.empty() && manual_compaction_ == nullptr &&
             purge_compaction_ == nullptr && !versions_->NeedsCompaction()) {
    // No work to be done
  } else {
    background_compaction_scheduled_ = true;
    env_->Schedule(&DBImpl::BGWork, this);
    // if (imm_ != nullptr) {
    //   background_compaction_scheduled_ = true;
    //   env_->Schedule(&DBImpl::BGWork, this);
    // }
  }
}

void DBImpl::BGWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCall();
}

void DBImpl::BackgroundCall() {
  MutexLock l(&mutex_);
  assert(background_compaction_scheduled_);
  if (shutting_down_.load(std::memory_order_acquire)) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundCompaction();
  }

  background_compaction_scheduled_ = false;

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  MaybeScheduleCompaction();
  background_work_finished_signal_.SignalAll();
}

void DBImpl::PurgeTime(int64_t time) {
  MutexLock l(&mutex_);
  purge_compaction_ = versions_->NewPurgeCompaction(time);
  MaybeScheduleCompaction();
}

void DBImpl::BackgroundCompaction() {
  mutex_.AssertHeld();

  // if (imm_ != nullptr) {
  if (!imms_.empty()) {
    CompactMemTable();
    return;
  }

  Compaction* c;
  bool is_manual = (manual_compaction_ != nullptr);
  InternalKey manual_end;
  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    c = versions_->CompactRange(m->level, m->begin, m->end);
    m->done = (c == nullptr);
    if (c != nullptr) {
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
    }
    Log(options_.info_log,
        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        m->level, (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else if (purge_compaction_ != nullptr) {
    purge_compaction_->AddPurgeDeletions(purge_compaction_->edit());
    Status status = versions_->LogAndApply(purge_compaction_->edit(), &mutex_);
    purge_compaction_->ReleaseInputs();
    RemoveObsoleteFiles();
    LOG_DEBUG << "[-] purge (" << purge_compaction_->purge_time() << ")"
              << " status:" << status.ToString()
              << " #files(L0):" << purge_compaction_->num_purge_files(0)
              << " #files(L1):" << purge_compaction_->num_purge_files(1)
              << " #files(L2):" << purge_compaction_->num_purge_files(2);
    purge_compaction_ = nullptr;
  } else {
    c = versions_->PickCompaction();
  }

  Status status;
  if (c == nullptr) {
  } else {
    CompactionState* compact = new CompactionState(c);
    status = DoCompactionWork(compact);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    CleanupCompaction(compact);
    c->ReleaseInputs();
    RemoveObsoleteFiles();

    // PrintLevel();
  }
  delete c;

  if (status.ok()) {
    // Done
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log, "Compaction error: %s", status.ToString().c_str());
  }

  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    if (!status.ok()) {
      m->done = true;
    }
    if (!m->done) {
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      m->tmp_storage = manual_end;
      m->begin = &m->tmp_storage;
    }
    manual_compaction_ = nullptr;
  }
}

void DBImpl::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld();
  if (compact->compaction->level() == 1) {
    for (size_t i = 0; i < compact->l2_builders.size(); i++) {
      for (size_t j = 0; j < compact->l2_builders[i].size(); j++) {
        for (size_t k = 0; k < compact->l2_builders[i][j].size(); k++) {
          if (compact->l2_builders[i][j][k] != nullptr) {
            compact->l2_builders[i][j][k]->Abandon();
            delete compact->l2_builders[i][j][k];
          }
        }
      }
    }
    for (size_t i = 0; i < compact->l2_outfiles.size(); i++) {
      for (size_t j = 0; j < compact->l2_outfiles[i].size(); j++) {
        for (size_t k = 0; k < compact->l2_outfiles[i][j].size(); k++) {
          delete compact->l2_outfiles[i][j][k];
        }
      }
    }
    for (size_t i = 0; i < compact->l2_outputs.size(); i++) {
      for (size_t j = 0; j < compact->l2_outputs[i].size(); j++) {
        for (size_t k = 0; k < compact->l2_outputs[i][j].size(); k++) {
          const CompactionState::Output& out = compact->l2_outputs[i][j][k];
          pending_outputs_.erase(out.number);
        }
      }
    }
  } else {
    for (size_t i = 0; i < compact->builders.size(); i++) {
      for (size_t j = 0; j < compact->builders[i].size(); j++) {
        if (compact->builders[i][j] != nullptr) {
          compact->builders[i][j]->Abandon();
          delete compact->builders[i][j];
        }
      }
    }
    for (size_t i = 0; i < compact->outfiles.size(); i++) {
      for (size_t j = 0; j < compact->outfiles[i].size(); j++) {
        delete compact->outfiles[i][j];
      }
    }
    for (size_t i = 0; i < compact->outputs.size(); i++) {
      for (size_t j = 0; j < compact->outputs[i].size(); j++) {
        const CompactionState::Output& out = compact->outputs[i][j];
        pending_outputs_.erase(out.number);
      }
    }
  }
  delete compact;
}

Status DBImpl::OpenCompactionOutputFile(CompactionState* compact,
                                        int64_t boundary, int64_t interval,
                                        uint64_t start_id) {
  assert(compact != nullptr);
  // assert(compact->builder == nullptr);
  uint64_t file_number;
  CompactionState::Output out;
  {
    mutex_.Lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    out.time_boundary = boundary;
    out.time_interval = interval;
    if (compact->compaction->level() == 1) out.start_id = start_id;
    out.end_id = 0;
    // out.start_id = compact->compaction->start_id();
    // out.end_id = compact->compaction->end_id();
    mutex_.Unlock();
  }

  WritableFile* outfile;
  TableBuilder* builder;

  // Make the output file
  std::string fname = TableFileName(dbname_, file_number);
  Status s;
  if (env_->GetCloudEnvOptionsPtr() != nullptr) {
    if (compact->compaction->level() == 1)
      s = env_->NewStringWritableFile(fname, &outfile);
    else
      s = env_->GetBaseEnv()->NewWritableFile(fname, &outfile);
  } else
    s = env_->NewWritableFile(fname, &outfile);
  if (s.ok()) {
    builder = new TableBuilder(options_, outfile);
  }
  if (compact->compaction->level() == 1)
    compact->l2_insert(out, outfile, builder);
  else
    compact->insert(out, outfile, builder);
  return s;
}

Status DBImpl::OpenCompactionOutputPatchFile(CompactionState* compact,
                                             uint64_t start_id, uint64_t end_id,
                                             int patch, int64_t boundary,
                                             int64_t interval) {
  assert(compact != nullptr);
  // assert(compact->builder == nullptr);
  uint64_t file_number;
  CompactionState::Output out;
  {
    mutex_.Lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    out.time_boundary = boundary;
    out.time_interval = interval;
    out.start_id = start_id;
    out.end_id = end_id;
    out.patch = patch;
    mutex_.Unlock();
  }

  WritableFile* outfile;
  TableBuilder* builder;

  // Make the output file
  std::string fname = TableFileName(dbname_, file_number);
  Status s;
  if (env_->GetCloudEnvOptionsPtr() != nullptr) {
    if (compact->compaction->level() == 1)
      s = env_->NewStringWritableFile(fname, &outfile);
    else
      s = env_->GetBaseEnv()->NewWritableFile(fname, &outfile);
  } else
    s = env_->NewWritableFile(fname, &outfile);
  if (s.ok()) {
    builder = new TableBuilder(options_, outfile);
  }
  compact->l2_insert(out, outfile, builder);
  return s;
}

Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input, int idx1, int idx2,
                                          int idx3) {
  assert(compact != nullptr);
  TableBuilder* b;
  WritableFile* f;
  if (compact->compaction->level() == 1) {
    b = compact->l2_builders[idx1][idx2][idx3];
    f = compact->l2_outfiles[idx1][idx2][idx3];
  } else {
    b = compact->builders[idx1][idx2];
    f = compact->outfiles[idx1][idx2];
  }
  assert(f != nullptr);
  assert(b != nullptr);

  uint64_t output_number;
  if (compact->compaction->level() == 1)
    output_number = compact->l2_outputs[idx1][idx2][idx3].number;
  else
    output_number = compact->outputs[idx1][idx2].number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = b->NumEntries();
  if (s.ok()) {
    s = b->Finish();
  } else {
    b->Abandon();
  }
  const uint64_t current_bytes = b->FileSize();
  if (compact->compaction->level() == 1)
    compact->l2_outputs[idx1][idx2][idx3].file_size = current_bytes;
  else
    compact->outputs[idx1][idx2].file_size = current_bytes;
  compact->total_bytes += current_bytes;
  delete b;
  if (compact->compaction->level() == 1)
    compact->l2_builders[idx1][idx2][idx3] = nullptr;
  else
    compact->builders[idx1][idx2] = nullptr;

  // Finish and check for file errors
  if (s.ok()) {
    s = f->Sync();
  }
  if (s.ok()) {
    s = f->Close();
  }
  delete f;
  if (compact->compaction->level() == 1)
    compact->l2_outfiles[idx1][idx2][idx3] = nullptr;
  else
    compact->outfiles[idx1][idx2] = nullptr;

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    Iterator* iter =
        table_cache_->NewIterator(ReadOptions(), output_number, current_bytes);
    s = iter->status();
    delete iter;
    if (s.ok()) {
      Log(options_.info_log, "Generated table #%llu@%d: %lld keys, %lld bytes",
          (unsigned long long)output_number, compact->compaction->level(),
          (unsigned long long)current_entries,
          (unsigned long long)current_bytes);
    }
  }
  return s;
}

Status DBImpl::InstallCompactionResults(CompactionState* compact) {
  mutex_.AssertHeld();
  Log(options_.info_log, "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0), compact->compaction->level(),
      compact->compaction->num_input_files(1), compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

  // unlock dynamic lock.
  if (DYNAMIC_PARTITION_LENGTH)
    __sync_lock_test_and_set(&DYNAMIC_CONTROL_LOCK, 0);

  // Add compaction outputs
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  const int level = compact->compaction->level();
  if (level == 1) {
    for (size_t i = 0; i < compact->l2_outputs.size(); i++) {
      for (size_t j = 0; j < compact->l2_outputs[i].size(); j++) {
        for (size_t k = 0; k < compact->l2_outputs[i][j].size(); k++) {
          const CompactionState::Output& out = compact->l2_outputs[i][j][k];
          compact->compaction->edit()->AddFile(
              level + 1, out.number, out.file_size, out.smallest, out.largest,
              out.time_boundary, out.time_interval, out.start_id, out.end_id,
              out.patch);
        }
      }
    }
  } else {
    for (size_t i = 0; i < compact->outputs.size(); i++) {
      for (size_t j = 0; j < compact->outputs[i].size(); j++) {
        const CompactionState::Output& out = compact->outputs[i][j];
        if (level == 0)
          compact->compaction->edit()->AddFile(
              level + 1, out.number, out.file_size, out.smallest, out.largest,
              out.time_boundary, out.time_interval);
        else if (level == 2)
          compact->compaction->edit()->AddFile(
              level, out.number, out.file_size, out.smallest, out.largest,
              out.time_boundary, out.time_interval, out.start_id, out.end_id,
              out.patch);
      }
    }
  }
  return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
}

void _value_decode_helper(tsdb::chunk::XORIterator* it,
                          std::vector<int64_t>* timestamps,
                          std::vector<double>* values, int64_t bound) {
  int64_t t;
  double v;
  int mid, low, high;
  while (it->next()) {
    it->at(&t, &v);

    if (t >= bound) return;
    if (timestamps->empty() || t > timestamps->back()) {
      timestamps->push_back(t);
      values->push_back(v);
    } else {
      // Deduplicate & handle out-of-order data.
      low = 0, high = timestamps->size();
      while (low < high) {
        mid = low + (high - low) / 2;
        if (t <= timestamps->at(mid))
          high = mid;
        else
          low = mid + 1;
      }

      if (low < timestamps->size() && timestamps->at(low) < t) low++;

      if (timestamps->at(low) == t)
        values->at(low) = v;
      else {
        timestamps->insert(timestamps->begin() + low, t);
        values->insert(values->begin() + low, v);
      }
    }
  }
}

void value_decode_helper(const Slice& value, std::vector<int64_t>* timestamps,
                         std::vector<double>* values) {
  tsdb::chunk::XORChunk c(reinterpret_cast<const uint8_t*>(value.data()),
                          value.size());
  auto it = c.xor_iterator(MEM_TUPLE_SIZE);
  _value_decode_helper(it.get(), timestamps, values,
                       std::numeric_limits<int64_t>::max());
}

void full_value_decode_helper(const Slice& value,
                              std::vector<int64_t>* timestamps,
                              std::vector<double>* values) {
  tsdb::chunk::XORChunk c(reinterpret_cast<const uint8_t*>(value.data()),
                          value.size());
  auto it = c.xor_iterator();
  _value_decode_helper(it.get(), timestamps, values,
                       std::numeric_limits<int64_t>::max());
}

void full_header_value_decode_helper(const Slice& value,
                                     std::vector<int64_t>* timestamps,
                                     std::vector<double>* values) {
  Slice tmp = value;
  uint64_t tdiff;
  GetVarint64(&tmp, &tdiff);
  tsdb::chunk::XORChunk c(reinterpret_cast<const uint8_t*>(tmp.data()),
                          tmp.size());
  auto it = c.xor_iterator();
  _value_decode_helper(it.get(), timestamps, values,
                       std::numeric_limits<int64_t>::max());
}

void group_value_decode_helper(const Slice& value,
                               std::vector<DecodedGroup*>* groups) {
  groups->push_back(new DecodedGroup());
  ::tsdb::chunk::NullSupportedXORGroupChunk c(
      reinterpret_cast<const uint8_t*>(value.data()), value.size());
  c.decode(&groups->back()->slots_, &groups->back()->timestamps_,
           &groups->back()->values_);
  groups->back()->mint_ = groups->back()->timestamps_.front();
  groups->back()->maxt_ = groups->back()->timestamps_.back();
}

void group_value_decode_helper(const Slice& value, DecodedGroup* group) {
  ::tsdb::chunk::NullSupportedXORGroupChunk c(
      reinterpret_cast<const uint8_t*>(value.data()), value.size());
  c.decode(&group->slots_, &group->timestamps_, &group->values_);
  group->mint_ = group->timestamps_.front();
  group->maxt_ = group->timestamps_.back();
}

void group_value_decode_helper(const Slice& value, int slot,
                               std::vector<int64_t>* timestamps,
                               std::vector<double>* values, bool filter) {
  ::tsdb::chunk::NullSupportedXORGroupChunk c(
      reinterpret_cast<const uint8_t*>(value.data()), value.size());
  c.decode(slot, timestamps, values, filter);
}

void header_group_value_decode_helper(const Slice& value, int slot,
                                      std::vector<int64_t>* timestamps,
                                      std::vector<double>* values,
                                      bool filter) {
  Slice tmp = value;
  uint64_t tdiff;
  GetVarint64(&tmp, &tdiff);
  ::tsdb::chunk::NullSupportedXORGroupChunk c(
      reinterpret_cast<const uint8_t*>(tmp.data()), tmp.size());
  c.decode(slot, timestamps, values, filter);
}

template <typename T>
int lower_bound_helper(T target, const std::vector<T>& arr) {
  int mid, low = 0, high = arr.size();
  while (low < high) {
    mid = low + (high - low) / 2;
    if (target <= arr[mid])
      high = mid;
    else
      low = mid + 1;
  }
  if (low < arr.size() && arr[low] < target) low++;

  return low;
}

void merge_nonoverlapping_groups(DecodedGroup* g1, DecodedGroup* g2) {
  // Merge slots for index g1.
  for (int slot : g2->slots_) {
    int idx = lower_bound_helper<int>(slot, g1->slots_);

    if (idx >= g1->slots_.size()) {
      g1->slots_.push_back(slot);
      g1->values_.emplace_back(g1->timestamps_.size(),
                               std::numeric_limits<double>::max());
    } else if (g1->slots_[idx] != slot) {
      g1->slots_.insert(g1->slots_.begin() + idx, slot);
      g1->values_.insert(
          g1->values_.begin() + idx,
          std::vector<double>(g1->timestamps_.size(),
                              std::numeric_limits<double>::max()));
    }
  }

  // Merge slots for index g2.
  for (int slot : g1->slots_) {
    int idx = lower_bound_helper<int>(slot, g2->slots_);

    if (idx >= g2->slots_.size()) {
      g2->slots_.push_back(slot);
      g2->values_.emplace_back(g2->timestamps_.size(),
                               std::numeric_limits<double>::max());
    } else if (g2->slots_[idx] != slot) {
      g2->slots_.insert(g2->slots_.begin() + idx, slot);
      g2->values_.insert(
          g2->values_.begin() + idx,
          std::vector<double>(g2->timestamps_.size(),
                              std::numeric_limits<double>::max()));
    }
  }

  assert(g1->values_.size() == g2->values_.size());

  // Merge timestamps and values.
  g1->timestamps_.insert(g1->timestamps_.end(), g2->timestamps_.begin(),
                         g2->timestamps_.end());
  for (size_t i = 0; i < g1->slots_.size(); i++)
    g1->values_[i].insert(g1->values_[i].end(), g2->values_[i].begin(),
                          g2->values_[i].end());
  g1->maxt_ = g2->maxt_;
}

void merge_overlapping_groups(std::vector<DecodedGroup*>* groups) {
  bool end = false;
  while (!end) {
    end = true;
    int i = 0;
    while (i < groups->size() - 1) {
      if (groups->at(i)->mint_ <= groups->at(i + 1)->maxt_ &&
          groups->at(i + 1)->mint_ <= groups->at(i)->maxt_) {
        end = false;

        // Merge slots for index i.
        for (int slot : groups->at(i + 1)->slots_) {
          int idx = lower_bound_helper<int>(slot, groups->at(i)->slots_);

          if (idx >= groups->at(i)->slots_.size()) {
            groups->at(i)->slots_.push_back(slot);
            groups->at(i)->values_.emplace_back(
                groups->at(i)->timestamps_.size(),
                std::numeric_limits<double>::max());
          } else if (groups->at(i)->slots_[idx] != slot) {
            groups->at(i)->slots_.insert(groups->at(i)->slots_.begin() + idx,
                                         slot);
            groups->at(i)->values_.insert(
                groups->at(i)->values_.begin() + idx,
                std::vector<double>(groups->at(i)->timestamps_.size(),
                                    std::numeric_limits<double>::max()));
          }
        }

        // Merge slots for index i + 1.
        for (int slot : groups->at(i)->slots_) {
          int idx = lower_bound_helper<int>(slot, groups->at(i + 1)->slots_);

          if (idx >= groups->at(i + 1)->slots_.size()) {
            groups->at(i + 1)->slots_.push_back(slot);
            groups->at(i + 1)->values_.emplace_back(
                groups->at(i + 1)->timestamps_.size(),
                std::numeric_limits<double>::max());
          } else if (groups->at(i + 1)->slots_[idx] != slot) {
            groups->at(i + 1)->slots_.insert(
                groups->at(i + 1)->slots_.begin() + idx, slot);
            groups->at(i + 1)->values_.insert(
                groups->at(i + 1)->values_.begin() + idx,
                std::vector<double>(groups->at(i + 1)->timestamps_.size(),
                                    std::numeric_limits<double>::max()));
          }
        }

        assert(groups->at(i)->values_.size() ==
               groups->at(i + 1)->values_.size());

        // Merge timestamps and values.
        for (int j = 0; j < groups->at(i + 1)->timestamps_.size(); j++) {
          int idx = lower_bound_helper<int64_t>(
              groups->at(i + 1)->timestamps_[j], groups->at(i)->timestamps_);

          if (idx >= groups->at(i)->timestamps_.size()) {
            groups->at(i)->timestamps_.push_back(
                groups->at(i + 1)->timestamps_[j]);
            for (size_t k = 0; k < groups->at(i)->values_.size(); k++)
              groups->at(i)->values_[k].push_back(
                  groups->at(i + 1)->values_[k][j]);
          } else if (groups->at(i)->timestamps_[idx] ==
                     groups->at(i + 1)->timestamps_[j]) {
            // Duplicated tiemstamp, replace with newer values.
            for (size_t k = 0; k < groups->at(i)->values_.size(); k++) {
              if (groups->at(i + 1)->values_[k][j] !=
                  std::numeric_limits<double>::max())
                groups->at(i)->values_[k][idx] =
                    groups->at(i + 1)->values_[k][j];
            }
          } else if (groups->at(i)->timestamps_[idx] !=
                     groups->at(i + 1)->timestamps_[j]) {
            groups->at(i)->timestamps_.insert(
                groups->at(i)->timestamps_.begin() + idx,
                groups->at(i + 1)->timestamps_[j]);
            for (size_t k = 0; k < groups->at(i)->values_.size(); k++)
              groups->at(i)->values_[k].insert(
                  groups->at(i)->values_[k].begin() + idx,
                  groups->at(i + 1)->values_[k][j]);
          }
        }
        if (groups->at(i + 1)->mint_ < groups->at(i)->mint_)
          groups->at(i)->mint_ = groups->at(i + 1)->mint_;
        if (groups->at(i + 1)->maxt_ > groups->at(i)->maxt_)
          groups->at(i)->maxt_ = groups->at(i + 1)->maxt_;

        delete groups->at(i + 1);
        groups->erase(groups->begin() + i + 1);
        break;
      }
      i++;
    }
  }
}

TempGroupWrapper::TempGroupWrapper(const Slice& s, int64_t st, int64_t et,
                                   bool decode)
    : st_(st), et_(et) {
  if (!decode) {
    decoded_ = false;
    chunk_.append(s.data(), s.size());
  } else {
    decoded_ = true;
    group_value_decode_helper(s, &g_);
  }
}

TempGroupWrapper::TempGroupWrapper(const Slice& key, const Slice& s, int64_t st,
                                   int64_t et, bool decode)
    : st_(st), et_(et) {
  key_.append(key.data(), key.size());
  if (!decode) {
    decoded_ = false;
    chunk_.append(s.data(), s.size());
  } else {
    decoded_ = true;
    group_value_decode_helper(s, &g_);
  }
}

void TempGroupWrapper::decode() {
  group_value_decode_helper(chunk_, &g_);
  decoded_ = true;
}

bool TempGroupWrapper::decoded() { return decoded_; }

void TempGroupWrapper::encode(std::string* s, bool with_header) const {
  if (decoded_) {
    std::string tmp;
    ::tsdb::chunk::NullSupportedXORGroupChunk::encode(
        &tmp, g_.slots_, g_.timestamps_, g_.values_);
    if (with_header) PutVarint32(s, tmp.size());
    s->append(tmp.c_str(), tmp.size());
  } else {
    if (with_header) PutVarint32(s, chunk_.size());
    s->append(chunk_.c_str(), chunk_.size());
  }
}

void merge_overlapping_groups(std::vector<TempGroupWrapper*>* groups) {
  bool end = false;
  while (!end) {
    end = true;
    int i = 0;
    while (i < groups->size() - 1) {
      if (!groups->at(i)->decoded()) groups->at(i)->decode();
      if (!groups->at(i + 1)->decoded()) groups->at(i + 1)->decode();
      if (groups->at(i)->g_.mint_ <= groups->at(i + 1)->g_.maxt_ &&
          groups->at(i + 1)->g_.mint_ <= groups->at(i)->g_.maxt_) {
        end = false;

        // Merge slots for index i.
        for (int slot : groups->at(i + 1)->g_.slots_) {
          int idx = lower_bound_helper<int>(slot, groups->at(i)->g_.slots_);

          if (idx >= groups->at(i)->g_.slots_.size()) {
            groups->at(i)->g_.slots_.push_back(slot);
            groups->at(i)->g_.values_.emplace_back(
                groups->at(i)->g_.timestamps_.size(),
                std::numeric_limits<double>::max());
          } else if (groups->at(i)->g_.slots_[idx] != slot) {
            groups->at(i)->g_.slots_.insert(
                groups->at(i)->g_.slots_.begin() + idx, slot);
            groups->at(i)->g_.values_.insert(
                groups->at(i)->g_.values_.begin() + idx,
                std::vector<double>(groups->at(i)->g_.timestamps_.size(),
                                    std::numeric_limits<double>::max()));
          }
        }

        // Merge slots for index i + 1.
        for (int slot : groups->at(i)->g_.slots_) {
          int idx = lower_bound_helper<int>(slot, groups->at(i + 1)->g_.slots_);

          if (idx >= groups->at(i + 1)->g_.slots_.size()) {
            groups->at(i + 1)->g_.slots_.push_back(slot);
            groups->at(i + 1)->g_.values_.emplace_back(
                groups->at(i + 1)->g_.timestamps_.size(),
                std::numeric_limits<double>::max());
          } else if (groups->at(i + 1)->g_.slots_[idx] != slot) {
            groups->at(i + 1)->g_.slots_.insert(
                groups->at(i + 1)->g_.slots_.begin() + idx, slot);
            groups->at(i + 1)->g_.values_.insert(
                groups->at(i + 1)->g_.values_.begin() + idx,
                std::vector<double>(groups->at(i + 1)->g_.timestamps_.size(),
                                    std::numeric_limits<double>::max()));
          }
        }

        assert(groups->at(i)->g_.values_.size() ==
               groups->at(i + 1)->g_.values_.size());

        // Merge timestamps and values.
        for (int j = 0; j < groups->at(i + 1)->g_.timestamps_.size(); j++) {
          int idx =
              lower_bound_helper<int64_t>(groups->at(i + 1)->g_.timestamps_[j],
                                          groups->at(i)->g_.timestamps_);

          if (idx >= groups->at(i)->g_.timestamps_.size()) {
            groups->at(i)->g_.timestamps_.push_back(
                groups->at(i + 1)->g_.timestamps_[j]);
            for (size_t k = 0; k < groups->at(i)->g_.values_.size(); k++)
              groups->at(i)->g_.values_[k].push_back(
                  groups->at(i + 1)->g_.values_[k][j]);
          } else if (groups->at(i)->g_.timestamps_[idx] ==
                     groups->at(i + 1)->g_.timestamps_[j]) {
            // Duplicated tiemstamp, replace with newer values.
            for (size_t k = 0; k < groups->at(i)->g_.values_.size(); k++) {
              if (groups->at(i + 1)->g_.values_[k][j] !=
                  std::numeric_limits<double>::max())
                groups->at(i)->g_.values_[k][idx] =
                    groups->at(i + 1)->g_.values_[k][j];
            }
          } else if (groups->at(i)->g_.timestamps_[idx] !=
                     groups->at(i + 1)->g_.timestamps_[j]) {
            groups->at(i)->g_.timestamps_.insert(
                groups->at(i)->g_.timestamps_.begin() + idx,
                groups->at(i + 1)->g_.timestamps_[j]);
            for (size_t k = 0; k < groups->at(i)->g_.values_.size(); k++)
              groups->at(i)->g_.values_[k].insert(
                  groups->at(i)->g_.values_[k].begin() + idx,
                  groups->at(i + 1)->g_.values_[k][j]);
          }
        }
        if (groups->at(i + 1)->g_.mint_ < groups->at(i)->g_.mint_)
          groups->at(i)->g_.mint_ = groups->at(i + 1)->g_.mint_;
        if (groups->at(i + 1)->g_.maxt_ > groups->at(i)->g_.maxt_)
          groups->at(i)->g_.maxt_ = groups->at(i + 1)->g_.maxt_;

        delete groups->at(i + 1);
        groups->erase(groups->begin() + i + 1);
        break;
      }
      i++;
    }
  }
}

void merge_group_wrappers(std::vector<TempGroupWrapper*>* wrappers,
                          const Slice& s, int64_t st, int64_t et) {
  wrappers->push_back(new TempGroupWrapper(s, st, et, false));
  if (wrappers->size() > 1 && wrappers->at(wrappers->size() - 2)->et_ >= st) {
    merge_overlapping_groups(wrappers);
  }
}

void merge_group_wrappers(std::vector<TempGroupWrapper*>* wrappers,
                          const Slice& key, const Slice& s, int64_t st,
                          int64_t et) {
  wrappers->push_back(new TempGroupWrapper(key, s, st, et, false));
  if (wrappers->size() > 1 && wrappers->at(wrappers->size() - 2)->et_ >= st) {
    merge_overlapping_groups(wrappers);
  }
}

void encode_group_wrappers(std::vector<TempGroupWrapper*>* wrappers,
                           std::string* s) {
  PutVarint64(s, wrappers->back()->et_ - wrappers->front()->st_);
  for (size_t i = 0; i < wrappers->size(); i++) wrappers->at(i)->encode(s);
}

void DBImpl::_encode_group_wrappers(std::vector<TempGroupWrapper*>* wrappers,
                                    CompactionState* compact,
                                    bool set_smallest_key, int compact_idx) {
  std::string val;
  for (size_t i = 0; i < wrappers->size(); i++) {
    val.clear();
    PutVarint64(&val, wrappers->at(i)->et_ - wrappers->at(i)->st_);
    wrappers->at(i)->encode(&val);
    if (set_smallest_key) {
      set_smallest_key = false;
      compact->outputs[compact_idx].back().smallest.DecodeFrom(
          wrappers->at(i)->key_);
    }
    compact->outputs[compact_idx].back().largest.DecodeFrom(
        wrappers->at(i)->key_);
    compact->builders[compact_idx].back()->Add(Slice(wrappers->at(i)->key_),
                                               Slice(val));
  }
}

TempChunkWrapper::TempChunkWrapper(const Slice& s, int64_t st, int64_t et,
                                   bool decode)
    : st_(st), et_(et) {
  if (!decode)
    chunk_.append(s.data(), s.size());
  else
    full_value_decode_helper(s, &timestamps_, &values_);
}

TempChunkWrapper::TempChunkWrapper(const Slice& key, const Slice& s, int64_t st,
                                   int64_t et, bool decode)
    : st_(st), et_(et) {
  key_.append(key.data(), key.size());
  if (!decode)
    chunk_.append(s.data(), s.size());
  else
    full_value_decode_helper(s, &timestamps_, &values_);
}

// Call when the chunk overlaps with other chunks.
void TempChunkWrapper::decode() {
  full_value_decode_helper(chunk_, &timestamps_, &values_);
}

bool TempChunkWrapper::decoded() { return !timestamps_.empty(); }

void TempChunkWrapper::encode(std::string* s, bool with_header) const {
  if (timestamps_.empty()) {
    if (with_header) PutVarint32(s, chunk_.size());
    s->append(chunk_.c_str(), chunk_.size());
  } else {
    assert(timestamps_.size() == values_.size());
    tsdb::chunk::XORChunk c;
    auto app = c.appender();
    for (size_t j = 0; j < timestamps_.size(); j++)
      app->append(timestamps_[j], values_[j]);
    if (with_header) PutVarint32(s, c.size());
    s->append(reinterpret_cast<const char*>(c.bytes()), c.size());
  }
}

void merge_chunk_wrappers(std::vector<TempChunkWrapper>* wrappers,
                          const Slice& s, int64_t st, int64_t et) {
  tsdb::chunk::XORChunk c(reinterpret_cast<const uint8_t*>(s.data()), s.size());
  auto it = c.xor_iterator();

  if (st >= wrappers->back().st_) {  // Only overlaps with the last one.
    if (!wrappers->back().decoded()) wrappers->back().decode();
    _value_decode_helper(it.get(), &wrappers->back().timestamps_,
                         &wrappers->back().values_,
                         std::numeric_limits<int64_t>::max());
    wrappers->back().et_ = wrappers->back().timestamps_.back();
  } else {
    // Find the overlapping range.
    // Find lower bound (right boundaries) for mint.
    // Note(Alec), the first element >= mint.
    int mid;
    int low = 0;
    int high = wrappers->size();
    while (low < high) {
      mid = low + (high - low) / 2;
      if (st <= wrappers->at(mid).et_)
        high = mid;
      else
        low = mid + 1;
    }
    int left = low;
    if (low >= wrappers->size()) return;

    // Find upper bound (left boundaries) for maxt.
    // Note(Alec), lower bound - 1 is the first element <= maxt.
    low = 0;
    high = wrappers->size();
    while (low < high) {
      mid = low + (high - low) / 2;
      if (et >= wrappers->at(mid).st_)
        low = mid + 1;
      else
        high = mid;
    }
    int right = low - 1;
    if (right == -1) return;

    for (int i = left; i < right; i++) {
      if (!wrappers->at(i).decoded()) wrappers->at(i).decode();
      _value_decode_helper(it.get(), &wrappers->at(i).timestamps_,
                           &wrappers->at(i).values_, wrappers->at(i + 1).st_);
      wrappers->at(i).st_ = wrappers->at(i).timestamps_.front();
      wrappers->at(i).et_ = wrappers->at(i).timestamps_.back();
    }
  }
}

void encode_chunk_wrappers(std::vector<TempChunkWrapper>* wrappers,
                           std::string* s) {
  PutVarint64(s, wrappers->back().et_ - wrappers->front().st_);
  for (size_t i = 0; i < wrappers->size(); i++) wrappers->at(i).encode(s);
}

// TODO(Alec), merge can be done here.
void DBImpl::_encode_chunk_wrappers(std::vector<TempChunkWrapper>* wrappers,
                                    CompactionState* compact,
                                    bool set_smallest_key, int compact_idx) {
  std::string val;
  for (size_t i = 0; i < wrappers->size(); i++) {
    val.clear();
    PutVarint64(&val, wrappers->at(i).et_ - wrappers->at(i).st_);
    wrappers->at(i).encode(&val);
    if (set_smallest_key) {
      set_smallest_key = false;
      compact->outputs[compact_idx].back().smallest.DecodeFrom(
          wrappers->at(i).key_);
    }
    compact->outputs[compact_idx].back().largest.DecodeFrom(
        wrappers->at(i).key_);
    compact->builders[compact_idx].back()->Add(Slice(wrappers->at(i).key_),
                                               Slice(val));
  }
}

Status DBImpl::DoL2CompactionWork(CompactionState* compact) {
  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  Iterator* input = versions_->MakeInputIterator(compact->compaction, false);

  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock();

  input->SeekToFirst();
  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;

  // NOTE(Alec): to cleanly separate SSTs according to tsid.
  uint64_t last_tsid = std::numeric_limits<uint64_t>::max(), current_tsid,
           tdiff;
  std::string current_key, current_value;
  int64_t current_tuple_time, prev_tuple_time;

  int cur_l2_idx1 = -2, cur_l2_idx2, tmp_l2_idx1, tmp_l2_idx2;
  bool is_current_file_patch = false;
  uint64_t current_file_start_id, current_file_end_id;

  compact->compaction->InitNumPatches();

  std::vector<TempChunkWrapper> chunks;
  std::vector<TempGroupWrapper*> groups;

  int64_t current_boundary = std::numeric_limits<int64_t>::max(), tmp_boundary,
          tmp_interval;
  int compact_idx1, compact_idx2;

  while (input->Valid() && !shutting_down_.load(std::memory_order_acquire)) {
    // Prioritize immutable compaction work
    if (has_imm_.load(std::memory_order_relaxed)) {
      const uint64_t imm_start = env_->NowMicros();
      mutex_.Lock();
      // if (imm_ != nullptr) {
      if (!imms_.empty()) {
        CompactMemTable();
        // Wake up MakeRoomForWrite() if necessary.
        background_work_finished_signal_.SignalAll();
      }
      mutex_.Unlock();
      imm_micros += (env_->NowMicros() - imm_start);
    }

    Slice key = input->key();

    // Handle key/value, add to state, etc.
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) !=
              0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      last_sequence_for_key = ikey.sequence;
    }

    decodeKey(ikey.user_key, nullptr, &current_tsid, &current_tuple_time);
    current_key.clear();
    current_key.append(key.data(), key.size());

    compact->compaction->output_time_range(current_tuple_time, &tmp_boundary,
                                           &tmp_interval);

    if (current_tsid != last_tsid ||
        (current_boundary != tmp_boundary &&
         current_boundary != std::numeric_limits<int64_t>::max())) {
      if (cur_l2_idx1 != -2) {
        // if ((cur_l2_idx1 != tmp_l2_idx1) ||
        //     (cur_l2_idx1 == tmp_l2_idx1 && cur_l2_idx2 != tmp_l2_idx2) ||
        compact->l2_get_partition_idx(prev_tuple_time, last_tsid, &compact_idx1,
                                      &compact_idx2);
        if (compact->l2_builders[compact_idx1][compact_idx2]
                .back()
                ->FileSize() >= compact->compaction->MaxOutputFileSize()) {
          if (!is_current_file_patch &&
              last_tsid >
                  compact->l2_outputs[compact_idx1][compact_idx2].back().end_id)
            compact->l2_outputs[compact_idx1][compact_idx2].back().end_id =
                last_tsid;

          compact->l2_close_partition_file(prev_tuple_time, last_tsid);
          status = FinishCompactionOutputFile(
              compact, input, compact_idx1, compact_idx2,
              compact->l2_outputs[compact_idx1][compact_idx2].size() - 1);
          if (!status.ok()) break;
          // LOG_DEBUG << "close " << compact_idx1 << " " << compact_idx2 << " "
          // << compact->l2_outputs[compact_idx1][compact_idx2].size() - 1;
        }
      }
    }

    compact->compaction->FindTSIDOverlap(current_tuple_time, current_tsid,
                                         &tmp_l2_idx1, &tmp_l2_idx2);
    cur_l2_idx1 = tmp_l2_idx1;
    cur_l2_idx2 = tmp_l2_idx2;
    if (cur_l2_idx1 == -1)
      is_current_file_patch = false;
    else
      is_current_file_patch = true;

    // Open output file if necessary
    if (!compact->l2_partition_file_opened(current_tuple_time, current_tsid)) {
      // LOG_DEBUG << "current_tuple_time:" << current_tuple_time << "
      // current_tsid:" << current_tsid << " tmp_boundary:" << tmp_boundary << "
      // cur_l2_idx1:" << cur_l2_idx1 << " cur_l2_idx2:" << cur_l2_idx2;
      if (is_current_file_patch) {
        FileMetaData* f =
            compact->compaction
                ->inputs_[1]
                         [compact->compaction
                              ->base_file_indexes_[cur_l2_idx1][cur_l2_idx2]];
        compact->compaction->num_patches_[cur_l2_idx1][cur_l2_idx2]++;
        status = OpenCompactionOutputPatchFile(
            compact, f->start_id, f->end_id,
            compact->compaction->num_patches_[cur_l2_idx1][cur_l2_idx2],
            tmp_boundary, tmp_interval);
      } else {
        status = OpenCompactionOutputFile(compact, tmp_boundary, tmp_interval,
                                          current_tsid);
      }

      compact->l2_get_partition_idx(current_tuple_time, current_tsid,
                                    &compact_idx1, &compact_idx2);

      compact->l2_set_smallest_key_[compact_idx1][compact_idx2] = false;
      if (!status.ok()) {
        break;
      }
    }

    // Read the end time.
    Slice val = input->value();
    uint32_t tmp_size;
    GetVarint64(&val, &tdiff);
    while (val.size() > 0) {
      GetVarint32(&val, &tmp_size);
      if ((current_tsid >> 63) == 0) {
        if (chunks.empty() || chunks.back().et_ < current_tuple_time)
          chunks.emplace_back(val, current_tuple_time,
                              current_tuple_time + tdiff, false);
        else
          merge_chunk_wrappers(&chunks, val, current_tuple_time,
                               current_tuple_time + tdiff);
      } else
        merge_group_wrappers(&groups, val, current_tuple_time,
                             current_tuple_time + tdiff);
      val = Slice(val.data() + tmp_size, val.size() - tmp_size);
    }

    if ((current_tsid >> 63) == 0) {
      encode_chunk_wrappers(&chunks, &current_value);
      chunks.clear();
    } else {
      encode_group_wrappers(&groups, &current_value);
      for (size_t k = 0; k < groups.size(); k++) delete groups[k];
      groups.clear();
    }

    compact->l2_get_partition_idx(current_tuple_time, current_tsid,
                                  &compact_idx1, &compact_idx2);

    if (!compact->l2_set_smallest_key_[compact_idx1][compact_idx2]) {
      compact->l2_set_smallest_key_[compact_idx1][compact_idx2] = true;
      compact->l2_outputs[compact_idx1][compact_idx2]
          .back()
          .smallest.DecodeFrom(current_key);
    }
    compact->l2_outputs[compact_idx1][compact_idx2].back().largest.DecodeFrom(
        current_key);
    compact->l2_builders[compact_idx1][compact_idx2].back()->Add(
        Slice(current_key), Slice(current_value));
    current_value.clear();
    current_key.clear();

    last_tsid = current_tsid;
    prev_tuple_time = current_tuple_time;
    current_boundary = tmp_boundary;

    if (!is_current_file_patch)
      compact->l2_outputs[compact_idx1][compact_idx2].back().end_id =
          current_tsid;

    input->Next();
  }

  if (status.ok() && shutting_down_.load(std::memory_order_acquire)) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok()) {
    for (size_t i = 0; i < compact->l2_outputs.size(); i++) {
      for (size_t j = 0; j < compact->l2_outputs[i].size(); j++) {
        for (size_t k = 0; k < compact->l2_outputs[i][j].size(); k++) {
          if (compact->l2_builders[i][j][k] != nullptr) {
            status = FinishCompactionOutputFile(compact, input, i, j, k);
            if (!status.ok()) break;
          }
        }
        if (!status.ok()) break;
      }
      if (!status.ok()) break;
    }
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = nullptr;

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }

  int num_outputs = 0;
  for (size_t i = 0; i < compact->l2_outputs.size(); i++) {
    for (size_t j = 0; j < compact->l2_outputs[i].size(); j++) {
      num_outputs += compact->l2_outputs[i][j].size();
      for (size_t k = 0; k < compact->l2_outputs[i][j].size(); k++) {
        stats.bytes_written += compact->l2_outputs[i][j][k].file_size;
        printf("%lu ", compact->l2_outputs[i][j][k].number);
      }
    }
  }
  printf("\n");

  mutex_.Lock();
  stats_[compact->compaction->level() + 1].Add(stats);

  if (status.ok()) {
    status = InstallCompactionResults(compact);
  }
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log, "compacted to: %s", versions_->LevelSummary(&tmp));
  LOG_DEBUG << "[+] compacted to L2"
            << " status:" << status.ToString()
            << " #files(L1):" << compact->compaction->num_input_files(0)
            << " #files(L2):" << compact->compaction->num_input_files(1)
            << " #output:" << num_outputs << " duration:" << stats.micros
            << "us";
  return status;
}

// The compactions from level 0 - 1, values do not have size header.
Status DBImpl::DoCompactionWork(CompactionState* compact) {
  if (compact->compaction->level() < 2) {
    Log(options_.info_log, "Compacting %d@%d + %d@%d files",
        compact->compaction->num_input_files(0), compact->compaction->level(),
        compact->compaction->num_input_files(1),
        compact->compaction->level() + 1);
    LOG_DEBUG << "Compacting " << compact->compaction->num_input_files(0) << "@"
              << compact->compaction->level() << " + "
              << compact->compaction->num_input_files(1) << "@"
              << compact->compaction->level() + 1;
  } else {
    Log(options_.info_log, "Merging %d L2 patch files",
        compact->compaction->num_input_files(0));
    LOG_DEBUG << "Merging " << compact->compaction->num_input_files(0)
              << " L2 patch files";
  }

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  // assert(compact->builder == nullptr);
  // assert(compact->outfile == nullptr);
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
  }

  if (compact->compaction->level() == 1) return DoL2CompactionWork(compact);

  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  Iterator* input = versions_->MakeInputIterator(compact->compaction, true);

  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock();

  input->SeekToFirst();
  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;

  // NOTE(Alec): to cleanly separate SSTs according to tsid.
  uint64_t last_tsid = std::numeric_limits<uint64_t>::max(), current_tsid,
           tdiff;
  int64_t current_tuple_time, prev_tuple_time;

  std::vector<TempChunkWrapper> chunks;
  std::vector<TempGroupWrapper*> groups;

  int64_t current_boundary = std::numeric_limits<int64_t>::max();
  int64_t output_interval = compact->compaction->time_interval();

  while (input->Valid() && !shutting_down_.load(std::memory_order_acquire)) {
    // Prioritize immutable compaction work
    if (has_imm_.load(std::memory_order_relaxed)) {
      const uint64_t imm_start = env_->NowMicros();
      mutex_.Lock();
      // if (imm_ != nullptr) {
      if (!imms_.empty()) {
        CompactMemTable();
        // Wake up MakeRoomForWrite() if necessary.
        background_work_finished_signal_.SignalAll();
      }
      mutex_.Unlock();
      imm_micros += (env_->NowMicros() - imm_start);
    }

    Slice key = input->key();

    // Handle key/value, add to state, etc.
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) !=
              0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      last_sequence_for_key = ikey.sequence;
    }

    decodeKey(ikey.user_key, nullptr, &current_tsid, &current_tuple_time);

    if ((current_tsid != last_tsid &&
         last_tsid != std::numeric_limits<uint64_t>::max()) ||
        (current_boundary !=
             current_tuple_time / output_interval * output_interval &&
         current_boundary != std::numeric_limits<int64_t>::max())) {
      int cur_compact_idx = compact->get_partition_idx(prev_tuple_time);

      // Flush data for last tsid.
      if (!chunks.empty()) {
        _encode_chunk_wrappers(&chunks, compact,
                               !compact->set_smallest_key_[cur_compact_idx],
                               cur_compact_idx);
        compact->set_smallest_key_[cur_compact_idx] = true;
        chunks.clear();
      } else if (!groups.empty()) {
        _encode_group_wrappers(&groups, compact,
                               !compact->set_smallest_key_[cur_compact_idx],
                               cur_compact_idx);
        compact->set_smallest_key_[cur_compact_idx] = true;
        for (size_t k = 0; k < groups.size(); k++) delete groups[k];
        groups.clear();
      }

      // If file full, clsoe the partition.
      if (compact->builders[cur_compact_idx].back()->FileSize() >=
          compact->compaction->MaxOutputFileSize()) {
        if (compact->compaction->level() == 2)
          compact->outputs[cur_compact_idx].back().end_id = last_tsid;
        compact->close_partition_file(prev_tuple_time);
        status = FinishCompactionOutputFile(
            compact, input, cur_compact_idx,
            compact->outputs[cur_compact_idx].size() - 1);
        if (!status.ok()) break;
      }
    }

    int cur_compact_idx;
    if (!compact->partition_file_opened(current_tuple_time)) {
      status = OpenCompactionOutputFile(
          compact, current_tuple_time / output_interval * output_interval,
          output_interval);
      if (!status.ok()) break;

      cur_compact_idx = compact->get_partition_idx(current_tuple_time);

      if (compact->compaction->level() == 2) {
        compact->outputs[cur_compact_idx].back().start_id = current_tsid;
        compact->end_ids_[cur_compact_idx] = current_tsid;
      }
      compact->set_smallest_key_[cur_compact_idx] = false;
    }

    cur_compact_idx = compact->get_partition_idx(current_tuple_time);
    // Read the end time.
    // TODO(Alec), split the tuple.
    Slice val = input->value();
    GetVarint64(&val, &tdiff);
    uint32_t tmp_size;
    while (val.size() > 0) {
      GetVarint32(&val, &tmp_size);
      if ((current_tsid >> 63) == 0) {
        if (chunks.empty() || chunks.back().et_ < current_tuple_time) {
          chunks.emplace_back(key, val, current_tuple_time,
                              current_tuple_time + tdiff, false);
        } else
          merge_chunk_wrappers(&chunks, val, current_tuple_time,
                               current_tuple_time + tdiff);
      } else
        merge_group_wrappers(&groups, key, val, current_tuple_time,
                             current_tuple_time + tdiff);
      val = Slice(val.data() + tmp_size, val.size() - tmp_size);
    }

    last_tsid = current_tsid;
    prev_tuple_time = current_tuple_time;
    current_boundary = current_tuple_time / output_interval * output_interval;
    compact->end_ids_[cur_compact_idx] = current_tsid;

    input->Next();
  }

  if (status.ok() && (!chunks.empty() || !groups.empty())) {
    int cur_compact_idx = compact->get_partition_idx(current_tuple_time);
    if (!chunks.empty()) {
      _encode_chunk_wrappers(&chunks, compact,
                             !compact->set_smallest_key_[cur_compact_idx],
                             cur_compact_idx);
      compact->set_smallest_key_[cur_compact_idx] = true;
      chunks.clear();
    } else if (!groups.empty()) {
      _encode_group_wrappers(&groups, compact,
                             !compact->set_smallest_key_[cur_compact_idx],
                             cur_compact_idx);
      compact->set_smallest_key_[cur_compact_idx] = true;
      for (size_t k = 0; k < groups.size(); k++) delete groups[k];
      groups.clear();
    }
    if (compact->partition_file_opened(cur_compact_idx) &&
        compact->compaction->level() == 2)
      compact->outputs[cur_compact_idx].back().end_id = current_tsid;
  }

  if (status.ok() && shutting_down_.load(std::memory_order_acquire)) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok()) {
    for (size_t i = 0; i < compact->outputs.size(); i++) {
      for (size_t j = 0; j < compact->outputs[i].size(); j++) {
        if (compact->builders[i][j] != nullptr) {
          status = FinishCompactionOutputFile(compact, input, i, j);
          if (!status.ok()) break;
        }
      }
      if (!status.ok()) break;
    }
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = nullptr;

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }
  int num_outputs = 0;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    num_outputs += compact->outputs[i].size();
    for (size_t j = 0; j < compact->outputs[i].size(); j++)
      stats.bytes_written += compact->outputs[i][j].file_size;
  }

  mutex_.Lock();
  stats_[compact->compaction->level() + 1].Add(stats);

  if (status.ok()) {
    status = InstallCompactionResults(compact);
  }
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log, "compacted to: %s", versions_->LevelSummary(&tmp));
  if (compact->compaction->level() < 2)
    LOG_DEBUG << "[+] compacted to L" << (compact->compaction->level() + 1)
              << " status:" << status.ToString()
              << " #files(L0):" << compact->compaction->num_input_files(0)
              << " #files(L1):" << compact->compaction->num_input_files(1)
              << " #output:" << num_outputs << " duration:" << stats.micros
              << "us"
              << " imm_micros:" << imm_micros << "us";
  else
    LOG_DEBUG << "Merge L2 patches"
              << " status:" << status.ToString()
              << " #files:" << compact->compaction->num_input_files(0)
              << " #output:" << num_outputs << " duration:" << stats.micros
              << "us"
              << " imm_micros:" << imm_micros << "us";
  return status;
}

namespace {

struct IterState {
  port::Mutex* const mu;
  Version* const version GUARDED_BY(mu);
  MemTable* const mem GUARDED_BY(mu);
  // MemTable* const imm GUARDED_BY(mu);
  std::vector<MemTable*> imms GUARDED_BY(mu);

  IterState(port::Mutex* mutex, MemTable* mem,
            const std::vector<MemTable*>& imms, Version* version)
      : mu(mutex), version(version), mem(mem), imms(imms) {}
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  // if (state->imm != nullptr) state->imm->Unref();
  for (auto* m : state->imms) m->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}

}  // anonymous namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
  mutex_.Lock();
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator());
  mem_->Ref();
  // if (imm_ != nullptr) {
  //   list.push_back(imm_->NewIterator());
  //   imm_->Ref();
  // }
  for (auto* m : imms_) {
    list.push_back(m->NewIterator());
    m->Ref();
  }
  versions_->current()->AddIterators(options, &list);
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();

  IterState* cleanup =
      new IterState(&mutex_, mem_, imms_, versions_->current());
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr);

  *seed = ++seed_;
  mutex_.Unlock();
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

void DBImpl::PrintLevel(bool hex, bool print_stats) {
  (void)(hex);
  (void)(print_stats);
  printf("---------- DBImpl::PrintLevel ---------------\n");
  printf("%s\n", versions_->current()->DebugString().c_str());
  printf("---------------------------------------------\n\n");
}

DBQuerier* DBImpl::Querier(::tsdb::head::HeadType* head, int64_t mint,
                           int64_t maxt) {
  DBQuerier* q = new DBQuerier(this, head, mint, maxt, options_.sample_cache);
  return q;
}

Status DBImpl::Get(const ReadOptions& options, const Slice& key,
                   std::string* value) {
  Status s;
  MutexLock l(&mutex_);
  SequenceNumber snapshot;
  if (options.snapshot != nullptr) {
    snapshot =
        static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number();
  } else {
    snapshot = versions_->LastSequence();
  }

  MemTable* mem = mem_;
  // MemTable* imm = imm_;
  std::vector<MemTable*> imms = imms_;
  Version* current = versions_->current();
  mem->Ref();
  // if (imm != nullptr) imm->Ref();
  for (auto* m : imms) m->Ref();
  current->Ref();

  bool have_stat_update = false;
  Version::GetStats stats;

  // Unlock while reading from files and memtables
  {
    mutex_.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    LookupKey lkey(key, snapshot);
    bool found = false;
    if (mem->Get(lkey, value, &s)) {
      // Done
      found = true;
      // } else if (imm != nullptr && imm->Get(lkey, value, &s)) {
    } else if (!imms.empty()) {
      for (int i = imms.size() - 1; i >= 0; i--) {
        if (imms[i]->Get(lkey, value, &s)) {
          found = true;
          break;
        }
      }
    }
    if (!found) {
      s = current->Get(options, lkey, value, &stats);
      have_stat_update = true;
    }
    mutex_.Lock();
  }

  if (have_stat_update && current->UpdateStats(stats)) {
    MaybeScheduleCompaction();
  }
  mem->Unref();
  // if (imm != nullptr) imm->Unref();
  for (auto* m : imms) m->Unref();
  current->Unref();
  return s;
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
  return NewDBIterator(this, user_comparator(), iter,
                       (options.snapshot != nullptr
                            ? static_cast<const SnapshotImpl*>(options.snapshot)
                                  ->sequence_number()
                            : latest_snapshot),
                       seed);
}

void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&mutex_);
  if (versions_->current()->RecordReadSample(key)) {
    MaybeScheduleCompaction();
  }
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
  MutexLock l(&mutex_);
  snapshots_.Delete(static_cast<const SnapshotImpl*>(snapshot));
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
  Writer w(&mutex_);
  w.batch = updates;
  w.sync = options.sync;
  w.done = false;

  MutexLock l(&mutex_);
  writers_.push_back(&w);
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }
  if (w.done) {
    return w.status;
  }

  // May temporarily unlock and wait.
  Status status = MakeRoomForWrite(updates == nullptr);
  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;
  if (status.ok() && updates != nullptr) {  // nullptr batch is for compactions
    WriteBatch* write_batch = BuildBatchGroup(&last_writer);
    WriteBatchInternal::SetSequence(write_batch, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(write_batch);

    // Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
    {
      mutex_.Unlock();
      bool sync_error = false;
      if (options_.use_log) {
        status = log_->AddRecord(WriteBatchInternal::Contents(write_batch));
        if (status.ok() && options.sync) {
          status = logfile_->Sync();
          if (!status.ok()) {
            sync_error = true;
          }
        }
      }
      if (status.ok()) {
        status = WriteBatchInternal::InsertInto(write_batch, mem_);
      }
      mutex_.Lock();
      if (sync_error) {
        // The state of the log file is indeterminate: the log record we
        // just added may or may not show up when the DB is re-opened.
        // So we force the DB into a mode where all future writes fail.
        RecordBackgroundError(status);
      }
    }
    if (write_batch == tmp_batch_) tmp_batch_->Clear();

    versions_->SetLastSequence(last_sequence);
  }

  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }

  // Notify new head of write queue
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }

  return status;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-null batch
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != nullptr);

  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128 << 10)) {
    max_size = size + (128 << 10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != nullptr) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      // Append to *result
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
  }
  return result;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status DBImpl::MakeRoomForWrite(bool force) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  bool allow_delay = !force;
  Status s;
  while (true) {
    if (!bg_error_.ok()) {
      // Yield previous error
      s = bg_error_;
      break;
    } else if (!force &&
               (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
      // There is room in current memtable
      break;
    } else if (imms_.size() >= options_.max_imm_num) {
      // We have filled up the current memtable, but the previous
      // one is still being compacted, so we wait.
      Log(options_.info_log, "Current memtable full; waiting...\n");
      background_work_finished_signal_.Wait();
    } else {
      // Attempt to switch to a new memtable and trigger compaction of old
      if (options_.use_log) {
        assert(versions_->PrevLogNumber() == 0);
        uint64_t new_log_number = versions_->NewFileNumber();
        WritableFile* lfile = nullptr;
        s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
        if (!s.ok()) {
          // Avoid chewing through file number space in a tight loop.
          versions_->ReuseFileNumber(new_log_number);
          break;
        }
        delete log_;
        delete logfile_;
        logfile_ = lfile;
        logfile_number_ = new_log_number;
        log_ = new log::Writer(lfile);
      }
      imms_.push_back(mem_);
      has_imm_.store(true, std::memory_order_release);
      mem_ = new MemTable(internal_comparator_);
      mem_->Ref();
      force = false;  // Do not force another compaction if have room
      MaybeScheduleCompaction();
    }
  }
  return s;
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();

  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      std::snprintf(buf, sizeof(buf), "%d",
                    versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;
      return true;
    }
  } else if (in == "stats") {
    char buf[200];
    std::snprintf(buf, sizeof(buf),
                  "                               Compactions\n"
                  "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
                  "--------------------------------------------------\n");
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {
        std::snprintf(buf, sizeof(buf), "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
                      level, files, versions_->NumLevelBytes(level) / 1048576.0,
                      stats_[level].micros / 1e6,
                      stats_[level].bytes_read / 1048576.0,
                      stats_[level].bytes_written / 1048576.0);
        value->append(buf);
      }
    }
    return true;
  } else if (in == "sstables") {
    *value = versions_->current()->DebugString();
    return true;
  } else if (in == "approximate-memory-usage") {
    size_t total_usage = options_.block_cache->TotalCharge();
    if (mem_) {
      total_usage += mem_->ApproximateMemoryUsage();
    }
    for (auto* m : imms_) total_usage += m->ApproximateMemoryUsage();
    char buf[50];
    std::snprintf(buf, sizeof(buf), "%llu",
                  static_cast<unsigned long long>(total_usage));
    value->append(buf);
    return true;
  }

  return false;
}

void DBImpl::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
  // TODO(opt): better implementation
  MutexLock l(&mutex_);
  Version* v = versions_->current();
  v->Ref();

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  v->Unref();
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(opt, &batch);
}

// TODO(Alec): may not need this.
Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

DB::~DB() = default;

Status DB::Open(const Options& options, const std::string& dbname, DB** dbptr) {
  *dbptr = nullptr;

  DBImpl* impl = new DBImpl(options, dbname);
  impl->mutex_.Lock();
  VersionEdit edit;
  // Recover handles create_if_missing, error_if_exists
  bool save_manifest = false;
  Status s = impl->Recover(&edit, &save_manifest);
  if (s.ok() && impl->mem_ == nullptr) {
    // Create new log and a corresponding memtable.
    uint64_t new_log_number;
    WritableFile* lfile;
    if (options.use_log) {
      new_log_number = impl->versions_->NewFileNumber();
      s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                                       &lfile);
    }
    if (s.ok()) {
      if (options.use_log) {
        edit.SetLogNumber(new_log_number);
        impl->logfile_ = lfile;
        impl->logfile_number_ = new_log_number;
        impl->log_ = new log::Writer(lfile);
      }
      impl->mem_ = new MemTable(impl->internal_comparator_);
      impl->mem_->Ref();
    }
  }
  if (s.ok() && save_manifest) {
    edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
    if (options.use_log) edit.SetLogNumber(impl->logfile_number_);
    s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
  }
  if (s.ok()) {
    impl->RemoveObsoleteFiles();
    impl->MaybeScheduleCompaction();
  }
  impl->mutex_.Unlock();
  if (s.ok()) {
    assert(impl->mem_ != nullptr);
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

Iterator* DBImpl::TEST_merge_iterator() {
  return versions_->current()->TEST_merge_iterator();
}

Snapshot::~Snapshot() = default;

Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  Status result = env->GetChildren(dbname, &filenames);
  if (!result.ok()) {
    // Ignore error in case directory does not exist
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del = env->RemoveFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->RemoveFile(lockname);
    env->RemoveDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}

}  // namespace leveldb
