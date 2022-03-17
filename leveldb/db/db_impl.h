// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DB_IMPL_H_
#define STORAGE_LEVELDB_DB_DB_IMPL_H_

#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/partition_index.h"
#include "db/snapshot.h"
#include <atomic>
#include <deque>
#include <set>
#include <string>

#include "leveldb/db.h"
#include "leveldb/env.h"

#include "port/port.h"
#include "port/thread_annotations.h"

#include "cloud/S3.hpp"
#include "label/Label.hpp"

namespace leveldb {

class MemTable;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;
class DBQuerier;
class MemSeriesIterator;
class L0SeriesIterator;
class Compaction;

// Note(Alec), helper functions.
extern void encodeKey(std::string* str, const std::string& metric,
                      uint64_t tsid, int64_t t);
extern void decodeKey(const Slice& s, std::string* metric, uint64_t* tsid,
                      int64_t* t);
extern ValueType logType(const Slice& s);
extern Status decodeLogKey(const Slice& s, Slice* key);
extern Status decodeLogKV(const Slice& s, Slice* key, Slice* value);
extern Status decodeLogIndex(const Slice& s, uint64_t* tsid,
                             int64_t* time_boundary,
                             ::tsdb::label::Labels& lset);
extern void value_decode_helper(const Slice& value,
                                std::vector<int64_t>* timestamps,
                                std::vector<double>* values);
extern void full_value_decode_helper(
    const Slice& value, std::vector<int64_t>* timestamps,
    std::vector<double>* values);  // A chunk with header(2-byte size).
void full_header_value_decode_helper(
    const Slice& value, std::vector<int64_t>* timestamps,
    std::vector<double>*
        values);  // A chunk with time diff and header(2-byte size).

// Group helper functions.
// TODO(Alec): add version number for deduplication.
struct DecodedGroup {
  int64_t mint_;
  int64_t maxt_;
  std::vector<int> slots_;
  std::vector<int64_t> timestamps_;
  std::vector<std::vector<double>> values_;
  DecodedGroup() = default;
  DecodedGroup(int64_t mint, int64_t maxt, const std::vector<int>& slots,
               const std::vector<int64_t>& timestamps,
               const std::vector<std::vector<double>>& values)
      : mint_(mint),
        maxt_(maxt),
        slots_(slots),
        timestamps_(timestamps),
        values_(values) {}
};

extern void merge_overlapping_groups(std::vector<DecodedGroup*>* groups);
extern void merge_nonoverlapping_groups(DecodedGroup* g1, DecodedGroup* g2);
extern void group_value_decode_helper(const Slice& value,
                                      std::vector<DecodedGroup*>* groups);
extern void group_value_decode_helper(const Slice& value, DecodedGroup* group);
extern void group_value_decode_helper(const Slice& value, int slot,
                                      std::vector<int64_t>* timestamps,
                                      std::vector<double>* values, bool filter);
extern void header_group_value_decode_helper(const Slice& value, int slot,
                                             std::vector<int64_t>* timestamps,
                                             std::vector<double>* values,
                                             bool filter);

class TempGroupWrapper {
 public:
  TempGroupWrapper(const Slice& s, int64_t st, int64_t et, bool decode);
  TempGroupWrapper(const Slice& key, const Slice& s, int64_t st, int64_t et,
                   bool decode);

  void decode();
  bool decoded();
  void encode(std::string* s, bool with_header = true) const;

  std::string key_;
  std::string chunk_;
  DecodedGroup g_;
  bool decoded_;
  int64_t st_;
  int64_t et_;
};

// This wraps the contents of a chunk,
// compressed form or decoded form.
class TempChunkWrapper {
 public:
  TempChunkWrapper(const Slice& s, int64_t st, int64_t et, bool decode);
  TempChunkWrapper(const Slice& key, const Slice& s, int64_t st, int64_t et,
                   bool decode);

  // Call when the chunk overlaps with other chunks.
  void decode();
  bool decoded();
  void encode(std::string* s, bool with_header = true) const;

  std::string key_;
  std::string chunk_;
  std::vector<int64_t> timestamps_;
  std::vector<double> values_;
  int64_t st_;
  int64_t et_;
};

class DBImpl : public DB {
 public:
  DBImpl(const Options& options, const std::string& dbname);

  DBImpl(const DBImpl&) = delete;
  DBImpl& operator=(const DBImpl&) = delete;

  ~DBImpl() override;

  // Implementations of the DB interface
  Status Put(const WriteOptions&, const Slice& key,
             const Slice& value) override;
  Status Delete(const WriteOptions&, const Slice& key) override;
  Status Write(const WriteOptions& options, WriteBatch* updates) override;
  Status Get(const ReadOptions& options, const Slice& key,
             std::string* value) override;
  Iterator* NewIterator(const ReadOptions&) override;
  const Snapshot* GetSnapshot() override;
  void ReleaseSnapshot(const Snapshot* snapshot) override;
  bool GetProperty(const Slice& property, std::string* value) override;
  void GetApproximateSizes(const Range* range, int n, uint64_t* sizes) override;
  void CompactRange(const Slice* begin, const Slice* end) override;

  // Extra methods (for testing) that are not in the public DB interface

  // Compact any files in the named level that overlap [*begin,*end]
  void TEST_CompactRange(int level, const Slice* begin, const Slice* end);

  // Force current memtable contents to be compacted.
  Status TEST_CompactMemTable();

  // Return an internal iterator over the current state of the database.
  // The keys of this iterator are internal keys (see format.h).
  // The returned iterator should be deleted when no longer needed.
  Iterator* TEST_NewInternalIterator();

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t TEST_MaxNextLevelOverlappingBytes();

  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.
  void RecordReadSample(Slice key);

  inline int64_t timePartitionBoundary(int64_t t) {
    return t / PARTITION_LENGTH * PARTITION_LENGTH;
  }

  virtual void PrintLevel(bool hex = false, bool print_stats = false) override;

  virtual DBQuerier* Querier(::tsdb::head::HeadType* head, int64_t mint,
                             int64_t maxt) override;

  virtual void SetHead(::tsdb::head::MMapHeadWithTrie* head) override {
    head_ = head;
  }

  virtual int64_t PartitionLength() override { return PARTITION_LENGTH; }

  virtual Cache* BlockCache() override { return options_.block_cache; }

  Iterator* TEST_merge_iterator();

  virtual void PurgeTime(int64_t) override;

 private:
  friend class DB;
  friend class DBQuerier;
  friend class MemSeriesIterator;
  friend class L0SeriesIterator;
  struct CompactionState;
  struct Writer;

  // Information for a manual compaction
  struct ManualCompaction {
    int level;
    bool done;
    const InternalKey* begin;  // null means beginning of key range
    const InternalKey* end;    // null means end of key range
    InternalKey tmp_storage;   // Used to keep track of compaction progress
  };

  // Per level compaction stats.  stats_[level] stores the stats for
  // compactions that produced data for the specified "level".
  struct CompactionStats {
    CompactionStats() : micros(0), bytes_read(0), bytes_written(0) {}

    void Add(const CompactionStats& c) {
      this->micros += c.micros;
      this->bytes_read += c.bytes_read;
      this->bytes_written += c.bytes_written;
    }

    int64_t micros;
    int64_t bytes_read;
    int64_t bytes_written;
  };

  void _encode_chunk_wrappers(std::vector<TempChunkWrapper>* wrappers,
                              CompactionState* compact, bool set_smallest_key,
                              int compact_idx);
  void _encode_group_wrappers(std::vector<TempGroupWrapper*>* wrappers,
                              CompactionState* compact, bool set_smallest_key,
                              int compact_idx);

  Iterator* NewInternalIterator(const ReadOptions&,
                                SequenceNumber* latest_snapshot,
                                uint32_t* seed);

  Status NewDB();

  // Recover the descriptor from persistent storage.  May do a significant
  // amount of work to recover recently logged updates.  Any changes to
  // be made to the descriptor are added to *edit.
  Status Recover(VersionEdit* edit, bool* save_manifest)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void MaybeIgnoreError(Status* s) const;

  // Delete any unneeded files and stale in-memory entries.
  void RemoveObsoleteFiles() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Compact the in-memory write buffer to disk.  Switches to a new
  // log-file/memtable and writes a new descriptor iff successful.
  // Errors are recorded in bg_error_.
  void CompactMemTable() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status RecoverLogFile(uint64_t log_number, bool last_log, bool* save_manifest,
                        VersionEdit* edit, SequenceNumber* max_sequence)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status MakeRoomForWrite(bool force /* compact even if there is room? */)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  WriteBatch* BuildBatchGroup(Writer** last_writer)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void RecordBackgroundError(const Status& s);

  void MaybeScheduleCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  static void BGWork(void* db);
  void BackgroundCall();
  void BackgroundCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void CleanupCompaction(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  Status DoCompactionWork(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  Status DoL2CompactionWork(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  Status DoL2MergeCompactionWork(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status OpenCompactionOutputFile(CompactionState* compact, int64_t boundary,
                                  int64_t interval, uint64_t start_id = 0);
  Status OpenCompactionOutputPatchFile(CompactionState* compact,
                                       uint64_t start_id, uint64_t end_id,
                                       int patch, int64_t boundary,
                                       int64_t interval);
  Status FinishCompactionOutputFile(CompactionState* compact, Iterator* input,
                                    int idx1, int idx2, int idx3 = 0);

  Status InstallCompactionResults(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  const Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }

  // Constant after construction
  Env* const env_;
  const InternalKeyComparator internal_comparator_;
  const SeqAsceInternalKeyComparator
      seq_asce_internal_comparator_;  // Used in L1 compaction.
  const InternalFilterPolicy internal_filter_policy_;
  const Options options_;  // options_.comparator == &internal_comparator_
  const bool owns_info_log_;
  const bool owns_cache_;
  const std::string dbname_;

  // table_cache_ provides its own synchronization
  TableCache* const table_cache_;

  // Lock over the persistent DB state.  Non-null iff successfully acquired.
  FileLock* db_lock_;

  // State below is protected by mutex_
  port::Mutex mutex_;
  std::atomic<bool> shutting_down_;
  port::CondVar background_work_finished_signal_ GUARDED_BY(mutex_);
  MemTable* mem_;
  // MemTable* imm_ GUARDED_BY(mutex_);  // Memtable being compacted
  std::vector<MemTable*> imms_ GUARDED_BY(mutex_);
  std::atomic<bool> has_imm_;  // So bg thread can detect non-null imm_
  WritableFile* logfile_;
  uint64_t logfile_number_ GUARDED_BY(mutex_);
  log::Writer* log_;
  uint32_t seed_ GUARDED_BY(mutex_);  // For sampling.

  // Queue of writers.
  std::deque<Writer*> writers_ GUARDED_BY(mutex_);
  WriteBatch* tmp_batch_ GUARDED_BY(mutex_);

  SnapshotList snapshots_ GUARDED_BY(mutex_);

  // Set of table files to protect from deletion because they are
  // part of ongoing compactions.
  std::set<uint64_t> pending_outputs_ GUARDED_BY(mutex_);

  // Has a background compaction been scheduled or is running?
  bool background_compaction_scheduled_ GUARDED_BY(mutex_);

  ManualCompaction* manual_compaction_ GUARDED_BY(mutex_);
  Compaction* purge_compaction_ GUARDED_BY(mutex_);

  VersionSet* const versions_ GUARDED_BY(mutex_);

  // Have we encountered a background error in paranoid mode?
  Status bg_error_ GUARDED_BY(mutex_);

  CompactionStats stats_[config::kNumLevels] GUARDED_BY(mutex_);

  std::unique_ptr<::tsdb::cloud::S3Wrapper> s3_wrapper_;

  ::tsdb::head::MMapHeadWithTrie* head_;  // used in BuildTables.
};

// Sanitize db options.  The caller should delete result.info_log if
// it is not equal to src.info_log.
Options SanitizeOptions(const std::string& db,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DB_IMPL_H_
