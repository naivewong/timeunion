#pragma once

#include <stdint.h>

#include <atomic>
#include <boost/filesystem.hpp>
#include <unordered_map>
#include <unordered_set>

#include "base/Atomic.hpp"
#include "base/Error.hpp"
#include "base/Logging.hpp"
#include "base/Mutex.hpp"
#include "base/ThreadPool.hpp"
#include "base/WaitGroup.hpp"
#include "block/BlockInterface.hpp"
#include "db/AppenderInterface.hpp"
#include "head/LFStripeSeries.hpp"
#include "head/StripeSeries.hpp"
#include "index/MemPostings.hpp"
#ifdef USE_PERSISTENT_CEDAR
#include "third_party/persistent_cedarpp.h"
#else
#include "third_party/cedarpp.h"
#endif
#include "leveldb/db.h"
#include "leveldb/db/log_writer.h"
#include "tsdbutil/MMapLinuxManager.hpp"
#include "tsdbutil/tsdbutils.hpp"

namespace leveldb {
class Status;
class WritableFile;
namespace log {
class Writer;
}
}  // namespace leveldb

namespace tsdb {
namespace head {

class MMapHeadWithTrieAppender;

class MMapHeadWithTrie {
 public:
  friend class MMapStripeSeries;
  friend class MMapStripeGroups;
  friend class MMapHeadWithTrieAppender;

  // For labels.
  std::unique_ptr<leveldb::WritableFile> log_file_;
  std::unique_ptr<leveldb::log::Writer> log_writer_;

  // For TS/Group samples.
  std::atomic<int> cur_samples_log_seq_;
  std::unique_ptr<leveldb::WritableFile> samples_log_file_;
  std::unique_ptr<leveldb::log::Writer> samples_log_writer_;

  // For flush marks.
  std::atomic<int> cur_flushes_log_seq_;
  std::unique_ptr<leveldb::WritableFile> flushes_log_file_;
  std::unique_ptr<leveldb::log::Writer> flushes_log_writer_;

  std::atomic<int> active_samples_logs_;  // number of active samples logs.

  std::string dir_;
  std::string idx_dir_;

  base::AtomicInt64 min_time;
  base::AtomicInt64 max_time;
  base::AtomicInt64 valid_time;  // Shouldn't be lower than the max_time of the
                                 // last persisted block
  base::AtomicUInt64 last_series_id;

  // All series addressable by hash or id
  std::unique_ptr<MMapStripeSeries> series_;

  std::unique_ptr<MMapStripeGroups> groups_;

  mutable base::RWMutexLock mutex_;

#if USE_PERSISTENT_CEDAR
  std::unique_ptr<pcedar::da<char>> symbols_;
  mutable std::unique_ptr<pcedar::da<char>> label_names_;
  mutable std::unique_ptr<pcedar::da<char>> label_values_;
#else
  std::unique_ptr<cedar::da<char>> symbols_;
  mutable std::unique_ptr<cedar::da<char>> label_names_;
  mutable std::unique_ptr<cedar::da<char>> label_values_;
#endif

  std::unique_ptr<MMapXORChunk> xor_array_;

  std::unique_ptr<MMapGroupXORChunk> group_xor_array_;

#if USE_MMAP_LABELS
  std::unique_ptr<MMapLabels> mmap_labels_;
  std::unique_ptr<MMapGroupLabels> mmap_group_labels_;
#endif

  std::unique_ptr<index::MemPostingsWithTrie> posting_list;

  leveldb::DB* db_;

  base::MutexLock bg_log_clean_stop_mutex_;
  base::Condition bg_log_clean_stop_signal_;
  bool bg_log_clean_stopped_;

  error::Error err_;

  MMapHeadWithTrie(uint64_t last_series_id, const std::string& dir,
                   const std::string& idx_dir, leveldb::DB* db);

  ~MMapHeadWithTrie();

  leveldb::Status recover_index_from_log();
#if USE_MMAP_LABELS
  void recover_index_from_mmap();
  void recover_group_index_from_mmap();
#endif

  leveldb::Status recover_samples_from_log();
  leveldb::Status clean_samples_logs();

  leveldb::Status try_extend_samples_logs();
  leveldb::Status try_extend_flushes_logs();

  void _print_usage();

  void set_db(leveldb::DB* db) { db_ = db; }

  int num_tags() { return label_values_->num_keys(); }

  void update_min_max_time(int64_t mint, int64_t maxt);

  std::unique_ptr<db::AppenderInterface> head_appender(bool write_flush_mark);

  std::unique_ptr<db::AppenderInterface> appender(bool write_flush_mark = true);

  std::unique_ptr<MMapHeadWithTrieAppender> TEST_appender();

  leveldb::Status write_flush_marks(
      const std::vector<::leveldb::log::RefFlush>& marks);

  // tombstones returns a TombstoneReader over the block's deleted data.
  std::pair<std::unique_ptr<tombstone::TombstoneReaderInterface>, bool>
  tombstones() const;

  int64_t MinTime() const {
    return const_cast<base::AtomicInt64*>(&min_time)->get();
  }
  int64_t MaxTime() const {
    return const_cast<base::AtomicInt64*>(&max_time)->get();
  }

  // init_time initializes a head with the first timestamp. This only needs to
  // be called for a completely fresh head with an empty WAL. Returns true if
  // the initialization took an effect.
  bool init_time(int64_t t);

  bool overlap_closed(int64_t mint, int64_t maxt) const {
    // The block itself is a half-open interval
    // [pb.meta.MinTime, pb.meta.MaxTime).
    return MinTime() <= maxt && mint < MaxTime();
  }

  void purge_time(int64_t);

  // If there are 2 threads calling this function at the same time,
  // it can be the situation that the 2 threads both generate an id.
  // But only one will be finally push into StripeSeries, and the other id
  // and its corresponding std::shared_ptr<MemSeries> will be abandoned.
  //
  // In a word, this function is thread-safe.
  std::pair<MMapMemSeries*, bool> get_or_create(uint64_t hash,
                                                const label::Labels& lset);

  std::pair<MMapMemGroup*, bool> get_or_create_group(
      uint64_t hash, const label::Labels& group_lset);

#if USE_MMAP_LABELS
  std::pair<MMapMemSeries*, bool> get_or_create_with_id(
      uint64_t id, uint64_t hash, const label::Labels& lset,
      bool alloc_mmap_slot, int64_t mmap_labels_idx);
  std::pair<MMapMemGroup*, bool> get_or_create_group_with_id(
      uint64_t id, uint64_t hash, const label::Labels& group_lset,
      bool alloc_mmap_slot, int64_t mmap_labels_idx);
#else
  std::pair<MMapMemSeries*, bool> get_or_create_with_id(
      uint64_t id, uint64_t hash, const label::Labels& lset,
      bool alloc_mmap_slot);
  std::pair<MMapMemGroup*, bool> get_or_create_group_with_id(
      uint64_t id, uint64_t hash, const label::Labels& group_lset,
      bool alloc_mmap_slot);
#endif

  error::Error error() const { return err_; }

  // Note(Alec), from HeadIndexReader.
  std::set<std::string> symbols();
  // const std::deque<std::string> &symbols_deque() const;

  std::vector<std::string> label_values(const std::string& name);
  std::pair<std::unique_ptr<index::PostingsInterface>, bool> postings(
      const std::string& name, const std::string& value) const;

  bool series(uint64_t ref, label::Labels& lset, std::string* chunk_contents);
  bool group(uint64_t ref,
             const std::vector<::tsdb::label::MatcherInterface*>& l,
             std::vector<int>* slots, label::Labels* group_lset,
             std::vector<label::Labels>* individual_lsets,
             std::string* chunk_contents);
  std::vector<std::string> label_names() const;

  std::unique_ptr<index::PostingsInterface> sorted_postings(
      std::unique_ptr<index::PostingsInterface>&& p);
};

}  // namespace head
}  // namespace tsdb
