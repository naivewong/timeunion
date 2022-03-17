#pragma once

#include <stdint.h>

#include <unordered_map>
#include <unordered_set>

#include "base/Atomic.hpp"
#include "base/Error.hpp"
#include "base/Mutex.hpp"
#include "base/ThreadPool.hpp"
#include "base/WaitGroup.hpp"
#include "block/BlockInterface.hpp"
#include "db/AppenderInterface.hpp"
#include "head/LFStripeSeries.hpp"
#include "head/StripeSeries.hpp"
#include "index/MemPostings.hpp"
#if USE_PERSISTENT_CEDAR
#include "third_party/persistent_cedarpp.h"
#else
#include "third_party/cedarpp.h"
#endif
#include "leveldb/db.h"
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

class HeadWithTrieAppender;

class HeadWithTrie {
 public:
  std::unique_ptr<leveldb::WritableFile> log_file_;
  std::unique_ptr<leveldb::log::Writer> log_writer_;

  int cur_samples_log_seq_;
  std::unique_ptr<leveldb::WritableFile> samples_log_file_;
  std::unique_ptr<leveldb::log::Writer> samples_log_writer_;
  std::string dir_;

  base::AtomicInt64 min_time;
  base::AtomicInt64 max_time;
  base::AtomicInt64 valid_time;  // Shouldn't be lower than the max_time of the
                                 // last persisted block
  base::AtomicUInt64 last_series_id;

  // All series addressable by hash or id
  std::unique_ptr<StripeSeries> series_;

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

  std::unique_ptr<index::MemPostingsWithTrie> posting_list;

  leveldb::DB *db_;

  error::Error err_;

  HeadWithTrie(uint64_t last_series_id, const std::string &dir,
               const std::string &idx_dir, leveldb::DB *db);

  leveldb::Status recover_index_from_log();

  leveldb::Status recover_samples_from_log();

  void set_db(leveldb::DB *db) { db_ = db; }

  int num_tags() { return label_values_->num_keys(); }

  void update_min_max_time(int64_t mint, int64_t maxt);

  std::unique_ptr<db::AppenderInterface> head_appender();

  std::unique_ptr<db::AppenderInterface> appender();

  std::unique_ptr<HeadWithTrieAppender> TEST_appender();

  // tombstones returns a TombstoneReader over the block's deleted data.
  std::pair<std::unique_ptr<tombstone::TombstoneReaderInterface>, bool>
  tombstones() const;

  int64_t MinTime() const {
    return const_cast<base::AtomicInt64 *>(&min_time)->get();
  }
  int64_t MaxTime() const {
    return const_cast<base::AtomicInt64 *>(&max_time)->get();
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

  // If there are 2 threads calling this function at the same time,
  // it can be the situation that the 2 threads both generate an id.
  // But only one will be finally push into StripeSeries, and the other id
  // and its corresponding std::shared_ptr<MemSeries> will be abandoned.
  //
  // In a word, this function is thread-safe.
  std::pair<std::shared_ptr<MemSeries>, bool> get_or_create(
      uint64_t hash, const label::Labels &lset);

  std::pair<std::shared_ptr<MemSeries>, bool> get_or_create_with_id(
      uint64_t id, uint64_t hash, const label::Labels &lset);

  error::Error error() const { return err_; }

  ~HeadWithTrie() {}

  // Note(Alec), from HeadIndexReader.
  std::set<std::string> symbols();
  // const std::deque<std::string> &symbols_deque() const;

  std::vector<std::string> label_values(const std::string &name);
  std::pair<std::unique_ptr<index::PostingsInterface>, bool> postings(
      const std::string &name, const std::string &value) const;

  bool series(uint64_t ref, label::Labels &lset, std::string *chunk_contents);
  std::vector<std::string> label_names() const;

  std::unique_ptr<index::PostingsInterface> sorted_postings(
      std::unique_ptr<index::PostingsInterface> &&p);
};

}  // namespace head
}  // namespace tsdb
