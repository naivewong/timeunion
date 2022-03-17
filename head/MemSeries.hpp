#ifndef MEMSERIES_H
#define MEMSERIES_H

#include <atomic>
#include <deque>

#include "base/Atomic.hpp"
#include "base/Mutex.hpp"
#include "chunk/BitStream.hpp"
#include "chunk/ChunkIteratorInterface.hpp"
#include "chunk/ChunkMeta.hpp"
#include "chunk/XORAppender.hpp"
#include "chunk/XORChunk.hpp"
#include "db/DBUtils.hpp"
#include "head/HeadUtils.hpp"
#include "label/Label.hpp"
#include "label/MatcherInterface.hpp"
#include "leveldb/db.h"

namespace tsdb {
namespace head {

typedef chunk::ChunkMeta MemChunk;

class MemIterator : public chunk::ChunkIteratorInterface {
 private:
  std::unique_ptr<ChunkIteratorInterface> iterator;
  mutable int id;  // chunk id.
  int total;
  Sample sample_buf[4];

 public:
  MemIterator(const std::shared_ptr<MemChunk>& m, const Sample* buf);
  std::pair<int64_t, double> at() const;
  bool next() const;
  bool error() const;
};

// TODO(Alec), should come up with some other ways of recording chunk ids when
// introducing UPDATE and Random Delete.
class MemSeries {
 public:
  base::AtomicInt mutex_;
  uint64_t ref;
  label::Labels labels;
  int64_t min_time_;
  int64_t max_time_;
  int num_samples_;
  int64_t time_boundary_;  // The time boundary of the first sample.
  int64_t tuple_st_;
#if HEAD_USE_XORCHUNK
  chunk::XORChunk chunk;
  std::unique_ptr<chunk::ChunkAppenderInterface> appender;
#else
  std::vector<int64_t> times_;
  std::vector<double> values_;
#endif
  std::atomic<int64_t> flushed_txn_;

  MemSeries(const label::Labels& labels, uint64_t id);

  int64_t min_time();
  int64_t max_time();

  // Return whether the tuple is flushed to LSM.
  bool append(leveldb::DB* db, int64_t timestamp, double value);

  bool TEST_append(int64_t timestamp, double value);

  void read_lock() {
    bool b = true;
    int x;
    while (b) {
      x = mutex_.get();
      if (x == -1) continue;
      if (mutex_.cas(x, x + 1)) b = false;
    }
  }

  void read_unlock() { mutex_.decrement(); }

  void write_lock() {
    bool b = true;
    int x;
    while (b) {
      x = mutex_.get();
      if (x > 0) continue;
      if (mutex_.cas(0, -1)) b = false;
    }
  }

  void write_unlock() { mutex_.increment(); }
};

class MMapMemSeries {
 public:
  base::AtomicInt mutex_;
  uint64_t ref;
#if USE_MMAP_LABELS
  MMapLabels* labels_;
  int64_t labels_idx_;
#else
  label::Labels labels;
#endif
  int64_t min_time_;
  int64_t max_time_;
  int64_t global_max_time_;
  int num_samples_;
  int64_t time_boundary_;  // The time boundary of the first sample.
  int64_t tuple_st_;

  MMapXORChunk* xor_array_;
  uint64_t slot_;
  uint8_t* ptr_;
  chunk::BitStreamV2* bstream_;
  chunk::XORAppenderV2* app_;
  std::atomic<int64_t> flushed_txn_;

  int64_t log_clean_txn_;  // used to store the temporary max txn when cleaning
                           // logs.

  std::string key_;
  std::string val_;

#if USE_MMAP_LABELS
  MMapMemSeries(const label::Labels& labels, uint64_t id,
                MMapXORChunk* xor_chunk, bool alloc_mmap_slot,
                MMapLabels* mlabels, int64_t labels_idx_);
#else
  MMapMemSeries(const label::Labels& labels, uint64_t id,
                MMapXORChunk* xor_chunk, bool alloc_mmap_slot);
#endif

  ~MMapMemSeries();

  int64_t min_time();
  int64_t max_time();

  int _lower_bound(const std::vector<int64_t>& times, int64_t target);
  leveldb::Status _flush(leveldb::DB* db, int64_t txn,
                         std::vector<std::string>* flush_keys = nullptr,
                         std::vector<std::string>* flush_values = nullptr);

  // Return whether the tuple is flushed to LSM.
  // NOTE(Alec): txn is encoded to the value when there is a flush,
  // which is used to remove the log of leveldb.
  bool append(leveldb::DB* db, int64_t timestamp, double value, int64_t txn = 0,
              std::vector<std::string>* flush_keys = nullptr,
              std::vector<std::string>* flush_values = nullptr);

  bool TEST_append(int64_t timestamp, double value);

  void read_lock() {
    bool b = true;
    while (b) {
      int x = mutex_.get();
      if (x == -1) continue;
      if (mutex_.cas(x, x + 1)) b = false;
    }
  }

  void read_unlock() { mutex_.decrement(); }

  void write_lock() {
    bool b = true;
    while (b) {
      int x = mutex_.get();
      if (x > 0) continue;
      if (mutex_.cas(0, -1)) b = false;
    }
  }

  void write_unlock() { mutex_.increment(); }
};

// Note(Alec): the individual labels are 2-dimentionally sorted.
class MMapMemGroup {
 public:
  base::AtomicInt mutex_;
  uint64_t ref;
#if USE_MMAP_LABELS
  MMapGroupLabels* labels_;
  int64_t group_labels_idx_;
  std::vector<int64_t> individual_labels_idx_;
#else
  label::Labels group_labels_;
  std::vector<label::Labels> individual_labels_;
#endif
  int64_t min_time_;
  int64_t max_time_;
  int64_t global_max_time_;
  int num_samples_;
  int64_t time_boundary_;  // The time boundary of the first sample.
  int64_t tuple_st_;

  MMapGroupXORChunk* xor_array_;
  uint64_t time_slot_;
  std::vector<uint64_t> value_slots_;
  uint8_t* t_ptr_;
  std::vector<uint8_t*> v_ptrs_;
  chunk::BitStreamV2* t_bstream_;
  std::vector<chunk::BitStreamV2*> v_bstreams_;
  chunk::NullSupportedXORGroupTimeAppenderV2* app_;
  std::vector<chunk::NullSupportedXORGroupValueAppenderV2*> apps_;
  std::atomic<int64_t> flushed_txn_;

  int64_t log_clean_txn_;  // used to store the temporary max txn when cleaning
                           // logs.

  std::set<int> current_idx_;

  // Inverted index.
  std::vector<std::string> tags_;
  std::vector<std::string> postings_;

#if USE_MMAP_LABELS
  MMapMemGroup(const label::Labels& group_lset,
               const std::vector<label::Labels>& individual_lsets, uint64_t id,
               MMapGroupXORChunk* xor_array, bool alloc_mmap_slot,
               MMapGroupLabels* mlabels, int64_t labels_idx);
  MMapMemGroup(const label::Labels& group_lset, uint64_t id,
               MMapGroupXORChunk* xor_array, bool alloc_mmap_slot,
               MMapGroupLabels* mlabels, int64_t labels_idx);
#else
  MMapMemGroup(const label::Labels& group_lset,
               const std::vector<label::Labels>& individual_lsets, uint64_t id,
               MMapGroupXORChunk* xor_array, bool alloc_mmap_slot);
  MMapMemGroup(const label::Labels& group_lset, uint64_t id,
               MMapGroupXORChunk* xor_array, bool alloc_mmap_slot);
#endif

  ~MMapMemGroup();

  int64_t min_time() {
    if (min_time_ == std::numeric_limits<int64_t>::max())
      return std::numeric_limits<int64_t>::min();
    return min_time_;
  }

  int64_t max_time() {
    if (max_time_ == std::numeric_limits<int64_t>::min())
      return std::numeric_limits<int64_t>::max();
    return max_time_;
  }

#if USE_MMAP_LABELS
  // Current solution is just to add new timeseries to the end of the array,
  // the labels are not guaranteed to be sorted.
  uint32_t add_timeseries(const label::Labels& lset, bool alloc_value_slot,
                          int64_t mmap_labels_slot);
#else
  uint32_t add_timeseries(const label::Labels& lset, bool alloc_value_slot);
#endif

  void _check_time_slot_init();
  void _check_value_slot_init(int idx);

  int _inverted_index_lower_bound(const std::string& s);
  void inverted_index_add(const std::string& tag_name,
                          const std::string& tag_value, int slot);
  void inverted_index_get(const std::string& tag_name,
                          const std::string& tag_value, std::vector<int>* l);

  int _lower_bound(const std::vector<int64_t>& times, int64_t target);
  void _insert_null_into_series(int i, int idx, int samples);

  // Return whether the tuple is flushed to LSM.
  // NOTE(Alec): txn is encoded to the value when there is a flush,
  // which is used to remove the log of leveldb.
  // 1. all timeseries, insert -1 to set.
  bool append(leveldb::DB* db, int64_t timestamp,
              const std::vector<double>& values, int64_t txn = 0,
              std::vector<std::string>* flush_keys = nullptr,
              std::vector<std::string>* flush_values = nullptr);
  // 2. some of timeseries, passed with slots.
  bool append(leveldb::DB* db, const std::vector<int>& slots, int64_t timestamp,
              const std::vector<double>& values, int64_t txn = 0,
              std::vector<std::string>* flush_keys = nullptr,
              std::vector<std::string>* flush_values = nullptr);
  // 3. some of timeseries, passed with individual labels.
  // Currently this allows merging new timeseries, while slow - O(N^2).
  // TODO(Alec): speed up.
  bool append(leveldb::DB* db, const std::vector<label::Labels>& lsets,
              int64_t timestamp, const std::vector<double>& values,
              std::vector<int>* slots);

  void TEST_flush();
  bool TEST_append(int64_t timestamp, const std::vector<double>& values);
  bool TEST_append(const std::vector<int>& slots, int64_t timestamp,
                   const std::vector<double>& values);

  // May merge new timeseries.
  void get_indices_with_labels(const std::vector<label::Labels>& lsets,
                               std::vector<int>* indices);
  void get_indices_with_labels(const std::vector<label::Labels>& lsets,
                               std::vector<int>* indices,
                               std::vector<int>* new_ts_idx,
                               bool alloc_value_slot, int64_t labels_idx);

  leveldb::Status flush(leveldb::DB* db, int64_t txn = 0,
                        std::vector<std::string>* flush_keys = nullptr,
                        std::vector<std::string>* flush_values = nullptr);

  void encode_group(std::string* s);

  // Some of timeseries.
  void encode_group(const std::vector<int>& slots, std::string* s);

  bool _match_group_labels(const ::tsdb::label::MatcherInterface* l);

  // NOTE(Alec): currently no sorting on labels,
  // may add sorting later if needed.
  void query(const std::vector<::tsdb::label::MatcherInterface*>& l,
             std::vector<int>* slots, label::Labels* group_lset,
             std::vector<label::Labels>* individual_lsets,
             std::string* chunk_contents);

  void read_lock() {
    bool b = true;
    while (b) {
      int x = mutex_.get();
      if (x == -1) continue;
      if (mutex_.cas(x, x + 1)) b = false;
    }
  }

  void read_unlock() { mutex_.decrement(); }

  void write_lock() {
    bool b = true;
    while (b) {
      int x = mutex_.get();
      if (x > 0) continue;
      if (mutex_.cas(0, -1)) b = false;
    }
  }

  void write_unlock() { mutex_.increment(); }
};

}  // namespace head
}  // namespace tsdb

#endif