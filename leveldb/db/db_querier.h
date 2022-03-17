#pragma once

#include "db/db_impl.h"

#include "index/PostingsInterface.hpp"
#include "partition_index.h"
#include "querier/QuerierInterface.hpp"
#include "querier/SeriesInterface.hpp"
#include "querier/SeriesSetInterface.hpp"

namespace leveldb {

struct CachedTSSamples {
  std::vector<int64_t>* timestamps;
  std::vector<double>* values;
};

class DBQuerier;
class MemTable;
class PartitionDiskIndexReader;

class MemSeriesIterator : public ::tsdb::querier::SeriesIteratorInterface {
 private:
  int64_t min_time_;
  int64_t max_time_;
  uint64_t tsid_;
  int slot_;
  Status s_;
  mutable ::tsdb::error::Error err_;

  std::unique_ptr<Iterator> iter_;

  mutable std::vector<int64_t>* t_;
  mutable std::vector<double>* v_;
  mutable int sub_idx_;
  mutable bool init_;

  Cache* cache_;
  mutable Cache::Handle* handle_;
  mutable Cache::Handle* handle2_;

  void decode_value(const Slice& key, const Slice& s) const;
  void lookup_cached_ts(const Slice& key, CachedTSSamples** samples,
                        bool* create_ts) const;
  void lookup_cached_group(const Slice& key, bool* create_times,
                           bool* create_vals) const;

 public:
  MemSeriesIterator(MemTable* mem, int64_t min_time, int64_t max_time,
                    uint64_t id, Cache* cache = nullptr);
  MemSeriesIterator(MemTable* mem, int64_t min_time, int64_t max_time,
                    uint64_t id, int slot, Cache* cache = nullptr);
  ~MemSeriesIterator();
  bool seek(int64_t t) const;
  std::pair<int64_t, double> at() const;
  bool next() const;
  bool error() const { return err_; }
};

class L0SeriesIterator : public ::tsdb::querier::SeriesIteratorInterface {
 private:
  const DBQuerier* q_;
  int partition_;
  uint64_t tsid_;
  int slot_;
  Status s_;
  mutable ::tsdb::error::Error err_;

  std::unique_ptr<Iterator> iter_;

  mutable std::vector<int64_t>* t_;
  mutable std::vector<double>* v_;
  mutable int sub_idx_;
  mutable bool init_;

  Cache* cache_;
  mutable Cache::Handle* handle_;
  mutable Cache::Handle* handle2_;

  void decode_value(const Slice& key, const Slice& s) const;
  void lookup_cached_ts(const Slice& key, CachedTSSamples** samples,
                        bool* create_ts) const;
  void lookup_cached_group(const Slice& key, bool* create_times,
                           bool* create_vals) const;

 public:
  L0SeriesIterator(const DBQuerier* q, int partition, uint64_t id,
                   Cache* cache = nullptr);
  L0SeriesIterator(const DBQuerier* q, int partition, uint64_t id, int slot,
                   Cache* cache = nullptr);
  L0SeriesIterator(const DBQuerier* q, uint64_t id, Iterator* it,
                   Cache* cache = nullptr);
  L0SeriesIterator(const DBQuerier* q, uint64_t id, int slot, Iterator* it,
                   Cache* cache = nullptr);
  ~L0SeriesIterator();
  bool seek(int64_t t) const;
  std::pair<int64_t, double> at() const;
  bool next() const;
  bool error() const { return err_; }
};

class L1SeriesIterator : public ::tsdb::querier::SeriesIteratorInterface {
 private:
  const DBQuerier* q_;
  int partition_;
  uint64_t tsid_;
  int slot_;
  mutable ::tsdb::error::Error err_;

  std::unique_ptr<Iterator> iter_;

  mutable std::vector<int64_t>* t_;
  mutable std::vector<double>* v_;
  mutable int sub_idx_;
  mutable bool init_;

  Cache* cache_;
  mutable Cache::Handle* handle_;
  mutable Cache::Handle* handle2_;

  void decode_value(const Slice& key, const Slice& s) const;
  void lookup_cached_ts(const Slice& key, CachedTSSamples** samples,
                        bool* create_ts) const;
  void lookup_cached_group(const Slice& key, bool* create_times,
                           bool* create_vals) const;

 public:
  L1SeriesIterator(const DBQuerier* q, int partition, uint64_t id,
                   Cache* cache = nullptr);
  L1SeriesIterator(const DBQuerier* q, int partition, uint64_t id, int slot,
                   Cache* cache = nullptr);
  ~L1SeriesIterator();
  bool seek(int64_t t) const;
  std::pair<int64_t, double> at() const;
  bool next() const;
  bool error() const { return err_; }
};

class L2SeriesIterator : public ::tsdb::querier::SeriesIteratorInterface {
 private:
  const DBQuerier* q_;
  int partition_;
  uint64_t tsid_;
  int slot_;
  mutable ::tsdb::error::Error err_;

  std::unique_ptr<Iterator> iter_;

  mutable std::vector<int64_t>* t_;
  mutable std::vector<double>* v_;
  mutable int sub_idx_;
  mutable bool init_;

  Cache* cache_;
  mutable Cache::Handle* handle_;
  mutable Cache::Handle* handle2_;

  void decode_value(const Slice& key, const Slice& s) const;
  void lookup_cached_ts(const Slice& key, CachedTSSamples** samples,
                        bool* create_ts) const;
  void lookup_cached_group(const Slice& key, bool* create_times,
                           bool* create_vals) const;

 public:
  L2SeriesIterator(const DBQuerier* q, int partition, uint64_t id,
                   Cache* cache = nullptr);
  L2SeriesIterator(const DBQuerier* q, int partition, uint64_t id, int slot,
                   Cache* cache = nullptr);
  L2SeriesIterator(const DBQuerier* q, uint64_t id, Iterator* it,
                   Cache* cache = nullptr);
  L2SeriesIterator(const DBQuerier* q, uint64_t id, int slot, Iterator* it,
                   Cache* cache = nullptr);
  ~L2SeriesIterator();
  bool seek(int64_t t) const;
  std::pair<int64_t, double> at() const;
  bool next() const;
  bool error() const { return err_; }
};

class MergeSeriesIterator : public ::tsdb::querier::SeriesIteratorInterface {
 private:
  std::vector<::tsdb::querier::SeriesIteratorInterface*> iters_;
  mutable int idx_;
  mutable bool err_;

 public:
  MergeSeriesIterator(
      const std::vector<::tsdb::querier::SeriesIteratorInterface*>& iters)
      : iters_(iters), idx_(0), err_(false) {}
  ~MergeSeriesIterator();

  bool seek(int64_t t) const;
  std::pair<int64_t, double> at() const { return iters_[idx_]->at(); }
  bool next() const;
  bool error() const { return err_; }
};

class DBSeries : public ::tsdb::querier::SeriesInterface {
 private:
  const DBQuerier* q_;
  uint64_t tsid_;
  mutable ::tsdb::label::Labels lset_;
  mutable std::string head_chunk_contents_;
  mutable bool init_;

 public:
  DBSeries(const DBQuerier* q, uint64_t id) : q_(q), tsid_(id), init_(false) {}

  const ::tsdb::label::Labels& labels() const;
  std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> iterator();
  std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> chain_iterator();

  bool next() { return false; }

  ::tsdb::querier::SeriesType type() { return ::tsdb::querier::kTypeSeries; }
};

class DBGroup : public ::tsdb::querier::SeriesInterface {
 private:
  const DBQuerier* q_;
  uint64_t gid_;
  mutable ::tsdb::label::Labels group_lset_;
  mutable std::string head_chunk_contents_;
  int cur_idx_;
  std::vector<int> slots_;
  std::vector<::tsdb::label::Labels> individual_lsets_;
  std::unique_ptr<::tsdb::chunk::NullSupportedXORGroupChunk> chunk_;
  bool err_;

 public:
  DBGroup(const DBQuerier* q, uint64_t id,
          const std::vector<::tsdb::label::MatcherInterface*>& l);

  // return group labels.
  const ::tsdb::label::Labels& labels() const { return group_lset_; }
  void labels(::tsdb::label::Labels& lset) {
    lset.insert(lset.end(), group_lset_.begin(), group_lset_.end());
    lset.insert(lset.end(), individual_lsets_[cur_idx_].begin(),
                individual_lsets_[cur_idx_].end());
  }

  std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> iterator();
  std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> chain_iterator();

  bool next() {
    if (err_) return false;
    ++cur_idx_;
    return cur_idx_ < slots_.size();
  }

  ::tsdb::querier::SeriesType type() { return ::tsdb::querier::kTypeGroup; }
};

class DBSeriesSet : public ::tsdb::querier::SeriesSetInterface {
 private:
  const DBQuerier* q_;
  std::unique_ptr<::tsdb::index::PostingsInterface> p_;
  mutable uint64_t cur_id_;
  ::tsdb::error::Error err_;
  std::vector<::tsdb::label::MatcherInterface*> matchers_;

 public:
  DBSeriesSet(const DBQuerier* q,
              const std::vector<::tsdb::label::MatcherInterface*>& l);

  bool next() const {
    if (err_) return false;

    if (!p_->next()) return false;

    cur_id_ = p_->at();
    return true;
  }

  std::unique_ptr<::tsdb::querier::SeriesInterface> at() {
    if (err_)
      nullptr;
    else if ((cur_id_ >> 63) == 0)
      return std::unique_ptr<::tsdb::querier::SeriesInterface>(
          new DBSeries(q_, cur_id_));
    return std::unique_ptr<::tsdb::querier::SeriesInterface>(
        new DBGroup(q_, cur_id_, matchers_));
  }

  uint64_t current_tsid() { return cur_id_; }
  bool error() const { return err_; }
};

class DBQuerier : public ::tsdb::querier::QuerierInterface {
 private:
  friend class MemSeries;
  friend class MemSeriesIterator;
  friend class L0Series;
  friend class L0SeriesIterator;
  friend class L1Series;
  friend class L1SeriesIterator;
  friend class L2Series;
  friend class L2SeriesIterator;
  friend class DBSeries;
  friend class DBGroup;
  friend class DBSeriesSet;

  mutable DBImpl* db_;
  mutable ::tsdb::head::HeadType* head_;
  mutable Version* current_;
  bool need_unref_current_;
  int64_t min_time_;
  int64_t max_time_;
  const Comparator* cmp_;
  mutable ::tsdb::error::Error err_;
  Status s_;

  MemTable* mem_;
  // MemTable* imm_;
  std::vector<MemTable*> imms_;

  std::vector<std::pair<int64_t, int64_t>> l0_partitions_;
  std::vector<int> l0_indexes_;
  std::vector<std::pair<int64_t, int64_t>> l1_partitions_;
  std::vector<int> l1_indexes_;
  std::vector<std::pair<int64_t, int64_t>> l2_partitions_;
  std::vector<int> l2_indexes_;

  Cache* cache_;

  // Needs to be protected by lock.
  void register_mem_partitions();
  void register_disk_partitions();

 public:
  DBQuerier(DBImpl* db, ::tsdb::head::HeadType* head, int64_t min_time,
            int64_t max_time, Cache* cache = nullptr);

  // Note(Alec). only for test.
  DBQuerier(MemTable* mem, ::tsdb::head::HeadType* head, int64_t min_time,
            int64_t max_time);
  DBQuerier(MemTable* mem1, MemTable* mem2, ::tsdb::head::HeadType* head,
            int64_t min_time, int64_t max_time);
  DBQuerier(::tsdb::head::HeadType* head, int l, Version* v,
            const std::vector<std::pair<int64_t, int64_t>>& partitions,
            const std::vector<int>& indexes, const Comparator* cmp,
            int64_t min_time, int64_t max_time);

  ~DBQuerier();

  int64_t mint() { return min_time_; }
  int64_t maxt() { return max_time_; }

  // Note(Alec), currently may only support equal matcher.
  std::unique_ptr<::tsdb::querier::SeriesSetInterface> select(
      const std::vector<::tsdb::label::MatcherInterface*>& l) const;

  std::vector<std::string> label_values(const std::string& s) const;

  std::vector<std::string> label_names() const;

  ::tsdb::error::Error error() const { return err_; }
};

}  // namespace leveldb
