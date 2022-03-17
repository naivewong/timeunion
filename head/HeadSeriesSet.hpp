#pragma once

#include "base/Error.hpp"
#include "chunk/XORChunk.hpp"
#include "head/Head.hpp"
#include "head/MMapHeadWithTrie.hpp"
#include "label/MatcherInterface.hpp"
#include "querier/QuerierInterface.hpp"
#include "querier/SeriesInterface.hpp"
#include "querier/SeriesIteratorInterface.hpp"
#include "querier/SeriesSetInterface.hpp"

namespace tsdb {
namespace head {

std::pair<std::unique_ptr<::tsdb::index::PostingsInterface>, bool>
postings_for_matcher(const HeadType* ir,
                     ::tsdb::label::MatcherInterface* matcher);

std::pair<std::unique_ptr<::tsdb::index::PostingsInterface>, bool>
postings_for_matchers(const HeadType* ir,
                      const std::vector<::tsdb::label::MatcherInterface*>& l);

class HeadIterator : public ::tsdb::querier::SeriesIteratorInterface {
 private:
  chunk::XORChunk chunk_;
  std::unique_ptr<chunk::XORIterator> iter_;
  mutable bool end_;
  int64_t min_time_;
  int64_t max_time_;
  mutable bool init_;

 public:
  HeadIterator(std::string* s, int64_t mint, int64_t maxt);
  bool seek(int64_t t) const;
  std::pair<int64_t, double> at() const;
  bool next() const;
  bool error() const { return end_; }
};

class HeadGroupIterator : public ::tsdb::querier::SeriesIteratorInterface {
 private:
  std::unique_ptr<chunk::ChunkIteratorInterface> iter_;
  mutable bool end_;
  int64_t min_time_;
  int64_t max_time_;
  mutable bool init_;

 public:
  HeadGroupIterator(std::unique_ptr<chunk::ChunkIteratorInterface>&& it,
                    int64_t mint, int64_t maxt);
  bool seek(int64_t t) const;
  std::pair<int64_t, double> at() const;
  bool next() const;
  bool error() const { return end_; }
  void init() const;
};

class HeadSeries : public ::tsdb::querier::SeriesInterface {
 private:
  HeadType* head_;
  uint64_t tsid_;
  mutable ::tsdb::label::Labels lset_;
  mutable std::string chunk_contents_;
  mutable bool init_;
  mutable bool err_;
  int64_t min_time_;
  int64_t max_time_;

 public:
  HeadSeries(HeadType* head, uint64_t id, int64_t mint, int64_t maxt)
      : head_(head),
        tsid_(id),
        init_(false),
        err_(false),
        min_time_(mint),
        max_time_(maxt) {}

  void init() const { err_ = !head_->series(tsid_, lset_, &chunk_contents_); }

  const ::tsdb::label::Labels& labels() const {
    if (!init_) init();

    return lset_;
  }
  std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> iterator() {
    if (!init_) init();

    if (err_)
      return std::unique_ptr<::tsdb::querier::SeriesIteratorInterface>(
          new ::tsdb::querier::EmptySeriesIterator());
    return std::unique_ptr<::tsdb::querier::SeriesIteratorInterface>(
        new HeadIterator(&chunk_contents_, min_time_, max_time_));
  }

  querier::SeriesType type() { return querier::kTypeSeries; }
  bool next() { return false; }
};

class HeadGroupSeries : public ::tsdb::querier::SeriesInterface {
 private:
  HeadType* head_;
  uint64_t gid_;
  mutable ::tsdb::label::Labels g_lset_;
  mutable std::string chunk_contents_;
  mutable bool init_;
  mutable bool err_;
  int64_t min_time_;
  int64_t max_time_;
  int cur_idx_;
  std::vector<int> slots_;
  std::vector<::tsdb::label::Labels> individual_lsets_;
  std::unique_ptr<chunk::NullSupportedXORGroupChunk> chunk_;

 public:
  HeadGroupSeries(HeadType* head, uint64_t id, int64_t mint, int64_t maxt,
                  const std::vector<::tsdb::label::MatcherInterface*>& l);

  bool next() {
    if (err_) return false;
    ++cur_idx_;
    return cur_idx_ < slots_.size();
  }

  const ::tsdb::label::Labels& labels() const { return g_lset_; }

  void labels(label::Labels& lset) {
    lset.insert(lset.end(), g_lset_.begin(), g_lset_.end());
    lset.insert(lset.end(), individual_lsets_[cur_idx_].begin(),
                individual_lsets_[cur_idx_].end());
  }

  std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> iterator() {
    if (err_)
      return std::unique_ptr<::tsdb::querier::SeriesIteratorInterface>(
          new ::tsdb::querier::EmptySeriesIterator());
    return std::unique_ptr<::tsdb::querier::SeriesIteratorInterface>(
        new HeadGroupIterator(std::move(chunk_->iterator(slots_[cur_idx_])),
                              min_time_, max_time_));
  }

  querier::SeriesType type() { return querier::kTypeGroup; }
};

class HeadSeriesSet : public ::tsdb::querier::SeriesSetInterface {
 private:
  HeadType* head_;
  std::unique_ptr<::tsdb::index::PostingsInterface> p_;
  ::tsdb::error::Error err_;

  mutable uint64_t cur_;
  int64_t min_time_;
  int64_t max_time_;
  std::vector<::tsdb::label::MatcherInterface*> matchers_;

 public:
  HeadSeriesSet(HeadType* head,
                const std::vector<::tsdb::label::MatcherInterface*>& l,
                int64_t mint, int64_t maxt);

  bool next() const;
  std::unique_ptr<::tsdb::querier::SeriesInterface> at();
  uint64_t current_tsid() { return cur_; }
  bool error() const { return err_; }
};

class HeadQuerier : public ::tsdb::querier::QuerierInterface {
 private:
  HeadType* head_;
  int64_t min_time_;
  int64_t max_time_;

 public:
  HeadQuerier(HeadType* head, int64_t mint, int64_t maxt)
      : head_(head), min_time_(mint), max_time_(maxt) {}

  std::unique_ptr<::tsdb::querier::SeriesSetInterface> select(
      const std::vector<::tsdb::label::MatcherInterface*>& l) const {
    return std::unique_ptr<::tsdb::querier::SeriesSetInterface>(
        new HeadSeriesSet(head_, l, min_time_, max_time_));
  }

  std::vector<std::string> label_values(const std::string& s) const {
    return head_->label_values(s);
  }

  std::vector<std::string> label_names() const { return head_->label_names(); }

  ::tsdb::error::Error error() const { return ::tsdb::error::Error(); }
};

}  // namespace head.
}  // namespace tsdb.