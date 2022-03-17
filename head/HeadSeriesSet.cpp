#include "head/HeadSeriesSet.hpp"

#include <limits>

#include "chunk/XORIterator.hpp"
#include "index/EmptyPostings.hpp"
#include "index/IntersectPostings.hpp"
#include "leveldb/db/log_writer.h"

namespace tsdb {
namespace head {

std::pair<std::unique_ptr<::tsdb::index::PostingsInterface>, bool>
postings_for_matcher(const HeadType* ir,
                     ::tsdb::label::MatcherInterface* matcher) {
  std::pair<std::unique_ptr<::tsdb::index::PostingsInterface>, bool> p =
      ir->postings(matcher->name(), matcher->value());
  if (!p.second) {
    return {std::unique_ptr<::tsdb::index::PostingsInterface>(
                new ::tsdb::index::EmptyPostings()),
            false};
  }

  return p;
}

std::pair<std::unique_ptr<::tsdb::index::PostingsInterface>, bool>
postings_for_matchers(const HeadType* ir,
                      const std::vector<::tsdb::label::MatcherInterface*>& l) {
  std::vector<std::unique_ptr<::tsdb::index::PostingsInterface>> r;
  for (::tsdb::label::MatcherInterface* matcher : l) {
    std::pair<std::unique_ptr<::tsdb::index::PostingsInterface>, bool> p =
        postings_for_matcher(ir, matcher);
    if (!p.second) {
      return {std::unique_ptr<::tsdb::index::PostingsInterface>(
                  new ::tsdb::index::EmptyPostings()),
              false};
    }
    r.emplace_back(std::move(p.first));
  }
  // Intersection
  return {std::unique_ptr<::tsdb::index::PostingsInterface>(
              new ::tsdb::index::IntersectPostings1(std::move(r))),
          true};
}

/**********************************************
 *                 HeadIterator               *
 **********************************************/
HeadIterator::HeadIterator(std::string* s, int64_t mint, int64_t maxt)
    : chunk_(reinterpret_cast<const uint8_t*>(s->data()), s->size()),
      end_(false),
      min_time_(mint),
      max_time_(maxt),
      init_(false) {
  iter_ = chunk_.xor_iterator();
}

bool HeadIterator::seek(int64_t t) const {
  if (t > max_time_) return false;
  if (!init_) {
    init_ = true;
    while (true) {
      if (!iter_->next()) {
        end_ = true;
        break;
      }
      if (iter_->at().first > max_time_) {
        end_ = true;
        break;
      } else if (iter_->at().first >= min_time_)
        break;
    }
  }
  if (end_) return false;
  while (iter_->next()) {
    if (iter_->at().first > max_time_) {
      end_ = true;
      return false;
    } else if (iter_->at().first >= t)
      return true;
  }
  end_ = true;
  return false;
}

std::pair<int64_t, double> HeadIterator::at() const {
  if (end_) return {0, 0};
  return iter_->at();
}

bool HeadIterator::next() const {
  if (!init_) {
    init_ = true;
    while (true) {
      if (!iter_->next()) {
        end_ = true;
        break;
      }
      if (iter_->at().first > max_time_) {
        end_ = true;
        break;
      } else if (iter_->at().first >= min_time_)
        break;
    }
    return !end_;
  }
  if (end_) return false;
  end_ = !iter_->next();
  if (!end_ && iter_->at().first > max_time_) end_ = true;
  return !end_;
}

/**********************************************
 *              HeadGroupIterator             *
 **********************************************/
HeadGroupIterator::HeadGroupIterator(
    std::unique_ptr<chunk::ChunkIteratorInterface>&& it, int64_t mint,
    int64_t maxt)
    : iter_(std::move(it)),
      end_(false),
      min_time_(mint),
      max_time_(maxt),
      init_(false) {}

inline void HeadGroupIterator::init() const {
  while (true) {
    if (!iter_->next()) {
      end_ = true;
      break;
    }
    if (iter_->at().first > max_time_) {
      end_ = true;
      break;
    } else if (iter_->at().second == std::numeric_limits<double>::max())
      continue;
    else if (iter_->at().first >= min_time_)
      break;
  }
  init_ = true;
}

bool HeadGroupIterator::seek(int64_t t) const {
  if (t > max_time_) return false;
  if (end_) return false;
  if (!init_) {
    init();
    return !end_;
  }
  while (iter_->next()) {
    if (iter_->at().first > max_time_) {
      end_ = true;
      return false;
    } else if (iter_->at().second == std::numeric_limits<double>::max())
      continue;
    else if (iter_->at().first >= t)
      return true;
  }
  end_ = true;
  return false;
}

std::pair<int64_t, double> HeadGroupIterator::at() const {
  if (end_) return {0, 0};
  return iter_->at();
}

bool HeadGroupIterator::next() const {
  if (end_) return false;
  if (!init_) {
    init();
    return !end_;
  }
  while (iter_->next()) {
    if (iter_->at().first > max_time_) {
      end_ = true;
      return false;
    } else if (iter_->at().second == std::numeric_limits<double>::max())
      continue;
    return true;
  }
  end_ = !iter_->next();
  if (!end_ && iter_->at().first > max_time_) end_ = true;
  return !end_;
}

/**********************************************
 *              HeadGroupSeries               *
 **********************************************/
HeadGroupSeries::HeadGroupSeries(
    HeadType* head, uint64_t id, int64_t mint, int64_t maxt,
    const std::vector<::tsdb::label::MatcherInterface*>& l)
    : head_(head),
      gid_(id),
      init_(false),
      err_(false),
      min_time_(mint),
      max_time_(maxt),
      cur_idx_(-1) {
  err_ = !head_->group(id, l, &slots_, &g_lset_, &individual_lsets_,
                       &chunk_contents_);
  if (!err_)
    chunk_.reset(new chunk::NullSupportedXORGroupChunk(
        reinterpret_cast<const uint8_t*>(chunk_contents_.c_str()),
        chunk_contents_.size()));
}

/**********************************************
 *                HeadSeriesSet               *
 **********************************************/
HeadSeriesSet::HeadSeriesSet(
    HeadType* head, const std::vector<::tsdb::label::MatcherInterface*>& l,
    int64_t mint, int64_t maxt)
    : head_(head), min_time_(mint), max_time_(maxt), matchers_(l) {
  auto p = postings_for_matchers(head, l);
  p_ = std::move(p.first);
}

bool HeadSeriesSet::next() const {
  if (err_) return false;

  if (!p_->next()) return false;

  cur_ = p_->at();
  return true;
}

std::unique_ptr<::tsdb::querier::SeriesInterface> HeadSeriesSet::at() {
  if (err_) nullptr;
  if ((cur_ >> 63) == 0)
    return std::unique_ptr<::tsdb::querier::SeriesInterface>(
        new HeadSeries(head_, cur_, min_time_, max_time_));
  return std::unique_ptr<::tsdb::querier::SeriesInterface>(
      new HeadGroupSeries(head_, cur_, min_time_, max_time_, matchers_));
}

}  // namespace head
}  // namespace tsdb