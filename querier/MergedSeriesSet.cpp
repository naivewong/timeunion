#include "querier/MergedSeriesSet.hpp"

#include "base/Logging.hpp"
#include "querier/ChainSeries.hpp"

namespace tsdb {
namespace querier {

// View it as a collections of blocks sorted by time.
MergedSeriesSet::MergedSeriesSet(const std::shared_ptr<SeriesSets> &ss)
    : ss(ss), series(new Series()), err_(false) {
  // To move one step for each SeriesInterface.
  ss->next();
  if (ss->empty()) {
    // LOG_DEBUG << "no next";
    err_ = true;
  }
}

bool MergedSeriesSet::next_helper() const {
  // To move one step for the former SeriesInterface s.
  ss->next(id);

  series->clear();
  id.clear();

  if (ss->empty()) {
    err_ = true;
    return false;
  }

  id.push_back(0);
  for (int i = 1; i < ss->size(); i++) {
    int cmp = label::lbs_compare(ss->at(i)->at()->labels(),
                                 ss->at(0)->at()->labels());
    if (cmp < 0) {
      // series->clear();
      id.clear();
      // series->push_back((*ss)[i]->at());
      id.push_back(i);
    } else if (cmp == 0) {
      // series->push_back((*ss)[i]->at());
      id.push_back(i);
    }
  }
  for (int i : id) series->push_back(ss->at(i)->at());
  return true;
}

bool MergedSeriesSet::next() const {
  if (err_) return false;
  return next_helper();
}

std::unique_ptr<SeriesInterface> MergedSeriesSet::at() {
  if (id.empty()) return nullptr;
  // else if (id.size() == 1)
  //   return (*series)[0];
  else {
    // LOG_INFO << "Create ChainSeries(same lset in several SeriesSetInterface),
    // num of SeriesInterface: " << series->size();
    return std::unique_ptr<SeriesInterface>(new ChainSeries(series));
  }
}

bool MergedSeriesSet::error() const { return err_; }

}  // namespace querier
}  // namespace tsdb