#ifndef MERGEDSERIESSET_H
#define MERGEDSERIESSET_H

#include <deque>

#include "querier/QuerierUtils.hpp"
#include "querier/SeriesInterface.hpp"
#include "querier/SeriesSetInterface.hpp"

namespace tsdb {
namespace querier {

// View it as a collections of blocks sorted by time.
class MergedSeriesSet : public SeriesSetInterface {
 private:
  mutable std::shared_ptr<SeriesSets> ss;
  // std::shared_ptr<SeriesInterface> cur;

  // For SeriesInterface s that have same labels.
  mutable std::shared_ptr<Series> series;
  mutable std::deque<int> id;
  mutable bool err_;

 public:
  MergedSeriesSet(const std::shared_ptr<SeriesSets> &ss);

  bool next_helper() const;

  bool next() const;

  std::unique_ptr<SeriesInterface> at();

  bool error() const;
};

}  // namespace querier
}  // namespace tsdb

#endif