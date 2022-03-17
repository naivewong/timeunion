#ifndef CHAINSERIESITERATOR_H
#define CHAINSERIESITERATOR_H

#include <utility>

#include "querier/QuerierUtils.hpp"
#include "querier/SeriesIteratorInterface.hpp"

namespace tsdb {
namespace querier {

// chainedSeriesIterator implements a series iterater over a list
// of time-sorted, non-overlapping iterators.
class ChainSeriesIterator : public SeriesIteratorInterface {
 private:
  std::shared_ptr<Series> series;
  mutable int i;
  mutable std::unique_ptr<SeriesIteratorInterface> cur;

 public:
  ChainSeriesIterator(const std::shared_ptr<Series> &series);

  bool seek(int64_t t) const;

  std::pair<int64_t, double> at() const;

  bool next() const;

  bool error() const;
};

}  // namespace querier
}  // namespace tsdb

#endif