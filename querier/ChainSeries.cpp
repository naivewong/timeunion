#include "querier/ChainSeries.hpp"

#include "querier/ChainSeriesIterator.hpp"

namespace tsdb {
namespace querier {

// chainedSeries implements a series for a list of time-sorted series.
// They all must have the same labels.
//
// NOTICE
// Never pass a temporary variable to it
ChainSeries::ChainSeries(const std::shared_ptr<Series> &series)
    : series(series) {}

const label::Labels &ChainSeries::labels() const {
  return series->at(0)->labels();
}

std::unique_ptr<SeriesIteratorInterface> ChainSeries::iterator() {
  return std::unique_ptr<SeriesIteratorInterface>(
      new ChainSeriesIterator(series));
}

}  // namespace querier
}  // namespace tsdb