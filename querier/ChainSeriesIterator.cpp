#include "querier/ChainSeriesIterator.hpp"

namespace tsdb {
namespace querier {

// chainedSeriesIterator implements a series iterater over a list
// of time-sorted, non-overlapping iterators.
ChainSeriesIterator::ChainSeriesIterator(const std::shared_ptr<Series> &series)
    : series(series), i(0) {
  cur.reset();
  if (!series->empty()) cur = series->at(0)->iterator();
}

bool ChainSeriesIterator::seek(int64_t t) const {
  for (int j = i; j < series->size(); j++) {
    std::unique_ptr<SeriesIteratorInterface> temp = series->at(i)->iterator();
    if (!temp->seek(t)) continue;
    cur.reset();
    cur = std::move(temp);
    i = j;
    return true;
  }
  return false;
}

std::pair<int64_t, double> ChainSeriesIterator::at() const { return cur->at(); }

bool ChainSeriesIterator::next() const {
  if (!cur) return false;
  if (cur->next()) return true;
  if (cur->error()) return false;
  if (i == series->size() - 1) return false;
  ++i;
  cur.reset();
  cur = series->at(i)->iterator();
  return next();
}

bool ChainSeriesIterator::error() const { return cur->error(); }

}  // namespace querier
}  // namespace tsdb