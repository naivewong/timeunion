#include "chunk/DeleteIterator.hpp"

namespace tsdb {
namespace chunk {

// NOTICE
// currently pass a r-value reference
DeleteIterator::DeleteIterator(
    std::unique_ptr<ChunkIteratorInterface> &&it,
    const tombstone::Intervals::const_iterator &itvls_begin,
    const tombstone::Intervals::const_iterator &itvls_end)
    : it(std::move(it)), itvls_begin(itvls_begin), itvls_end(itvls_end) {}

std::pair<int64_t, double> DeleteIterator::at() const { return it->at(); }

bool DeleteIterator::next() const {
Outer:
  while (it->next()) {
    std::pair<int, int> p = it->at();
    while (itvls_begin != itvls_end) {
      if (itvls_begin->in_bounds(p.first)) goto Outer;

      if (p.first > itvls_begin->max_time) {
        ++itvls_begin;
        continue;
      }
      return true;
    }
    return true;
  }
  return false;
}

bool DeleteIterator::error() const { return it->error(); }

}  // namespace chunk
}  // namespace tsdb