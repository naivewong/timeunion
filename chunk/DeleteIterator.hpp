#ifndef DELETEITERATOR_H
#define DELETEITERATOR_H

#include <memory>

#include "chunk/ChunkIteratorInterface.hpp"
#include "tombstone/Interval.hpp"

namespace tsdb {
namespace chunk {

// DeletedIterator wraps an Iterator and makes sure any deleted metrics are not
// returned.
class DeleteIterator : public ChunkIteratorInterface {
 private:
  std::unique_ptr<ChunkIteratorInterface> it;
  mutable tombstone::Intervals::const_iterator itvls_begin;
  tombstone::Intervals::const_iterator itvls_end;

 public:
  // NOTICE
  // currently pass a r-value reference
  DeleteIterator(std::unique_ptr<ChunkIteratorInterface> &&it,
                 const tombstone::Intervals::const_iterator &itvls_begin,
                 const tombstone::Intervals::const_iterator &itvls_end);

  std::pair<int64_t, double> at() const;

  bool next() const;

  bool error() const;
};

}  // namespace chunk
}  // namespace tsdb

#endif