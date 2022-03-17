#ifndef EMPTYITERATOR_H
#define EMPTYITERATOR_H

#include "chunk/ChunkIteratorInterface.hpp"

namespace tsdb {
namespace chunk {

class EmptyIterator : public ChunkIteratorInterface {
 public:
  std::pair<int64_t, double> at() const { return std::make_pair(0, 0); }

  bool next() const { return false; }

  bool error() const { return true; }
};

}  // namespace chunk
}  // namespace tsdb

#endif