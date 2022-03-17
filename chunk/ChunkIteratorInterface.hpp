#ifndef CHUNKITERATORINTERFACE_H
#define CHUNKITERATORINTERFACE_H

#include <stdint.h>

#include <utility>

namespace tsdb {
namespace chunk {

class ChunkIteratorInterface {
 public:
  virtual std::pair<int64_t, double> at() const = 0;
  virtual void at(int64_t* t, double* v) const {}
  virtual bool seek(int64_t t) const { return false; }
  virtual bool next() const = 0;
  virtual bool error() const = 0;
  virtual ~ChunkIteratorInterface() = default;
};

}  // namespace chunk
}  // namespace tsdb

#endif