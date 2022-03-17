#ifndef CHUNKINTERFACE_H
#define CHUNKINTERFACE_H

#include <stdint.h>

#include <deque>
#include <memory>
#include <vector>

#include "chunk/ChunkAppenderInterface.hpp"
#include "chunk/ChunkIteratorInterface.hpp"

namespace tsdb {
namespace chunk {

enum Encoding { EncNone, EncXOR, EncGM1, EncGD1, EncGD2, EncGHC };

class GroupMemoryChunk1;
class ChunkInterface {
  // NOTE Can only have one appender at the same time.
 public:
  virtual const uint8_t* bytes() = 0;
  virtual uint8_t encoding() = 0;
  virtual std::unique_ptr<ChunkAppenderInterface> appender() = 0;
  virtual std::unique_ptr<ChunkIteratorInterface> iterator() = 0;
  virtual std::unique_ptr<ChunkIteratorInterface> iterator(
      int64_t start, uint64_t series_num) {
    return nullptr;
  }
  virtual int num_samples() = 0;
  virtual uint64_t size() = 0;
  virtual ~ChunkInterface() = default;

  virtual GroupMemoryChunk1* get_gmc() { return nullptr; }
};

}  // namespace chunk
}  // namespace tsdb

#endif