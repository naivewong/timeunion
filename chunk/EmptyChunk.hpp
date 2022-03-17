#ifndef EMPTYCHUNK_H
#define EMPTYCHUNK_H

#include "chunk/ChunkInterface.hpp"
#include "chunk/EmptyAppender.hpp"
#include "chunk/EmptyIterator.hpp"

namespace tsdb {
namespace chunk {

class EmptyChunk : public ChunkInterface {
 public:
  const uint8_t *bytes() { return NULL; }

  uint8_t encoding() { return static_cast<uint8_t>(EncNone); }

  std::unique_ptr<ChunkAppenderInterface> appender() {
    return std::unique_ptr<ChunkAppenderInterface>(new EmptyAppender());
  }

  std::unique_ptr<ChunkIteratorInterface> iterator() {
    return std::unique_ptr<ChunkIteratorInterface>(new EmptyIterator());
  }

  int num_samples() { return 0; }

  uint64_t size() { return 0; }
};

}  // namespace chunk
}  // namespace tsdb

#endif