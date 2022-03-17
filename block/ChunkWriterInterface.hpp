#ifndef CHUNKWRITERINTERFACE_H
#define CHUNKWRITERINTERFACE_H

#include <deque>
#include <initializer_list>

#include "chunk/ChunkMeta.hpp"

namespace tsdb {

namespace block {

class ChunkWriterInterface {
 public:
  virtual void write_chunks(
      const std::deque<std::shared_ptr<chunk::ChunkMeta>> &chunks) = 0;
  virtual void close() = 0;
  virtual ~ChunkWriterInterface() = default;
};

}  // namespace block

}  // namespace tsdb

#endif