#ifndef EMPTYAPPENDER_H
#define EMPTYAPPENDER_H

#include "chunk/ChunkAppenderInterface.hpp"

namespace tsdb {
namespace chunk {

class EmptyAppender : public ChunkAppenderInterface {
 public:
  // EmptyAppender()=default;
  void append(int64_t timestamp, double value) {}
};

}  // namespace chunk
}  // namespace tsdb

#endif