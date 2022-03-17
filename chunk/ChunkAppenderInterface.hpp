#ifndef CHUNKAPPENDERINTERFACE_H
#define CHUNKAPPENDERINTERFACE_H

#include <stdint.h>

#include <deque>
#include <initializer_list>

namespace tsdb {
namespace chunk {

class ChunkAppenderInterface {
 public:
  virtual void append(int64_t timestamp, double value) = 0;
  virtual void append_group(int64_t timestamp,
                            const std::deque<double> &values) {}
  virtual void append_group(int64_t timestamp,
                            const std::initializer_list<double> &values) {}
  virtual ~ChunkAppenderInterface() = default;
};

}  // namespace chunk
}  // namespace tsdb

#endif