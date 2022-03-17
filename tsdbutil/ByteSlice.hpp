#ifndef BYTESLICE_H
#define BYTESLICE_H

#include <stdint.h>

#include <utility>

namespace tsdb {
namespace tsdbutil {

class ByteSlice {
 public:
  virtual int len() const = 0;
  virtual std::pair<const uint8_t *, int> range(
      int begin, int end) const = 0;  // (pointer, size)
  virtual ~ByteSlice() {}
};

}  // namespace tsdbutil
}  // namespace tsdb

#endif