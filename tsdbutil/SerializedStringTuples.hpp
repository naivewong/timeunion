#ifndef SERIALIZEDSTRINGTUPLES_H
#define SERIALIZEDSTRINGTUPLES_H

#include <stdint.h>

#include <boost/function.hpp>

#include "tsdbutil/StringTuplesInterface.hpp"

namespace tsdb {
namespace tsdbutil {

class SerializedStringTuples : public StringTuplesInterface {
 public:
  typedef boost::function<std::string(uint64_t)> Lookup;

  const uint8_t *b;
  uint32_t tuple_size;  // Currently should be 1
  uint32_t size;
  uint32_t len_;
  Lookup lookup;
  bool clean_;

  SerializedStringTuples() : size(0), len_(0), clean_(false) {}
  SerializedStringTuples(const uint8_t *b, uint32_t tuple_size, uint32_t size,
                         const Lookup &lookup, bool clean = false)
      : b(b),
        size(size),
        tuple_size(tuple_size),
        len_(size / (tuple_size * 4)),
        lookup(lookup),
        clean_(clean) {}
  ~SerializedStringTuples() {
    if (clean_) delete b;
  }

  uint32_t len() { return len_; }

  std::string at(int i) {
    if (i >= len_) return "";
    return lookup(base::get_uint32_big_endian(b + 4 * tuple_size * i));
  }
};

}  // namespace tsdbutil
}  // namespace tsdb

#endif