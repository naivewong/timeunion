#ifndef INCREMENT_POSTINGS_H
#define INCREMENT_POSTINGS_H

#include "index/PostingsInterface.hpp"

namespace tsdb {
namespace index {

// Should be positive range.
class IncrementPostings : public PostingsInterface {
 private:
  int begin;
  mutable int i;
  int end;

 public:
  IncrementPostings(int begin, int end)
      : begin(begin), i(begin - 1), end(end) {}

  bool next() const {
    ++i;
    if (i == end) return false;
    return true;
  }

  bool seek(uint64_t v) const {
    if (v < begin || v >= end) return false;
    i = static_cast<int>(v);
    return true;
  }

  // next() or seek() must be called first.
  uint64_t at() const { return static_cast<uint64_t>(i); }
};

}  // namespace index
}  // namespace tsdb

#endif