#ifndef EMPTYPOSTINGS_H
#define EMPTYPOSTINGS_H

#include "index/PostingsInterface.hpp"

namespace tsdb {
namespace index {

class EmptyPostings : public PostingsInterface {
 public:
  bool next() const { return false; }

  bool seek(uint64_t v) const { return false; }

  uint64_t at() const { return 0; }
};

}  // namespace index
}  // namespace tsdb

#endif