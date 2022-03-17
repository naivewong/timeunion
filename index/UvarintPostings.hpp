#ifndef UVARINT_POSTINGS_H
#define UVARINT_POSTINGS_H

#include "index/PostingsInterface.hpp"

namespace tsdb {
namespace index {

class UvarintPostings : public PostingsInterface {
 private:
  const uint8_t *p;
  uint32_t size;
  mutable uint32_t index;
  mutable uint64_t cur;

 public:
  UvarintPostings(const uint8_t *p, uint32_t size);

  bool next() const;

  bool seek(uint64_t v) const;

  uint64_t at() const;
};

}  // namespace index
}  // namespace tsdb

#endif