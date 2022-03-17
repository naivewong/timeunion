#include "index/UvarintPostings.hpp"

#include <algorithm>

#include "base/Endian.hpp"

namespace tsdb {
namespace index {

UvarintPostings::UvarintPostings(const uint8_t *p, uint32_t size)
    : p(p), size(size), index(0) {}

bool UvarintPostings::next() const {
  if (index >= size || size == 0) return false;

  int decoded = 0;
  try {
    cur = base::decode_unsigned_varint(
        p + index, decoded,
        std::min(static_cast<int>(size - index), base::MAX_VARINT_LEN_64));
  } catch (const base::TSDBException &e) {
    index = size;
    return false;
  }
  index += decoded;
  return true;
}

bool UvarintPostings::seek(uint64_t v) const {
  if (size == 0) return false;
  if (index == 0) {
    if (!next()) {
      return false;
    }
  }
  if (cur >= v) return true;

  uint32_t i = index;
  while (i < size) {
    int decoded = 0;
    try {
      cur = base::decode_unsigned_varint(
          p + i, decoded,
          std::min(static_cast<int>(size - i), base::MAX_VARINT_LEN_64));
    } catch (const base::TSDBException &e) {
      index = size;
      return false;
    }
    i += decoded;
    if (cur >= v) {
      index = i;
      return true;
    }
  }
  index = i;
  return false;
}

uint64_t UvarintPostings::at() const { return cur; }

}  // namespace index
}  // namespace tsdb