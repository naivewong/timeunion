#include "index/Uint32BEPostings.hpp"

#include "base/Endian.hpp"

namespace tsdb {
namespace index {

// Need to check if size = 4x before using
Uint32BEPostings::Uint32BEPostings(const uint8_t *p, uint32_t size, bool clean)
    : p(p), size(size), index(-4), clean(clean) {}

bool Uint32BEPostings::next() const {
  if (index + 8 > size || size == 0) return false;

  index += 4;
  cur = static_cast<uint64_t>(base::get_uint32_big_endian(p + index));
  return true;
}

uint32_t Uint32BEPostings::lower_bound(uint32_t i, uint64_t v) const {
  uint32_t count = (size - i) / 4;
  while (count > 0) {
    uint32_t it = i;
    uint32_t step = count / 2;
    it += step * 4;
    if ((cur = static_cast<uint64_t>(base::get_uint32_big_endian(p + it))) <
        v) {
      i = it + 1;
      count -= step + 1;
    } else
      count = step;
  }
  return i;
}

bool Uint32BEPostings::seek(uint64_t v) const {
  if (size == 0) return false;
  if (index < 0) {
    if (!next()) {
      return false;
    }
  }
  if (cur >= v) return true;

  uint32_t i = lower_bound(index + 4, v);
  cur = static_cast<uint64_t>(base::get_uint32_big_endian(p + i));

  index = i;
  if (i >= size) return false;
  return true;
}

uint64_t Uint32BEPostings::at() const { return cur; }

Uint32BEDoublePostings::Uint32BEDoublePostings(const uint8_t *p, uint32_t size,
                                               bool clean)
    : p(p), size(size), index(-8), clean(clean) {}

bool Uint32BEDoublePostings::next() const {
  if (index + 8 >= size || size == 0) return false;

  index += 8;
  cur1 = static_cast<uint64_t>(base::get_uint32_big_endian(p + index));
  cur2 = static_cast<uint64_t>(base::get_uint32_big_endian(p + index + 4));
  return true;
}

uint32_t Uint32BEDoublePostings::lower_bound(uint32_t i, uint64_t v) const {
  uint32_t count = (size - i) / 8;
  while (count > 0) {
    uint32_t it = i;
    uint32_t step = count / 2;
    it += step * 8;
    if ((cur1 = static_cast<uint64_t>(base::get_uint32_big_endian(p + it))) <
        v) {
      i = it + 1;
      count -= step + 1;
    } else
      count = step;
  }
  return i;
}

bool Uint32BEDoublePostings::seek(uint64_t v) const {
  if (size == 0) return false;
  if (index < 0) {
    if (!next()) {
      return false;
    }
  }
  if (cur1 >= v) return true;

  uint32_t i = lower_bound(index + 8, v);
  cur1 = static_cast<uint64_t>(base::get_uint32_big_endian(p + i));
  cur2 = static_cast<uint64_t>(base::get_uint32_big_endian(p + i + 4));

  index = i;
  if (i >= size) return false;
  return true;
}

void Uint32BEDoublePostings::at(uint64_t *off, uint64_t *tsid) const {
  *off = cur1;
  *tsid = cur2;
}

Uint32BEDoubleSkipPostings::Uint32BEDoubleSkipPostings(const uint8_t *p,
                                                       uint32_t size,
                                                       bool clean)
    : p(p), size(size), index(-8), clean(clean) {}

bool Uint32BEDoubleSkipPostings::next() const {
  if (index + 8 >= size || size == 0) return false;

  index += 8;
  cur = static_cast<uint64_t>(base::get_uint32_big_endian(p + index));
  return true;
}

uint32_t Uint32BEDoubleSkipPostings::lower_bound(uint32_t i, uint64_t v) const {
  uint32_t count = (size - i) / 8;
  while (count > 0) {
    uint32_t it = i;
    uint32_t step = count / 2;
    it += step * 8;
    if ((cur = static_cast<uint64_t>(base::get_uint32_big_endian(p + it))) <
        v) {
      i = it + 1;
      count -= step + 1;
    } else
      count = step;
  }
  return i;
}

bool Uint32BEDoubleSkipPostings::seek(uint64_t v) const {
  if (size == 0) return false;
  if (index < 0) {
    if (!next()) {
      return false;
    }
  }
  if (cur >= v) return true;

  uint32_t i = lower_bound(index + 8, v);
  cur = static_cast<uint64_t>(base::get_uint32_big_endian(p + i));

  index = i;
  if (i >= size) return false;
  return true;
}

}  // namespace index
}  // namespace tsdb