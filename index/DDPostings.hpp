#ifndef DDPOSTINGS_H
#define DDPOSTINGS_H

#include <limits>

#include "base/Endian.hpp"
#include "base/TSDBException.hpp"
#include "chunk/BitStream.hpp"
#include "index/PostingsInterface.hpp"

namespace tsdb {
namespace index {

class DDPostings : public PostingsInterface {
 private:
  const uint8_t *p_;
  int64_t size_;
  uint32_t num_;
  mutable uint32_t index_;
  mutable int64_t cur_;
  mutable chunk::BitStream bstream_;
  mutable uint64_t delta_timestamp_;
  mutable bool err_;
  bool clean_;

 public:
  // Need to check if size = 4x before using
  DDPostings(const uint8_t *p, uint32_t size, bool clean = false)
      : p_(p),
        size_(size),
        index_(0),
        cur_(std::numeric_limits<int64_t>::min()),
        bstream_(p, size),
        delta_timestamp_(0),
        err_(false),
        clean_(clean) {
    num_ = base::get_uint32_big_endian(bstream_.bytes_ptr());
    bstream_.pop_front();
    bstream_.pop_front();
    bstream_.pop_front();
    bstream_.pop_front();
  }
  ~DDPostings() {
    if (clean_) delete p_;
  }

  bool next() const {
    if (err_ || index_ >= num_) return false;

    if (index_ == 0) {
      try {
        cur_ = bstream_.read_signed_varint();
      } catch (const base::TSDBException &e) {
        err_ = true;
        return false;
      }
      index_++;
      return true;
    } else if (index_ == 1) {
      try {
        delta_timestamp_ = bstream_.read_signed_varint();
      } catch (const base::TSDBException &e) {
        err_ = true;
        return false;
      }

      cur_ += delta_timestamp_;
      index_++;
      return true;
    }

    // Read timestamp delta-delta
    uint8_t type = 0;
    int64_t delta_delta = 0;
    for (int i = 0; i < 4; i++) {
      try {
        bool bit = bstream_.read_bit();
        type <<= 1;
        if (!bit) break;
        type |= 1;
      } catch (const base::TSDBException &e) {
        err_ = true;
        return false;
      }
    }

    int size = 0;
    switch (type) {
      case 0x02:
        size = 14;
        break;
      case 0x06:
        size = 17;
        break;
      case 0x0e:
        size = 20;
        break;
      case 0x0f:
        try {
          delta_delta = static_cast<int64_t>(bstream_.read_bits(64));
        } catch (const base::TSDBException &e) {
          err_ = true;
          return false;
        }
    }
    if (size != 0) {
      try {
        delta_delta = static_cast<int64_t>(bstream_.read_bits(size));
        if (delta_delta > (1 << (size - 1))) {
          delta_delta -= (1 << size);
        }
      } catch (const base::TSDBException &e) {
        err_ = true;
        return false;
      }
    }

    // Possible overflow
    delta_timestamp_ = static_cast<uint64_t>(
        delta_delta + static_cast<int64_t>(delta_timestamp_));
    cur_ += static_cast<int64_t>(delta_timestamp_);
    index_++;
    return true;
  }

  bool seek(uint64_t v) const {
    if (err_ || index_ >= num_) return false;
    while (cur_ < v && next()) {
    }
    if (err_ || index_ >= num_ || cur_ < v) return false;
    return true;
  }

  uint64_t at() const { return cur_; }
};

}  // namespace index
}  // namespace tsdb

#endif