#ifndef XORIterator_H
#define XORIterator_H

#include <limits>

#include "chunk/BitStream.hpp"
#include "chunk/ChunkIteratorInterface.hpp"
// #include <iostream>

namespace tsdb {
namespace chunk {

class XORIterator : public ChunkIteratorInterface {
 public:
  mutable BitStream bstream;
  mutable int64_t timestamp;  // Millisecond
  mutable double value;
  mutable uint64_t delta_timestamp;
  mutable uint8_t leading_zero;
  mutable uint8_t trailing_zero;
  mutable uint16_t num_total;
  mutable uint16_t num_read;
  mutable bool err_;
  bool safe_mode;

 public:
  // Read mode BitStream
  XORIterator(BitStream& bstream, bool safe_mode);
  // Headless.
  XORIterator(BitStream& bstream, bool safe_mode, int num_samples);

  std::pair<int64_t, double> at() const {
    return std::make_pair(timestamp, value);
  }
  void at(int64_t* t, double* v) const {
    *t = timestamp;
    *v = value;
  }

  bool next() const;

  bool read_value() const;

  bool error() const { return err_; }
};

class XORIteratorV2 : public ChunkIteratorInterface {
 public:
  mutable BitStreamV2 bstream;
  mutable int64_t timestamp;  // Millisecond
  mutable double value;
  mutable uint64_t delta_timestamp;
  mutable uint8_t leading_zero;
  mutable uint8_t trailing_zero;
  mutable uint16_t num_total;
  mutable uint16_t num_read;
  mutable bool err_;

 public:
  // Read mode BitStreamV2
  XORIteratorV2(BitStreamV2& bstream);
  // Headless.
  XORIteratorV2(BitStreamV2& bstream, int num_samples);
  XORIteratorV2(uint8_t* ptr, int size);

  std::pair<int64_t, double> at() const {
    return std::make_pair(timestamp, value);
  }
  void at(int64_t* t, double* v) const {
    *t = timestamp;
    *v = value;
  }

  bool next() const;

  bool read_value() const;

  bool error() const { return err_; }
};

class NullSupportedXORIterator : public ChunkIteratorInterface {
 public:
  mutable BitStream bstream;
  mutable int64_t timestamp;  // Millisecond
  mutable double value;
  mutable uint64_t delta_timestamp;
  mutable uint8_t leading_zero;
  mutable uint8_t trailing_zero;
  mutable uint16_t num_total;
  mutable uint16_t num_read;
  mutable bool err_;
  mutable bool null_value_;
  bool safe_mode;

 public:
  // Read mode BitStream
  NullSupportedXORIterator(BitStream& bstream, bool safe_mode);
  // Headless.
  NullSupportedXORIterator(BitStream& bstream, bool safe_mode, int num_samples);

  std::pair<int64_t, double> at() const {
    if (null_value_)
      return std::make_pair(timestamp, std::numeric_limits<double>::max());
    return std::make_pair(timestamp, value);
  }
  void at(int64_t* t, double* v) const {
    *t = timestamp;
    if (null_value_)
      *v = std::numeric_limits<double>::max();
    else
      *v = value;
  }

  bool next() const;

  bool read_value() const;

  bool error() const { return err_; }
};

class NullSupportedXORGroupIterator : public ChunkIteratorInterface {
 public:
  mutable BitStream t_bstream_;
  mutable BitStream v_bstream_;
  mutable int64_t timestamp;  // Millisecond
  mutable double value;
  mutable uint64_t delta_timestamp;
  mutable uint8_t leading_zero;
  mutable uint8_t trailing_zero;
  mutable uint16_t num_total;
  mutable uint16_t num_read;
  mutable bool err_;
  bool read_mode_chunk_;
  mutable bool null_value_;

 public:
  NullSupportedXORGroupIterator(BitStream& t, BitStream& v);
  NullSupportedXORGroupIterator(const uint8_t* ptr, int off,
                                int size);  // not strict size.
  NullSupportedXORGroupIterator(const uint8_t* ptr1, int size1,
                                const uint8_t* ptr2, int size2);

  std::pair<int64_t, double> at() const {
    if (null_value_)
      return std::make_pair(timestamp, std::numeric_limits<double>::max());
    return std::make_pair(timestamp, value);
  }
  void at(int64_t* t, double* v) const {
    *t = timestamp;
    if (null_value_)
      *v = std::numeric_limits<double>::max();
    else
      *v = value;
  }

  bool next() const;

  bool read_value() const;

  bool error() const { return err_; }
};

class NullSupportedXORGroupIteratorV2 : public ChunkIteratorInterface {
 public:
  mutable BitStreamV2 t_bstream_;
  mutable BitStreamV2 v_bstream_;
  mutable int64_t timestamp;  // Millisecond
  mutable double value;
  mutable uint64_t delta_timestamp;
  mutable uint8_t leading_zero;
  mutable uint8_t trailing_zero;
  mutable uint16_t num_total;
  mutable uint16_t num_read;
  mutable bool err_;
  bool read_mode_chunk_;
  mutable bool null_value_;

 public:
  NullSupportedXORGroupIteratorV2(BitStreamV2& t, BitStreamV2& v);

  std::pair<int64_t, double> at() const {
    if (null_value_)
      return std::make_pair(timestamp, std::numeric_limits<double>::max());
    return std::make_pair(timestamp, value);
  }
  void at(int64_t* t, double* v) const {
    *t = timestamp;
    if (null_value_)
      *v = std::numeric_limits<double>::max();
    else
      *v = value;
  }

  bool next() const;

  bool read_value() const;

  bool error() const { return err_; }
};

class NullSupportedXORGroupTimeIterator {
 public:
  mutable BitStream t_bstream_;
  mutable int64_t timestamp;  // Millisecond
  mutable uint64_t delta_timestamp;
  mutable uint16_t num_total;
  mutable uint16_t num_read;
  mutable bool err_;

  NullSupportedXORGroupTimeIterator(const uint8_t* ptr, int size);
  int64_t at() const { return timestamp; }
  bool next() const;
};

class NullSupportedXORGroupValueIterator {
 public:
  mutable BitStream v_bstream_;
  mutable double value;
  mutable uint8_t leading_zero;
  mutable uint8_t trailing_zero;
  mutable uint16_t num_total;
  mutable uint16_t num_read;
  mutable bool err_;
  mutable bool null_value_;

  NullSupportedXORGroupValueIterator(const uint8_t* ptr, int size,
                                     int num_samples);
  double at() const {
    if (null_value_) return std::numeric_limits<double>::max();
    return value;
  }
  bool next() const;
  bool read_value() const;
};

}  // namespace chunk
}  // namespace tsdb

#endif