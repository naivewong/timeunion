#ifndef XORAPPENDER_H
#define XORAPPENDER_H

#include "chunk/BitStream.hpp"
#include "chunk/ChunkAppenderInterface.hpp"

namespace tsdb {
namespace chunk {

extern bool bit_range(int64_t delta_delta_timestamp, int num);

class XORAppender : public ChunkAppenderInterface {
 private:
  BitStream& bstream;
  int64_t timestamp;  // Millisecond
  double value;
  uint64_t delta_timestamp;
  uint8_t leading_zero;
  uint8_t trailing_zero;

 public:
  // Must be vector mode BitStream
  XORAppender(BitStream& bstream, int64_t timestamp, double value,
              uint64_t delta_timestamp, uint8_t leading_zero,
              uint8_t trailing_zero);

  void set_leading_zero(uint8_t lz);

  void set_trailing_zero(uint8_t tz);

  void write_delta_value(double value);

  void append(int64_t timestamp, double value);
};

class XORAppenderV2 : public ChunkAppenderInterface {
 private:
  BitStreamV2& bstream;
  int64_t timestamp;  // Millisecond
  double value;
  uint64_t delta_timestamp;
  uint8_t leading_zero;
  uint8_t trailing_zero;

 public:
  // Must be vector mode BitStream
  XORAppenderV2(BitStreamV2& bstream, int64_t timestamp, double value,
                uint64_t delta_timestamp, uint8_t leading_zero,
                uint8_t trailing_zero);

  void set_leading_zero(uint8_t lz);

  void set_trailing_zero(uint8_t tz);

  void write_delta_value(double value);

  void append(int64_t timestamp, double value);
};

class NullSupportedXORAppender : public ChunkAppenderInterface {
 private:
  BitStream& bstream;
  int64_t timestamp;  // Millisecond
  double value;
  uint64_t delta_timestamp;
  uint8_t leading_zero;
  uint8_t trailing_zero;

 public:
  // Must be vector mode BitStream
  NullSupportedXORAppender(BitStream& bstream, int64_t timestamp, double value,
                           uint64_t delta_timestamp, uint8_t leading_zero,
                           uint8_t trailing_zero);

  void set_leading_zero(uint8_t lz);

  void set_trailing_zero(uint8_t tz);

  void write_delta_value(double value);

  void append(int64_t timestamp, double value);
};

class NullSupportedXORGroupAppender {
 private:
  BitStream* t_bstream_;
  std::vector<BitStream>* v_bstreams_;
  int64_t timestamp_;  // Millisecond
  std::vector<double> values_;
  uint64_t delta_timestamp_;
  std::vector<uint8_t> leading_zero_;
  std::vector<uint8_t> trailing_zero_;

 public:
  // Must be vector mode BitStream
  NullSupportedXORGroupAppender(BitStream* bstream,
                                std::vector<BitStream>* v_bstreams);

  void set_leading_zero(int idx, uint8_t lz);

  void set_trailing_zero(int idx, uint8_t tz);

  void write_delta_value(int idx, double value);

  void append(int64_t timestamp, const std::vector<double>& value);
};

class NullSupportedXORGroupAppenderV2 {
 private:
  BitStreamV2* t_bstream_;
  std::vector<BitStreamV2>* v_bstreams_;
  int64_t timestamp_;  // Millisecond
  std::vector<double> values_;
  uint64_t delta_timestamp_;
  std::vector<uint8_t> leading_zero_;
  std::vector<uint8_t> trailing_zero_;

 public:
  // Must be vector mode BitStreamV2
  NullSupportedXORGroupAppenderV2(BitStreamV2* bstream,
                                  std::vector<BitStreamV2>* v_bstreams);

  void set_leading_zero(int idx, uint8_t lz);

  void set_trailing_zero(int idx, uint8_t tz);

  void write_delta_value(int idx, double value);

  void append(int64_t timestamp, const std::vector<double>& value);
};

class NullSupportedXORGroupTimeAppender {
 private:
  BitStream* bstream_;
  int64_t timestamp_;  // Millisecond
  uint64_t delta_timestamp_;

 public:
  // Must be vector mode BitStream
  NullSupportedXORGroupTimeAppender(BitStream* bstream);

  void append(int64_t timestamp);
};

class NullSupportedXORGroupTimeAppenderV2 {
 private:
  BitStreamV2* bstream_;
  int64_t timestamp_;  // Millisecond
  uint64_t delta_timestamp_;

 public:
  // Must be vector mode BitStream
  NullSupportedXORGroupTimeAppenderV2(BitStreamV2* bstream);

  void append(int64_t timestamp);
};

class NullSupportedXORGroupValueAppender {
 private:
  BitStream* bstream_;
  double value_;
  uint8_t leading_zero_;
  uint8_t trailing_zero_;
  int num_samples_;

 public:
  // Must be vector mode BitStream
  NullSupportedXORGroupValueAppender(BitStream* bstream);

  void write_delta_value(double value);

  void append(double value);
};

class NullSupportedXORGroupValueAppenderV2 {
 private:
  BitStreamV2* bstream_;
  double value_;
  uint8_t leading_zero_;
  uint8_t trailing_zero_;
  int num_samples_;

 public:
  // Must be vector mode BitStream
  NullSupportedXORGroupValueAppenderV2(BitStreamV2* bstream);

  void write_delta_value(double value);

  void append(double value);
};

}  // namespace chunk
}  // namespace tsdb

#endif