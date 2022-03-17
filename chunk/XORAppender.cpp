#include "chunk/XORAppender.hpp"

#include <cassert>
#include <limits>

#include "base/Endian.hpp"

namespace tsdb {
namespace chunk {

/**********************************************
 *                 XORAppender                *
 **********************************************/
// Must be vector mode BitStream
XORAppender::XORAppender(BitStream& bstream, int64_t timestamp, double value,
                         uint64_t delta_timestamp, uint8_t leading_zero,
                         uint8_t trailing_zero)
    : bstream(bstream),
      timestamp(timestamp),
      value(value),
      delta_timestamp(delta_timestamp),
      leading_zero(leading_zero),
      trailing_zero(trailing_zero) {}

void XORAppender::set_leading_zero(uint8_t lz) { leading_zero = lz; }

void XORAppender::set_trailing_zero(uint8_t tz) { trailing_zero = tz; }

void XORAppender::write_delta_value(double value) {
  uint64_t delta_vale =
      base::encode_double(value) ^ base::encode_double(this->value);

  if (delta_vale == 0) {
    bstream.write_bit(ZERO);
    return;
  }

  bstream.write_bit(ONE);  // First control bit

  // TODO compiler unrelated
  uint8_t lz = static_cast<uint8_t>(__builtin_clzll(delta_vale));
  uint8_t tz = static_cast<uint8_t>(__builtin_ctzll(delta_vale));

  // Clamp number of leading zeros to avoid overflow when encoding.
  if (lz >= 32) lz = 31;

  if (leading_zero != 0xff && lz >= leading_zero && tz >= trailing_zero) {
    // std::cout << "write bits " << 64 - static_cast<int>(leading_zero) -
    // static_cast<int>(trailing_zero) << std::endl;
    bstream.write_bit(ZERO);  // Second control bit
    bstream.write_bits(
        delta_vale >> trailing_zero,
        64 - static_cast<int>(leading_zero) - static_cast<int>(trailing_zero));
  } else {
    leading_zero = lz;
    trailing_zero = tz;

    bstream.write_bit(ONE);  // Second control bit
    bstream.write_bits(static_cast<uint64_t>(lz), 5);

    // Note that if leading == trailing == 0, then sigbits == 64.  But that
    // value doesn't actually fit into the 6 bits we have. Luckily, we never
    // need to encode 0 significant bits, since that would put us in the other
    // case (vdelta == 0). So instead we write out a 0 and adjust it back to 64
    // on unpacking.
    int sigbits =
        64 - static_cast<int>(leading_zero) - static_cast<int>(trailing_zero);
    bstream.write_bits(static_cast<uint64_t>(sigbits), 6);
    bstream.write_bits(delta_vale >> trailing_zero, sigbits);
  }
}

void XORAppender::append(int64_t timestamp, double value) {
  uint64_t current_delta_timestamp = 0;
  int num_samples = base::get_uint16_big_endian(*bstream.bytes());

  if (num_samples == 0) {
    uint8_t temp[base::MAX_VARINT_LEN_64];
    int encoded = base::encode_signed_varint(temp, timestamp);
    for (int i = 0; i < encoded; i++) bstream.write_byte(temp[i]);

    bstream.write_bits(base::encode_double(value), 64);
  } else if (num_samples == 1) {
    current_delta_timestamp = timestamp - this->timestamp;

    uint8_t temp[base::MAX_VARINT_LEN_64];
    int encoded = base::encode_signed_varint(temp, current_delta_timestamp);
    for (int i = 0; i < encoded; i++) bstream.write_byte(temp[i]);

    write_delta_value(value);
  } else {
    current_delta_timestamp = timestamp - this->timestamp;
    int64_t delta_delta_timestamp =
        current_delta_timestamp - this->delta_timestamp;

    if (delta_delta_timestamp == 0) {
      bstream.write_bit(ZERO);
    } else if (bit_range(delta_delta_timestamp, 14)) {
      bstream.write_bits(0x02, 2);  // 10
      bstream.write_bits(static_cast<uint64_t>(delta_delta_timestamp), 14);
    } else if (bit_range(delta_delta_timestamp, 17)) {
      bstream.write_bits(0x06, 3);  // 110
      bstream.write_bits(static_cast<uint64_t>(delta_delta_timestamp), 17);
    } else if (bit_range(delta_delta_timestamp, 20)) {
      bstream.write_bits(0x0e, 4);  // 1110
      bstream.write_bits(static_cast<uint64_t>(delta_delta_timestamp), 20);
    }
    // The above 3 cases need to consider the sign when decoding
    else {
      bstream.write_bits(0x0f, 4);  // 1111
      bstream.write_bits(static_cast<uint64_t>(delta_delta_timestamp), 64);
    }

    write_delta_value(value);
  }

  this->timestamp = timestamp;
  this->value = value;
  base::put_uint16_big_endian(*bstream.bytes(), num_samples + 1);
  this->delta_timestamp = current_delta_timestamp;
}

/**********************************************
 *               XORAppenderV2                *
 **********************************************/
XORAppenderV2::XORAppenderV2(BitStreamV2& bstream, int64_t timestamp,
                             double value, uint64_t delta_timestamp,
                             uint8_t leading_zero, uint8_t trailing_zero)
    : bstream(bstream),
      timestamp(timestamp),
      value(value),
      delta_timestamp(delta_timestamp),
      leading_zero(leading_zero),
      trailing_zero(trailing_zero) {}

void XORAppenderV2::set_leading_zero(uint8_t lz) { leading_zero = lz; }

void XORAppenderV2::set_trailing_zero(uint8_t tz) { trailing_zero = tz; }

void XORAppenderV2::write_delta_value(double value) {
  uint64_t delta_vale =
      base::encode_double(value) ^ base::encode_double(this->value);

  if (delta_vale == 0) {
    bstream.write_bit(ZERO);
    return;
  }

  bstream.write_bit(ONE);  // First control bit

  // TODO compiler unrelated
  uint8_t lz = static_cast<uint8_t>(__builtin_clzll(delta_vale));
  uint8_t tz = static_cast<uint8_t>(__builtin_ctzll(delta_vale));

  // Clamp number of leading zeros to avoid overflow when encoding.
  if (lz >= 32) lz = 31;

  if (leading_zero != 0xff && lz >= leading_zero && tz >= trailing_zero) {
    bstream.write_bit(ZERO);  // Second control bit
    bstream.write_bits(
        delta_vale >> trailing_zero,
        64 - static_cast<int>(leading_zero) - static_cast<int>(trailing_zero));
  } else {
    leading_zero = lz;
    trailing_zero = tz;

    bstream.write_bit(ONE);  // Second control bit
    bstream.write_bits(static_cast<uint64_t>(lz), 5);

    // Note that if leading == trailing == 0, then sigbits == 64.  But that
    // value doesn't actually fit into the 6 bits we have. Luckily, we never
    // need to encode 0 significant bits, since that would put us in the other
    // case (vdelta == 0). So instead we write out a 0 and adjust it back to 64
    // on unpacking.
    int sigbits =
        64 - static_cast<int>(leading_zero) - static_cast<int>(trailing_zero);
    bstream.write_bits(static_cast<uint64_t>(sigbits), 6);
    bstream.write_bits(delta_vale >> trailing_zero, sigbits);
  }
}

void XORAppenderV2::append(int64_t timestamp, double value) {
  uint64_t current_delta_timestamp = 0;
  int num_samples = base::get_uint16_big_endian(bstream.bytes());

  if (num_samples == 0) {
    uint8_t temp[base::MAX_VARINT_LEN_64];
    int encoded = base::encode_signed_varint(temp, timestamp);
    for (int i = 0; i < encoded; i++) bstream.write_byte(temp[i]);

    bstream.write_bits(base::encode_double(value), 64);
  } else if (num_samples == 1) {
    current_delta_timestamp = timestamp - this->timestamp;

    uint8_t temp[base::MAX_VARINT_LEN_64];
    int encoded = base::encode_signed_varint(temp, current_delta_timestamp);
    for (int i = 0; i < encoded; i++) bstream.write_byte(temp[i]);

    write_delta_value(value);
  } else {
    current_delta_timestamp = timestamp - this->timestamp;
    int64_t delta_delta_timestamp =
        current_delta_timestamp - this->delta_timestamp;

    if (delta_delta_timestamp == 0) {
      bstream.write_bit(ZERO);
    } else if (bit_range(delta_delta_timestamp, 14)) {
      bstream.write_bits(0x02, 2);  // 10
      bstream.write_bits(static_cast<uint64_t>(delta_delta_timestamp), 14);
    } else if (bit_range(delta_delta_timestamp, 17)) {
      bstream.write_bits(0x06, 3);  // 110
      bstream.write_bits(static_cast<uint64_t>(delta_delta_timestamp), 17);
    } else if (bit_range(delta_delta_timestamp, 20)) {
      bstream.write_bits(0x0e, 4);  // 1110
      bstream.write_bits(static_cast<uint64_t>(delta_delta_timestamp), 20);
    }
    // The above 3 cases need to consider the sign when decoding
    else {
      bstream.write_bits(0x0f, 4);  // 1111
      bstream.write_bits(static_cast<uint64_t>(delta_delta_timestamp), 64);
    }

    write_delta_value(value);
  }

  this->timestamp = timestamp;
  this->value = value;
  base::put_uint16_big_endian(bstream.bytes(), num_samples + 1);
  this->delta_timestamp = current_delta_timestamp;
}

/**********************************************
 *          NullSupportedXORAppender          *
 **********************************************/
// Must be vector mode BitStream
NullSupportedXORAppender::NullSupportedXORAppender(
    BitStream& bstream, int64_t timestamp, double value,
    uint64_t delta_timestamp, uint8_t leading_zero, uint8_t trailing_zero)
    : bstream(bstream),
      timestamp(timestamp),
      value(value),
      delta_timestamp(delta_timestamp),
      leading_zero(leading_zero),
      trailing_zero(trailing_zero) {}

void NullSupportedXORAppender::set_leading_zero(uint8_t lz) {
  leading_zero = lz;
}

void NullSupportedXORAppender::set_trailing_zero(uint8_t tz) {
  trailing_zero = tz;
}

void NullSupportedXORAppender::write_delta_value(double value) {
  if (value == std::numeric_limits<double>::max()) {
    bstream.write_bit(ZERO);  // First control bit
    return;
  }
  bstream.write_bit(ONE);  // First control bit

  uint64_t delta_vale =
      base::encode_double(value) ^ base::encode_double(this->value);

  if (delta_vale == 0) {
    bstream.write_bit(ZERO);
    return;
  }

  bstream.write_bit(ONE);  // Second control bit

  // TODO compiler unrelated
  uint8_t lz = static_cast<uint8_t>(__builtin_clzll(delta_vale));
  uint8_t tz = static_cast<uint8_t>(__builtin_ctzll(delta_vale));

  // Clamp number of leading zeros to avoid overflow when encoding.
  if (lz >= 32) lz = 31;

  if (leading_zero != 0xff && lz >= leading_zero && tz >= trailing_zero) {
    bstream.write_bit(ZERO);  // Third control bit
    bstream.write_bits(
        delta_vale >> trailing_zero,
        64 - static_cast<int>(leading_zero) - static_cast<int>(trailing_zero));
  } else {
    leading_zero = lz;
    trailing_zero = tz;

    bstream.write_bit(ONE);  // Third control bit
    bstream.write_bits(static_cast<uint64_t>(lz), 5);

    // Note that if leading == trailing == 0, then sigbits == 64.  But that
    // value doesn't actually fit into the 6 bits we have. Luckily, we never
    // need to encode 0 significant bits, since that would put us in the other
    // case (vdelta == 0). So instead we write out a 0 and adjust it back to 64
    // on unpacking.
    int sigbits =
        64 - static_cast<int>(leading_zero) - static_cast<int>(trailing_zero);
    bstream.write_bits(static_cast<uint64_t>(sigbits), 6);
    bstream.write_bits(delta_vale >> trailing_zero, sigbits);
  }
}

void NullSupportedXORAppender::append(int64_t timestamp, double value) {
  uint64_t current_delta_timestamp = 0;
  int num_samples = base::get_uint16_big_endian(*bstream.bytes());

  if (num_samples == 0) {
    uint8_t temp[base::MAX_VARINT_LEN_64];
    int encoded = base::encode_signed_varint(temp, timestamp);
    for (int i = 0; i < encoded; i++) bstream.write_byte(temp[i]);

    if (value == std::numeric_limits<double>::max())
      bstream.write_bit(ZERO);
    else {
      bstream.write_bit(ONE);
      bstream.write_bits(base::encode_double(value), 64);
    }
  } else if (num_samples == 1) {
    current_delta_timestamp = timestamp - this->timestamp;

    uint8_t temp[base::MAX_VARINT_LEN_64];
    int encoded = base::encode_signed_varint(temp, current_delta_timestamp);
    for (int i = 0; i < encoded; i++) bstream.write_byte(temp[i]);

    write_delta_value(value);
  } else {
    current_delta_timestamp = timestamp - this->timestamp;
    int64_t delta_delta_timestamp =
        current_delta_timestamp - this->delta_timestamp;

    if (delta_delta_timestamp == 0) {
      bstream.write_bit(ZERO);
    } else if (bit_range(delta_delta_timestamp, 14)) {
      bstream.write_bits(0x02, 2);  // 10
      bstream.write_bits(static_cast<uint64_t>(delta_delta_timestamp), 14);
    } else if (bit_range(delta_delta_timestamp, 17)) {
      bstream.write_bits(0x06, 3);  // 110
      bstream.write_bits(static_cast<uint64_t>(delta_delta_timestamp), 17);
    } else if (bit_range(delta_delta_timestamp, 20)) {
      bstream.write_bits(0x0e, 4);  // 1110
      bstream.write_bits(static_cast<uint64_t>(delta_delta_timestamp), 20);
    }
    // The above 3 cases need to consider the sign when decoding
    else {
      bstream.write_bits(0x0f, 4);  // 1111
      bstream.write_bits(static_cast<uint64_t>(delta_delta_timestamp), 64);
    }

    write_delta_value(value);
  }

  this->timestamp = timestamp;
  if (value != std::numeric_limits<double>::max()) this->value = value;
  base::put_uint16_big_endian(*bstream.bytes(), num_samples + 1);
  this->delta_timestamp = current_delta_timestamp;
}

/**********************************************
 *        NullSupportedXORGroupAppender       *
 **********************************************/
NullSupportedXORGroupAppender::NullSupportedXORGroupAppender(
    BitStream* bstream, std::vector<BitStream>* v_bstreams)
    : t_bstream_(bstream),
      v_bstreams_(v_bstreams),
      timestamp_(0),
      values_(v_bstreams->size(), 0),
      leading_zero_(v_bstreams->size(), 0xff),
      trailing_zero_(v_bstreams->size(), 0) {}

void NullSupportedXORGroupAppender::set_leading_zero(int idx, uint8_t lz) {
  leading_zero_[idx] = lz;
}

void NullSupportedXORGroupAppender::set_trailing_zero(int idx, uint8_t tz) {
  trailing_zero_[idx] = tz;
}

void NullSupportedXORGroupAppender::write_delta_value(int idx, double value) {
  if (value == std::numeric_limits<double>::max()) {
    v_bstreams_->at(idx).write_bit(ZERO);  // First control bit
    return;
  }
  v_bstreams_->at(idx).write_bit(ONE);  // First control bit

  uint64_t delta_vale =
      base::encode_double(value) ^ base::encode_double(this->values_[idx]);

  if (delta_vale == 0) {
    v_bstreams_->at(idx).write_bit(ZERO);
    return;
  }

  v_bstreams_->at(idx).write_bit(ONE);  // Second control bit

  // TODO compiler unrelated
  uint8_t lz = static_cast<uint8_t>(__builtin_clzll(delta_vale));
  uint8_t tz = static_cast<uint8_t>(__builtin_ctzll(delta_vale));

  // Clamp number of leading zeros to avoid overflow when encoding.
  if (lz >= 32) lz = 31;

  if (leading_zero_[idx] != 0xff && lz >= leading_zero_[idx] &&
      tz >= trailing_zero_[idx]) {
    v_bstreams_->at(idx).write_bit(ZERO);  // Third control bit
    v_bstreams_->at(idx).write_bits(delta_vale >> trailing_zero_[idx],
                                    64 - static_cast<int>(leading_zero_[idx]) -
                                        static_cast<int>(trailing_zero_[idx]));
  } else {
    leading_zero_[idx] = lz;
    trailing_zero_[idx] = tz;

    v_bstreams_->at(idx).write_bit(ONE);  // Third control bit
    v_bstreams_->at(idx).write_bits(static_cast<uint64_t>(lz), 5);

    // Note that if leading == trailing == 0, then sigbits == 64.  But that
    // value doesn't actually fit into the 6 bits we have. Luckily, we never
    // need to encode 0 significant bits, since that would put us in the other
    // case (vdelta == 0). So instead we write out a 0 and adjust it back to 64
    // on unpacking.
    int sigbits = 64 - static_cast<int>(leading_zero_[idx]) -
                  static_cast<int>(trailing_zero_[idx]);
    v_bstreams_->at(idx).write_bits(static_cast<uint64_t>(sigbits), 6);
    v_bstreams_->at(idx).write_bits(delta_vale >> trailing_zero_[idx], sigbits);
  }
}

void NullSupportedXORGroupAppender::append(int64_t timestamp,
                                           const std::vector<double>& values) {
  assert(values.size() == v_bstreams_->size());

  uint64_t current_delta_timestamp = 0;
  int num_samples = base::get_uint16_big_endian(*(t_bstream_->bytes()));

  if (num_samples == 0) {
    uint8_t temp[base::MAX_VARINT_LEN_64];
    int encoded = base::encode_signed_varint(temp, timestamp);
    for (int i = 0; i < encoded; i++) t_bstream_->write_byte(temp[i]);

    for (size_t idx = 0; idx < values.size(); idx++) {
      if (values[idx] == std::numeric_limits<double>::max())
        v_bstreams_->at(idx).write_bit(ZERO);
      else {
        v_bstreams_->at(idx).write_bit(ONE);
        v_bstreams_->at(idx).write_bits(base::encode_double(values[idx]), 64);
      }
    }
  } else if (num_samples == 1) {
    current_delta_timestamp = timestamp - this->timestamp_;

    uint8_t temp[base::MAX_VARINT_LEN_64];
    int encoded = base::encode_signed_varint(temp, current_delta_timestamp);
    for (int i = 0; i < encoded; i++) t_bstream_->write_byte(temp[i]);

    for (size_t idx = 0; idx < values.size(); idx++)
      write_delta_value(idx, values[idx]);
  } else {
    current_delta_timestamp = timestamp - this->timestamp_;
    int64_t delta_delta_timestamp =
        current_delta_timestamp - this->delta_timestamp_;

    if (delta_delta_timestamp == 0) {
      t_bstream_->write_bit(ZERO);
    } else if (bit_range(delta_delta_timestamp, 14)) {
      t_bstream_->write_bits(0x02, 2);  // 10
      t_bstream_->write_bits(static_cast<uint64_t>(delta_delta_timestamp), 14);
    } else if (bit_range(delta_delta_timestamp, 17)) {
      t_bstream_->write_bits(0x06, 3);  // 110
      t_bstream_->write_bits(static_cast<uint64_t>(delta_delta_timestamp), 17);
    } else if (bit_range(delta_delta_timestamp, 20)) {
      t_bstream_->write_bits(0x0e, 4);  // 1110
      t_bstream_->write_bits(static_cast<uint64_t>(delta_delta_timestamp), 20);
    }
    // The above 3 cases need to consider the sign when decoding
    else {
      t_bstream_->write_bits(0x0f, 4);  // 1111
      t_bstream_->write_bits(static_cast<uint64_t>(delta_delta_timestamp), 64);
    }

    for (size_t idx = 0; idx < values.size(); idx++)
      write_delta_value(idx, values[idx]);
  }

  this->timestamp_ = timestamp;
  for (size_t idx = 0; idx < values.size(); idx++) {
    if (values[idx] != std::numeric_limits<double>::max())
      this->values_[idx] = values[idx];
  }
  base::put_uint16_big_endian(*(t_bstream_->bytes()), num_samples + 1);
  this->delta_timestamp_ = current_delta_timestamp;
}

/**********************************************
 *       NullSupportedXORGroupAppenderV2      *
 **********************************************/
NullSupportedXORGroupAppenderV2::NullSupportedXORGroupAppenderV2(
    BitStreamV2* bstream, std::vector<BitStreamV2>* v_bstreams)
    : t_bstream_(bstream),
      v_bstreams_(v_bstreams),
      timestamp_(0),
      values_(v_bstreams->size(), 0),
      leading_zero_(v_bstreams->size(), 0xff),
      trailing_zero_(v_bstreams->size(), 0) {}

void NullSupportedXORGroupAppenderV2::set_leading_zero(int idx, uint8_t lz) {
  leading_zero_[idx] = lz;
}

void NullSupportedXORGroupAppenderV2::set_trailing_zero(int idx, uint8_t tz) {
  trailing_zero_[idx] = tz;
}

void NullSupportedXORGroupAppenderV2::write_delta_value(int idx, double value) {
  if (value == std::numeric_limits<double>::max()) {
    v_bstreams_->at(idx).write_bit(ZERO);  // First control bit
    return;
  }
  v_bstreams_->at(idx).write_bit(ONE);  // First control bit

  uint64_t delta_vale =
      base::encode_double(value) ^ base::encode_double(this->values_[idx]);

  if (delta_vale == 0) {
    v_bstreams_->at(idx).write_bit(ZERO);
    return;
  }

  v_bstreams_->at(idx).write_bit(ONE);  // Second control bit

  // TODO compiler unrelated
  uint8_t lz = static_cast<uint8_t>(__builtin_clzll(delta_vale));
  uint8_t tz = static_cast<uint8_t>(__builtin_ctzll(delta_vale));

  // Clamp number of leading zeros to avoid overflow when encoding.
  if (lz >= 32) lz = 31;

  if (leading_zero_[idx] != 0xff && lz >= leading_zero_[idx] &&
      tz >= trailing_zero_[idx]) {
    v_bstreams_->at(idx).write_bit(ZERO);  // Third control bit
    v_bstreams_->at(idx).write_bits(delta_vale >> trailing_zero_[idx],
                                    64 - static_cast<int>(leading_zero_[idx]) -
                                        static_cast<int>(trailing_zero_[idx]));
  } else {
    leading_zero_[idx] = lz;
    trailing_zero_[idx] = tz;

    v_bstreams_->at(idx).write_bit(ONE);  // Third control bit
    v_bstreams_->at(idx).write_bits(static_cast<uint64_t>(lz), 5);

    // Note that if leading == trailing == 0, then sigbits == 64.  But that
    // value doesn't actually fit into the 6 bits we have. Luckily, we never
    // need to encode 0 significant bits, since that would put us in the other
    // case (vdelta == 0). So instead we write out a 0 and adjust it back to 64
    // on unpacking.
    int sigbits = 64 - static_cast<int>(leading_zero_[idx]) -
                  static_cast<int>(trailing_zero_[idx]);
    v_bstreams_->at(idx).write_bits(static_cast<uint64_t>(sigbits), 6);
    v_bstreams_->at(idx).write_bits(delta_vale >> trailing_zero_[idx], sigbits);
  }
}

void NullSupportedXORGroupAppenderV2::append(
    int64_t timestamp, const std::vector<double>& values) {
  assert(values.size() == v_bstreams_->size());

  uint64_t current_delta_timestamp = 0;
  int num_samples = base::get_uint16_big_endian(t_bstream_->bytes());

  if (num_samples == 0) {
    uint8_t temp[base::MAX_VARINT_LEN_64];
    int encoded = base::encode_signed_varint(temp, timestamp);
    for (int i = 0; i < encoded; i++) t_bstream_->write_byte(temp[i]);

    for (size_t idx = 0; idx < values.size(); idx++) {
      if (values[idx] == std::numeric_limits<double>::max())
        v_bstreams_->at(idx).write_bit(ZERO);
      else {
        v_bstreams_->at(idx).write_bit(ONE);
        v_bstreams_->at(idx).write_bits(base::encode_double(values[idx]), 64);
      }
    }
  } else if (num_samples == 1) {
    current_delta_timestamp = timestamp - this->timestamp_;

    uint8_t temp[base::MAX_VARINT_LEN_64];
    int encoded = base::encode_signed_varint(temp, current_delta_timestamp);
    for (int i = 0; i < encoded; i++) t_bstream_->write_byte(temp[i]);

    for (size_t idx = 0; idx < values.size(); idx++)
      write_delta_value(idx, values[idx]);
  } else {
    current_delta_timestamp = timestamp - this->timestamp_;
    int64_t delta_delta_timestamp =
        current_delta_timestamp - this->delta_timestamp_;

    if (delta_delta_timestamp == 0) {
      t_bstream_->write_bit(ZERO);
    } else if (bit_range(delta_delta_timestamp, 14)) {
      t_bstream_->write_bits(0x02, 2);  // 10
      t_bstream_->write_bits(static_cast<uint64_t>(delta_delta_timestamp), 14);
    } else if (bit_range(delta_delta_timestamp, 17)) {
      t_bstream_->write_bits(0x06, 3);  // 110
      t_bstream_->write_bits(static_cast<uint64_t>(delta_delta_timestamp), 17);
    } else if (bit_range(delta_delta_timestamp, 20)) {
      t_bstream_->write_bits(0x0e, 4);  // 1110
      t_bstream_->write_bits(static_cast<uint64_t>(delta_delta_timestamp), 20);
    }
    // The above 3 cases need to consider the sign when decoding
    else {
      t_bstream_->write_bits(0x0f, 4);  // 1111
      t_bstream_->write_bits(static_cast<uint64_t>(delta_delta_timestamp), 64);
    }

    for (size_t idx = 0; idx < values.size(); idx++)
      write_delta_value(idx, values[idx]);
  }

  this->timestamp_ = timestamp;
  for (size_t idx = 0; idx < values.size(); idx++) {
    if (values[idx] != std::numeric_limits<double>::max())
      this->values_[idx] = values[idx];
  }
  base::put_uint16_big_endian(t_bstream_->bytes(), num_samples + 1);
  this->delta_timestamp_ = current_delta_timestamp;
}

/**********************************************
 *      NullSupportedXORGroupTimeAppender     *
 **********************************************/
NullSupportedXORGroupTimeAppender::NullSupportedXORGroupTimeAppender(
    BitStream* bstream)
    : bstream_(bstream), timestamp_(0) {}

void NullSupportedXORGroupTimeAppender::append(int64_t timestamp) {
  uint64_t current_delta_timestamp = 0;
  int num_samples = base::get_uint16_big_endian(*(bstream_->bytes()));

  if (num_samples == 0) {
    uint8_t temp[base::MAX_VARINT_LEN_64];
    int encoded = base::encode_signed_varint(temp, timestamp);
    for (int i = 0; i < encoded; i++) bstream_->write_byte(temp[i]);
  } else if (num_samples == 1) {
    current_delta_timestamp = timestamp - this->timestamp_;

    uint8_t temp[base::MAX_VARINT_LEN_64];
    int encoded = base::encode_signed_varint(temp, current_delta_timestamp);
    for (int i = 0; i < encoded; i++) bstream_->write_byte(temp[i]);
  } else {
    current_delta_timestamp = timestamp - this->timestamp_;
    int64_t delta_delta_timestamp =
        current_delta_timestamp - this->delta_timestamp_;

    if (delta_delta_timestamp == 0) {
      bstream_->write_bit(ZERO);
    } else if (bit_range(delta_delta_timestamp, 14)) {
      bstream_->write_bits(0x02, 2);  // 10
      bstream_->write_bits(static_cast<uint64_t>(delta_delta_timestamp), 14);
    } else if (bit_range(delta_delta_timestamp, 17)) {
      bstream_->write_bits(0x06, 3);  // 110
      bstream_->write_bits(static_cast<uint64_t>(delta_delta_timestamp), 17);
    } else if (bit_range(delta_delta_timestamp, 20)) {
      bstream_->write_bits(0x0e, 4);  // 1110
      bstream_->write_bits(static_cast<uint64_t>(delta_delta_timestamp), 20);
    }
    // The above 3 cases need to consider the sign when decoding
    else {
      bstream_->write_bits(0x0f, 4);  // 1111
      bstream_->write_bits(static_cast<uint64_t>(delta_delta_timestamp), 64);
    }
  }

  this->timestamp_ = timestamp;
  base::put_uint16_big_endian(*(bstream_->bytes()), num_samples + 1);
  this->delta_timestamp_ = current_delta_timestamp;
}

/**********************************************
 *     NullSupportedXORGroupTimeAppenderV2    *
 **********************************************/
NullSupportedXORGroupTimeAppenderV2::NullSupportedXORGroupTimeAppenderV2(
    BitStreamV2* bstream)
    : bstream_(bstream), timestamp_(0) {}

void NullSupportedXORGroupTimeAppenderV2::append(int64_t timestamp) {
  uint64_t current_delta_timestamp = 0;
  int num_samples = base::get_uint16_big_endian(bstream_->bytes());

  if (num_samples == 0) {
    uint8_t temp[base::MAX_VARINT_LEN_64];
    int encoded = base::encode_signed_varint(temp, timestamp);
    for (int i = 0; i < encoded; i++) bstream_->write_byte(temp[i]);
  } else if (num_samples == 1) {
    current_delta_timestamp = timestamp - this->timestamp_;

    uint8_t temp[base::MAX_VARINT_LEN_64];
    int encoded = base::encode_signed_varint(temp, current_delta_timestamp);
    for (int i = 0; i < encoded; i++) bstream_->write_byte(temp[i]);
  } else {
    current_delta_timestamp = timestamp - this->timestamp_;
    int64_t delta_delta_timestamp =
        current_delta_timestamp - this->delta_timestamp_;

    if (delta_delta_timestamp == 0) {
      bstream_->write_bit(ZERO);
    } else if (bit_range(delta_delta_timestamp, 14)) {
      bstream_->write_bits(0x02, 2);  // 10
      bstream_->write_bits(static_cast<uint64_t>(delta_delta_timestamp), 14);
    } else if (bit_range(delta_delta_timestamp, 17)) {
      bstream_->write_bits(0x06, 3);  // 110
      bstream_->write_bits(static_cast<uint64_t>(delta_delta_timestamp), 17);
    } else if (bit_range(delta_delta_timestamp, 20)) {
      bstream_->write_bits(0x0e, 4);  // 1110
      bstream_->write_bits(static_cast<uint64_t>(delta_delta_timestamp), 20);
    }
    // The above 3 cases need to consider the sign when decoding
    else {
      bstream_->write_bits(0x0f, 4);  // 1111
      bstream_->write_bits(static_cast<uint64_t>(delta_delta_timestamp), 64);
    }
  }

  this->timestamp_ = timestamp;
  base::put_uint16_big_endian(bstream_->bytes(), num_samples + 1);
  this->delta_timestamp_ = current_delta_timestamp;
}

/**********************************************
 *      NullSupportedXORGroupValueAppender    *
 **********************************************/
NullSupportedXORGroupValueAppender::NullSupportedXORGroupValueAppender(
    BitStream* bstream)
    : bstream_(bstream),
      value_(0),
      leading_zero_(0xff),
      trailing_zero_(0),
      num_samples_(0) {}

void NullSupportedXORGroupValueAppender::write_delta_value(double value) {
  if (value == std::numeric_limits<double>::max()) {
    bstream_->write_bit(ZERO);  // First control bit
    return;
  }
  bstream_->write_bit(ONE);  // First control bit

  uint64_t delta_vale =
      base::encode_double(value) ^ base::encode_double(this->value_);

  if (delta_vale == 0) {
    bstream_->write_bit(ZERO);
    return;
  }

  bstream_->write_bit(ONE);  // Second control bit

  // TODO compiler unrelated
  uint8_t lz = static_cast<uint8_t>(__builtin_clzll(delta_vale));
  uint8_t tz = static_cast<uint8_t>(__builtin_ctzll(delta_vale));

  // Clamp number of leading zeros to avoid overflow when encoding.
  if (lz >= 32) lz = 31;

  if (leading_zero_ != 0xff && lz >= leading_zero_ && tz >= trailing_zero_) {
    bstream_->write_bit(ZERO);  // Third control bit
    bstream_->write_bits(delta_vale >> trailing_zero_,
                         64 - static_cast<int>(leading_zero_) -
                             static_cast<int>(trailing_zero_));
  } else {
    leading_zero_ = lz;
    trailing_zero_ = tz;

    bstream_->write_bit(ONE);  // Third control bit
    bstream_->write_bits(static_cast<uint64_t>(lz), 5);

    // Note that if leading == trailing == 0, then sigbits == 64.  But that
    // value doesn't actually fit into the 6 bits we have. Luckily, we never
    // need to encode 0 significant bits, since that would put us in the other
    // case (vdelta == 0). So instead we write out a 0 and adjust it back to 64
    // on unpacking.
    int sigbits =
        64 - static_cast<int>(leading_zero_) - static_cast<int>(trailing_zero_);
    bstream_->write_bits(static_cast<uint64_t>(sigbits), 6);
    bstream_->write_bits(delta_vale >> trailing_zero_, sigbits);
  }
}

void NullSupportedXORGroupValueAppender::append(double value) {
  if (num_samples_ == 0) {
    if (value == std::numeric_limits<double>::max())
      bstream_->write_bit(ZERO);
    else {
      bstream_->write_bit(ONE);
      bstream_->write_bits(base::encode_double(value), 64);
    }
  } else {
    write_delta_value(value);
  }

  if (value != std::numeric_limits<double>::max()) this->value_ = value;
  num_samples_++;
}

/**********************************************
 *    NullSupportedXORGroupValueAppenderV2    *
 **********************************************/
NullSupportedXORGroupValueAppenderV2::NullSupportedXORGroupValueAppenderV2(
    BitStreamV2* bstream)
    : bstream_(bstream),
      value_(0),
      leading_zero_(0xff),
      trailing_zero_(0),
      num_samples_(0) {}

void NullSupportedXORGroupValueAppenderV2::write_delta_value(double value) {
  if (value == std::numeric_limits<double>::max()) {
    bstream_->write_bit(ZERO);  // First control bit
    return;
  }
  bstream_->write_bit(ONE);  // First control bit

  uint64_t delta_vale =
      base::encode_double(value) ^ base::encode_double(this->value_);

  if (delta_vale == 0) {
    bstream_->write_bit(ZERO);
    return;
  }

  bstream_->write_bit(ONE);  // Second control bit

  // TODO compiler unrelated
  uint8_t lz = static_cast<uint8_t>(__builtin_clzll(delta_vale));
  uint8_t tz = static_cast<uint8_t>(__builtin_ctzll(delta_vale));

  // Clamp number of leading zeros to avoid overflow when encoding.
  if (lz >= 32) lz = 31;

  if (leading_zero_ != 0xff && lz >= leading_zero_ && tz >= trailing_zero_) {
    bstream_->write_bit(ZERO);  // Third control bit
    bstream_->write_bits(delta_vale >> trailing_zero_,
                         64 - static_cast<int>(leading_zero_) -
                             static_cast<int>(trailing_zero_));
  } else {
    leading_zero_ = lz;
    trailing_zero_ = tz;

    bstream_->write_bit(ONE);  // Third control bit
    bstream_->write_bits(static_cast<uint64_t>(lz), 5);

    // Note that if leading == trailing == 0, then sigbits == 64.  But that
    // value doesn't actually fit into the 6 bits we have. Luckily, we never
    // need to encode 0 significant bits, since that would put us in the other
    // case (vdelta == 0). So instead we write out a 0 and adjust it back to 64
    // on unpacking.
    int sigbits =
        64 - static_cast<int>(leading_zero_) - static_cast<int>(trailing_zero_);
    bstream_->write_bits(static_cast<uint64_t>(sigbits), 6);
    bstream_->write_bits(delta_vale >> trailing_zero_, sigbits);
  }
}

void NullSupportedXORGroupValueAppenderV2::append(double value) {
  if (num_samples_ == 0) {
    if (value == std::numeric_limits<double>::max())
      bstream_->write_bit(ZERO);
    else {
      bstream_->write_bit(ONE);
      bstream_->write_bits(base::encode_double(value), 64);
    }
  } else {
    write_delta_value(value);
  }

  if (value != std::numeric_limits<double>::max()) this->value_ = value;
  num_samples_++;
}

bool bit_range(int64_t delta_delta_timestamp, int num) {
  return ((-((1 << (num - 1)) - 1) <= delta_delta_timestamp) &&
          (delta_delta_timestamp <= 1 << (num - 1)));
}

}  // namespace chunk
}  // namespace tsdb