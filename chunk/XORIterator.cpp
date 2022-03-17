#include "chunk/XORIterator.hpp"

#include <string.h>

#include "base/Endian.hpp"

namespace tsdb {
namespace chunk {

/**********************************************
 *                   XORIterator              *
 **********************************************/
// Read mode BitStream
XORIterator::XORIterator(BitStream &bstream, bool safe_mode)
    : safe_mode(safe_mode),
      timestamp(0),
      value(0),
      delta_timestamp(0),
      leading_zero(0),
      trailing_zero(0),
      num_read(0),
      err_(false) {
  // This is to prevent pointer invalidation when vector resizes during
  // appending new data. For those XORChunk not created in read mode.
  if (safe_mode) {
    std::vector<uint8_t> stream(bstream.size(), 0);
    memcpy(&(stream.front()), bstream.bytes_ptr(), bstream.size());
    this->bstream = BitStream(stream);
  } else
    this->bstream = BitStream(bstream.bytes_ptr(), bstream.size());

  num_total = base::get_uint16_big_endian(bstream.bytes_ptr());
  // Pop the first two bytes
  this->bstream.pop_front();
  this->bstream.pop_front();
}

XORIterator::XORIterator(BitStream &bstream, bool safe_mode, int num_samples)
    : safe_mode(safe_mode),
      timestamp(0),
      value(0),
      delta_timestamp(0),
      leading_zero(0),
      trailing_zero(0),
      num_total(num_samples),
      num_read(0),
      err_(false) {
  if (safe_mode) {
    std::vector<uint8_t> stream(bstream.size(), 0);
    memcpy(&(stream.front()), bstream.bytes_ptr(), bstream.size());
    this->bstream = BitStream(stream);
  } else
    this->bstream = BitStream(bstream.bytes_ptr(), bstream.size());
}

bool XORIterator::next() const {
  if (err_ || num_read == num_total) return false;

  if (num_read == 0) {
    int64_t current_t;
    try {
      current_t = bstream.read_signed_varint();
    } catch (const base::TSDBException &e) {
      err_ = true;
      return false;
    }

    uint64_t current_v;
    try {
      current_v = bstream.read_bits(64);
    } catch (const base::TSDBException &e) {
      err_ = true;
      return false;
    }

    timestamp = current_t;
    if (timestamp == std::numeric_limits<int64_t>::max()) return false;
    value = base::decode_double(current_v);
    ++num_read;
    return true;
  } else if (num_read == 1) {
    int64_t delta_t;
    try {
      delta_t = bstream.read_signed_varint();
    } catch (const base::TSDBException &e) {
      err_ = true;
      return false;
    }

    delta_timestamp = delta_t;
    timestamp += delta_t;

    if (timestamp == std::numeric_limits<int64_t>::max()) return false;

    return read_value();
  }

  // Read timestamp delta-delta
  uint8_t type = 0;
  int64_t delta_delta = 0;
  for (int i = 0; i < 4; i++) {
    try {
      bool bit = bstream.read_bit();
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
        delta_delta = static_cast<int64_t>(bstream.read_bits(64));
      } catch (const base::TSDBException &e) {
        err_ = true;
        return false;
      }
  }
  if (size != 0) {
    try {
      delta_delta = static_cast<int64_t>(bstream.read_bits(size));
      if (delta_delta > (1 << (size - 1))) {
        delta_delta -= (1 << size);
      }
    } catch (const base::TSDBException &e) {
      err_ = true;
      return false;
    }
  }

  // Possible overflow
  delta_timestamp = static_cast<uint64_t>(
      delta_delta + static_cast<int64_t>(delta_timestamp));
  timestamp += static_cast<int64_t>(delta_timestamp);
  if (timestamp == std::numeric_limits<int64_t>::max()) return false;
  return read_value();
}

bool XORIterator::read_value() const {
  bool control_bit;
  try {
    control_bit = bstream.read_bit();  // First control bit
  } catch (const base::TSDBException &e) {
    return false;
  }

  if (control_bit == ZERO) {
    // it.val = it.val
  } else {
    try {
      control_bit = bstream.read_bit();  // Second control bit
    } catch (const base::TSDBException &e) {
      return false;
    }

    if (control_bit != ZERO) {
      uint8_t bits;
      try {
        bits = static_cast<uint8_t>(bstream.read_bits(5));
      } catch (const base::TSDBException &e) {
        return false;
      }
      leading_zero = bits;

      try {
        bits = static_cast<uint8_t>(bstream.read_bits(6));
      } catch (const base::TSDBException &e) {
        return false;
      }
      // 0 significant bits here means we overflowed and we actually need 64;
      // see comment in encoder
      if (bits == 0) {
        bits = 64;
      }
      trailing_zero = static_cast<uint8_t>(64 - leading_zero - bits);
    }

    uint64_t bits;
    try {
      bits = bstream.read_bits(
          static_cast<int>(64 - leading_zero - trailing_zero));
    } catch (const base::TSDBException &e) {
      return false;
    }
    uint64_t vbits = base::encode_double(value);
    vbits ^= (bits << trailing_zero);
    value = base::decode_double(vbits);
  }

  ++num_read;
  return true;
}

/**********************************************
 *                 XORIteratorV2              *
 **********************************************/
XORIteratorV2::XORIteratorV2(BitStreamV2 &bstream)
    : bstream(bstream.bytes(), bstream.size(), true),
      timestamp(0),
      value(0),
      delta_timestamp(0),
      leading_zero(0),
      trailing_zero(0),
      num_read(0),
      err_(false) {
  num_total = base::get_uint16_big_endian(bstream.bytes());
  // Pop the first two bytes
  this->bstream.pop_front();
  this->bstream.pop_front();
}

XORIteratorV2::XORIteratorV2(uint8_t *ptr, int size)
    : bstream(ptr, size, true),
      timestamp(0),
      value(0),
      delta_timestamp(0),
      leading_zero(0),
      trailing_zero(0),
      num_read(0),
      err_(false) {
  num_total = base::get_uint16_big_endian(bstream.bytes());
  // Pop the first two bytes
  this->bstream.pop_front();
  this->bstream.pop_front();
}

XORIteratorV2::XORIteratorV2(BitStreamV2 &bstream, int num_samples)
    : bstream(bstream.bytes(), bstream.size(), true),
      timestamp(0),
      value(0),
      delta_timestamp(0),
      leading_zero(0),
      trailing_zero(0),
      num_total(num_samples),
      num_read(0),
      err_(false) {}

bool XORIteratorV2::next() const {
  if (err_ || num_read == num_total) return false;

  if (num_read == 0) {
    int64_t current_t;
    try {
      current_t = bstream.read_signed_varint();
    } catch (const base::TSDBException &e) {
      err_ = true;
      return false;
    }

    uint64_t current_v;
    try {
      current_v = bstream.read_bits(64);
    } catch (const base::TSDBException &e) {
      err_ = true;
      return false;
    }

    timestamp = current_t;
    if (timestamp == std::numeric_limits<int64_t>::max()) return false;
    value = base::decode_double(current_v);
    ++num_read;
    return true;
  } else if (num_read == 1) {
    int64_t delta_t;
    try {
      delta_t = bstream.read_signed_varint();
    } catch (const base::TSDBException &e) {
      err_ = true;
      return false;
    }

    delta_timestamp = delta_t;
    timestamp += delta_t;

    if (timestamp == std::numeric_limits<int64_t>::max()) return false;

    return read_value();
  }

  // Read timestamp delta-delta
  uint8_t type = 0;
  int64_t delta_delta = 0;
  for (int i = 0; i < 4; i++) {
    try {
      bool bit = bstream.read_bit();
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
        delta_delta = static_cast<int64_t>(bstream.read_bits(64));
      } catch (const base::TSDBException &e) {
        err_ = true;
        return false;
      }
  }
  if (size != 0) {
    try {
      delta_delta = static_cast<int64_t>(bstream.read_bits(size));
      if (delta_delta > (1 << (size - 1))) {
        delta_delta -= (1 << size);
      }
    } catch (const base::TSDBException &e) {
      err_ = true;
      return false;
    }
  }

  // Possible overflow
  delta_timestamp = static_cast<uint64_t>(
      delta_delta + static_cast<int64_t>(delta_timestamp));
  timestamp += static_cast<int64_t>(delta_timestamp);
  if (timestamp == std::numeric_limits<int64_t>::max()) return false;
  return read_value();
}

bool XORIteratorV2::read_value() const {
  bool control_bit;
  try {
    control_bit = bstream.read_bit();  // First control bit
  } catch (const base::TSDBException &e) {
    return false;
  }

  if (control_bit == ZERO) {
    // it.val = it.val
  } else {
    try {
      control_bit = bstream.read_bit();  // Second control bit
    } catch (const base::TSDBException &e) {
      return false;
    }

    if (control_bit != ZERO) {
      uint8_t bits;
      try {
        bits = static_cast<uint8_t>(bstream.read_bits(5));
      } catch (const base::TSDBException &e) {
        return false;
      }
      leading_zero = bits;

      try {
        bits = static_cast<uint8_t>(bstream.read_bits(6));
      } catch (const base::TSDBException &e) {
        return false;
      }
      // 0 significant bits here means we overflowed and we actually need 64;
      // see comment in encoder
      if (bits == 0) {
        bits = 64;
      }
      trailing_zero = static_cast<uint8_t>(64 - leading_zero - bits);
    }

    uint64_t bits;
    try {
      bits = bstream.read_bits(
          static_cast<int>(64 - leading_zero - trailing_zero));
    } catch (const base::TSDBException &e) {
      return false;
    }
    uint64_t vbits = base::encode_double(value);
    vbits ^= (bits << trailing_zero);
    value = base::decode_double(vbits);
  }

  ++num_read;
  return true;
}

/**********************************************
 *           NullSupportedXORIterator         *
 **********************************************/
// Read mode BitStream
NullSupportedXORIterator::NullSupportedXORIterator(BitStream &bstream,
                                                   bool safe_mode)
    : safe_mode(safe_mode),
      timestamp(0),
      value(0),
      delta_timestamp(0),
      leading_zero(0),
      trailing_zero(0),
      num_read(0),
      err_(false),
      null_value_(false) {
  // This is to prevent pointer invalidation when vector resizes during
  // appending new data. For those XORChunk not created in read mode.
  if (safe_mode) {
    std::vector<uint8_t> stream(bstream.size(), 0);
    memcpy(&(stream.front()), bstream.bytes_ptr(), bstream.size());
    this->bstream = BitStream(stream);
  } else
    this->bstream = BitStream(bstream.bytes_ptr(), bstream.size());

  num_total = base::get_uint16_big_endian(bstream.bytes_ptr());
  // Pop the first two bytes
  this->bstream.pop_front();
  this->bstream.pop_front();
}

NullSupportedXORIterator::NullSupportedXORIterator(BitStream &bstream,
                                                   bool safe_mode,
                                                   int num_samples)
    : safe_mode(safe_mode),
      timestamp(0),
      value(0),
      delta_timestamp(0),
      leading_zero(0),
      trailing_zero(0),
      num_total(num_samples),
      num_read(0),
      err_(false),
      null_value_(false) {
  if (safe_mode) {
    std::vector<uint8_t> stream(bstream.size(), 0);
    memcpy(&(stream.front()), bstream.bytes_ptr(), bstream.size());
    this->bstream = BitStream(stream);
  } else
    this->bstream = BitStream(bstream.bytes_ptr(), bstream.size());
}

bool NullSupportedXORIterator::next() const {
  if (err_ || num_read == num_total) return false;

  null_value_ = false;

  if (num_read == 0) {
    int64_t current_t;
    try {
      current_t = bstream.read_signed_varint();
    } catch (const base::TSDBException &e) {
      err_ = true;
      return false;
    }

    uint64_t current_v;
    bool first_bit;
    try {
      first_bit = bstream.read_bit();
      if (first_bit == ONE) current_v = bstream.read_bits(64);
    } catch (const base::TSDBException &e) {
      err_ = true;
      return false;
    }

    timestamp = current_t;
    if (timestamp == std::numeric_limits<int64_t>::max()) return false;
    if (first_bit == ONE)
      value = base::decode_double(current_v);
    else
      null_value_ = true;
    // value = std::numeric_limits<double>::max();
    ++num_read;
    return true;
  } else if (num_read == 1) {
    int64_t delta_t;
    try {
      delta_t = bstream.read_signed_varint();
    } catch (const base::TSDBException &e) {
      err_ = true;
      return false;
    }

    delta_timestamp = delta_t;
    timestamp += delta_t;

    if (timestamp == std::numeric_limits<int64_t>::max()) return false;

    return read_value();
  }

  // Read timestamp delta-delta
  uint8_t type = 0;
  int64_t delta_delta = 0;
  for (int i = 0; i < 4; i++) {
    try {
      bool bit = bstream.read_bit();
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
        delta_delta = static_cast<int64_t>(bstream.read_bits(64));
      } catch (const base::TSDBException &e) {
        err_ = true;
        return false;
      }
  }
  if (size != 0) {
    try {
      delta_delta = static_cast<int64_t>(bstream.read_bits(size));
      if (delta_delta > (1 << (size - 1))) {
        delta_delta -= (1 << size);
      }
    } catch (const base::TSDBException &e) {
      err_ = true;
      return false;
    }
  }

  // Possible overflow
  delta_timestamp = static_cast<uint64_t>(
      delta_delta + static_cast<int64_t>(delta_timestamp));
  timestamp += static_cast<int64_t>(delta_timestamp);
  if (timestamp == std::numeric_limits<int64_t>::max()) return false;
  return read_value();
}

bool NullSupportedXORIterator::read_value() const {
  bool control_bit;

  try {
    control_bit = bstream.read_bit();  // First control bit
  } catch (const base::TSDBException &e) {
    return false;
  }

  if (control_bit == ZERO) {
    null_value_ = true;
    // value = std::numeric_limits<double>::max();
  } else {
    try {
      control_bit = bstream.read_bit();  // Second control bit
    } catch (const base::TSDBException &e) {
      return false;
    }

    if (control_bit == ZERO) {
      // it.val = it.val
    } else {
      try {
        control_bit = bstream.read_bit();  // Third control bit
      } catch (const base::TSDBException &e) {
        return false;
      }

      if (control_bit != ZERO) {
        uint8_t bits;
        try {
          bits = static_cast<uint8_t>(bstream.read_bits(5));
        } catch (const base::TSDBException &e) {
          return false;
        }
        leading_zero = bits;

        try {
          bits = static_cast<uint8_t>(bstream.read_bits(6));
        } catch (const base::TSDBException &e) {
          return false;
        }
        // 0 significant bits here means we overflowed and we actually need 64;
        // see comment in encoder
        if (bits == 0) {
          bits = 64;
        }
        trailing_zero = static_cast<uint8_t>(64 - leading_zero - bits);
      }

      uint64_t bits;
      try {
        bits = bstream.read_bits(
            static_cast<int>(64 - leading_zero - trailing_zero));
      } catch (const base::TSDBException &e) {
        return false;
      }
      uint64_t vbits = base::encode_double(value);
      vbits ^= (bits << trailing_zero);
      value = base::decode_double(vbits);
    }
  }

  ++num_read;
  return true;
}

/**********************************************
 *      NullSupportedXORGroupIterator         *
 **********************************************/
NullSupportedXORGroupIterator::NullSupportedXORGroupIterator(BitStream &t,
                                                             BitStream &v)
    : timestamp(0),
      value(0),
      delta_timestamp(0),
      leading_zero(0),
      trailing_zero(0),
      num_read(0),
      err_(false),
      read_mode_chunk_(false),
      null_value_(false) {
  std::vector<uint8_t> stream1(t.size(), 0);
  memcpy(&(stream1.front()), t.bytes_ptr(), t.size());
  this->t_bstream_ = BitStream(stream1);
  std::vector<uint8_t> stream2(v.size(), 0);
  memcpy(&(stream2.front()), v.bytes_ptr(), v.size());
  this->v_bstream_ = BitStream(stream2);

  num_total = base::get_uint16_big_endian(t_bstream_.bytes_ptr());
  // Pop the first two bytes
  this->t_bstream_.pop_front();
  this->t_bstream_.pop_front();
}

NullSupportedXORGroupIterator::NullSupportedXORGroupIterator(const uint8_t *ptr,
                                                             int off, int size)
    : t_bstream_(ptr, size),
      v_bstream_(ptr + off, size),
      timestamp(0),
      value(0),
      delta_timestamp(0),
      leading_zero(0),
      trailing_zero(0),
      num_read(0),
      err_(false),
      read_mode_chunk_(true),
      null_value_(false) {
  num_total = base::get_uint16_big_endian(t_bstream_.bytes_ptr());
  // Pop the first two bytes
  this->t_bstream_.pop_front();
  this->t_bstream_.pop_front();
}

NullSupportedXORGroupIterator::NullSupportedXORGroupIterator(
    const uint8_t *ptr1, int size1, const uint8_t *ptr2, int size2)
    : t_bstream_(ptr1, size1),
      v_bstream_(ptr2, size2),
      timestamp(0),
      value(0),
      delta_timestamp(0),
      leading_zero(0),
      trailing_zero(0),
      num_read(0),
      err_(false),
      read_mode_chunk_(true),
      null_value_(false) {
  num_total = base::get_uint16_big_endian(t_bstream_.bytes_ptr());
  // Pop the first two bytes
  this->t_bstream_.pop_front();
  this->t_bstream_.pop_front();
}

bool NullSupportedXORGroupIterator::next() const {
  if (err_ || num_read == num_total) return false;

  null_value_ = false;

  if (num_read == 0) {
    int64_t current_t;
    try {
      current_t = t_bstream_.read_signed_varint();
    } catch (const base::TSDBException &e) {
      err_ = true;
      return false;
    }

    uint64_t current_v;
    bool first_bit;
    try {
      first_bit = v_bstream_.read_bit();
      if (first_bit == ONE) current_v = v_bstream_.read_bits(64);
    } catch (const base::TSDBException &e) {
      err_ = true;
      return false;
    }

    timestamp = current_t;
    if (timestamp == std::numeric_limits<int64_t>::max()) return false;
    if (first_bit == ONE)
      value = base::decode_double(current_v);
    else
      null_value_ = true;
    // value = std::numeric_limits<double>::max();
    ++num_read;
    return true;
  } else if (num_read == 1) {
    int64_t delta_t;
    try {
      delta_t = t_bstream_.read_signed_varint();
    } catch (const base::TSDBException &e) {
      err_ = true;
      return false;
    }

    delta_timestamp = delta_t;
    timestamp += delta_t;

    if (timestamp == std::numeric_limits<int64_t>::max()) return false;

    return read_value();
  }

  // Read timestamp delta-delta
  uint8_t type = 0;
  int64_t delta_delta = 0;
  for (int i = 0; i < 4; i++) {
    try {
      bool bit = t_bstream_.read_bit();
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
        delta_delta = static_cast<int64_t>(t_bstream_.read_bits(64));
      } catch (const base::TSDBException &e) {
        err_ = true;
        return false;
      }
  }
  if (size != 0) {
    try {
      delta_delta = static_cast<int64_t>(t_bstream_.read_bits(size));
      if (delta_delta > (1 << (size - 1))) {
        delta_delta -= (1 << size);
      }
    } catch (const base::TSDBException &e) {
      err_ = true;
      return false;
    }
  }

  // Possible overflow
  delta_timestamp = static_cast<uint64_t>(
      delta_delta + static_cast<int64_t>(delta_timestamp));
  timestamp += static_cast<int64_t>(delta_timestamp);
  if (timestamp == std::numeric_limits<int64_t>::max()) return false;
  return read_value();
}

bool NullSupportedXORGroupIterator::read_value() const {
  bool control_bit;

  try {
    control_bit = v_bstream_.read_bit();  // First control bit
  } catch (const base::TSDBException &e) {
    return false;
  }

  if (control_bit == ZERO) {
    null_value_ = true;
    // value = std::numeric_limits<double>::max();
  } else {
    try {
      control_bit = v_bstream_.read_bit();  // Second control bit
    } catch (const base::TSDBException &e) {
      return false;
    }

    if (control_bit == ZERO) {
      // it.val = it.val
    } else {
      try {
        control_bit = v_bstream_.read_bit();  // Third control bit
      } catch (const base::TSDBException &e) {
        return false;
      }

      if (control_bit != ZERO) {
        uint8_t bits;
        try {
          bits = static_cast<uint8_t>(v_bstream_.read_bits(5));
        } catch (const base::TSDBException &e) {
          return false;
        }
        leading_zero = bits;

        try {
          bits = static_cast<uint8_t>(v_bstream_.read_bits(6));
        } catch (const base::TSDBException &e) {
          return false;
        }
        // 0 significant bits here means we overflowed and we actually need 64;
        // see comment in encoder
        if (bits == 0) {
          bits = 64;
        }
        trailing_zero = static_cast<uint8_t>(64 - leading_zero - bits);
      }

      uint64_t bits;
      try {
        bits = v_bstream_.read_bits(
            static_cast<int>(64 - leading_zero - trailing_zero));
      } catch (const base::TSDBException &e) {
        return false;
      }
      uint64_t vbits = base::encode_double(value);
      vbits ^= (bits << trailing_zero);
      value = base::decode_double(vbits);
    }
  }

  ++num_read;
  return true;
}

/**********************************************
 *      NullSupportedXORGroupIteratorV2       *
 **********************************************/
NullSupportedXORGroupIteratorV2::NullSupportedXORGroupIteratorV2(BitStreamV2 &t,
                                                                 BitStreamV2 &v)
    : t_bstream_(t.bytes(), t.size(), true),
      v_bstream_(v.bytes(), v.size(), true),
      timestamp(0),
      value(0),
      delta_timestamp(0),
      leading_zero(0),
      trailing_zero(0),
      num_read(0),
      err_(false),
      read_mode_chunk_(false),
      null_value_(false) {
  num_total = base::get_uint16_big_endian(t_bstream_.bytes_ptr());
  // Pop the first two bytes
  this->t_bstream_.pop_front();
  this->t_bstream_.pop_front();
}

bool NullSupportedXORGroupIteratorV2::next() const {
  if (err_ || num_read == num_total) return false;

  null_value_ = false;

  if (num_read == 0) {
    int64_t current_t;
    try {
      current_t = t_bstream_.read_signed_varint();
    } catch (const base::TSDBException &e) {
      err_ = true;
      return false;
    }

    uint64_t current_v;
    bool first_bit;
    try {
      first_bit = v_bstream_.read_bit();
      if (first_bit == ONE) current_v = v_bstream_.read_bits(64);
    } catch (const base::TSDBException &e) {
      err_ = true;
      return false;
    }

    timestamp = current_t;
    if (timestamp == std::numeric_limits<int64_t>::max()) return false;
    if (first_bit == ONE)
      value = base::decode_double(current_v);
    else
      null_value_ = true;
    // value = std::numeric_limits<double>::max();
    ++num_read;
    return true;
  } else if (num_read == 1) {
    int64_t delta_t;
    try {
      delta_t = t_bstream_.read_signed_varint();
    } catch (const base::TSDBException &e) {
      err_ = true;
      return false;
    }

    delta_timestamp = delta_t;
    timestamp += delta_t;

    if (timestamp == std::numeric_limits<int64_t>::max()) return false;

    return read_value();
  }

  // Read timestamp delta-delta
  uint8_t type = 0;
  int64_t delta_delta = 0;
  for (int i = 0; i < 4; i++) {
    try {
      bool bit = t_bstream_.read_bit();
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
        delta_delta = static_cast<int64_t>(t_bstream_.read_bits(64));
      } catch (const base::TSDBException &e) {
        err_ = true;
        return false;
      }
  }
  if (size != 0) {
    try {
      delta_delta = static_cast<int64_t>(t_bstream_.read_bits(size));
      if (delta_delta > (1 << (size - 1))) {
        delta_delta -= (1 << size);
      }
    } catch (const base::TSDBException &e) {
      err_ = true;
      return false;
    }
  }

  // Possible overflow
  delta_timestamp = static_cast<uint64_t>(
      delta_delta + static_cast<int64_t>(delta_timestamp));
  timestamp += static_cast<int64_t>(delta_timestamp);
  if (timestamp == std::numeric_limits<int64_t>::max()) return false;
  return read_value();
}

bool NullSupportedXORGroupIteratorV2::read_value() const {
  bool control_bit;

  try {
    control_bit = v_bstream_.read_bit();  // First control bit
  } catch (const base::TSDBException &e) {
    return false;
  }

  if (control_bit == ZERO) {
    null_value_ = true;
    // value = std::numeric_limits<double>::max();
  } else {
    try {
      control_bit = v_bstream_.read_bit();  // Second control bit
    } catch (const base::TSDBException &e) {
      return false;
    }

    if (control_bit == ZERO) {
      // it.val = it.val
    } else {
      try {
        control_bit = v_bstream_.read_bit();  // Third control bit
      } catch (const base::TSDBException &e) {
        return false;
      }

      if (control_bit != ZERO) {
        uint8_t bits;
        try {
          bits = static_cast<uint8_t>(v_bstream_.read_bits(5));
        } catch (const base::TSDBException &e) {
          return false;
        }
        leading_zero = bits;

        try {
          bits = static_cast<uint8_t>(v_bstream_.read_bits(6));
        } catch (const base::TSDBException &e) {
          return false;
        }
        // 0 significant bits here means we overflowed and we actually need 64;
        // see comment in encoder
        if (bits == 0) {
          bits = 64;
        }
        trailing_zero = static_cast<uint8_t>(64 - leading_zero - bits);
      }

      uint64_t bits;
      try {
        bits = v_bstream_.read_bits(
            static_cast<int>(64 - leading_zero - trailing_zero));
      } catch (const base::TSDBException &e) {
        return false;
      }
      uint64_t vbits = base::encode_double(value);
      vbits ^= (bits << trailing_zero);
      value = base::decode_double(vbits);
    }
  }

  ++num_read;
  return true;
}

/**********************************************
 *      NullSupportedXORGroupTimeIterator     *
 **********************************************/
NullSupportedXORGroupTimeIterator::NullSupportedXORGroupTimeIterator(
    const uint8_t *ptr, int size)
    : t_bstream_(ptr, size),
      timestamp(0),
      delta_timestamp(0),
      num_read(0),
      err_(false) {
  num_total = base::get_uint16_big_endian(t_bstream_.bytes_ptr());
  // Pop the first two bytes
  this->t_bstream_.pop_front();
  this->t_bstream_.pop_front();
}

bool NullSupportedXORGroupTimeIterator::next() const {
  if (err_ || num_read == num_total) return false;

  if (num_read == 0) {
    int64_t current_t;
    try {
      current_t = t_bstream_.read_signed_varint();
    } catch (const base::TSDBException &e) {
      err_ = true;
      return false;
    }

    timestamp = current_t;
    if (timestamp == std::numeric_limits<int64_t>::max()) return false;
    ++num_read;
    return true;
  } else if (num_read == 1) {
    int64_t delta_t;
    try {
      delta_t = t_bstream_.read_signed_varint();
    } catch (const base::TSDBException &e) {
      err_ = true;
      return false;
    }

    delta_timestamp = delta_t;
    timestamp += delta_t;

    if (timestamp == std::numeric_limits<int64_t>::max()) return false;

    ++num_read;
    return true;
  }

  // Read timestamp delta-delta
  uint8_t type = 0;
  int64_t delta_delta = 0;
  for (int i = 0; i < 4; i++) {
    try {
      bool bit = t_bstream_.read_bit();
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
        delta_delta = static_cast<int64_t>(t_bstream_.read_bits(64));
      } catch (const base::TSDBException &e) {
        err_ = true;
        return false;
      }
  }
  if (size != 0) {
    try {
      delta_delta = static_cast<int64_t>(t_bstream_.read_bits(size));
      if (delta_delta > (1 << (size - 1))) {
        delta_delta -= (1 << size);
      }
    } catch (const base::TSDBException &e) {
      err_ = true;
      return false;
    }
  }

  ++num_read;
  // Possible overflow
  delta_timestamp = static_cast<uint64_t>(
      delta_delta + static_cast<int64_t>(delta_timestamp));
  timestamp += static_cast<int64_t>(delta_timestamp);
  if (timestamp == std::numeric_limits<int64_t>::max()) return false;
  return true;
}

/**********************************************
 *     NullSupportedXORGroupValueIterator     *
 **********************************************/
NullSupportedXORGroupValueIterator::NullSupportedXORGroupValueIterator(
    const uint8_t *ptr, int size, int num_samples)
    : v_bstream_(ptr, size),
      value(0),
      leading_zero(0),
      trailing_zero(0),
      num_total(num_samples),
      num_read(0),
      err_(false),
      null_value_(false) {}

bool NullSupportedXORGroupValueIterator::next() const {
  if (err_ || num_read == num_total) return false;

  null_value_ = false;

  if (num_read == 0) {
    uint64_t current_v;
    bool first_bit;
    try {
      first_bit = v_bstream_.read_bit();
      if (first_bit == ONE) current_v = v_bstream_.read_bits(64);
    } catch (const base::TSDBException &e) {
      err_ = true;
      return false;
    }

    if (first_bit == ONE)
      value = base::decode_double(current_v);
    else
      // value = std::numeric_limits<double>::max();
      null_value_ = true;
    ++num_read;
    return true;
  }

  return read_value();
}

bool NullSupportedXORGroupValueIterator::read_value() const {
  bool control_bit;

  try {
    control_bit = v_bstream_.read_bit();  // First control bit
  } catch (const base::TSDBException &e) {
    return false;
  }

  if (control_bit == ZERO) {
    // value = std::numeric_limits<double>::max();
    null_value_ = true;
  } else {
    try {
      control_bit = v_bstream_.read_bit();  // Second control bit
    } catch (const base::TSDBException &e) {
      return false;
    }

    if (control_bit == ZERO) {
      // it.val = it.val
    } else {
      try {
        control_bit = v_bstream_.read_bit();  // Third control bit
      } catch (const base::TSDBException &e) {
        return false;
      }

      if (control_bit != ZERO) {
        uint8_t bits;
        try {
          bits = static_cast<uint8_t>(v_bstream_.read_bits(5));
        } catch (const base::TSDBException &e) {
          return false;
        }
        leading_zero = bits;

        try {
          bits = static_cast<uint8_t>(v_bstream_.read_bits(6));
        } catch (const base::TSDBException &e) {
          return false;
        }
        // 0 significant bits here means we overflowed and we actually need 64;
        // see comment in encoder
        if (bits == 0) {
          bits = 64;
        }
        trailing_zero = static_cast<uint8_t>(64 - leading_zero - bits);
      }

      uint64_t bits;
      try {
        bits = v_bstream_.read_bits(
            static_cast<int>(64 - leading_zero - trailing_zero));
      } catch (const base::TSDBException &e) {
        return false;
      }
      uint64_t vbits = base::encode_double(value);
      vbits ^= (bits << trailing_zero);
      value = base::decode_double(vbits);
    }
  }

  ++num_read;
  return true;
}

}  // namespace chunk
}  // namespace tsdb