#include "base/Endian.hpp"

namespace tsdb {
namespace base {

const int MAX_VARINT_LEN_64 = 10;  // 7 * 9 + 1
const int MAX_VARINT_LEN_32 = 5;

// https://github.com/golang/go/blob/master/src/math/bits/bits_tables.go.
const uint8_t len8table[256] = {
    0x00, 0x01, 0x02, 0x02, 0x03, 0x03, 0x03, 0x03, 0x04, 0x04, 0x04, 0x04,
    0x04, 0x04, 0x04, 0x04, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05,
    0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x06, 0x06, 0x06, 0x06,
    0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06,
    0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06,
    0x06, 0x06, 0x06, 0x06, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07,
    0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07,
    0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07,
    0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07,
    0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07,
    0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x08, 0x08, 0x08, 0x08,
    0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08,
    0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08,
    0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08,
    0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08,
    0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08,
    0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08,
    0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08,
    0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08,
    0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08,
    0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08,
    0x08, 0x08, 0x08, 0x08};

const int64_t bytes64pretable[8] = {static_cast<int64_t>(0xffffffffffffff00),
                                    static_cast<int64_t>(0xffffffffffff0000),
                                    static_cast<int64_t>(0xffffffffff000000),
                                    static_cast<int64_t>(0xffffffff00000000),
                                    static_cast<int64_t>(0xffffff0000000000),
                                    static_cast<int64_t>(0xffff000000000000),
                                    static_cast<int64_t>(0xff00000000000000),
                                    static_cast<int64_t>(0x0)};

void put_uint16_big_endian(std::deque<uint8_t> &bytes, int num) {
  bytes[1] = (num & 0xff);
  bytes[0] = ((num >> 8) & 0xff);
}

int get_uint16_big_endian(const std::deque<uint8_t> &bytes) {
  return static_cast<int>((static_cast<int>(bytes[0]) << 8) +
                          static_cast<int>(bytes[1]));
}

void put_uint16_big_endian(std::vector<uint8_t> &bytes, int num) {
  bytes[1] = (num & 0xff);
  bytes[0] = ((num >> 8) & 0xff);
}

int get_uint16_big_endian(const std::vector<uint8_t> &bytes) {
  return static_cast<int>((static_cast<int>(bytes[0]) << 8) +
                          static_cast<int>(bytes[1]));
}

void put_uint16_big_endian(uint8_t *bytes, int num) {
  bytes[1] = (num & 0xff);
  bytes[0] = ((num >> 8) & 0xff);
}

int get_uint16_big_endian(const uint8_t *bytes) {
  return static_cast<int>((static_cast<int>(bytes[0]) << 8) +
                          static_cast<int>(bytes[1]));
}

uint32_t get_uint32_big_endian(const char *bytes) {
  return static_cast<uint32_t>(
      static_cast<uint32_t>((uint32_t)((uint8_t)(bytes[0])) << 24) +
      (static_cast<uint32_t>((uint8_t)(bytes[1])) << 16) +
      (static_cast<uint32_t>((uint8_t)(bytes[2])) << 8) +
      static_cast<uint32_t>((uint8_t)(bytes[3])));
}
uint32_t get_uint32_big_endian(const uint8_t *bytes) {
  return static_cast<uint32_t>(
      static_cast<uint32_t>((uint32_t)(bytes[0]) << 24) +
      (static_cast<uint32_t>(bytes[1]) << 16) +
      (static_cast<uint32_t>(bytes[2]) << 8) + static_cast<uint32_t>(bytes[3]));
}

void put_uint32_big_endian(uint8_t *bytes, uint32_t num) {
  bytes[3] = (num & 0xff);
  bytes[2] = ((num >> 8) & 0xff);
  bytes[1] = ((num >> 16) & 0xff);
  bytes[0] = ((num >> 24) & 0xff);
}

uint32_t get_uint32_big_endian(const std::deque<uint8_t> &bytes) {
  return static_cast<uint32_t>(static_cast<uint32_t>((bytes[0]) << 24) +
                               (static_cast<uint32_t>(bytes[1]) << 16) +
                               (static_cast<uint32_t>(bytes[2]) << 8) +
                               static_cast<uint32_t>(bytes[3]));
}

void put_uint32_big_endian(std::deque<uint8_t> &bytes, uint32_t num) {
  bytes[3] = (num & 0xff);
  bytes[2] = ((num >> 8) & 0xff);
  bytes[1] = ((num >> 16) & 0xff);
  bytes[0] = ((num >> 24) & 0xff);
}

uint32_t get_uint32_big_endian(const std::vector<uint8_t> &bytes) {
  return static_cast<uint32_t>(static_cast<uint32_t>((bytes[0]) << 24) +
                               (static_cast<uint32_t>(bytes[1]) << 16) +
                               (static_cast<uint32_t>(bytes[2]) << 8) +
                               static_cast<uint32_t>(bytes[3]));
}

void put_uint32_big_endian(std::vector<uint8_t> &bytes, uint32_t num) {
  bytes[3] = (num & 0xff);
  bytes[2] = ((num >> 8) & 0xff);
  bytes[1] = ((num >> 16) & 0xff);
  bytes[0] = ((num >> 24) & 0xff);
}

uint64_t get_uint64_big_endian(const std::vector<uint8_t> &bytes) {
  return static_cast<uint64_t>((static_cast<uint64_t>(bytes[0]) << 56) +
                               (static_cast<uint64_t>(bytes[1]) << 48) +
                               (static_cast<uint64_t>(bytes[2]) << 40) +
                               (static_cast<uint64_t>(bytes[3]) << 32) +
                               (static_cast<uint64_t>(bytes[4]) << 24) +
                               (static_cast<uint64_t>(bytes[5]) << 16) +
                               (static_cast<uint64_t>(bytes[6]) << 8) +
                               static_cast<uint64_t>(bytes[7]));
}

void put_uint64_big_endian(std::vector<uint8_t> &bytes, uint64_t num) {
  bytes[7] = (num & 0xff);
  bytes[6] = ((num >> 8) & 0xff);
  bytes[5] = ((num >> 16) & 0xff);
  bytes[4] = ((num >> 24) & 0xff);
  bytes[3] = ((num >> 32) & 0xff);
  bytes[2] = ((num >> 40) & 0xff);
  bytes[1] = ((num >> 48) & 0xff);
  bytes[0] = ((num >> 56) & 0xff);
}

uint64_t get_uint64_big_endian(const uint8_t *bytes) {
  return static_cast<uint64_t>((static_cast<uint64_t>(bytes[0]) << 56) +
                               (static_cast<uint64_t>(bytes[1]) << 48) +
                               (static_cast<uint64_t>(bytes[2]) << 40) +
                               (static_cast<uint64_t>(bytes[3]) << 32) +
                               (static_cast<uint64_t>(bytes[4]) << 24) +
                               (static_cast<uint64_t>(bytes[5]) << 16) +
                               (static_cast<uint64_t>(bytes[6]) << 8) +
                               static_cast<uint64_t>(bytes[7]));
}
uint64_t get_uint64_big_endian(const char *bytes) {
  return static_cast<uint64_t>(
      (static_cast<uint64_t>((uint8_t)(bytes[0])) << 56) +
      (static_cast<uint64_t>((uint8_t)(bytes[1])) << 48) +
      (static_cast<uint64_t>((uint8_t)(bytes[2])) << 40) +
      (static_cast<uint64_t>((uint8_t)(bytes[3])) << 32) +
      (static_cast<uint64_t>((uint8_t)(bytes[4])) << 24) +
      (static_cast<uint64_t>((uint8_t)(bytes[5])) << 16) +
      (static_cast<uint64_t>((uint8_t)(bytes[6])) << 8) +
      static_cast<uint64_t>((uint8_t)(bytes[7])));
}

void put_uint64_big_endian(uint8_t *bytes, uint64_t num) {
  bytes[7] = (num & 0xff);
  bytes[6] = ((num >> 8) & 0xff);
  bytes[5] = ((num >> 16) & 0xff);
  bytes[4] = ((num >> 24) & 0xff);
  bytes[3] = ((num >> 32) & 0xff);
  bytes[2] = ((num >> 40) & 0xff);
  bytes[1] = ((num >> 48) & 0xff);
  bytes[0] = ((num >> 56) & 0xff);
}

uint64_t encode_double(double value) {
  union {
    double f;
    uint64_t i;
  };
  f = value;
  return i;
}

double decode_double(uint64_t value) {
  union {
    double f;
    uint64_t i;
  };
  i = value;
  return f;
}

uint64_t decode_unsigned_varint(const std::deque<uint8_t> &data,
                                int &decoded_bytes) {
  int i = 0;
  uint64_t decoded_value = 0;
  int shift_amount = 0;

  do {
    decoded_value |= static_cast<uint64_t>(data[i] & 0x7F) << shift_amount;
    shift_amount += 7;
  } while (i < data.size() && (data[i++] & 0x80) != 0);

  if (i == data.size() && (data[i - 1] & 0x80) != 0)
    throw TSDBException("Early decoding EOF");

  decoded_bytes = i;
  return decoded_value;
}

int64_t decode_signed_varint(const std::deque<uint8_t> &data,
                             int &decoded_bytes) {
  uint64_t unsigned_value = decode_unsigned_varint(data, decoded_bytes);
  return static_cast<uint64_t>(unsigned_value & 1 ? ~(unsigned_value >> 1)
                                                  : (unsigned_value >> 1));
}

uint64_t decode_unsigned_varint(const uint8_t *data, int &decoded_bytes,
                                int size) {
  int i = 0;
  uint64_t decoded_value = 0;
  int shift_amount = 0;

  do {
    decoded_value |= static_cast<uint64_t>(data[i] & 0x7F) << shift_amount;
    shift_amount += 7;
  } while (i < size && (data[i++] & 0x80) != 0);

  if (i == size && (data[i - 1] & 0x80) != 0)
    throw TSDBException("Early decoding EOF");

  decoded_bytes = i;
  return decoded_value;
}

int64_t decode_signed_varint(const uint8_t *data, int &decoded_bytes,
                             int size) {
  uint64_t unsigned_value = decode_unsigned_varint(data, decoded_bytes, size);
  return static_cast<uint64_t>(unsigned_value & 1 ? ~(unsigned_value >> 1)
                                                  : (unsigned_value >> 1));
}

// Encode an unsigned 64-bit varint.  Returns number of encoded bytes.
// Buffer's size is at least 10
int encode_unsigned_varint(uint8_t *const buffer, uint64_t value) {
  int encoded = 0;

  do {
    uint8_t next_byte = value & 0x7F;
    value >>= 7;

    if (value) next_byte |= 0x80;

    buffer[encoded++] = next_byte;

  } while (value);

  return encoded;
}

// Encode a signed 64-bit varint.  Works by first zig-zag transforming
// signed value into an unsigned value, and then reusing the unsigned
// encoder.
// Buffer's size is at least 10
int encode_signed_varint(uint8_t *const buffer, int64_t value) {
  uint64_t uvalue;

  uvalue = static_cast<uint64_t>(value < 0 ? ~(value << 1) : (value << 1));

  return encode_unsigned_varint(buffer, uvalue);
}

// Encode an unsigned 64-bit varint.  Returns number of encoded bytes.
// Buffer's size is at least 10
int encode_unsigned_varint(std::vector<uint8_t> &buffer, uint64_t value) {
  int encoded = 0;

  do {
    uint8_t next_byte = value & 0x7F;
    value >>= 7;

    if (value) next_byte |= 0x80;

    buffer[encoded++] = next_byte;

  } while (value);

  return encoded;
}

// Encode a signed 64-bit varint.  Works by first zig-zag transforming
// signed value into an unsigned value, and then reusing the unsigned
// encoder.
// Buffer's size is at least 10
int encode_signed_varint(std::vector<uint8_t> &buffer, int64_t value) {
  uint64_t uvalue;

  uvalue = static_cast<uint64_t>(value < 0 ? ~(value << 1) : (value << 1));

  return encode_unsigned_varint(buffer, uvalue);
}

uint64_t encode_signed_varint(int64_t value) {
  return static_cast<uint64_t>(value < 0 ? ~(value << 1) : (value << 1));
}

int64_t decode_signed_varint(uint64_t unsigned_value) {
  return static_cast<uint64_t>(unsigned_value & 1 ? ~(unsigned_value >> 1)
                                                  : (unsigned_value >> 1));
}

uint8_t max_bits(const std::deque<int64_t> &d) {
  int64_t max = d.front();
  int64_t min = d.front();
  for (int i = 1; i < d.size(); ++i) {
    if (d[i] > max) max = d[i];
    if (d[i] < min) min = d[i];
  }
  if (min < 0) {
    uint8_t min_bits_ = static_cast<uint8_t>(__builtin_clzll(~min)) - 1;
    if (max < 0) return 64 - min_bits_;
    uint8_t max_bits_ = static_cast<uint8_t>(__builtin_clzll(max));
    return (min_bits_ < max_bits_ ? 64 - min_bits_ : 65 - max_bits_);
  }
  return static_cast<uint8_t>(65 - __builtin_clzll(max));
}
uint8_t max_bits(const std::vector<int64_t> &v) {
  int64_t max = v.front();
  int64_t min = v.front();
  for (int i = 1; i < v.size(); ++i) {
    if (v[i] > max) max = v[i];
    if (v[i] < min) min = v[i];
  }
  if (min < 0) {
    uint8_t min_bits_ = static_cast<uint8_t>(__builtin_clzll(~min)) - 1;
    if (max < 0) return 64 - min_bits_;
    uint8_t max_bits_ = static_cast<uint8_t>(__builtin_clzll(max));
    return (min_bits_ < max_bits_ ? 64 - min_bits_ : 65 - max_bits_);
  }
  return static_cast<uint8_t>(65 - __builtin_clzll(max));
}

uint8_t max_bytes(const std::deque<int64_t> &d) {
  return (max_bits(d) + 7) >> 3;
}
uint8_t max_bytes(const std::vector<int64_t> &v) {
  return (max_bits(v) + 7) >> 3;
}

// int64_t get_origin_bytes(int64_t data, int bytes){
//     bytes <<= 3;
//     if((data >> (bytes - 1)) == 1)
//         return static_cast<int64_t>(data | ~((static_cast<int64_t>(1) <<
//         bytes) - 1));
//     return data;
// }
int64_t get_origin(uint64_t data, int bits) {
  if ((data >> (bits - 1)) == 1)
    return static_cast<int64_t>(data |
                                ~((static_cast<int64_t>(1) << bits) - 1));
  return static_cast<int64_t>(data);
}

int64_t get_max(int bits) {
  return (static_cast<int64_t>(1) << (bits - 1)) - 1;
}
int64_t get_min(int bits) {
  return -1 * (static_cast<int64_t>(1) << (bits - 1));
}

}  // namespace base
}  // namespace tsdb