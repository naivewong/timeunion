#ifndef ENDIAN_H
#define ENDIAN_H

#include <stdint.h>

#include <deque>
#include <string>
#include <vector>

#include "base/TSDBException.hpp"

namespace tsdb {
namespace base {

extern const int MAX_VARINT_LEN_64;
extern const int MAX_VARINT_LEN_32;

// https://github.com/golang/go/blob/master/src/math/bits/bits_tables.go.
extern const uint8_t len8table[256];

extern const int64_t bytes64pretable[8];

// Decoding for char array
template <typename T>
uint16_t big_endian_uint16(T arr) {
  return static_cast<uint16_t>((arr[0] << 8) + arr[1]);
}

void put_uint16_big_endian(std::deque<uint8_t> &bytes, int num);

int get_uint16_big_endian(const std::deque<uint8_t> &bytes);

void put_uint16_big_endian(std::vector<uint8_t> &bytes, int num);

int get_uint16_big_endian(const std::vector<uint8_t> &bytes);

void put_uint16_big_endian(uint8_t *bytes, int num);

int get_uint16_big_endian(const uint8_t *bytes);

uint32_t get_uint32_big_endian(const char *bytes);
uint32_t get_uint32_big_endian(const uint8_t *bytes);

void put_uint32_big_endian(uint8_t *bytes, uint32_t num);

uint32_t get_uint32_big_endian(const std::deque<uint8_t> &bytes);

void put_uint32_big_endian(std::deque<uint8_t> &bytes, uint32_t num);

uint32_t get_uint32_big_endian(const std::vector<uint8_t> &bytes);

void put_uint32_big_endian(std::vector<uint8_t> &bytes, uint32_t num);

uint64_t get_uint64_big_endian(const std::vector<uint8_t> &bytes);

void put_uint64_big_endian(std::vector<uint8_t> &bytes, uint64_t num);

uint64_t get_uint64_big_endian(const char *bytes);
uint64_t get_uint64_big_endian(const uint8_t *bytes);

void put_uint64_big_endian(uint8_t *bytes, uint64_t num);

uint64_t encode_double(double value);

double decode_double(uint64_t value);

uint64_t decode_unsigned_varint(const std::deque<uint8_t> &data,
                                int &decoded_bytes);

int64_t decode_signed_varint(const std::deque<uint8_t> &data,
                             int &decoded_bytes);

uint64_t decode_unsigned_varint(const uint8_t *data, int &decoded_bytes,
                                int size);

int64_t decode_signed_varint(const uint8_t *data, int &decoded_bytes, int size);

// Encode an unsigned 64-bit varint.  Returns number of encoded bytes.
// Buffer's size is at least 10
int encode_unsigned_varint(uint8_t *const buffer, uint64_t value);

// Encode a signed 64-bit varint.  Works by first zig-zag transforming
// signed value into an unsigned value, and then reusing the unsigned
// encoder.
// Buffer's size is at least 10
int encode_signed_varint(uint8_t *const buffer, int64_t value);

// Encode an unsigned 64-bit varint.  Returns number of encoded bytes.
// Buffer's size is at least 10
int encode_unsigned_varint(std::vector<uint8_t> &buffer, uint64_t value);

// Encode a signed 64-bit varint.  Works by first zig-zag transforming
// signed value into an unsigned value, and then reusing the unsigned
// encoder.
// Buffer's size is at least 10
int encode_signed_varint(std::vector<uint8_t> &buffer, int64_t value);

uint64_t encode_signed_varint(int64_t value);

int64_t decode_signed_varint(uint64_t unsigned_value);

template <typename I>
std::string n2hexstr(I w, size_t hex_len = sizeof(I) << 1) {
  static const char *digits = "0123456789ABCDEF";
  std::string rc(hex_len, '0');
  for (size_t i = 0, j = (hex_len - 1) * 4; i < hex_len; ++i, j -= 4)
    rc[i] = digits[(w >> j) & 0x0f];
  return rc;
}

// https://github.com/golang/go/blob/c11f6c4929efff3ef02aff9a3de9c0f4799bc276/src/math/bits/bits.go#L330
inline uint8_t max_bits64(uint64_t v) {
  uint8_t n = 0;
  if (v >= static_cast<uint64_t>(1) << 32) {
    v >>= 32;
    n = 32;
  }
  if (v >= static_cast<uint64_t>(1) << 16) {
    v >>= 16;
    n += 16;
  }
  if (v >= static_cast<uint64_t>(1) << 8) {
    v >>= 16;
    n += 8;
  }
  return n + len8table[v];
}

inline uint8_t max_bytes64(uint64_t v) {
  uint8_t n = 0;
  if (v >= static_cast<uint64_t>(1) << 32) {
    v >>= 32;
    n = 4;
  }
  if (v >= static_cast<uint64_t>(1) << 16) {
    v >>= 16;
    n += 2;
  }
  if (v >= static_cast<uint64_t>(1) << 8) {
    v >>= 16;
    n += 1;
  }
  return n + 1;
}

uint8_t max_bits(const std::deque<int64_t> &d);
uint8_t max_bits(const std::vector<int64_t> &v);
uint8_t max_bytes(const std::deque<int64_t> &d);
uint8_t max_bytes(const std::vector<int64_t> &v);
int64_t get_origin(uint64_t data, int bits);

// Should be smaller than 63.
int64_t get_max(int bits);
int64_t get_min(int bits);
}  // namespace base
}  // namespace tsdb

#endif