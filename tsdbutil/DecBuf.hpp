#ifndef DECBUF_H
#define DECBUF_H

#include <cstring>
#include <string>

#include "base/Endian.hpp"

namespace tsdb {
namespace tsdbutil {

const uint8_t NO_ERR = 0;
const uint8_t ERR_INVALID_SIZE = 1;
const uint8_t ERR_INVALID_CHECKSUM = 2;

class DecBuf {
 public:
  const uint8_t *b;
  uint64_t size;
  uint64_t index;
  uint8_t err;

  DecBuf(const uint8_t *b, uint64_t size)
      : b(b), size(size), index(0), err(NO_ERR) {}

  uint8_t error() { return err; }

  std::string error_str() {
    switch (err) {
      case 1:
        return "invalid size";
      case 2:
        return "invalid checksum";
      default:
        return "no error";
    }
  }

  uint64_t len() { return size - index; }

  // Careful use
  const uint8_t *get() { return b; }

  DecBuf get_DecBuf() { return DecBuf(b, size); }

  uint8_t get_byte() {
    if (err != NO_ERR) return 0;
    if (size - index < 1) {
      err = ERR_INVALID_SIZE;
      return 0;
    }
    return b[index++];
  }

  int get_BE_uint16() {
    if (err != NO_ERR) return 0;
    if (size - index < 2) {
      err = ERR_INVALID_SIZE;
      return 0;
    }
    int r = base::get_uint16_big_endian(b + index);
    index += 2;
    return r;
  }

  uint32_t get_BE_uint32() {
    if (err != NO_ERR) return 0;
    if (size - index < 4) {
      err = ERR_INVALID_SIZE;
      return 0;
    }
    uint32_t r = base::get_uint32_big_endian(b + index);
    index += 4;
    return r;
  }

  uint64_t get_BE_uint64() {
    if (err != NO_ERR) return 0;
    if (size - index < 8) {
      err = ERR_INVALID_SIZE;
      return 0;
    }
    uint64_t r = base::get_uint64_big_endian(b + index);
    index += 8;
    return r;
  }

  uint64_t get_unsigned_variant() {
    if (err != NO_ERR) return 0;
    if (index >= size) {
      err = ERR_INVALID_SIZE;
      return 0;
    }
    int decoded = 0;
    uint64_t r = 0;
    try {
      r = base::decode_unsigned_varint(b + index, decoded, size - index);
    } catch (base::TSDBException e) {
      err = ERR_INVALID_SIZE;
      return 0;
    }
    index += decoded;
    return r;
  }

  int64_t get_signed_variant() {
    if (err != NO_ERR) return 0;
    if (index >= size) {
      err = ERR_INVALID_SIZE;
      return 0;
    }
    int decoded = 0;
    int64_t r = 0;
    try {
      r = base::decode_signed_varint(b + index, decoded, size - index);
    } catch (base::TSDBException e) {
      err = ERR_INVALID_SIZE;
      return 0;
    }
    index += decoded;
    return r;
  }

  std::string get_uvariant_string() {
    uint64_t len = get_unsigned_variant();
    if (err != NO_ERR) return "";
    if (size - index < len) {
      err = ERR_INVALID_SIZE;
      return "";
    }
    std::string r(reinterpret_cast<const char *>(b + index), len);
    index += len;

    return r;
  }
};

}  // namespace tsdbutil
}  // namespace tsdb

#endif