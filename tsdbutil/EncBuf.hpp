#ifndef ENCBUF_H
#define ENCBUF_H

#include <stdint.h>

#include <cstring>
#include <string>
#include <vector>

#include "base/Endian.hpp"

namespace tsdb {
namespace tsdbutil {

class EncBuf {
 public:
  // TODO need to checkout if vector<POD>::clear() is constant time complexity
  std::vector<uint8_t> b;
  int index;

  EncBuf() : index(0) {}

  EncBuf(int size) {
    b = std::vector<uint8_t>();
    b.reserve(size);
    this->index = 0;
  }

  void reset() { index = 0; }

  std::pair<const uint8_t *, int> get() {
    return std::make_pair(&(b[0]), index);
  }

  std::vector<uint8_t> get_vector() { return b; }

  int len() { return index; }

  void put_string(const std::string &str) {
    if (str.length() + index > b.size()) b.resize(str.length() + index + 1);
    std::memcpy(&(b[index]), str.c_str(), str.length());
    index += str.length();
  }

  void put_uvariant_string(const std::string &str) {
    put_unsigned_variant(str.length());
    put_string(str);
  }

  void put_byte(char byte) {
    if (index == b.size())
      b.push_back(static_cast<uint8_t>(byte));
    else
      b[index] = static_cast<uint8_t>(byte);
    ++index;
  }

  void put_byte(uint8_t byte) {
    if (index == b.size())
      b.push_back(byte);
    else
      b[index] = byte;
    ++index;
  }

  void put_BE_uint16(int num) {
    int padding = index + 2 - b.size();
    for (int i = 0; i < padding; i++) b.push_back(0);
    base::put_uint16_big_endian(&(b[index]), num);
    index += 2;
  }

  void put_BE_uint32(uint32_t num) {
    int padding = index + 4 - b.size();
    for (int i = 0; i < padding; i++) b.push_back(0);
    base::put_uint32_big_endian(&(b[index]), num);
    index += 4;
  }

  void put_BE_uint64(uint64_t num) {
    int padding = index + 8 - b.size();
    for (int i = 0; i < padding; i++) b.push_back(0);
    base::put_uint64_big_endian(&(b[index]), num);
    index += 8;
  }

  void put_unsigned_variant(uint64_t num) {
    int padding = index + base::MAX_VARINT_LEN_64 - b.size();
    for (int i = 0; i < padding; i++) b.push_back(0);
    int encoded = base::encode_unsigned_varint(&(b[index]), num);
    index += encoded;
  }

  void put_signed_variant(int64_t num) {
    int padding = index + base::MAX_VARINT_LEN_64 - b.size();
    for (int i = 0; i < padding; i++) b.push_back(0);
    int encoded = base::encode_signed_varint(&(b[index]), num);
    index += encoded;
  }
};

}  // namespace tsdbutil
}  // namespace tsdb

#endif