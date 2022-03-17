#ifndef CHECKSUM_H
#define CHECKSUM_H
#include <stdint.h>

#include <boost/crc.hpp>
#include <string>

#include "base/Endian.hpp"

namespace tsdb {
namespace base {

class CRC32 {
 private:
  boost::crc_32_type result;

 public:
  CRC32() {}

  void process_bytes(const std::string &my_string) {
    result.process_bytes(my_string.data(), my_string.length());
  }

  void process_bytes(const std::vector<uint8_t> &my_string) {
    result.process_bytes(reinterpret_cast<const void *>(&my_string[0]),
                         my_string.size());
  }

  void process_bytes(const std::deque<uint8_t> &my_string) {
    for (uint8_t i : my_string) result.process_byte(i);
  }

  void process_bytes(const uint8_t *my_string, int size) {
    result.process_bytes(reinterpret_cast<const void *>(my_string), size);
  }

  void process_bytes(std::pair<const uint8_t *, int> p) {
    result.process_bytes(reinterpret_cast<const void *>(p.first), p.second);
  }

  void reset() { result.reset(); }

  uint32_t checksum() { return result.checksum(); }
};

uint32_t GetCrc32(const std::string &my_string);

uint32_t GetCrc32(const std::deque<uint8_t> &my_string);

uint32_t GetCrc32(const std::vector<uint8_t> &my_string);

uint32_t GetCrc32(const uint8_t *my_string, int size);
uint32_t GetCrc32(const char *my_string, int size);

uint32_t GetCrc32(std::pair<const uint8_t *, int> p);

}  // namespace base
}  // namespace tsdb

#endif