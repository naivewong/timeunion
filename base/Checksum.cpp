#include "base/Checksum.hpp"

namespace tsdb {
namespace base {

uint32_t GetCrc32(const std::string &my_string) {
  boost::crc_32_type result;
  result.process_bytes(my_string.data(), my_string.length());
  return result.checksum();
}

uint32_t GetCrc32(const std::deque<uint8_t> &my_string) {
  boost::crc_32_type result;
  for (uint8_t i : my_string) result.process_byte(i);
  return result.checksum();
}

uint32_t GetCrc32(const std::vector<uint8_t> &my_string) {
  boost::crc_32_type result;
  // void  process_bytes( void const *buffer, std::size_t byte_count );
  result.process_bytes(reinterpret_cast<const void *>(&my_string[0]),
                       my_string.size());
  return result.checksum();
}

uint32_t GetCrc32(const char *my_string, int size) {
  boost::crc_32_type result;
  // void  process_bytes( void const *buffer, std::size_t byte_count );
  result.process_bytes(reinterpret_cast<const void *>(my_string), size);
  return result.checksum();
}

uint32_t GetCrc32(const uint8_t *my_string, int size) {
  boost::crc_32_type result;
  // void  process_bytes( void const *buffer, std::size_t byte_count );
  result.process_bytes(reinterpret_cast<const void *>(my_string), size);
  return result.checksum();
}

uint32_t GetCrc32(std::pair<const uint8_t *, int> p) {
  boost::crc_32_type result;
  // void  process_bytes( void const *buffer, std::size_t byte_count );
  result.process_bytes(reinterpret_cast<const void *>(p.first), p.second);
  return result.checksum();
}

}  // namespace base
}  // namespace tsdb