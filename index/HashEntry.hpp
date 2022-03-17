#ifndef HASHENTRY_H
#define HASHENTRY_H

#include <stdint.h>

#include <deque>

namespace tsdb {
namespace index {

class HashEntry {
 public:
  std::deque<std::string> keys;
  uint64_t offset;

  HashEntry(const std::deque<std::string> &keys, uint64_t offset)
      : keys(keys), offset(offset) {}
  HashEntry(const std::string &key, uint64_t offset) : offset(offset) {
    keys.push_back(key);
  }
};

}  // namespace index
}  // namespace tsdb

#endif