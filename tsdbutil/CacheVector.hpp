#ifndef CACHEVECTOR_H
#define CACHEVECTOR_H

#include <vector>

namespace tsdb {
namespace tsdbutil {

template <typename T>
class CacheVector {
 public:
  std::vector<T> cache;
  int index;

  CacheVector<T>() {
    cache = std::vector<T>();
    index = 0;
  }

  CacheVector<T>(int size) {
    cache = std::vector<T>();
    cache.reserve(size);
    index = 0;
  }

  void push_back(const T &item) {
    if (index >= cache.size())
      cache.push_back(item);
    else
      cache[index] = item;
    ++index;
  }

  void clear() { index = 0; }

  void sort() { std::sort(cache.begin(), cache.begin() + index); }

  int size() { return index; }

  typename std::vector<T>::iterator begin() { return cache.begin(); }

  typename std::vector<T>::iterator end() { return cache.begin() + index; }
};

}  // namespace tsdbutil
}  // namespace tsdb

#endif