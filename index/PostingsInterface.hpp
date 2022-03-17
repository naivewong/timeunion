#ifndef POSTINGSINTERFACE_H
#define POSTINGSINTERFACE_H

#include <stdint.h>

#include <algorithm>
#include <deque>
#include <memory>
#include <vector>
// #include <iostream>

namespace tsdb {
namespace index {

template <typename T, typename V>
int binary_search(const T &d, V v) {
  int left = 0;
  int right = d.size();
  int middle = (left + right) / 2;
  while (left < right) {
    if (d[middle] == v)
      return middle;
    else if (d[middle] < v)
      left = middle;
    else
      right = middle;
    middle = (left + right) / 2;
  }
  if (d[middle] == v)
    return middle;
  else
    return d.size();
}

template <typename T>
bool binary_cut(std::deque<T> *d, T v) {
  auto it = std::lower_bound(d->begin(), d->end(), v);
  if (it == d->end()) {
    d->clear();
    return false;
  }
  d->erase(d->begin(), it);
  return true;
}

template <typename T>
void binary_insert(std::deque<T> *d, T v) {
  auto it = std::lower_bound(d->begin(), d->end(), v);
  if (it == d->end())
    d->emplace_back(v);
  else if (v < *it)
    d->emplace_front(v);
  else if (v != *it)
    d->insert(it + 1, v);
}

// Sorted postings
class PostingsInterface {
 public:
  virtual bool next() const = 0;
  virtual bool seek(uint64_t v) const = 0;
  virtual uint64_t at() const = 0;
  virtual void at(uint64_t *off, uint64_t *tsid) const {}
  virtual ~PostingsInterface() {}
};

inline std::deque<uint64_t> expand_postings(
    const std::shared_ptr<PostingsInterface> &p) {
  std::deque<uint64_t> d;
  while (p->next()) d.push_back(p->at());
  return d;
}

}  // namespace index
}  // namespace tsdb

#endif