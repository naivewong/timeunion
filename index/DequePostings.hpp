#ifndef DEQUE_POSTINGS_H
#define DEQUE_POSTINGS_H

#include <deque>
#include <iostream>

#include "index/PostingsInterface.hpp"

namespace tsdb {
namespace index {

class DequePostings : public PostingsInterface {
 private:
  std::deque<uint64_t> list;
  mutable int i;

 public:
  DequePostings() : i(-1) {}
  DequePostings(std::deque<uint64_t> &&list) : list(std::move(list)), i(-1) {}

  bool next() const {
    // NOTICE(Alec), static_cast to int first.
    if (i >= static_cast<int>(list.size()) - 1) return false;
    ++i;
    return true;
  }

  bool seek(uint64_t v) const {
    if (list.empty() || i >= static_cast<int>(list.size())) return false;
    if (i < 0) i = 0;
    if (list[i] >= v) return true;
    auto it = std::lower_bound(list.begin() + i, list.end(), v);
    if (it == list.end()) {
      i = list.size();
      return false;
    }
    i = it - list.begin();
    return true;
  }

  // next() or seek() must be called first.
  uint64_t at() const { return list[i]; }

  void push_back(uint64_t t) { list.push_back(t); }

  int size() const { return list.size(); }

  void sort() { std::sort(list.begin(), list.end()); }
};

}  // namespace index
}  // namespace tsdb

#endif