#include "index/ListPostings.hpp"
// #include <iostream>

namespace tsdb {
namespace index {

// NOTICE!!!
// Must be called based on existed deque/vector, cannot pass temporary
// deque/vector into it
ListPostings::ListPostings() : size(0), index(-1) {}
ListPostings::ListPostings(const std::deque<uint64_t> &list)
    : begin(list.cbegin()), size(list.size()), index(-1) {}
ListPostings::ListPostings(const std::deque<uint64_t> *const list)
    : begin(list->cbegin()), size(list->size()), index(-1) {}
// ListPostings::ListPostings(const std::vector<uint64_t> & list):
// begin(list.cbegin()), size(list.size()), index(-1){}
// ListPostings::ListPostings(const std::vector<uint64_t> * const list):
// begin(list->cbegin()), size(list->size()), index(-1){}

bool ListPostings::next() const {
  ++index;
  if (index >= size) return false;
  return true;
}

bool ListPostings::seek(uint64_t v) const {
  if (size == 0 || index >= size) return false;
  if (index < 0) index = 0;
  if (*(begin + index) >= v) return true;

  auto it = std::lower_bound(begin + index, begin + size, v);
  if (it == begin + size) {
    index = size;
    return false;
  }
  index = it - begin;
  return true;
}

uint64_t ListPostings::at() const { return *(begin + index); }

}  // namespace index
}  // namespace tsdb