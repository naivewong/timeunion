#include "index/UnionPostings.hpp"

#include <set>
// #include <iostream>

namespace tsdb {
namespace index {

UnionPostings::UnionPostings(
    const std::deque<std::shared_ptr<PostingsInterface>> &p) {
  std::set<uint64_t> s;
  for (auto &ptr : p) {
    while (ptr->next()) s.insert(ptr->at());
  }
  for (auto &i : s) d.push_back(i);
  this->p = ListPostings(d);
}

UnionPostings::UnionPostings(
    const std::deque<std::unique_ptr<PostingsInterface>> &p) {
  std::set<uint64_t> s;
  for (auto &ptr : p) {
    while (ptr->next()) s.insert(ptr->at());
  }
  for (auto &i : s) d.push_back(i);
  this->p = ListPostings(d);
}

bool UnionPostings::next() const { return p.next(); }

bool UnionPostings::seek(uint64_t v) const { return p.seek(v); }

uint64_t UnionPostings::at() const { return p.at(); }

std::shared_ptr<PostingsInterface> merge_s(
    const std::deque<std::shared_ptr<PostingsInterface>> &list) {
  return std::shared_ptr<PostingsInterface>(new UnionPostings(list));
}

std::shared_ptr<PostingsInterface> merge_s(
    const std::deque<std::unique_ptr<PostingsInterface>> &list) {
  return std::shared_ptr<PostingsInterface>(new UnionPostings(list));
}

std::unique_ptr<PostingsInterface> merge_u(
    const std::deque<std::shared_ptr<PostingsInterface>> &list) {
  return std::unique_ptr<PostingsInterface>(new UnionPostings(list));
}

std::unique_ptr<PostingsInterface> merge_u(
    const std::deque<std::unique_ptr<PostingsInterface>> &list) {
  return std::unique_ptr<PostingsInterface>(new UnionPostings(list));
}

}  // namespace index
}  // namespace tsdb