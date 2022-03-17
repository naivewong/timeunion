#include "index/IntersectPostings.hpp"

#include "index/EmptyPostings.hpp"

namespace tsdb {
namespace index {

IntersectPostings::IntersectPostings(
    std::deque<std::shared_ptr<PostingsInterface>> &&p_s)
    : mode(false), p_s(std::move(p_s)) {}

IntersectPostings::IntersectPostings(
    std::deque<std::unique_ptr<PostingsInterface>> &&p_u)
    : mode(true), p_u(std::move(p_u)) {}

bool IntersectPostings::recursive_next_u(uint64_t max) const {
  while (true) {
    bool find = true;
    for (auto &ptr : p_u) {
      bool has_next = ptr->seek(max);
      if (!has_next) return false;
      if (ptr->at() > max) {
        max = ptr->at();
        find = false;
      }
    }
    if (find) return true;
  }
}

bool IntersectPostings::recursive_next_s(uint64_t max) const {
  while (true) {
    bool find = true;
    for (auto &ptr : p_s) {
      bool has_next = ptr->seek(max);
      if (!has_next) return false;
      if (ptr->at() > max) {
        max = ptr->at();
        find = false;
      }
    }
    if (find) return true;
  }
}

bool IntersectPostings::next() const {
  uint64_t max = 0;
  if (mode) {
    for (auto &ptr : p_u) {
      bool has_next = ptr->next();
      if (!has_next) return false;
      if (ptr->at() > max) max = ptr->at();
    }
    return recursive_next_u(max);
  } else {
    for (auto &ptr : p_s) {
      bool has_next = ptr->next();
      if (!has_next) return false;
      if (ptr->at() > max) max = ptr->at();
    }
    return recursive_next_s(max);
  }
}

bool IntersectPostings::seek(uint64_t v) const {
  uint64_t max = 0;
  if (mode) {
    for (auto &ptr : p_u) {
      bool has_next = ptr->seek(v);
      if (!has_next) return false;
      if (ptr->at() > max) max = ptr->at();
    }
    return recursive_next_u(max);
  } else {
    for (auto &ptr : p_s) {
      bool has_next = ptr->seek(v);
      if (!has_next) return false;
      if (ptr->at() > max) max = ptr->at();
    }
    return recursive_next_s(max);
  }
}

uint64_t IntersectPostings::at() const {
  return (mode ? p_u[0]->at() : p_s[0]->at());
}

// Pass r-value reference
std::shared_ptr<PostingsInterface> intersect_s(
    std::deque<std::shared_ptr<PostingsInterface>> &&list) {
  if (list.size() == 0)
    return std::shared_ptr<PostingsInterface>(new EmptyPostings());
  else if (list.size() == 1)
    return std::shared_ptr<PostingsInterface>(std::move(list.front()));
  return std::shared_ptr<PostingsInterface>(
      new IntersectPostings(std::move(list)));
}

std::shared_ptr<PostingsInterface> intersect_s(
    std::deque<std::unique_ptr<PostingsInterface>> &&list) {
  if (list.size() == 0)
    return std::shared_ptr<PostingsInterface>(new EmptyPostings());
  else if (list.size() == 1)
    return std::shared_ptr<PostingsInterface>(std::move(list.front()));
  return std::shared_ptr<PostingsInterface>(
      new IntersectPostings(std::move(list)));
}

// std::unique_ptr<PostingsInterface>
// intersect_u(std::deque<std::shared_ptr<PostingsInterface> > && list){
//     if(list.size() == 0)
//         return
//         std::unique_ptr<PostingsInterface>(dynamic_cast<PostingsInterface*>(new
//         EmptyPostings()));
//     else if(list.size() == 1)
//         return
//         std::unique_ptr<PostingsInterface>(std::move(*(list.begin())));
//     return
//     std::unique_ptr<PostingsInterface>(dynamic_cast<PostingsInterface*>(new
//     IntersectPostings(std::move(list))));
// }

std::unique_ptr<PostingsInterface> intersect_u(
    std::deque<std::unique_ptr<PostingsInterface>> &&list) {
  if (list.size() == 0)
    return std::unique_ptr<PostingsInterface>(new EmptyPostings());
  else if (list.size() == 1)
    return std::unique_ptr<PostingsInterface>(std::move(*(list.begin())));
  return std::unique_ptr<PostingsInterface>(
      new IntersectPostings(std::move(list)));
}

}  // namespace index
}  // namespace tsdb