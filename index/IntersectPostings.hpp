#ifndef INTERSECTPOSTINGS_H
#define INTERSECTPOSTINGS_H

#include <deque>

#include "index/PostingsInterface.hpp"

namespace tsdb {
namespace index {

// Note(Alec), deprecated.
class IntersectPostings : public PostingsInterface {
 private:
  std::deque<std::shared_ptr<PostingsInterface>> p_s;
  std::deque<std::unique_ptr<PostingsInterface>> p_u;
  bool mode;

 public:
  IntersectPostings(std::deque<std::shared_ptr<PostingsInterface>> &&p_s);
  IntersectPostings(std::deque<std::unique_ptr<PostingsInterface>> &&p_u);

  bool recursive_next_u(uint64_t max) const;

  bool recursive_next_s(uint64_t max) const;

  bool next() const;

  bool seek(uint64_t v) const;

  uint64_t at() const;
  // ~IntersectPostings(){std::cout << "intersect" << std::endl;}
};

// Pass r-value reference
std::shared_ptr<PostingsInterface> intersect_s(
    std::deque<std::shared_ptr<PostingsInterface>> &&list);

std::shared_ptr<PostingsInterface> intersect_s(
    std::deque<std::unique_ptr<PostingsInterface>> &&list);

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
    std::deque<std::unique_ptr<PostingsInterface>> &&list);

class IntersectPostings1 : public ::tsdb::index::PostingsInterface {
 private:
  std::vector<std::unique_ptr<PostingsInterface>> p_u;

 public:
  IntersectPostings1(std::vector<std::unique_ptr<PostingsInterface>> &&p_u)
      : p_u(std::move(p_u)) {}

  bool recursive_next(uint64_t max) const {
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

  bool next() const {
    uint64_t max = 0;
    for (auto &ptr : p_u) {
      bool has_next = ptr->next();
      if (!has_next) return false;
      if (ptr->at() > max) max = ptr->at();
    }
    return recursive_next(max);
  }

  bool seek(uint64_t v) const {
    uint64_t max = 0;

    for (auto &ptr : p_u) {
      bool has_next = ptr->seek(v);
      if (!has_next) return false;
      if (ptr->at() > max) max = ptr->at();
    }
    return recursive_next(max);
  }

  uint64_t at() const { return p_u[0]->at(); }
};

class IntersectPostings2 : public ::tsdb::index::PostingsInterface {
 private:
  std::vector<std::unique_ptr<PostingsInterface>> p_u;

 public:
  IntersectPostings2(std::vector<std::unique_ptr<PostingsInterface>> &&p_u)
      : p_u(std::move(p_u)) {}

  bool recursive_next(uint64_t max) const {
    uint64_t ref, id;
    while (true) {
      bool find = true;
      for (auto &ptr : p_u) {
        bool has_next = ptr->seek(max);
        if (!has_next) return false;
        ptr->at(&ref, &id);
        if (id > max) {
          max = id;
          find = false;
        }
      }
      if (find) return true;
    }
  }

  bool next() const {
    uint64_t max = 0, ref, id;
    for (auto &ptr : p_u) {
      bool has_next = ptr->next();
      if (!has_next) return false;
      ptr->at(&ref, &id);
      if (id > max) max = id;
    }
    return recursive_next(max);
  }

  bool seek(uint64_t v) const {
    uint64_t max = 0, ref, id;
    for (auto &ptr : p_u) {
      bool has_next = ptr->seek(v);
      if (!has_next) return false;
      ptr->at(&ref, &id);
      if (id > max) max = id;
    }
    return recursive_next(max);
  }

  uint64_t at() const {
    uint64_t ref, id;
    p_u[0]->at(&ref, &id);
    return id;
  }

  void at(uint64_t *ref, uint64_t *id) const { p_u[0]->at(ref, id); }
};

}  // namespace index
}  // namespace tsdb

#endif