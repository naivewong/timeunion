#ifndef UNIONPOSTINGS_H
#define UNIONPOSTINGS_H

#include <deque>

#include "index/ListPostings.hpp"
#include "index/PostingsInterface.hpp"

namespace tsdb {
namespace index {

class UnionPostings : public PostingsInterface {
 private:
  std::deque<uint64_t> d;
  ListPostings p;

 public:
  UnionPostings(const std::deque<std::shared_ptr<PostingsInterface>> &p);
  UnionPostings(const std::deque<std::unique_ptr<PostingsInterface>> &p);

  bool next() const;

  bool seek(uint64_t v) const;

  uint64_t at() const;
  // ~UnionPostings(){std::cout << "union" << std::endl;}
};

std::shared_ptr<PostingsInterface> merge_s(
    const std::deque<std::shared_ptr<PostingsInterface>> &list);

std::shared_ptr<PostingsInterface> merge_s(
    const std::deque<std::unique_ptr<PostingsInterface>> &list);

std::unique_ptr<PostingsInterface> merge_u(
    const std::deque<std::shared_ptr<PostingsInterface>> &list);

std::unique_ptr<PostingsInterface> merge_u(
    const std::deque<std::unique_ptr<PostingsInterface>> &list);

}  // namespace index
}  // namespace tsdb

#endif