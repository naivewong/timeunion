#ifndef SUBTRACTIONPOSTINGS_H
#define SUBTRACTIONPOSTINGS_H

#include "index/PostingsInterface.hpp"

namespace tsdb {
namespace index {

class SubtractionPostings : public PostingsInterface {
 private:
  const std::shared_ptr<PostingsInterface> full;
  const std::shared_ptr<PostingsInterface> drop;
  mutable bool full_ok;
  mutable bool drop_ok;
  mutable uint64_t cur;
  mutable bool initialized;

 public:
  SubtractionPostings(std::shared_ptr<PostingsInterface> &&full,
                      std::shared_ptr<PostingsInterface> &&drop);

  bool next() const;

  bool seek(uint64_t v) const;

  uint64_t at() const;
};

std::shared_ptr<PostingsInterface> without_s(
    std::shared_ptr<PostingsInterface> &&p1,
    std::shared_ptr<PostingsInterface> &&p2);

std::unique_ptr<PostingsInterface> without_u(
    std::shared_ptr<PostingsInterface> &&p1,
    std::shared_ptr<PostingsInterface> &&p2);

}  // namespace index
}  // namespace tsdb

#endif