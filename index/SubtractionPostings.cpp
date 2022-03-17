#include "index/SubtractionPostings.hpp"
// #include <iostream>

namespace tsdb {
namespace index {

SubtractionPostings::SubtractionPostings(
    std::shared_ptr<PostingsInterface> &&full,
    std::shared_ptr<PostingsInterface> &&drop)
    : full(std::move(full)), drop(std::move(drop)), initialized(false) {}

bool SubtractionPostings::next() const {
  if (!initialized) {
    full_ok = full->next();
    drop_ok = drop->next();
    initialized = true;
  }
  while (true) {
    if (!full_ok) return false;
    if (!drop_ok) {
      cur = full->at();
      full_ok = full->next();
      return true;
    }
    uint64_t full_cur = full->at();
    uint64_t drop_cur = drop->at();
    // std::cout << "fcur:" << full_cur << " dcur:" << drop_cur << std::endl;
    if (full_cur < drop_cur) {
      cur = full_cur;
      full_ok = full->next();
      return true;
    } else if (full_cur > drop_cur)
      drop_ok = drop->seek(full_cur);
    else
      full_ok = full->next();
  }
}

bool SubtractionPostings::seek(uint64_t v) const {
  if (cur >= v) return true;
  full_ok = full->seek(v);
  drop_ok = drop->seek(v);
  initialized = true;
  return next();
}

uint64_t SubtractionPostings::at() const { return cur; }

std::shared_ptr<PostingsInterface> without_s(
    std::shared_ptr<PostingsInterface> &&p1,
    std::shared_ptr<PostingsInterface> &&p2) {
  return std::shared_ptr<PostingsInterface>(
      new SubtractionPostings(std::move(p1), std::move(p2)));
}

std::unique_ptr<PostingsInterface> without_u(
    std::shared_ptr<PostingsInterface> &&p1,
    std::shared_ptr<PostingsInterface> &&p2) {
  return std::unique_ptr<PostingsInterface>(
      new SubtractionPostings(std::move(p1), std::move(p2)));
}

}  // namespace index
}  // namespace tsdb