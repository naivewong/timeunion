#ifndef LISTPOSTINGS_H
#define LISTPOSTINGS_H

#include "index/PostingsInterface.hpp"

namespace tsdb {
namespace index {

// NOTICE!!!
// Must be called based on existed deque/vector, cannot pass temporary
// deque/vector into it
class ListPostings : public PostingsInterface {
 private:
  // const std::deque<uint64_t> & list;
  std::deque<uint64_t>::const_iterator begin;
  // std::vector<uint64_t>::const_iterator begin;
  mutable int size;
  mutable int index;

 public:
  ListPostings();
  ListPostings(const std::deque<uint64_t> &list);
  ListPostings(const std::deque<uint64_t> *const list);
  // ListPostings(const std::vector<uint64_t> & list);
  // ListPostings(const std::vector<uint64_t> * const list);

  bool next() const;

  bool seek(uint64_t v) const;

  uint64_t at() const;
  // ~ListPostings(){std::cout << "List" << std::endl;}
};

}  // namespace index
}  // namespace tsdb

#endif