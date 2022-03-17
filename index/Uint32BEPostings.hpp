#ifndef UINT32BEPOSTINGS_H
#define UINT32BEPOSTINGS_H

#include "index/PostingsInterface.hpp"

namespace tsdb {
namespace index {

class Uint32BEPostings : public PostingsInterface {
 private:
  const uint8_t *p;
  uint32_t size;
  mutable uint32_t index;
  mutable uint64_t cur;

  bool clean;

  uint32_t lower_bound(uint32_t i, uint64_t v) const;

 public:
  // Need to check if size = 4x before using
  Uint32BEPostings(const uint8_t *p, uint32_t size, bool clean = false);
  ~Uint32BEPostings() {
    if (clean) delete p;
  }

  bool next() const;

  bool seek(uint64_t v) const;

  uint64_t at() const;
};

class Uint32BEDoublePostings : public PostingsInterface {
 private:
  const uint8_t *p;
  uint32_t size;
  mutable uint32_t index;
  mutable uint64_t cur1;
  mutable uint64_t cur2;

  bool clean;

  uint32_t lower_bound(uint32_t i, uint64_t v) const;

 public:
  // Need to check if size = 4x before using
  Uint32BEDoublePostings(const uint8_t *p, uint32_t size, bool clean = false);
  ~Uint32BEDoublePostings() {
    if (clean) delete p;
  }

  bool next() const;

  bool seek(uint64_t v) const;

  uint64_t at() const { return 0; }
  void at(uint64_t *off, uint64_t *tsid) const;
};

class Uint32BEDoubleSkipPostings : public PostingsInterface {
 private:
  const uint8_t *p;
  uint32_t size;
  mutable uint32_t index;
  mutable uint64_t cur;

  bool clean;

  uint32_t lower_bound(uint32_t i, uint64_t v) const;

 public:
  // Need to check if size = 4x before using
  Uint32BEDoubleSkipPostings(const uint8_t *p, uint32_t size,
                             bool clean = false);
  ~Uint32BEDoubleSkipPostings() {
    if (clean) delete p;
  }

  bool next() const;

  bool seek(uint64_t v) const;

  uint64_t at() const { return cur; }
  void at(uint64_t *off, uint64_t *tsid) const {}
};

}  // namespace index
}  // namespace tsdb

#endif