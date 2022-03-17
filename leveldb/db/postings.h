#pragma once

#include "index/PostingsInterface.hpp"

namespace leveldb {

class BaseDeltaPostings : public ::tsdb::index::PostingsInterface {
 public:
  bool next() const override {}

  bool seek(uint64_t v) const override {}

  uint64_t at() const override {}

 private:
};

}  // namespace leveldb