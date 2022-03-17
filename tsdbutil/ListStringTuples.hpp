#ifndef LISTSTRINGTUPLES_H
#define LISTSTRINGTUPLES_H

#include <deque>
#include <vector>

#include "tsdbutil/StringTuplesInterface.hpp"

namespace tsdb {
namespace tsdbutil {

class ListStringTuples : public StringTuplesInterface {
 private:
  std::vector<std::string> tpls;

 public:
  ListStringTuples() = default;
  ListStringTuples(int size) { tpls.reserve(size); }
  ListStringTuples(const std::vector<std::string> &tpls) : tpls(tpls) {}
  ListStringTuples(const std::deque<std::string> &tpls)
      : tpls(tpls.begin(), tpls.end()) {}

  void push_back(const std::string &s) { tpls.push_back(s); }

  void sort() { std::sort(tpls.begin(), tpls.end()); }

  uint32_t len() { return tpls.size(); }

  std::string at(int i) {
    if (i >= tpls.size()) return "";
    return tpls[i];
  }
};

}  // namespace tsdbutil
}  // namespace tsdb

#endif