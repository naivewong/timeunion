#include "tsdbutil/tsdbutils.hpp"

#include <boost/filesystem.hpp>

namespace tsdb {
namespace tsdbutil {

std::string filepath_join(const std::string &f1, const std::string &f2) {
  return (boost::filesystem::path(f1) / boost::filesystem::path(f2)).string();
}

bool is_number(const std::string &s) {
  std::string::const_iterator it = s.begin();
  while (it != s.end() && std::isdigit(*it)) ++it;
  return !s.empty() && it == s.end();
}

std::pair<int64_t, int64_t> clamp_interval(int64_t a, int64_t b, int64_t mint,
                                           int64_t maxt) {
  if (a < mint) a = mint;
  if (b > maxt) b = maxt;
  return {a, b};
}

// WAL specific record.
const RECORD_ENTRY_TYPE RECORD_INVALID = 255;
const RECORD_ENTRY_TYPE RECORD_SERIES = 1;
const RECORD_ENTRY_TYPE RECORD_SAMPLES = 2;
const RECORD_ENTRY_TYPE RECORD_TOMBSTONES = 3;
const RECORD_ENTRY_TYPE RECORD_GROUP_SERIES = 4;
const RECORD_ENTRY_TYPE RECORD_GROUP_SAMPLES = 5;
const RECORD_ENTRY_TYPE RECORD_GROUP_TOMBSTONES = 6;

}  // namespace tsdbutil
}  // namespace tsdb