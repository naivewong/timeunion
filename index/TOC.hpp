#ifndef INDEX_TOC_H
#define INDEX_TOC_H

#include <stdint.h>

#include <string>

#include "tsdbutil/ByteSlice.hpp"

namespace tsdb {
namespace index {

extern const int INDEX_TOC_LEN;        // 7 fields + crc32
extern const int INDEX_GROUP_TOC_LEN;  // 8 fields + crc32

class TOC {
 public:
  TOC()
      : symbols(0),
        series(0),
        label_indices(0),
        label_indices_table(0),
        postings(0),
        postings_table(0),
        sst_table(0) {}
  uint64_t symbols;
  uint64_t series;
  uint64_t label_indices;
  uint64_t label_indices_table;
  uint64_t postings;
  uint64_t postings_table;
  uint64_t sst_table;
};

class GroupTOC {
 public:
  uint64_t symbols;
  uint64_t series;
  uint64_t label_indices;
  uint64_t label_indices_table;
  uint64_t postings;
  uint64_t postings_table;
  uint64_t group_postings;
  uint64_t group_postings_table;
};

// return false if error
std::pair<TOC, bool> toc_from_ByteSlice(const tsdbutil::ByteSlice *bs);
std::pair<GroupTOC, bool> group_toc_from_ByteSlice(
    const tsdbutil::ByteSlice *bs);

std::string toc_string(const TOC &toc);
std::string group_toc_string(const GroupTOC &toc);

std::string toc_portion_string(const TOC &toc, uint64_t pos);
std::string group_toc_portion_string(const GroupTOC &toc, uint64_t pos);

}  // namespace index
}  // namespace tsdb

#endif