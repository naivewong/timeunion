#include "index/TOC.hpp"

#include "base/Checksum.hpp"
#include "base/Endian.hpp"
#include "tsdbutil/DecBuf.hpp"

namespace tsdb {
namespace index {

const int INDEX_TOC_LEN = 7 * 8 + 4;        // 7 fields + crc32
const int INDEX_GROUP_TOC_LEN = 8 * 8 + 4;  // 8 fields + crc32

// return false if error
std::pair<TOC, bool> toc_from_ByteSlice(const tsdbutil::ByteSlice *bs) {
  if (bs->len() < INDEX_TOC_LEN) return std::make_pair<TOC, bool>(TOC(), false);
  std::pair<const uint8_t *, int> b =
      bs->range(bs->len() - INDEX_TOC_LEN, bs->len());

  uint32_t crc1 = base::get_uint32_big_endian(b.first + b.second - 4);
  uint32_t crc2 = base::GetCrc32(b.first, b.second - 4);

  if (crc1 != crc2) return std::make_pair<TOC, bool>(TOC(), false);

  tsdbutil::DecBuf dec(b.first, b.second - 4);

  TOC r;
  r.symbols = dec.get_BE_uint64();
  r.series = dec.get_BE_uint64();
  r.label_indices = dec.get_BE_uint64();
  r.label_indices_table = dec.get_BE_uint64();
  r.postings = dec.get_BE_uint64();
  r.postings_table = dec.get_BE_uint64();
  r.sst_table = dec.get_BE_uint64();
  return std::make_pair(r, true);
}
std::pair<GroupTOC, bool> group_toc_from_ByteSlice(
    const tsdbutil::ByteSlice *bs) {
  if (bs->len() < INDEX_GROUP_TOC_LEN)
    return std::make_pair<GroupTOC, bool>(GroupTOC(), false);
  std::pair<const uint8_t *, int> b =
      bs->range(bs->len() - INDEX_GROUP_TOC_LEN, bs->len());

  uint32_t crc1 = base::get_uint32_big_endian(b.first + b.second - 4);
  uint32_t crc2 = base::GetCrc32(b.first, b.second - 4);

  if (crc1 != crc2) return std::make_pair<GroupTOC, bool>(GroupTOC(), false);

  tsdbutil::DecBuf dec(b.first, b.second - 4);

  GroupTOC r;
  r.symbols = dec.get_BE_uint64();
  r.series = dec.get_BE_uint64();
  r.label_indices = dec.get_BE_uint64();
  r.label_indices_table = dec.get_BE_uint64();
  r.postings = dec.get_BE_uint64();
  r.postings_table = dec.get_BE_uint64();
  r.group_postings = dec.get_BE_uint64();
  r.group_postings_table = dec.get_BE_uint64();
  return std::make_pair(r, true);
}

std::string toc_string(const TOC &toc) {
  std::string s =
      "symbols:" + std::to_string(toc.symbols) +
      " series:" + std::to_string(toc.series) +
      " label_indices:" + std::to_string(toc.label_indices) +
      " postings:" + std::to_string(toc.postings) +
      " label_indices_table:" + std::to_string(toc.label_indices_table) +
      " postings_table:" + std::to_string(toc.postings_table) +
      " sst_table:" + std::to_string(toc.sst_table);
  return s;
}
std::string group_toc_string(const GroupTOC &toc) {
  std::string s =
      "symbols:" + std::to_string(toc.symbols) +
      " series:" + std::to_string(toc.series) +
      " label_indices:" + std::to_string(toc.label_indices) +
      " postings:" + std::to_string(toc.postings) +
      " group postings:" + std::to_string(toc.group_postings) +
      " label_indices_table:" + std::to_string(toc.label_indices_table) +
      " postings_table:" + std::to_string(toc.postings_table) +
      " group_postings_table:" + std::to_string(toc.group_postings_table);
  return s;
}

std::string toc_portion_string(const TOC &toc, uint64_t pos) {
  char temp[100];
  std::sprintf(
      temp, "%.4f",
      static_cast<double>(toc.series - toc.symbols) / static_cast<double>(pos));
  std::string s = "symbols:" + std::string(temp);
  std::sprintf(temp, "%.4f",
               static_cast<double>(toc.label_indices - toc.series) /
                   static_cast<double>(pos));
  s += " series:" + std::string(temp);
  std::sprintf(temp, "%.4f",
               static_cast<double>(toc.postings - toc.label_indices) /
                   static_cast<double>(pos));
  s += " label_indices:" + std::string(temp);
  std::sprintf(temp, "%.4f",
               static_cast<double>(toc.label_indices_table - toc.postings) /
                   static_cast<double>(pos));
  s += " postings:" + std::string(temp);
  std::sprintf(
      temp, "%.4f",
      static_cast<double>(toc.postings_table - toc.label_indices_table) /
          static_cast<double>(pos));
  s += " label_indices_table:" + std::string(temp);
  std::sprintf(
      temp, "%.4f",
      static_cast<double>(pos - toc.postings_table) / static_cast<double>(pos));
  s += " postings_table:" + std::string(temp);
  return s;
}
std::string group_toc_portion_string(const GroupTOC &toc, uint64_t pos) {
  char temp[100];
  std::sprintf(
      temp, "%.4f",
      static_cast<double>(toc.series - toc.symbols) / static_cast<double>(pos));
  std::string s = "symbols:" + std::string(temp);
  std::sprintf(temp, "%.4f",
               static_cast<double>(toc.label_indices - toc.series) /
                   static_cast<double>(pos));
  s += " series:" + std::string(temp);
  std::sprintf(temp, "%.4f",
               static_cast<double>(toc.postings - toc.label_indices) /
                   static_cast<double>(pos));
  s += " label_indices:" + std::string(temp);
  std::sprintf(temp, "%.4f",
               static_cast<double>(toc.group_postings - toc.postings) /
                   static_cast<double>(pos));
  s += " postings:" + std::string(temp);
  std::sprintf(
      temp, "%.4f",
      static_cast<double>(toc.label_indices_table - toc.group_postings) /
          static_cast<double>(pos));
  s += " group postings:" + std::string(temp);
  std::sprintf(
      temp, "%.4f",
      static_cast<double>(toc.postings_table - toc.label_indices_table) /
          static_cast<double>(pos));
  s += " label_indices_table:" + std::string(temp);
  std::sprintf(
      temp, "%.4f",
      static_cast<double>(toc.group_postings_table - toc.postings_table) /
          static_cast<double>(pos));
  s += " postings_table:" + std::string(temp);
  std::sprintf(temp, "%.4f",
               static_cast<double>(pos - toc.group_postings_table) /
                   static_cast<double>(pos));
  s += " group postings_table:" + std::string(temp);
  return s;
}

}  // namespace index
}  // namespace tsdb