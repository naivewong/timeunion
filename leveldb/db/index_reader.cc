#include <cstring>
// #include <iostream>
#include <boost/bind.hpp>
// #include <boost/function.hpp>
#include "db/index_reader.h"
#include <boost/tokenizer.hpp>

#include "leveldb/env.h"

#include "base/Endian.hpp"
#include "base/Logging.hpp"
#include "index/EmptyPostings.hpp"
#include "index/IndexReader.hpp"
#include "index/ListPostings.hpp"
#include "index/Uint32BEPostings.hpp"
#include "index/VectorPostings.hpp"
#include "label/Label.hpp"
#include "tsdbutil/DecBuf.hpp"
#include "tsdbutil/MMapSlice.hpp"

namespace leveldb {

std::pair<::tsdb::index::TOC, bool> toc_from_random_access_file(
    RandomAccessFile* b) {
  char* tmp_b = new char[::tsdb::index::INDEX_TOC_LEN];
  Slice tmp_s;
  Status s = b->Read(b->FileSize() - ::tsdb::index::INDEX_TOC_LEN,
                     ::tsdb::index::INDEX_TOC_LEN, &tmp_s, tmp_b);
  if (!s.ok()) {
    delete tmp_b;
    return std::make_pair<::tsdb::index::TOC, bool>(::tsdb::index::TOC(),
                                                    false);
  }

  uint32_t crc1 = ::tsdb::base::get_uint32_big_endian(
      tmp_s.data() + ::tsdb::index::INDEX_TOC_LEN - 4);
  uint32_t crc2 =
      ::tsdb::base::GetCrc32(tmp_s.data(), ::tsdb::index::INDEX_TOC_LEN - 4);

  if (crc1 != crc2) {
    delete tmp_b;
    return std::make_pair<::tsdb::index::TOC, bool>(::tsdb::index::TOC(),
                                                    false);
  }

  ::tsdb::tsdbutil::DecBuf dec(reinterpret_cast<const uint8_t*>(tmp_s.data()),
                               ::tsdb::index::INDEX_TOC_LEN - 4);

  ::tsdb::index::TOC r;
  r.symbols = dec.get_BE_uint64();
  r.series = dec.get_BE_uint64();
  r.label_indices = dec.get_BE_uint64();
  r.label_indices_table = dec.get_BE_uint64();
  r.postings = dec.get_BE_uint64();
  r.postings_table = dec.get_BE_uint64();
  r.sst_table = dec.get_BE_uint64();
  delete tmp_b;
  return std::make_pair(r, true);
}

IndexReader::IndexReader(const std::string& name) : err_(false) {
  Env* env = Env::Default();
  Status s = env->NewRandomAccessFile(name, &b);
  if (!s.ok()) {
    delete b;
    b = nullptr;
    err_ = true;
    return;
  }
  if (!validate()) {
    b = nullptr;
    LOG_ERROR << "Fail to create IndexReader, invalid ByteSlice";
    err_ = true;
    return;
  }
  init();
  get_all_postings();
}

IndexReader::IndexReader(RandomAccessFile* f) : b(f), err_(false) {
  if (!validate()) {
    b = nullptr;
    LOG_ERROR << "Fail to create IndexReader, invalid ByteSlice";
    err_ = true;
    return;
  }
  init();
  get_all_postings();
}

void IndexReader::init() {
  std::pair<::tsdb::index::TOC, bool> toc_pair = toc_from_random_access_file(b);
  if (!toc_pair.second) {
    LOG_ERROR << "Fail to create IndexReader, error reading TOC";
    err_ = true;
    return;
  }

  // Read symbols
  if (!read_symbols(toc_pair.first)) {
    LOG_ERROR << "Fail to create IndexReader, error reading symbols";
    err_ = true;
    return;
  }

  // Read SST ranges
  if (toc_pair.first.sst_table != 0 &&
      !read_sst_range(toc_pair.first.sst_table)) {
    LOG_ERROR << "Fail to create IndexReader, error reading sst ranges";
    err_ = true;
    return;
  }

  // Read label indices table
  if (!read_offset_table(toc_pair.first.label_indices_table, true)) {
    LOG_ERROR
        << "Fail to create IndexReader, error reading label indices table";
    err_ = true;
    return;
  }

  // Read postings table
  if (!read_offset_table(toc_pair.first.postings_table, false)) {
    LOG_ERROR << "Fail to create IndexReader, error reading postings table";
    err_ = true;
    return;
  }
}

// OPTIONAL(Alec).
// NOTE(Alec), all_postings is already a sorted postings
// because the offset determines the order.
void IndexReader::get_all_postings() {
  std::set<uint64_t> all;
  for (auto const& p_table : postings_table) {
    for (auto const& p_pair : p_table.second) {
      std::pair<std::unique_ptr<::tsdb::index::PostingsInterface>, bool> p =
          postings(p_table.first, p_pair.first);
      while (p.first->next()) all.insert(p.first->at());
    }
  }
  all_postings.insert(all_postings.end(), all.begin(), all.end());
}

bool IndexReader::validate() {
  if (b->FileSize() < 4) {
    LOG_ERROR << "index size < 4";
    return false;
  }
  char tmp_b[4];
  Slice tmp_s;
  Status s = b->Read(0, 4, &tmp_s, tmp_b);
  if (!s.ok()) {
    LOG_ERROR << s.ToString();
    return false;
  }
  if (::tsdb::base::get_uint32_big_endian(tmp_s.data()) !=
      ::tsdb::index::MAGIC_INDEX) {
    LOG_ERROR << "Not beginning with MAGIC_INDEX got:"
              << ::tsdb::base::get_uint32_big_endian(tmp_s.data());
    return false;
  }
  s = b->Read(4, 1, &tmp_s, tmp_b);
  if (!s.ok()) {
    LOG_ERROR << s.ToString();
    return false;
  }
  if (tmp_s.data()[0] != ::tsdb::index::INDEX_VERSION_V1) {
    LOG_ERROR << "Invalid Index Version";
    return false;
  }
  return true;
}

// ┌────────────────────┬─────────────────────┐
// │ len <4b>           │ #symbols <4b>       │
// ├────────────────────┴─────────────────────┤
// │ ┌──────────────────────┬───────────────┐ │
// │ │ len(str_1) <uvarint> │ str_1 <bytes> │ │
// │ ├──────────────────────┴───────────────┤ │
// │ │                . . .                 │ │
// │ ├──────────────────────┬───────────────┤ │
// │ │ len(str_n) <uvarint> │ str_n <bytes> │ │
// │ └──────────────────────┴───────────────┘ │
// ├──────────────────────────────────────────┤
// │ CRC32 <4b>                               │
// └──────────────────────────────────────────┘
bool IndexReader::read_symbols(const ::tsdb::index::TOC& toc) {
  char tmp_b[4];
  Slice tmp_s;
  Status s = b->Read(toc.symbols, 4, &tmp_s, tmp_b);
  if (!s.ok()) return false;
  uint32_t len = ::tsdb::base::get_uint32_big_endian(tmp_s.data());
  char* tmp_b2 = new char[len];
  s = b->Read(toc.symbols + 4, len, &tmp_s, tmp_b2);
  if (!s.ok()) {
    delete tmp_b2;
    return false;
  }
  ::tsdb::tsdbutil::DecBuf dec_buf(
      reinterpret_cast<const uint8_t*>(tmp_s.data()), len);

  // #symbols <4b>
  uint32_t num_symbols = dec_buf.get_BE_uint32();
  for (int i = 0; i < num_symbols; i++)
    symbols_.push_back(dec_buf.get_uvariant_string());
  delete tmp_b2;
  if (dec_buf.err != ::tsdb::tsdbutil::NO_ERR)
    return false;
  else
    return true;
}

// ┌─────────────────────┬────────────────────┐
// │ len <4b>            │ #entries <4b>      │
// ├─────────────────────┴────────────────────┤
// │ ┌──────────────────────────────────────┐ │
// │ │  n = #strs <uvarint>                 │ │
// │ ├──────────────────────┬───────────────┤ │
// │ │ len(str_1) <uvarint> │ str_1 <bytes> │ │
// │ ├──────────────────────┴───────────────┤ │
// │ │  ...                                 │ │
// │ ├──────────────────────┬───────────────┤ │
// │ │ len(str_n) <uvarint> │ str_n <bytes> │ │
// │ ├──────────────────────┴───────────────┤ │
// │ │  offset <uvarint>                    │ │
// │ └──────────────────────────────────────┘ │
// │                  . . .                   │
// ├──────────────────────────────────────────┤
// │  CRC32 <4b>                              │
// └──────────────────────────────────────────┘
bool IndexReader::read_offset_table(uint64_t offset, bool label_index) {
  char tmp_b[8];
  Slice tmp_s;
  Status s = b->Read(offset, 8, &tmp_s, tmp_b);
  if (!s.ok()) return false;
  uint32_t len = ::tsdb::base::get_uint32_big_endian(tmp_s.data());
  uint32_t num_entries = ::tsdb::base::get_uint32_big_endian(tmp_s.data() + 4);
  char* tmp_b2 = new char[len - 4];
  s = b->Read(offset + 8, len - 4, &tmp_s, tmp_b2);
  if (!s.ok()) {
    delete tmp_b2;
    return false;
  }
  ::tsdb::tsdbutil::DecBuf dec_buf(
      reinterpret_cast<const uint8_t*>(tmp_s.data()), len - 4);

  if (label_index) {
    // Read label name to offset of label value
    for (int i = 0; i < num_entries; i++) {
      uint64_t temp_num =
          dec_buf.get_unsigned_variant();  // Normally temp_num = 1
      std::string label_name = dec_buf.get_uvariant_string();
      for (int j = 1; j < temp_num; j++) {
        label_name +=
            ::tsdb::index::LABEL_NAME_SEPARATOR + dec_buf.get_uvariant_string();
      }
      label_indices_table[label_name] = dec_buf.get_unsigned_variant();
    }
  } else {
    // Read label name, label value to offset of postings
    for (int i = 0; i < num_entries; i++) {
      uint64_t temp_num = dec_buf.get_unsigned_variant();
      // Should be 2
      if (temp_num != 2) return false;
      std::string label_name = dec_buf.get_uvariant_string();
      std::string label_value = dec_buf.get_uvariant_string();

      postings_table[label_name][label_value] = dec_buf.get_unsigned_variant();
    }
  }
  delete tmp_b2;
  if (dec_buf.err != ::tsdb::tsdbutil::NO_ERR)
    return false;
  else
    return true;
}

bool IndexReader::read_sst_range(uint64_t offset) {
  char tmp_b[8];
  Slice tmp_s;
  Status s = b->Read(offset, 8, &tmp_s, tmp_b);
  if (!s.ok()) return false;
  uint32_t len = ::tsdb::base::get_uint32_big_endian(tmp_s.data());
  uint32_t num_entries = ::tsdb::base::get_uint32_big_endian(tmp_s.data() + 4);

  char* tmp_b2 = new char[len - 4];
  s = b->Read(offset + 8, len - 4, &tmp_s, tmp_b2);
  if (!s.ok()) {
    delete tmp_b2;
    return false;
  }
  ::tsdb::tsdbutil::DecBuf dec_buf(
      reinterpret_cast<const uint8_t*>(tmp_s.data()), len - 4);
  ssts_.reserve(num_entries);
  sst_tsids_.reserve(num_entries);
  for (uint32_t i = 0; i < num_entries; i++) {
    if (dec_buf.err != ::tsdb::tsdbutil::NO_ERR) return false;
    ssts_.push_back(dec_buf.get_BE_uint32());
    sst_tsids_.push_back(dec_buf.get_BE_uint64());
  }
  if (dec_buf.err != ::tsdb::tsdbutil::NO_ERR) return false;
  return true;
}

std::set<std::string> IndexReader::symbols() {
  return std::set<std::string>(symbols_.begin(), symbols_.end());
}

const std::deque<std::string>& IndexReader::symbols_deque() const {
  return symbols_;
}

std::string IndexReader::lookup_symbols(uint64_t offset) {
  return symbols_[offset];
}

std::deque<std::string> IndexReader::label_names() {
  auto cmp = [](std::string a, std::string b) -> bool {
    return a.compare(b) < 0;
  };
  std::set<std::string, decltype(cmp)> r(cmp);
  // r.reserve(label_indices_table.size());
  boost::char_separator<char> tokenSep(
      ::tsdb::index::LABEL_NAME_SEPARATOR.c_str());
  for (auto const& p : label_indices_table) {
    if (p.first.length() == 0) continue;
    boost::tokenizer<boost::char_separator<char>> label_names_tokens(p.first,
                                                                     tokenSep);
    for (auto const& token : label_names_tokens) {
      r.insert(token);
      // LOG_INFO << token.data();
    }
  }
  // LOG_INFO << "set size: " << r.size();
  std::deque<std::string> v(r.begin(), r.end());
  // LOG_INFO << "vector size: " << v.size();
  // std::sort(v.begin(), v.end(), [](boost::string_ref a, boost::string_ref b)
  // {
  //     return strcmp(a.data(), b.data()) <= 0;
  // });
  // std::sort(v.begin(), v.end());
  return v;
}

// ┌───────────────┬────────────────┬────────────────┐
// │ len <4b>      │ #names <4b>    │ #entries <4b>  │
// ├───────────────┴────────────────┴────────────────┤
// │ ┌─────────────────────────────────────────────┐ │
// │ │ ref(value_0) <4b>                           │ │
// │ ├─────────────────────────────────────────────┤ │
// │ │ ...                                         │ │
// │ ├─────────────────────────────────────────────┤ │
// │ │ ref(value_n) <4b>                           │ │
// │ └─────────────────────────────────────────────┘ │
// │                      . . .                      │
// ├─────────────────────────────────────────────────┤
// │ CRC32 <4b>                                      │
// └─────────────────────────────────────────────────┘
// Align to 4 bytes
std::unique_ptr<::tsdb::tsdbutil::StringTuplesInterface>
IndexReader::label_values(const std::initializer_list<std::string>& names) {
  if (!b) return nullptr;

  auto it = names.begin();
  std::string label_name = std::string((*it).data(), (*it).size());
  ++it;
  while (it != names.end()) {
    label_name += ::tsdb::index::LABEL_NAME_SEPARATOR +
                  std::string((*it).data(), (*it).size());
    ++it;
  }
  if (label_indices_table.find(label_name) == label_indices_table.end())
    return nullptr;

  uint64_t offset = label_indices_table[label_name];
  char tmp_b[12];
  Slice tmp_s;
  Status s = b->Read(offset, 12, &tmp_s, tmp_b);
  if (!s.ok()) return nullptr;
  uint32_t len = ::tsdb::base::get_uint32_big_endian(tmp_s.data());
  uint32_t tuple_size = ::tsdb::base::get_uint32_big_endian(tmp_s.data() + 4);

  char* tmp_b2 = new char[len - 8];
  s = b->Read(offset + 12, len - 8, &tmp_s, tmp_b2);
  if (!s.ok()) {
    delete tmp_b2;
    return nullptr;
  }

  // NOTICE
  // Important to add placeholder when boost::bind
  bool need_clean = (b->Type() == kPosix);
  if (!need_clean) delete tmp_b2;
  return std::unique_ptr<::tsdb::tsdbutil::StringTuplesInterface>(
      new ::tsdb::tsdbutil::SerializedStringTuples(
          reinterpret_cast<const uint8_t*>(tmp_s.data()), tuple_size, len - 8,
          boost::bind(&IndexReader::lookup_symbols, this, _1), need_clean));
}

// ┌────────────────────┬────────────────────┐
// │ len <4b>           │ #entries <4b>      │
// ├────────────────────┴────────────────────┤
// │ ┌─────────────────────────────────────┐ │
// │ │ ref(series_1) <4b>                  │ │
// │ ├─────────────────────────────────────┤ │
// │ │ ...                                 │ │
// │ ├─────────────────────────────────────┤ │
// │ │ ref(series_n) <4b>                  │ │
// │ └─────────────────────────────────────┘ │
// ├─────────────────────────────────────────┤
// │ CRC32 <4b>                              │
// └─────────────────────────────────────────┘
// Return a Postings list containing the (offset / 16) of each series
std::pair<std::unique_ptr<::tsdb::index::PostingsInterface>, bool>
IndexReader::postings(const std::string& name, const std::string& value) {
  if (!b)
    return {std::unique_ptr<::tsdb::index::PostingsInterface>(
                new ::tsdb::index::EmptyPostings()),
            false};

  if (name == ::tsdb::label::ALL_POSTINGS_KEYS.label &&
      value == ::tsdb::label::ALL_POSTINGS_KEYS.value)
    return {std::unique_ptr<::tsdb::index::PostingsInterface>(
                new ::tsdb::index::ListPostings(all_postings)),
            true};

  if (postings_table.find(name) == postings_table.end()) {
    LOG_ERROR << "Fail to read postings, label name:" << name << " not existed";
    return {std::unique_ptr<::tsdb::index::PostingsInterface>(
                new ::tsdb::index::EmptyPostings()),
            false};
  }
  if (postings_table[name].find(value) == postings_table[name].end()) {
    LOG_ERROR << "Fail to read postings, label value:" << value
              << " not existed";
    return {std::unique_ptr<::tsdb::index::PostingsInterface>(
                new ::tsdb::index::EmptyPostings()),
            false};
  }

  uint64_t offset = postings_table[name][value];
  char tmp_b[4];
  Slice tmp_s;
  Status s = b->Read(offset, 4, &tmp_s, tmp_b);
  uint32_t len = ::tsdb::base::get_uint32_big_endian(tmp_s.data());

  bool need_clean = (b->Type() == kPosix);
  char* tmp_b2;
  if (need_clean) tmp_b2 = new char[len - 4];
  s = b->Read(offset + 8, len - 4, &tmp_s, tmp_b2);
  if (!s.ok()) {
    if (need_clean) delete tmp_b2;
    return {std::unique_ptr<::tsdb::index::PostingsInterface>(
                new ::tsdb::index::EmptyPostings()),
            false};
  }

  return {std::unique_ptr<::tsdb::index::PostingsInterface>(
              new ::tsdb::index::Uint32BEDoubleSkipPostings(
                  reinterpret_cast<const uint8_t*>(tmp_s.data()), len - 4,
                  need_clean)),
          true};
}

std::pair<std::unique_ptr<::tsdb::index::PostingsInterface>, bool>
IndexReader::postings_and_tsids(const std::string& name,
                                const std::string& value) {
  if (!b)
    return {std::unique_ptr<::tsdb::index::PostingsInterface>(
                new ::tsdb::index::EmptyPostings()),
            false};

  if (name == ::tsdb::label::ALL_POSTINGS_KEYS.label &&
      value == ::tsdb::label::ALL_POSTINGS_KEYS.value)
    return {std::unique_ptr<::tsdb::index::PostingsInterface>(
                new ::tsdb::index::ListPostings(all_postings)),
            true};

  if (postings_table.find(name) == postings_table.end()) {
    LOG_ERROR << "Fail to read postings, label name:" << name << " not existed";
    return {std::unique_ptr<::tsdb::index::PostingsInterface>(
                new ::tsdb::index::EmptyPostings()),
            false};
  }
  if (postings_table[name].find(value) == postings_table[name].end()) {
    LOG_ERROR << "Fail to read postings, label value:" << value
              << " not existed";
    return {std::unique_ptr<::tsdb::index::PostingsInterface>(
                new ::tsdb::index::EmptyPostings()),
            false};
  }

  uint64_t offset = postings_table[name][value];
  char tmp_b[4];
  Slice tmp_s;
  Status s = b->Read(offset, 4, &tmp_s, tmp_b);
  uint32_t len = ::tsdb::base::get_uint32_big_endian(tmp_s.data());

  bool need_clean = (b->Type() == kPosix);
  char* tmp_b2;
  if (need_clean) tmp_b2 = new char[len - 4];
  s = b->Read(offset + 8, len - 4, &tmp_s, tmp_b2);
  if (!s.ok()) {
    if (need_clean) delete tmp_b2;
    return {std::unique_ptr<::tsdb::index::PostingsInterface>(
                new ::tsdb::index::EmptyPostings()),
            false};
  }

  return {std::unique_ptr<::tsdb::index::PostingsInterface>(
              new ::tsdb::index::Uint32BEDoublePostings(
                  reinterpret_cast<const uint8_t*>(tmp_s.data()), len - 4,
                  need_clean)),
          true};
}

// NOTE(Alec), don't use this.
// std::pair<std::unique_ptr<::tsdb::index::PostingsInterface>, bool>
// IndexReader::postings_tsid(
//     const std::string &name, const std::string &value) {
//   if (!b)
//     return {std::unique_ptr<::tsdb::index::PostingsInterface>(new
//     ::tsdb::index::EmptyPostings()), false};

//   if (name == ::tsdb::label::ALL_POSTINGS_KEYS.label &&
//       value == ::tsdb::label::ALL_POSTINGS_KEYS.value)
//     return {std::unique_ptr<::tsdb::index::PostingsInterface>(new
//     ::tsdb::index::ListPostings(all_postings)),
//             true};

//   if (postings_table.find(name) == postings_table.end()) {
//     LOG_ERROR << "Fail to read postings, label name:" << name << " not
//     existed"; return {std::unique_ptr<::tsdb::index::PostingsInterface>(new
//     ::tsdb::index::EmptyPostings()), false};
//   }
//   if (postings_table[name].find(value) == postings_table[name].end()) {
//     LOG_ERROR << "Fail to read postings, label value:" << value
//               << " not existed";
//     return {std::unique_ptr<::tsdb::index::PostingsInterface>(new
//     ::tsdb::index::EmptyPostings()), false};
//   }

//   uint64_t offset = postings_table[name][value];
//   char tmp_b[32];
//   Slice tmp_s;
//   Status s = b->Read(offset, 4, &tmp_s, tmp_b);
//   uint32_t len = ::tsdb::base::get_uint32_big_endian(tmp_s.data());

//   char* tmp_b2 = new char[len - 4];
//   Slice tmp_s2;
//   s = b->Read(offset + 8, len - 4, &tmp_s2, tmp_b2);
//   if (!s.ok()) {
//     delete tmp_b2;
//     return {std::unique_ptr<::tsdb::index::PostingsInterface>(new
//     ::tsdb::index::EmptyPostings()), false};
//   }
//   ::tsdb::index::Uint32BEPostings off_ptr(reinterpret_cast<const
//   uint8_t*>(tmp_s2.data()), len - 4);

//   int decoded1, decoded2;
//   ::tsdb::index::VectorPostings* result = new
//   ::tsdb::index::VectorPostings(); while (off_ptr.next()) {
//     s = b->Read(off_ptr.at() * 16, ::tsdb::base::MAX_VARINT_LEN_64 * 2,
//     &tmp_s, tmp_b); if (!s.ok()) return
//     {std::unique_ptr<::tsdb::index::PostingsInterface>(new
//     ::tsdb::index::EmptyPostings()), false};
//     ::tsdb::base::decode_unsigned_varint(reinterpret_cast<const
//     uint8_t*>(tmp_s.data()), decoded1, ::tsdb::base::MAX_VARINT_LEN_64);
//     result->push_back(::tsdb::base::decode_unsigned_varint(reinterpret_cast<const
//     uint8_t*>(tmp_s.data() + decoded1), decoded2,
//     ::tsdb::base::MAX_VARINT_LEN_64));
//   }
//   delete tmp_b2;
//   return {std::unique_ptr<::tsdb::index::PostingsInterface>(result), true};
// }

// ┌─────────────────────────────────────────────────────────────────────────┐
// │ len <uvarint>                                                           │
// ├─────────────────────────────────────────────────────────────────────────┤
// │ ┌──────────────────┬──────────────────────────────────────────────────┐ │
// │ │                  │ ┌──────────────────────────────────────────┐     │ │
// │ │                  │ │ ref(l_i.name) <uvarint>                  │     │ │
// │ │     #labels      │ ├──────────────────────────────────────────┤ ... │ │
// │ │    <uvarint>     │ │ ref(l_i.value) <uvarint>                 │     │ │
// │ │                  │ └──────────────────────────────────────────┘     │ │
// │ ├──────────────────┼──────────────────────────────────────────────────┤ │
// │ │                  │ ┌──────────────────────────────────────────┐     │ │
// │ │                  │ │ c_0.mint <varint>                        │     │ │
// │ │                  │ ├──────────────────────────────────────────┤     │ │
// │ │                  │ │ c_0.maxt - c_0.mint <uvarint>            │     │ │
// │ │                  │ ├──────────────────────────────────────────┤     │ │
// │ │                  │ │ ref(c_0.data) <uvarint>                  │     │ │
// │ │      #chunks     │ └──────────────────────────────────────────┘     │ │
// │ │     <uvarint>    │ ┌──────────────────────────────────────────┐     │ │
// │ │                  │ │ c_i.mint - c_i-1.maxt <uvarint>          │     │ │
// │ │                  │ ├──────────────────────────────────────────┤     │ │
// │ │                  │ │ c_i.maxt - c_i.mint <uvarint>            │     │ │
// │ │                  │ ├──────────────────────────────────────────┤ ... │ │
// │ │                  │ │ ref(c_i.data) - ref(c_i-1.data) <varint> │     │ │
// │ │                  │ └──────────────────────────────────────────┘     │ │
// │ └──────────────────┴──────────────────────────────────────────────────┘ │
// ├─────────────────────────────────────────────────────────────────────────┤
// │ CRC32 <4b>                                                              │
// └─────────────────────────────────────────────────────────────────────────┘
//
// Reference is the offset of Series entry / 16
// lset and chunks supposed to be empty
bool IndexReader::series(
    uint64_t ref, ::tsdb::label::Labels& lset,
    std::deque<std::shared_ptr<::tsdb::chunk::ChunkMeta>>& chunks) {
  if (!b) return false;

  ref *= 16;

  char tmp_b[16];
  Slice tmp_s;
  Status s = b->Read(ref, ::tsdb::base::MAX_VARINT_LEN_64, &tmp_s, tmp_b);
  if (!s.ok()) return false;

  int decoded;
  uint64_t len = ::tsdb::base::decode_unsigned_varint(
      reinterpret_cast<const uint8_t*>(tmp_s.data()), decoded,
      ::tsdb::base::MAX_VARINT_LEN_64);

  char* tmp_b2 = new char[len];
  s = b->Read(ref + decoded, len, &tmp_s, tmp_b2);
  if (!s.ok()) {
    delete tmp_b2;
    return false;
  }
  ::tsdb::tsdbutil::DecBuf dec_buf(
      reinterpret_cast<const uint8_t*>(tmp_s.data()), len);

  // Decode the Labels
  uint64_t num_labels = dec_buf.get_unsigned_variant();
  for (uint64_t i = 0; i < num_labels; i++) {
    uint64_t ref_name = dec_buf.get_unsigned_variant();
    uint64_t ref_value = dec_buf.get_unsigned_variant();

    if (dec_buf.err != ::tsdb::tsdbutil::NO_ERR) {
      LOG_ERROR << "Fail to read series, fail to read labels";
      delete tmp_b2;
      return false;
    }
    if (ref_name >= symbols_.size() || ref_value >= symbols_.size()) {
      LOG_ERROR << "Fail to read series, invalid index of label name or value";
      delete tmp_b2;
      return false;
    }
    lset.emplace_back(symbols_[ref_name], symbols_[ref_value]);
  }

  // Decode the Chunks
  uint64_t num_chunks = dec_buf.get_unsigned_variant();
  if (num_chunks == 0) {
    delete tmp_b2;
    return true;
  }

  // First chunk meta
  int64_t last_t = dec_buf.get_signed_variant();
  uint64_t delta_t = dec_buf.get_unsigned_variant();
  int64_t last_ref = static_cast<int64_t>(dec_buf.get_unsigned_variant());
  if (dec_buf.err != ::tsdb::tsdbutil::NO_ERR) {
    LOG_ERROR << "Fail to read series, fail to read chunk meta 0";
    delete tmp_b2;
    return false;
  }
  // LOG_INFO << last_t << " " << delta_t;
  chunks.push_back(std::shared_ptr<::tsdb::chunk::ChunkMeta>(
      new ::tsdb::chunk::ChunkMeta(static_cast<uint64_t>(last_ref), last_t,
                                   static_cast<int64_t>(delta_t) + last_t)));

  for (int i = 1; i < num_chunks; i++) {
    last_t += static_cast<int64_t>(dec_buf.get_unsigned_variant() + delta_t);
    delta_t = dec_buf.get_unsigned_variant();
    last_ref += dec_buf.get_signed_variant();
    if (dec_buf.err != ::tsdb::tsdbutil::NO_ERR) {
      LOG_ERROR << "Fail to read series, fail to read chunk meta " << i;
      delete tmp_b2;
      return false;
    }
    // LOG_INFO << last_t << " " << delta_t;
    chunks.push_back(std::shared_ptr<::tsdb::chunk::ChunkMeta>(
        new ::tsdb::chunk::ChunkMeta(static_cast<uint64_t>(last_ref), last_t,
                                     static_cast<int64_t>(delta_t) + last_t)));
  }
  delete tmp_b2;
  return true;
}

bool IndexReader::series(uint64_t ref, ::tsdb::label::Labels& lset,
                         uint64_t* tsid) {
  if (!b) return false;

  ref *= 16;

  char tmp_b[32];
  Slice tmp_s;
  Status s = b->Read(ref, 2 * ::tsdb::base::MAX_VARINT_LEN_64, &tmp_s, tmp_b);
  if (!s.ok()) return false;

  int decoded1;
  int decoded2;
  uint64_t len = ::tsdb::base::decode_unsigned_varint(
      reinterpret_cast<const uint8_t*>(tmp_s.data()), decoded1,
      ::tsdb::base::MAX_VARINT_LEN_64);
  *tsid = ::tsdb::base::decode_unsigned_varint(
      reinterpret_cast<const uint8_t*>(tmp_s.data() + decoded1), decoded2,
      ::tsdb::base::MAX_VARINT_LEN_64);

  char* tmp_b2 = new char[len];
  s = b->Read(ref + decoded1 + decoded2, len, &tmp_s, tmp_b2);
  if (!s.ok()) {
    delete tmp_b2;
    return false;
  }
  ::tsdb::tsdbutil::DecBuf dec_buf(
      reinterpret_cast<const uint8_t*>(tmp_s.data()), len);

  // Decode the Labels
  uint64_t num_labels = dec_buf.get_unsigned_variant();
  for (uint64_t i = 0; i < num_labels; i++) {
    uint64_t ref_name = dec_buf.get_unsigned_variant();
    uint64_t ref_value = dec_buf.get_unsigned_variant();

    if (dec_buf.err != ::tsdb::tsdbutil::NO_ERR) {
      LOG_ERROR << "Fail to read series, fail to read labels";
      delete tmp_b2;
      return false;
    }
    if (ref_name >= symbols_.size() || ref_value >= symbols_.size()) {
      LOG_ERROR << "Fail to read series, invalid index of label name or value";
      delete tmp_b2;
      return false;
    }
    lset.emplace_back(symbols_[ref_name], symbols_[ref_value]);
  }

  delete tmp_b2;
  return true;
}

std::unique_ptr<::tsdb::index::PostingsInterface> IndexReader::sorted_postings(
    std::unique_ptr<::tsdb::index::PostingsInterface>&& p) {
  return std::move(p);
}

std::unique_ptr<::tsdb::index::PostingsInterface>
IndexReader::sorted_postings() {
  return std::unique_ptr<::tsdb::index::PostingsInterface>(
      new ::tsdb::index::ListPostings(all_postings));
}

// NOTE(Alec), convert offsets to tsids.
std::unique_ptr<::tsdb::index::PostingsInterface>
IndexReader::sorted_postings_tsid() {
  if (!all_postings_tsid.empty())
    return std::unique_ptr<::tsdb::index::PostingsInterface>(
        new ::tsdb::index::ListPostings(all_postings_tsid));

  uint64_t ref, len;
  int decoded1, decoded2;
  char tmp_b[32];
  Slice tmp_s;
  for (size_t i = 0; i < all_postings.size(); i++) {
    ref = all_postings[i] * 16;
    Status s = b->Read(ref, 2 * ::tsdb::base::MAX_VARINT_LEN_64, &tmp_s, tmp_b);
    if (!s.ok()) return nullptr;
    len = ::tsdb::base::decode_unsigned_varint(
        reinterpret_cast<const uint8_t*>(tmp_s.data()), decoded1,
        ::tsdb::base::MAX_VARINT_LEN_64);
    all_postings_tsid.push_back(::tsdb::base::decode_unsigned_varint(
        reinterpret_cast<const uint8_t*>(tmp_s.data() + decoded1), decoded2,
        ::tsdb::base::MAX_VARINT_LEN_64));
  }
  return std::unique_ptr<::tsdb::index::PostingsInterface>(
      new ::tsdb::index::ListPostings(all_postings_tsid));
}

bool IndexReader::error() { return err_; }

uint64_t IndexReader::size() {
  if (!b) return 0;
  return b->FileSize();
}

}  // namespace leveldb