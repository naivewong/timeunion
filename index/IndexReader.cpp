#include <cstring>
// #include <iostream>
#include <boost/bind.hpp>
// #include <boost/function.hpp>
#include <boost/tokenizer.hpp>

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

namespace tsdb {
namespace index {

IndexReader::IndexReader(std::shared_ptr<tsdbutil::ByteSlice> b) : err_(false) {
  if (!validate(b)) {
    LOG_ERROR << "Fail to create IndexReader, invalid ByteSlice";
    err_ = true;
    return;
  }
  this->b = b;
  init();
  get_all_postings();
}

IndexReader::IndexReader(const std::string &filename) : err_(false) {
  std::shared_ptr<tsdbutil::ByteSlice> temp =
      std::shared_ptr<tsdbutil::ByteSlice>(new tsdbutil::MMapSlice(filename));
  if (!validate(temp)) {
    LOG_ERROR << "Fail to create IndexReader, invalid ByteSlice";
    err_ = true;
    return;
  }
  this->b = temp;
  init();
  get_all_postings();
}

void IndexReader::init() {
  std::pair<TOC, bool> toc_pair = toc_from_ByteSlice(b.get());
  if (!toc_pair.second) {
    LOG_ERROR << "Fail to create IndexReader, error reading TOC";
    b.reset();
    err_ = true;
    return;
  }

  // Read symbols
  if (!read_symbols(toc_pair.first)) {
    LOG_ERROR << "Fail to create IndexReader, error reading symbols";
    b.reset();
    err_ = true;
    return;
  }

  // Read label indices table
  if (!read_offset_table(toc_pair.first.label_indices_table, true)) {
    LOG_ERROR
        << "Fail to create IndexReader, error reading label indices table";
    b.reset();
    err_ = true;
    return;
  }

  // Read postings table
  if (!read_offset_table(toc_pair.first.postings_table, false)) {
    LOG_ERROR << "Fail to create IndexReader, error reading postings table";
    b.reset();
    err_ = true;
    return;
  }
}

// OPTIONAL(Alec).
// NOTE(Alec), all_postings is already a sorted postings
// because the offset determines the order.
void IndexReader::get_all_postings() {
  std::set<uint64_t> all;
  for (auto const &p_table : postings_table) {
    for (auto const &p_pair : p_table.second) {
      std::pair<std::unique_ptr<PostingsInterface>, bool> p =
          postings(p_table.first, p_pair.first);
      while (p.first->next()) all.insert(p.first->at());
    }
  }
  all_postings.insert(all_postings.end(), all.begin(), all.end());
}

bool IndexReader::validate(const std::shared_ptr<tsdbutil::ByteSlice> &b) {
  if (b->len() < 4) {
    LOG_ERROR << "Length of ByteSlice < 4";
    return false;
  }
  if (base::get_uint32_big_endian((b->range(0, 4)).first) != MAGIC_INDEX) {
    LOG_ERROR << "Not beginning with MAGIC_INDEX";
    return false;
  }
  if (*((b->range(4, 5)).first) != INDEX_VERSION_V1) {
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
bool IndexReader::read_symbols(const TOC &toc) {
  std::pair<const uint8_t *, int> symbols_begin =
      b->range(toc.symbols, toc.symbols + 4);
  if (symbols_begin.second != 4) return false;
  uint32_t len = base::get_uint32_big_endian(symbols_begin.first);
  tsdbutil::DecBuf dec_buf(symbols_begin.first + 4, len);

  // #symbols <4b>
  uint32_t num_symbols = dec_buf.get_BE_uint32();
  for (int i = 0; i < num_symbols; i++)
    symbols_.push_back(dec_buf.get_uvariant_string());
  if (dec_buf.err != tsdbutil::NO_ERR)
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
  std::pair<const uint8_t *, int> table_begin = b->range(offset, offset + 4);
  if (table_begin.second != 4) return false;
  uint32_t len = base::get_uint32_big_endian(table_begin.first);
  uint32_t num_entries = base::get_uint32_big_endian(table_begin.first + 4);
  tsdbutil::DecBuf dec_buf(table_begin.first + 8, len - 4);

  if (label_index) {
    // Read label name to offset of label value
    for (int i = 0; i < num_entries; i++) {
      uint64_t temp_num =
          dec_buf.get_unsigned_variant();  // Normally temp_num = 1
      std::string label_name = dec_buf.get_uvariant_string();
      for (int j = 1; j < temp_num; j++) {
        label_name += LABEL_NAME_SEPARATOR + dec_buf.get_uvariant_string();
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
  if (dec_buf.err != tsdbutil::NO_ERR)
    return false;
  else
    return true;
}

std::set<std::string> IndexReader::symbols() {
  return std::set<std::string>(symbols_.begin(), symbols_.end());
}

const std::deque<std::string> &IndexReader::symbols_deque() const {
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
  boost::char_separator<char> tokenSep(LABEL_NAME_SEPARATOR.c_str());
  for (auto const &p : label_indices_table) {
    if (p.first.length() == 0) continue;
    boost::tokenizer<boost::char_separator<char>> label_names_tokens(p.first,
                                                                     tokenSep);
    for (auto const &token : label_names_tokens) {
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
std::unique_ptr<tsdbutil::StringTuplesInterface> IndexReader::label_values(
    const std::initializer_list<std::string> &names) {
  if (!b) return nullptr;

  auto it = names.begin();
  std::string label_name = std::string((*it).data(), (*it).size());
  ++it;
  while (it != names.end()) {
    label_name +=
        LABEL_NAME_SEPARATOR + std::string((*it).data(), (*it).size());
    ++it;
  }
  if (label_indices_table.find(label_name) == label_indices_table.end())
    return nullptr;

  uint64_t offset = label_indices_table[label_name];
  const uint8_t *start = (b->range(offset, offset + 4)).first;
  uint32_t len = base::get_uint32_big_endian(start);
  uint32_t tuple_size = base::get_uint32_big_endian(start + 4);

  // NOTICE
  // Important to add placeholder when boost::bind
  return std::unique_ptr<tsdbutil::StringTuplesInterface>(
      new tsdbutil::SerializedStringTuples(
          start + 12, tuple_size, len - 8,
          boost::bind(&IndexReader::lookup_symbols, this, _1)));
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
std::pair<std::unique_ptr<PostingsInterface>, bool> IndexReader::postings(
    const std::string &name, const std::string &value) {
  if (!b)
    return {std::unique_ptr<PostingsInterface>(new EmptyPostings()), false};

  if (name == label::ALL_POSTINGS_KEYS.label &&
      value == label::ALL_POSTINGS_KEYS.value)
    return {std::unique_ptr<PostingsInterface>(new ListPostings(all_postings)),
            true};

  if (postings_table.find(name) == postings_table.end()) {
    LOG_ERROR << "Fail to read postings, label name:" << name << " not existed";
    return {std::unique_ptr<PostingsInterface>(new EmptyPostings()), false};
  }
  if (postings_table[name].find(value) == postings_table[name].end()) {
    LOG_ERROR << "Fail to read postings, label value:" << value
              << " not existed";
    return {std::unique_ptr<PostingsInterface>(new EmptyPostings()), false};
  }

  uint64_t offset = postings_table[name][value];
  const uint8_t *start = (b->range(offset, offset + 4)).first;
  uint32_t len = base::get_uint32_big_endian(start);
  // LOG_INFO << len;
  return {std::unique_ptr<PostingsInterface>(
              new Uint32BEPostings(start + 8, len - 4)),
          true};
}

std::pair<std::unique_ptr<PostingsInterface>, bool> IndexReader::postings_tsid(
    const std::string &name, const std::string &value) {
  if (!b)
    return {std::unique_ptr<PostingsInterface>(new EmptyPostings()), false};

  if (name == label::ALL_POSTINGS_KEYS.label &&
      value == label::ALL_POSTINGS_KEYS.value)
    return {std::unique_ptr<PostingsInterface>(new ListPostings(all_postings)),
            true};

  if (postings_table.find(name) == postings_table.end()) {
    LOG_ERROR << "Fail to read postings, label name:" << name << " not existed";
    return {std::unique_ptr<PostingsInterface>(new EmptyPostings()), false};
  }
  if (postings_table[name].find(value) == postings_table[name].end()) {
    LOG_ERROR << "Fail to read postings, label value:" << value
              << " not existed";
    return {std::unique_ptr<PostingsInterface>(new EmptyPostings()), false};
  }

  uint64_t offset = postings_table[name][value];
  const uint8_t *start = (b->range(offset, offset + 4)).first;
  uint32_t len = base::get_uint32_big_endian(start);
  Uint32BEPostings off_ptr(start + 8, len - 4);

  int decoded1, decoded2;
  VectorPostings *result = new VectorPostings();
  while (off_ptr.next()) {
    const uint8_t *start =
        (b->range(off_ptr.at() * 16,
                  off_ptr.at() * 16 + base::MAX_VARINT_LEN_64))
            .first;
    base::decode_unsigned_varint(start, decoded1, base::MAX_VARINT_LEN_64);
    result->push_back(base::decode_unsigned_varint(start + decoded1, decoded2,
                                                   base::MAX_VARINT_LEN_64));
  }
  return {std::unique_ptr<PostingsInterface>(result), true};
}

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
    uint64_t ref, label::Labels &lset,
    std::deque<std::shared_ptr<chunk::ChunkMeta>> &chunks) {
  if (!b) return false;

  ref *= 16;

  const uint8_t *start = (b->range(ref, ref + base::MAX_VARINT_LEN_64)).first;
  int decoded;
  uint64_t len =
      base::decode_unsigned_varint(start, decoded, base::MAX_VARINT_LEN_64);
  tsdbutil::DecBuf dec_buf(start + decoded, len);

  // Decode the Labels
  uint64_t num_labels = dec_buf.get_unsigned_variant();
  for (uint64_t i = 0; i < num_labels; i++) {
    uint64_t ref_name = dec_buf.get_unsigned_variant();
    uint64_t ref_value = dec_buf.get_unsigned_variant();

    if (dec_buf.err != tsdbutil::NO_ERR) {
      LOG_ERROR << "Fail to read series, fail to read labels";
      return false;
    }
    if (ref_name >= symbols_.size() || ref_value >= symbols_.size()) {
      LOG_ERROR << "Fail to read series, invalid index of label name or value";
      return false;
    }
    lset.emplace_back(symbols_[ref_name], symbols_[ref_value]);
  }

  // Decode the Chunks
  uint64_t num_chunks = dec_buf.get_unsigned_variant();
  if (num_chunks == 0) return true;

  // First chunk meta
  int64_t last_t = dec_buf.get_signed_variant();
  uint64_t delta_t = dec_buf.get_unsigned_variant();
  int64_t last_ref = static_cast<int64_t>(dec_buf.get_unsigned_variant());
  if (dec_buf.err != tsdbutil::NO_ERR) {
    LOG_ERROR << "Fail to read series, fail to read chunk meta 0";
    return false;
  }
  // LOG_INFO << last_t << " " << delta_t;
  chunks.push_back(std::shared_ptr<chunk::ChunkMeta>(
      new chunk::ChunkMeta(static_cast<uint64_t>(last_ref), last_t,
                           static_cast<int64_t>(delta_t) + last_t)));

  for (int i = 1; i < num_chunks; i++) {
    last_t += static_cast<int64_t>(dec_buf.get_unsigned_variant() + delta_t);
    delta_t = dec_buf.get_unsigned_variant();
    last_ref += dec_buf.get_signed_variant();
    if (dec_buf.err != tsdbutil::NO_ERR) {
      LOG_ERROR << "Fail to read series, fail to read chunk meta " << i;
      return false;
    }
    // LOG_INFO << last_t << " " << delta_t;
    chunks.push_back(std::shared_ptr<chunk::ChunkMeta>(
        new chunk::ChunkMeta(static_cast<uint64_t>(last_ref), last_t,
                             static_cast<int64_t>(delta_t) + last_t)));
  }
  return true;
}

bool IndexReader::series(uint64_t ref, label::Labels &lset, uint64_t *tsid) {
  if (!b) return false;

  ref *= 16;

  const uint8_t *start = (b->range(ref, ref + base::MAX_VARINT_LEN_64)).first;
  int decoded1;
  int decoded2;
  uint64_t len =
      base::decode_unsigned_varint(start, decoded1, base::MAX_VARINT_LEN_64);
  *tsid = base::decode_unsigned_varint(start + decoded1, decoded2,
                                       base::MAX_VARINT_LEN_64);
  tsdbutil::DecBuf dec_buf(start + decoded1 + decoded2, len);

  // Decode the Labels
  uint64_t num_labels = dec_buf.get_unsigned_variant();
  for (uint64_t i = 0; i < num_labels; i++) {
    uint64_t ref_name = dec_buf.get_unsigned_variant();
    uint64_t ref_value = dec_buf.get_unsigned_variant();

    if (dec_buf.err != tsdbutil::NO_ERR) {
      LOG_ERROR << "Fail to read series, fail to read labels";
      return false;
    }
    if (ref_name >= symbols_.size() || ref_value >= symbols_.size()) {
      LOG_ERROR << "Fail to read series, invalid index of label name or value";
      return false;
    }
    lset.emplace_back(symbols_[ref_name], symbols_[ref_value]);
  }

  return true;
}

std::unique_ptr<index::PostingsInterface> IndexReader::sorted_postings(
    std::unique_ptr<index::PostingsInterface> &&p) {
  return std::move(p);
}

std::unique_ptr<index::PostingsInterface> IndexReader::sorted_postings() {
  return std::unique_ptr<PostingsInterface>(new ListPostings(all_postings));
}

// NOTE(Alec), convert offsets to tsids.
std::unique_ptr<index::PostingsInterface> IndexReader::sorted_postings_tsid() {
  if (!all_postings_tsid.empty())
    return std::unique_ptr<PostingsInterface>(
        new ListPostings(all_postings_tsid));

  uint64_t ref, len;
  int decoded1, decoded2;
  for (size_t i = 0; i < all_postings.size(); i++) {
    ref = all_postings[i] * 16;
    const uint8_t *start = (b->range(ref, ref + base::MAX_VARINT_LEN_64)).first;
    len =
        base::decode_unsigned_varint(start, decoded1, base::MAX_VARINT_LEN_64);
    all_postings_tsid.push_back(base::decode_unsigned_varint(
        start + decoded1, decoded2, base::MAX_VARINT_LEN_64));
  }
  return std::unique_ptr<PostingsInterface>(
      new ListPostings(all_postings_tsid));
}

bool IndexReader::error() { return err_; }

uint64_t IndexReader::size() {
  if (!b) return 0;
  return b->len();
}

uint64_t symbol_table_size(const IndexReader &indexr) {
  uint64_t size = 0;
  for (const std::string &s : indexr.symbols_deque()) size += s.length() + 8;
  return size;
}

}  // namespace index
}  // namespace tsdb