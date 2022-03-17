#include "index/IndexWriter.hpp"

#include <stdio.h>

#include <boost/filesystem.hpp>
#include <unordered_map>
#include <vector>

#include "base/Checksum.hpp"
#include "base/Logging.hpp"
#include "cloud/S3File.hpp"
#include "label/Label.hpp"
#include "tsdbutil/StringTuples.hpp"

namespace tsdb {
namespace index {

// All the dirs inside filename should be existed.
IndexWriter::IndexWriter(const std::string &filename, cloud::S3Wrapper *wrapper)
    : pos(0),
      stage(IDX_STAGE_NONE),
      buf1(1 << 22),
      buf2(1 << 22),
      uint32_cache(1 << 15),
      filename_(filename),
      s3_wrapper_(wrapper) {
  boost::filesystem::path p(filename);
  if (boost::filesystem::exists(p)) boost::filesystem::remove_all(p);

  // Create parent path if not existed.
  if (p.has_parent_path() && !boost::filesystem::exists(p.parent_path()))
    boost::filesystem::create_directories(p.parent_path());

  f = fopen(filename.c_str(), "wb+");

  // TODO(Alec), if really needed.
  symbols.reserve(1 << 13);
  series = std::unordered_map<uint64_t, uint64_t>();
  series.reserve(1 << 16);

  write_meta();
}

void IndexWriter::write_meta() {
  buf1.reset();
  buf1.put_BE_uint32(MAGIC_INDEX);
  buf1.put_byte(INDEX_VERSION_V1);
  write({buf1.get()});
}

void IndexWriter::write(
    std::initializer_list<std::pair<const uint8_t *, int>> l) {
  for (auto &p : l) {
    fwrite(p.first, 1, p.second, f);
    pos += static_cast<uint64_t>(p.second);
  }
}

void IndexWriter::add_padding(uint64_t padding) {
  uint64_t p = pos % padding;
  if (p != 0) {
    uint8_t s[padding - p];
    memset(s, 0, padding - p);
    write({std::pair<const uint8_t *, int>(s, padding - p)});
  }
}

// 0 succeed, -1 error
int IndexWriter::ensure_stage(IndexWriterStage s) {
  if (s == stage) {
    return 0;
  }
  if (s < stage) {
    LOG_ERROR << "Invalid stage: " << stage_to_string(s) << ", current at "
              << stage_to_string(stage);
    return -1;
  }
  // Set stage
  if (s == IDX_STAGE_SYMBOLS)
    toc.symbols = pos;
  else if (s == IDX_STAGE_SERIES)
    toc.series = pos;
  else if (s == IDX_STAGE_LABEL_INDEX)
    toc.label_indices = pos;
  else if (s == IDX_STAGE_POSTINGS)
    toc.postings = pos;
  else if (s == IDX_STAGE_SST)
    toc.sst_table = pos;
  else if (s == IDX_STAGE_DONE) {
    toc.label_indices_table = pos;
    write_offset_table(label_indices);

    toc.postings_table = pos;
    write_offset_table(postings);

    write_TOC();
  }
  stage = s;
  return 0;
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
// Will sort symbols
int IndexWriter::add_symbols(const std::unordered_set<std::string> &sym) {
  if (ensure_stage(IDX_STAGE_SYMBOLS) == -1) return INVALID_STAGE;

  std::vector<std::string> arr(sym.begin(), sym.end());
  std::sort(arr.begin(), arr.end());

  buf1.reset();
  buf2.reset();

  buf2.put_BE_uint32(arr.size());
  for (int i = 0; i < arr.size(); i++) {
    symbols[arr[i]] = i;
    buf2.put_uvariant_string(arr[i]);
  }

  buf1.put_BE_uint32(buf2.len());
  buf2.put_BE_uint32(base::GetCrc32(buf2.get()));

  write({buf1.get(), buf2.get()});
  return 0;
}

int IndexWriter::add_symbols(const std::set<std::string> &sym) {
  if (ensure_stage(IDX_STAGE_SYMBOLS) == -1) return INVALID_STAGE;

  buf1.reset();
  buf2.reset();

  buf2.put_BE_uint32(sym.size());
  int i = 0;
  for (const std::string &s : sym) {
    symbols[s] = i++;
    buf2.put_uvariant_string(s);
  }

  buf1.put_BE_uint32(buf2.len());
  buf2.put_BE_uint32(base::GetCrc32(buf2.get()));

  write({buf1.get(), buf2.get()});
  return 0;
}

// ┌──────────────────────────────────────────────────────────────────────────┐
// │ len <uvarint>                                                            │
// ├──────────────────────────────────────────────────────────────────────────┤
// │ ┌──────────────────────────────────────────────────────────────────────┐ │
// │ │                     labels count <uvarint64>                         │ │
// │ ├──────────────────────────────────────────────────────────────────────┤ │
// │ │              ┌────────────────────────────────────────────┐          │ │
// │ │              │ ref(l_i.name) <uvarint32>                  │          │ │
// │ │              ├────────────────────────────────────────────┤          │ │
// │ │              │ ref(l_i.value) <uvarint32>                 │          │ │
// │ │              └────────────────────────────────────────────┘          │ │
// │ │                             ...                                      │ │
// │ ├──────────────────────────────────────────────────────────────────────┤ │
// │ │                     chunks count <uvarint64>                         │ │
// │ ├──────────────────────────────────────────────────────────────────────┤ │
// │ │              ┌────────────────────────────────────────────┐          │ │
// │ │              │ c_0.mint <varint64>                        │          │ │
// │ │              ├────────────────────────────────────────────┤          │ │
// │ │              │ c_0.maxt - c_0.mint <uvarint64>            │          │ │
// │ │              ├────────────────────────────────────────────┤          │ │
// │ │              │ ref(c_0.data) <uvarint64>                  │          │ │
// │ │              └────────────────────────────────────────────┘          │ │
// │ │              ┌────────────────────────────────────────────┐          │ │
// │ │              │ c_i.mint - c_i-1.maxt <uvarint64>          │          │ │
// │ │              ├────────────────────────────────────────────┤          │ │
// │ │              │ c_i.maxt - c_i.mint <uvarint64>            │          │ │
// │ │              ├────────────────────────────────────────────┤          │ │
// │ │              │ ref(c_i.data) - ref(c_i-1.data) <varint64> │          │ │
// │ │              └────────────────────────────────────────────┘          │ │
// │ │                             ...                                      │ │
// │ └──────────────────────────────────────────────────────────────────────┘ │
// ├──────────────────────────────────────────────────────────────────────────┤
// │ CRC32 <4b>                                                               │
// └──────────────────────────────────────────────────────────────────────────┘
//
// Adrress series entry by 4 bytes reference
// Align to 16 bytes
// NOTICE: The ref here is just a temporary ref assigned as monotonically
// increasing id in memory.
int IndexWriter::add_series(
    uint64_t ref, const label::Labels &l,
    const std::deque<std::shared_ptr<chunk::ChunkMeta>> &chunks) {
  if (ensure_stage(IDX_STAGE_SERIES) == -1) return INVALID_STAGE;
  if (label::lbs_compare(l, last_series) < 0) {
    LOG_ERROR << "Out of order series";
    return OUT_OF_ORDER;
  }

  if (series.find(ref) != series.end()) {
    LOG_ERROR << "Labels already added";
    return EXISTED;
  }

  // Align each entry to 16 bytes
  add_padding(16);
  series[ref] = pos / 16;
  buf2.reset();
  buf2.put_unsigned_variant(l.size());
  for (auto &label : l) {
    if (symbols.find(label.label) == symbols.end()) {
      LOG_ERROR << "Symbol not existed: " << label.label;
      return CANNOT_FIND;
    }
    buf2.put_unsigned_variant(symbols[label.label]);
    if (symbols.find(label.value) == symbols.end()) {
      LOG_ERROR << "Symbol not existed: " << label.value;
      return CANNOT_FIND;
    }
    buf2.put_unsigned_variant(symbols[label.value]);
  }
  // LOG_INFO << "stage1";
  buf2.put_unsigned_variant(chunks.size());
  if (chunks.size() > 0) {
    int64_t last_t = chunks[0]->max_time;
    uint64_t last_ref = chunks[0]->ref;
    buf2.put_signed_variant(chunks[0]->min_time);
    buf2.put_unsigned_variant(
        static_cast<uint64_t>(chunks[0]->max_time - chunks[0]->min_time));
    buf2.put_unsigned_variant(chunks[0]->ref);

    for (int i = 1; i < chunks.size(); i++) {
      // LOG_INFO << chunks[i]->min_time - last_t;
      buf2.put_unsigned_variant(
          static_cast<uint64_t>(chunks[i]->min_time - last_t));
      // LOG_INFO << chunks[i]->max_time - chunks[i]->min_time;
      buf2.put_unsigned_variant(
          static_cast<uint64_t>(chunks[i]->max_time - chunks[i]->min_time));
      buf2.put_signed_variant(static_cast<int64_t>(chunks[i]->ref - last_ref));
      last_t = chunks[i]->max_time;
      last_ref = chunks[i]->ref;
    }
  }
  // LOG_INFO << "stage2";
  buf1.reset();
  buf1.put_unsigned_variant(buf2.len());           // Len in the beginning
  buf2.put_BE_uint32(base::GetCrc32(buf2.get()));  // Crc32 in the end

  write({buf1.get(), buf2.get()});

  last_series = l;
  return 0;
}

int IndexWriter::add_series(uint64_t id, const label::Labels &l, uint64_t ref) {
  if (ensure_stage(IDX_STAGE_SERIES) == -1) return INVALID_STAGE;
  if (label::lbs_compare(l, last_series) < 0) {
    LOG_ERROR << "Out of order series";
    return OUT_OF_ORDER;
  }

  if (series.find(id) != series.end()) {
    LOG_ERROR << "Labels already added";
    return EXISTED;
  }

  // Align each entry to 16 bytes
  add_padding(16);
  series[id] = pos / 16;
  buf2.reset();
  buf2.put_unsigned_variant(l.size());
  for (auto &label : l) {
    if (symbols.find(label.label) == symbols.end()) {
      LOG_ERROR << "Symbol not existed: " << label.label;
      return CANNOT_FIND;
    }
    buf2.put_unsigned_variant(symbols[label.label]);
    if (symbols.find(label.value) == symbols.end()) {
      LOG_ERROR << "Symbol not existed: " << label.value;
      return CANNOT_FIND;
    }
    buf2.put_unsigned_variant(symbols[label.value]);
  }
  // LOG_INFO << "stage2";
  buf1.reset();
  buf1.put_unsigned_variant(buf2.len());           // Len in the beginning
  buf1.put_unsigned_variant(ref);                  // TSID.
  buf2.put_BE_uint32(base::GetCrc32(buf2.get()));  // Crc32 in the end

  write({buf1.get(), buf2.get()});

  last_series = l;
  return 0;
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
//
// Will sort values
int IndexWriter::write_label_index(const std::deque<std::string> &names,
                                   const std::deque<std::string> &values) {
  if (values.size() % names.size() != 0) {
    LOG_ERROR << "Invalid length for names and values";
    return INVALID_LENGTH;
  }
  if (ensure_stage(IDX_STAGE_LABEL_INDEX) == -1) return INVALID_STAGE;

  tsdbutil::StringTuples value_tuples(values, names.size());
  value_tuples.sort();

  add_padding(4);

  // So the label values can be tracked by names
  label_indices.emplace_back(names, pos);

  // | #names | #entries |
  buf2.reset();
  buf2.put_BE_uint32(names.size());
  buf2.put_BE_uint32(value_tuples.len());

  for (std::string &str : value_tuples.entries) {
    if (symbols.find(str) == symbols.end()) {
      LOG_ERROR << "Symbol not existed: " << str;
      return CANNOT_FIND;
    }
    buf2.put_BE_uint32(symbols[str]);
  }

  buf1.reset();
  buf1.put_BE_uint32(buf2.len());                  // Len in the beginning
  buf2.put_BE_uint32(base::GetCrc32(buf2.get()));  // Crc32 in the end

  write({buf1.get(), buf2.get()});
  return 0;
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
// Align to 4 bytes
int IndexWriter::write_postings(const std::string &name,
                                const std::string &value,
                                const PostingsInterface *values) {
  if (ensure_stage(IDX_STAGE_POSTINGS) == -1) return INVALID_STAGE;

  add_padding(4);

  // So the postings list can be tracked by the label pair
  postings.emplace_back(std::deque<std::string>({name, value}), pos);

  uint32_cache.clear();

  while (values->next()) {
    uint64_t ref = values->at();
    if (series.find(ref) == series.end()) {
      std::cout << "Series reference not existed: " << ref << std::endl;
      LOG_ERROR << "Series reference not existed: " << ref;
      return CANNOT_FIND;
    }
    uint64_t offset = series[ref];
    if (offset > (static_cast<uint64_t>(1) << 32) - 1) {
      LOG_ERROR << "Series offset exceeds 4 bytes: " << offset;
      return INVALID_LENGTH;
    }
    uint32_cache.push_back(static_cast<uint32_t>(offset));
  }

  uint32_cache.sort();

  buf2.reset();
  buf2.put_BE_uint32(uint32_cache.size());  // #entries

  for (auto it = uint32_cache.begin(); it != uint32_cache.end(); it++) {
    buf2.put_BE_uint32(*it);
  }

  buf1.reset();
  buf1.put_BE_uint32(buf2.len());
  buf2.put_BE_uint32(base::GetCrc32(buf2.get()));  // Crc32 in the end

  write({buf1.get(), buf2.get()});
  return 0;
}

int IndexWriter::write_postings_and_tsids(const std::string &name,
                                          const std::string &value,
                                          const PostingsInterface *values) {
  if (ensure_stage(IDX_STAGE_POSTINGS) == -1) return INVALID_STAGE;

  add_padding(4);

  // So the postings list can be tracked by the label pair
  postings.emplace_back(std::deque<std::string>({name, value}), pos);

  uint32_cache.clear();

  std::unordered_map<uint32_t, uint64_t> tmp_m;
  while (values->next()) {
    uint64_t ref = values->at();
    if (series.find(ref) == series.end()) {
      std::cout << "Series reference not existed: " << ref << std::endl;
      LOG_ERROR << "Series reference not existed: " << ref;
      return CANNOT_FIND;
    }
    uint64_t offset = series[ref];
    if (offset > (static_cast<uint64_t>(1) << 32) - 1) {
      LOG_ERROR << "Series offset exceeds 4 bytes: " << offset;
      return INVALID_LENGTH;
    }
    uint32_cache.push_back(static_cast<uint32_t>(offset));
    tmp_m.emplace(offset, ref);
  }

  uint32_cache.sort();

  buf2.reset();
  buf2.put_BE_uint32(uint32_cache.size());  // #entries

  for (auto it = uint32_cache.begin(); it != uint32_cache.end(); it++) {
    buf2.put_BE_uint32(*it);
    buf2.put_BE_uint32(tmp_m[*it]);
  }

  buf1.reset();
  buf1.put_BE_uint32(buf2.len());
  buf2.put_BE_uint32(base::GetCrc32(buf2.get()));  // Crc32 in the end

  write({buf1.get(), buf2.get()});
  return 0;
}

// ┌────────────────────┬────────────────────┐
// │ len <4b>           │ #entries <4b>      │
// ├────────────────────┴────────────────────┤
// │ ┌──────────────────┬──────────────────┐ │
// │ │ SST1 <4b>        │ TSID1 <8b>       │ │
// │ ├──────────────────┴──────────────────┤ │
// │ │ ...                                 │ │
// │ ├──────────────────┬──────────────────┤ │
// │ │ SSTn <4b>        │ TSIDn <8b>       │ │
// │ └──────────────────┴──────────────────┘ │
// ├─────────────────────────────────────────┤
// │ CRC32 <4b>                              │
// └─────────────────────────────────────────┘
int IndexWriter::write_sst_range(const std::vector<uint32_t> &sst,
                                 const std::vector<uint64_t> &tsid) {
  if (ensure_stage(IDX_STAGE_SST) == -1) return INVALID_STAGE;

  buf2.reset();
  buf2.put_BE_uint32(sst.size());  // #entries

  for (size_t i = 0; i < sst.size(); i++) {
    buf2.put_BE_uint32(sst[i]);
    buf2.put_BE_uint64(tsid[i]);
  }

  buf1.reset();
  buf1.put_BE_uint32(buf2.len());
  buf2.put_BE_uint32(base::GetCrc32(buf2.get()));  // Crc32 in the end

  write({buf1.get(), buf2.get()});
  return 0;
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
// Need to record the starting offset in the TOC first
void IndexWriter::write_offset_table(const std::deque<HashEntry> &table) {
  buf2.reset();
  buf2.put_BE_uint32(table.size());

  for (HashEntry const &hashentry : table) {
    buf2.put_unsigned_variant(hashentry.keys.size());
    for (std::string const &key : hashentry.keys) {
      buf2.put_uvariant_string(key);
    }
    buf2.put_unsigned_variant(hashentry.offset);
  }

  buf1.reset();
  buf1.put_BE_uint32(buf2.len());
  buf2.put_BE_uint32(base::GetCrc32(buf2.get()));  // Crc32 in the end

  write({buf1.get(), buf2.get()});
}

// ┌─────────────────────────────────────────┐
// │ ref(symbols) <8b>                       │
// ├─────────────────────────────────────────┤
// │ ref(series) <8b>                        │
// ├─────────────────────────────────────────┤
// │ ref(label indices start) <8b>           │
// ├─────────────────────────────────────────┤
// │ ref(label indices table) <8b>           │
// ├─────────────────────────────────────────┤
// │ ref(postings start) <8b>                │
// ├─────────────────────────────────────────┤
// │ ref(postings table) <8b>                │
// ├─────────────────────────────────────────┤
// │ CRC32 <4b>                              │
// └─────────────────────────────────────────┘
void IndexWriter::write_TOC() {
  buf1.reset();
  buf1.put_BE_uint64(toc.symbols);
  buf1.put_BE_uint64(toc.series);
  buf1.put_BE_uint64(toc.label_indices);
  buf1.put_BE_uint64(toc.label_indices_table);
  buf1.put_BE_uint64(toc.postings);
  buf1.put_BE_uint64(toc.postings_table);
  buf1.put_BE_uint64(toc.sst_table);
  buf1.put_BE_uint32(base::GetCrc32(buf1.get()));  // Crc32 in the end

  write({buf1.get()});
}

void IndexWriter::close() {
  if (f == nullptr) return;
  ensure_stage(IDX_STAGE_DONE);
  // #ifdef INDEX_DEBUG
  // LOG_DEBUG << toc_string(toc) << " end:" << pos;
  // LOG_DEBUG << toc_portion_string(toc, pos);
  // #endif
  fflush(f);
  fclose(f);
  f = nullptr;

  if (s3_wrapper_) {
    std::unique_ptr<cloud::S3WritableFile> wf;
    s3_wrapper_->NewWritableFile(filename_, &wf);
  }
}

IndexWriter::~IndexWriter() { close(); }

}  // namespace index
}  // namespace tsdb