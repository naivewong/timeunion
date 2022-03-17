#ifndef INDEXWRITER_H
#define INDEXWRITER_H

#include <deque>
#include <initializer_list>
#include <set>
#include <unordered_map>

#include "block/IndexWriterInterface.hpp"
#include "cloud/S3.hpp"
#include "index/HashEntry.hpp"
#include "index/IndexUtils.hpp"
#include "index/TOC.hpp"
#include "tsdbutil/CacheVector.hpp"
#include "tsdbutil/EncBuf.hpp"

namespace tsdb {
namespace index {

class IndexWriter : public block::IndexWriterInterface {
 private:
  FILE *f;
  uint64_t pos;

  TOC toc;
  IndexWriterStage stage;

  tsdbutil::EncBuf buf1;
  tsdbutil::EncBuf buf2;

  tsdbutil::CacheVector<uint32_t>
      uint32_cache;  // For sorting when writing postings list

  std::unordered_map<std::string, uint32_t> symbols;  // Symbols offsets
  std::unordered_map<uint64_t, uint64_t> series;      // Series offsets
  std::deque<HashEntry> label_indices;                // Label index offsets
  std::deque<HashEntry> postings;                     // Postings offsets

  label::Labels last_series;
  int version;

  std::string filename_;
  cloud::S3Wrapper *s3_wrapper_;

 public:
  // All the dirs inside filename should be existed.
  IndexWriter(const std::string &filename, cloud::S3Wrapper *wrapper = nullptr);
  IndexWriter() : f(nullptr), stage(IDX_STAGE_NONE) {}

  void write_meta();

  void write(std::initializer_list<std::pair<const uint8_t *, int>> l);

  void add_padding(uint64_t padding);

  // 0 succeed, -1 error
  int ensure_stage(IndexWriterStage s);

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
  int add_symbols(const std::unordered_set<std::string> &sym);
  int add_symbols(const std::set<std::string> &sym);

  // ┌──────────────────────────────────────────────────────────────────────────┐
  // │ len <uvarint> │
  // ├──────────────────────────────────────────────────────────────────────────┤
  // │ ┌──────────────────────────────────────────────────────────────────────┐
  // │ │ │                     labels count <uvarint64> │ │ │
  // ├──────────────────────────────────────────────────────────────────────┤ │
  // │ │              ┌────────────────────────────────────────────┐          │
  // │ │ │              │ ref(l_i.name) <uvarint32>                  │ │ │ │ │
  // ├────────────────────────────────────────────┤          │ │ │ │ │
  // ref(l_i.value) <uvarint32>                 │          │ │ │ │
  // └────────────────────────────────────────────┘          │ │ │ │ ... │ │ │
  // ├──────────────────────────────────────────────────────────────────────┤ │
  // │ │                     chunks count <uvarint64>                         │
  // │ │
  // ├──────────────────────────────────────────────────────────────────────┤ │
  // │ │              ┌────────────────────────────────────────────┐          │
  // │ │ │              │ c_0.mint <varint64>                        │ │ │ │ │
  // ├────────────────────────────────────────────┤          │ │ │ │ │ c_0.maxt
  // - c_0.mint <uvarint64>            │          │ │ │ │
  // ├────────────────────────────────────────────┤          │ │ │ │ │
  // ref(c_0.data) <uvarint64>                  │          │ │ │ │
  // └────────────────────────────────────────────┘          │ │ │ │
  // ┌────────────────────────────────────────────┐          │ │ │ │ │ c_i.mint
  // - c_i-1.maxt <uvarint64>          │          │ │ │ │
  // ├────────────────────────────────────────────┤          │ │ │ │ │ c_i.maxt
  // - c_i.mint <uvarint64>            │          │ │ │ │
  // ├────────────────────────────────────────────┤          │ │ │ │ │
  // ref(c_i.data) - ref(c_i-1.data) <varint64> │          │ │ │ │
  // └────────────────────────────────────────────┘          │ │ │ │ ... │ │ │
  // └──────────────────────────────────────────────────────────────────────┘ │
  // ├──────────────────────────────────────────────────────────────────────────┤
  // │ CRC32 <4b> │
  // └──────────────────────────────────────────────────────────────────────────┘
  //
  // Adrress series entry by 4 bytes reference
  // Align to 16 bytes
  // NOTICE: The ref here is just a temporary ref assigned as monotonically
  // increasing id in memory.
  //
  // chunks here better to be sorted by time.
  int add_series(uint64_t ref, const label::Labels &l,
                 const std::deque<std::shared_ptr<chunk::ChunkMeta>> &chunks);

  // NOTE(Alec), for LSM index.
  int add_series(uint64_t id, const label::Labels &l, uint64_t ref);

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
  int write_label_index(const std::deque<std::string> &names,
                        const std::deque<std::string> &values);

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
  int write_postings(const std::string &name, const std::string &value,
                     const PostingsInterface *values);
  int write_postings_and_tsids(const std::string &name,
                               const std::string &value,
                               const PostingsInterface *values);

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
  int write_sst_range(const std::vector<uint32_t> &sst,
                      const std::vector<uint64_t> &tsid);

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
  void write_offset_table(const std::deque<HashEntry> &table);

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
  void write_TOC();

  void close();
  ~IndexWriter();
};

}  // namespace index
}  // namespace tsdb

#endif