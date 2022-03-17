#pragma once

#include "leveldb/env.h"

#include "block/BlockUtils.hpp"
#include "block/IndexReaderInterface.hpp"
#include "index/IndexUtils.hpp"
#include "index/TOC.hpp"
#include "tsdbutil/ByteSlice.hpp"
#include "tsdbutil/SerializedStringTuples.hpp"

namespace leveldb {

class IndexReader : public ::tsdb::block::IndexReaderInterface {
 private:
  RandomAccessFile* b;
  bool err_;

  // offset table
  std::unordered_map<std::string, uint64_t> label_indices_table;
  std::unordered_map<std::string, std::unordered_map<std::string, uint64_t>>
      postings_table;

  std::deque<std::string> symbols_;

  // OPTIONAL(Alec).
  std::deque<uint64_t> all_postings;

  std::deque<uint64_t> all_postings_tsid;

  // SST ranges.
  std::vector<uint32_t> ssts_;
  std::vector<uint64_t> sst_tsids_;

 public:
  IndexReader(const std::string& name);
  IndexReader(RandomAccessFile* b);
  ~IndexReader() {
    if (b) delete b;
  }

  void init();

  // OPTIONAL(Alec).
  void get_all_postings();

  bool validate();

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
  bool read_symbols(const ::tsdb::index::TOC& toc);

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
  bool read_offset_table(uint64_t offset, bool label_index);

  bool read_sst_range(uint64_t offset);

  std::set<std::string> symbols();

  const std::deque<std::string>& symbols_deque() const;

  std::string lookup_symbols(uint64_t offset);

  std::deque<std::string> label_names();

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
  std::unique_ptr<::tsdb::tsdbutil::StringTuplesInterface> label_values(
      const std::initializer_list<std::string>& names);

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
  std::pair<std::unique_ptr<::tsdb::index::PostingsInterface>, bool> postings(
      const std::string& name, const std::string& value);
  std::pair<std::unique_ptr<::tsdb::index::PostingsInterface>, bool>
  postings_tsid(const std::string& name, const std::string& value) {
    return {nullptr, false};
  }

  // NOTE(Alec), only use this when using IndexWriter::write_postings_and_tsids.
  std::pair<std::unique_ptr<::tsdb::index::PostingsInterface>, bool>
  postings_and_tsids(const std::string& name, const std::string& value);

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
  bool series(uint64_t ref, ::tsdb::label::Labels& lset,
              std::deque<std::shared_ptr<::tsdb::chunk::ChunkMeta>>& chunks);

  // NOTE(Alec), for LSM index.
  bool series(uint64_t ref, ::tsdb::label::Labels& lset, uint64_t* tsid);

  std::unique_ptr<::tsdb::index::PostingsInterface> sorted_postings(
      std::unique_ptr<::tsdb::index::PostingsInterface>&& p);

  // Return a postings of offsets.
  std::unique_ptr<::tsdb::index::PostingsInterface> sorted_postings();

  // Return a postings of tsid instead of offsets.
  std::unique_ptr<::tsdb::index::PostingsInterface> sorted_postings_tsid();

  std::vector<uint32_t> get_ssts() { return ssts_; }
  std::vector<uint64_t> get_sst_tsids() { return sst_tsids_; }
  uint32_t get_sst(uint64_t tsid) {
    // lower bound.
    int mid;
    int low = 0;
    int high = sst_tsids_.size();
    while (low < high) {
      mid = low + (high - low) / 2;
      if (tsid <= sst_tsids_[mid])
        high = mid;
      else
        low = mid + 1;
    }

    // Protect the overflow.
    if (low == sst_tsids_.size()) return 0;
    return ssts_[low];
  }

  bool error();
  uint64_t size();
};

}  // namespace leveldb
