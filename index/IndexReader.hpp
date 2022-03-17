#ifndef INDEXREADER_H
#define INDEXREADER_H

#include "block/BlockUtils.hpp"
#include "block/IndexReaderInterface.hpp"
#include "index/IndexUtils.hpp"
#include "index/TOC.hpp"
#include "tsdbutil/ByteSlice.hpp"
#include "tsdbutil/SerializedStringTuples.hpp"

namespace tsdb {
namespace index {

class IndexReader : public block::IndexReaderInterface {
 private:
  std::shared_ptr<tsdbutil::ByteSlice> b;

  bool err_;

  // offset table
  std::unordered_map<std::string, uint64_t> label_indices_table;
  std::unordered_map<std::string, std::unordered_map<std::string, uint64_t>>
      postings_table;

  std::deque<std::string> symbols_;

  // OPTIONAL(Alec).
  std::deque<uint64_t> all_postings;

  std::deque<uint64_t> all_postings_tsid;

 public:
  IndexReader(std::shared_ptr<tsdbutil::ByteSlice> b);
  IndexReader(const std::string &filename);

  void init();

  // OPTIONAL(Alec).
  void get_all_postings();

  bool validate(const std::shared_ptr<tsdbutil::ByteSlice> &b);

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
  bool read_symbols(const TOC &toc);

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

  std::set<std::string> symbols();

  const std::deque<std::string> &symbols_deque() const;

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
  std::unique_ptr<tsdbutil::StringTuplesInterface> label_values(
      const std::initializer_list<std::string> &names);

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
  std::pair<std::unique_ptr<PostingsInterface>, bool> postings(
      const std::string &name, const std::string &value);
  std::pair<std::unique_ptr<PostingsInterface>, bool> postings_tsid(
      const std::string &name, const std::string &value);

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
  bool series(uint64_t ref, label::Labels &lset,
              std::deque<std::shared_ptr<chunk::ChunkMeta>> &chunks);

  // NOTE(Alec), for LSM index.
  bool series(uint64_t ref, label::Labels &lset, uint64_t *tsid);

  std::unique_ptr<index::PostingsInterface> sorted_postings(
      std::unique_ptr<index::PostingsInterface> &&p);

  // Return a postings of offsets.
  std::unique_ptr<index::PostingsInterface> sorted_postings();

  // Return a postings of tsid instead of offsets.
  std::unique_ptr<index::PostingsInterface> sorted_postings_tsid();

  bool error();
  uint64_t size();
};

uint64_t symbol_table_size(const IndexReader &indexr);

}  // namespace index
}  // namespace tsdb

#endif