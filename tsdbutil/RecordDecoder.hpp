#ifndef RECORDDECODER_H
#define RECORDDECODER_H

#include <deque>
#include <vector>

#include "base/Error.hpp"
#include "tsdbutil/DecBuf.hpp"
#include "tsdbutil/tsdbutils.hpp"

namespace tsdb {
namespace tsdbutil {

// RecordDecoder decodes series, sample, and tombstone records.
// The zero value is ready to use.
class RecordDecoder {
 public:
  static RECORD_ENTRY_TYPE type(const std::vector<uint8_t> &rec);
  static RECORD_ENTRY_TYPE type(const uint8_t *rec, int length);

  // ┌────────────────────────────────────────────┐
  // │ type = 1 <1b>                              │
  // ├────────────────────────────────────────────┤
  // │ ┌─────────┬──────────────────────────────┐ │
  // │ │ id <8b> │ n = len(labels) <uvarint>    │ │
  // │ ├─────────┴────────────┬─────────────────┤ │
  // │ │ len(str_1) <uvarint> │ str_1 <bytes>   │ │
  // │ ├──────────────────────┴─────────────────┤ │
  // │ │  ...                                   │ │
  // │ ├───────────────────────┬────────────────┤ │
  // │ │ len(str_2n) <uvarint> │ str_2n <bytes> │ │
  // │ └───────────────────────┴────────────────┘ │
  // │                  . . .                     │
  // └────────────────────────────────────────────┘
  //
  // Must pass an existed array.
  // Series appends series in rec to the given slice.
  static error::Error series(const std::vector<uint8_t> &rec,
                             std::deque<RefSeries> &refseries);
  static error::Error series(const uint8_t *rec, int length,
                             std::deque<RefSeries> &refseries);

  // ┌────────────────────────────────────────────────┐
  // │ type = 4 <1b>                                  │
  // ├────────────────────────────────────────────────┤
  // │ ┌────────────────────────────────────────────┐ │
  // │ │ ┌───────────────┬────────────────────────┐ │ │
  // │ │ │ group id <8b> │ num_series <uvarint>   │ │ │
  // │ │ └───────────────┴────────────────────────┘ │ │
  // │ │ ┌─────────┬──────────────────────────────┐ │ │
  // │ │ │ id <8b> │ n = len(labels) <uvarint>    │ │ │
  // │ │ ├─────────┴────────────┬─────────────────┤ │ │
  // │ │ │ len(str_1) <uvarint> │ str_1 <bytes>   │ │ │
  // │ │ ├──────────────────────┴─────────────────┤ │ │
  // │ │ │  ...                                   │ │ │
  // │ │ ├───────────────────────┬────────────────┤ │ │
  // │ │ │ len(str_2n) <uvarint> │ str_2n <bytes> │ │ │
  // │ │ └───────────────────────┴────────────────┘ │ │
  // │ │                  . . .                     │ │
  // │ └────────────────────────────────────────────┘ │
  // │                    . . .                       │
  // └────────────────────────────────────────────────┘
  // static error::Error group_series(const std::vector<uint8_t> &rec,
  //                                  std::deque<RefGroupSeries> &rgs);
  // static error::Error group_series(const uint8_t *rec, int length,
  //                                  std::deque<RefGroupSeries> &rgs);

  // ┌──────────────────────────────────────────────────────────────────┐
  // │ type = 2 <1b>                                                    │
  // ├──────────────────────────────────────────────────────────────────┤
  // │ ┌────────────────────┬───────────────────────────┬─────────────┐ │
  // │ │ id <8b>            │ timestamp <8b>            │ value <8b>  │ │
  // │ └────────────────────┴───────────────────────────┴─────────────┘ │
  // │ ┌────────────────────┬───────────────────────────┬─────────────┐ │
  // │ │ id_delta <varint>  │ timestamp_delta <varint>  │ value <8b>  │ │
  // │ └────────────────────┴───────────────────────────┴─────────────┘ │
  // │                              . . .                               │
  // └──────────────────────────────────────────────────────────────────┘
  //
  // Samples appends samples in rec to the given slice.
  static error::Error samples(const std::vector<uint8_t> &rec,
                              std::deque<RefSample> &refsamples);
  static error::Error samples(const uint8_t *rec, int length,
                              std::deque<RefSample> &refsamples);

  // ┌──────────────────────────────────────────────────────────────────────┐
  // │ type = 5 <1b>                                                        │
  // ├──────────────────────────────────────────────────────────────────────┤
  // │ ┌──────────────────────────────────────────────────────────────────┐ │
  // │ │ ┌───────────────┬──────────────────────┬───────────────────────┐ │ │
  // │ │ │ group id <8b> │ num_series <uvarint> │ timestamp <8b>        │ │ │
  // │ │ └───────────────┴──────────────────────┴───────────────────────┘ │ │
  // │ │ ┌────────────────────┬─────────────────────────────────────────┐ │ │
  // │ │ │ id <8b>            │ value <8b>                              │ │ │
  // │ │ └────────────────────┴─────────────────────────────────────────┘ │ │
  // │ │ ┌────────────────────┬─────────────────────────────────────────┐ │ │
  // │ │ │ id_delta <uvarint> │ value <8b>                              │ │ │
  // │ │ └────────────────────┴─────────────────────────────────────────┘ │ │
  // │ │                              . . .                               │ │
  // │ └──────────────────────────────────────────────────────────────────┘ │
  // │                                . . .                                 │
  // └──────────────────────────────────────────────────────────────────────┘
  // static error::Error group_samples(const std::vector<uint8_t> &rec,
  //                                   std::deque<RefGroupSample> &rgs);
  // static error::Error group_samples(const uint8_t *rec, int length,
  //                                   std::deque<RefGroupSample> &rgs);

  // ┌─────────────────────────────────────────────────────┐
  // │ type = 3 <1b>                                       │
  // ├─────────────────────────────────────────────────────┤
  // │ ┌─────────┬───────────────────┬───────────────────┐ │
  // │ │ id <8b> │ min_time <varint> │ max_time <varint> │ │
  // │ └─────────┴───────────────────┴───────────────────┘ │
  // │                        . . .                        │
  // └─────────────────────────────────────────────────────┘
  //
  // Tombstones appends tombstones in rec to the given slice.
  static error::Error tombstones(const std::vector<uint8_t> &rec,
                                 std::deque<Stone> &stones);
  static error::Error tombstones(const uint8_t *rec, int length,
                                 std::deque<Stone> &stones);

  // ┌─────────────────────────────────────────────────────┐
  // │ type = 6 <1b>                                       │
  // ├─────────────────────────────────────────────────────┤
  // │ ┌─────────┬───────────────────┬───────────────────┐ │
  // │ │ id <8b> │ min_time <varint> │ max_time <varint> │ │
  // │ └─────────┴───────────────────┴───────────────────┘ │
  // │                        . . .                        │
  // └─────────────────────────────────────────────────────┘
  // static error::Error group_tombstones(const std::vector<uint8_t> &rec,
  //                                      std::deque<Stone> &stones);
  // static error::Error group_tombstones(const uint8_t *rec, int length,
  //                                      std::deque<Stone> &stones);
};

}  // namespace tsdbutil
}  // namespace tsdb

#endif