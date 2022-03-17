#include "tsdbutil/RecordDecoder.hpp"

namespace tsdb {
namespace tsdbutil {

RECORD_ENTRY_TYPE RecordDecoder::type(const std::vector<uint8_t> &rec) {
  if (rec.empty()) return RECORD_INVALID;
  if (rec[0] != RECORD_SERIES && rec[0] != RECORD_SAMPLES &&
      rec[0] != RECORD_TOMBSTONES)
    return RECORD_INVALID;
  return rec[0];
}
RECORD_ENTRY_TYPE RecordDecoder::type(const uint8_t *rec, int length) {
  if (length < 1) return RECORD_INVALID;
  if (rec[0] != RECORD_SERIES && rec[0] != RECORD_SAMPLES &&
      rec[0] != RECORD_TOMBSTONES)
    return RECORD_INVALID;
  return rec[0];
}

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
error::Error RecordDecoder::series(const std::vector<uint8_t> &rec,
                                   std::deque<RefSeries> &refseries) {
  tsdbutil::DecBuf decbuf(&(rec[0]), rec.size());
  if (decbuf.get_byte() != RECORD_SERIES)
    return error::Error("invalid record type");

  while (decbuf.len() > 0 && decbuf.error() == NO_ERR) {
    uint64_t ref = decbuf.get_BE_uint64();

    uint64_t num_lbs = decbuf.get_unsigned_variant();
    label::Labels lset;
    for (int i = 0; i < num_lbs; ++i) {
      std::string label = decbuf.get_uvariant_string();
      lset.emplace_back(label, decbuf.get_uvariant_string());
    }

    if (decbuf.error() != NO_ERR) return error::Error(decbuf.error_str());

    std::sort(lset.begin(), lset.end());
    refseries.emplace_back(ref, lset);
  }

  if (decbuf.error() != NO_ERR) return error::Error(decbuf.error_str());
  if (decbuf.len() > 0)
    return error::Error("unexpected " + std::to_string(decbuf.len()) +
                        " bytes left in entry");
  return error::Error();
}
error::Error RecordDecoder::series(const uint8_t *rec, int length,
                                   std::deque<RefSeries> &refseries) {
  tsdbutil::DecBuf decbuf(rec, length);
  if (decbuf.get_byte() != RECORD_SERIES)
    return error::Error("invalid record type");

  while (decbuf.len() > 0 && decbuf.error() == NO_ERR) {
    uint64_t ref = decbuf.get_BE_uint64();

    uint64_t num_lbs = decbuf.get_unsigned_variant();
    label::Labels lset;
    for (int i = 0; i < num_lbs; ++i) {
      std::string label = decbuf.get_uvariant_string();
      lset.emplace_back(label, decbuf.get_uvariant_string());
    }

    if (decbuf.error() != NO_ERR) return error::Error(decbuf.error_str());

    std::sort(lset.begin(), lset.end());
    refseries.emplace_back(ref, lset);
  }

  if (decbuf.error() != NO_ERR) return error::Error(decbuf.error_str());
  if (decbuf.len() > 0)
    return error::Error("unexpected " + std::to_string(decbuf.len()) +
                        " bytes left in entry");
  return error::Error();
}

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
// error::Error RecordDecoder::group_series(const std::vector<uint8_t> &rec,
//                                          std::deque<RefGroupSeries> &rgs) {
//   tsdbutil::DecBuf decbuf(&(rec[0]), rec.size());
//   if (decbuf.get_byte() != RECORD_GROUP_SERIES)
//     return error::Error("invalid record type");

//   while (decbuf.len() > 0 && decbuf.error() == NO_ERR) {
//     uint64_t group_ref = decbuf.get_BE_uint64();
//     uint64_t num_series = decbuf.get_unsigned_variant();
//     rgs.emplace_back(group_ref);

//     for (int i = 0; i < num_series; ++i) {
//       uint64_t ref = decbuf.get_BE_uint64();

//       uint64_t num_lbs = decbuf.get_unsigned_variant();
//       label::Labels lset;
//       for (int i = 0; i < num_lbs; ++i) {
//         std::string label = decbuf.get_uvariant_string();
//         lset.emplace_back(label, decbuf.get_uvariant_string());
//       }

//       if (decbuf.error() != NO_ERR) return error::Error(decbuf.error_str());

//       std::sort(lset.begin(), lset.end());
//       rgs.back().push_back(ref, lset);
//     }
//   }

//   if (decbuf.error() != NO_ERR) return error::Error(decbuf.error_str());
//   if (decbuf.len() > 0)
//     return error::Error("unexpected " + std::to_string(decbuf.len()) +
//                         " bytes left in entry");
//   return error::Error();
// }
// error::Error RecordDecoder::group_series(const uint8_t *rec, int length,
//                                          std::deque<RefGroupSeries> &rgs) {
//   tsdbutil::DecBuf decbuf(rec, length);
//   if (decbuf.get_byte() != RECORD_GROUP_SERIES)
//     return error::Error("invalid record type");

//   while (decbuf.len() > 0 && decbuf.error() == NO_ERR) {
//     uint64_t group_ref = decbuf.get_BE_uint64();
//     uint64_t num_series = decbuf.get_unsigned_variant();
//     rgs.emplace_back(group_ref);

//     for (int i = 0; i < num_series; ++i) {
//       uint64_t ref = decbuf.get_BE_uint64();

//       uint64_t num_lbs = decbuf.get_unsigned_variant();
//       label::Labels lset;
//       for (int i = 0; i < num_lbs; ++i) {
//         std::string label = decbuf.get_uvariant_string();
//         lset.emplace_back(label, decbuf.get_uvariant_string());
//       }

//       if (decbuf.error() != NO_ERR) return error::Error(decbuf.error_str());

//       std::sort(lset.begin(), lset.end());
//       rgs.back().push_back(ref, lset);
//     }
//   }

//   if (decbuf.error() != NO_ERR) return error::Error(decbuf.error_str());
//   if (decbuf.len() > 0)
//     return error::Error("unexpected " + std::to_string(decbuf.len()) +
//                         " bytes left in entry");
//   return error::Error();
// }

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
error::Error RecordDecoder::samples(const std::vector<uint8_t> &rec,
                                    std::deque<RefSample> &refsamples) {
  tsdbutil::DecBuf decbuf(&(rec[0]), rec.size());

  if (decbuf.get_byte() != RECORD_SAMPLES)
    return error::Error("invalid record type");
  if (decbuf.len() == 0) return error::Error();

  uint64_t base_ref = decbuf.get_BE_uint64();
  int64_t base_time = static_cast<int64_t>(decbuf.get_BE_uint64());
  uint64_t value = decbuf.get_BE_uint64();
  refsamples.emplace_back(base_ref, base_time, base::decode_double(value));
  while (decbuf.len() > 0 && decbuf.error() == NO_ERR) {
    int64_t id_delta = decbuf.get_signed_variant();
    int64_t time_delta = decbuf.get_signed_variant();
    value = decbuf.get_BE_uint64();
    refsamples.emplace_back(
        static_cast<uint64_t>(static_cast<int64_t>(base_ref) + id_delta),
        base_time + time_delta, base::decode_double(value));
  }

  if (decbuf.error() != NO_ERR) return error::Error(decbuf.error_str());
  if (decbuf.len() > 0)
    return error::Error("unexpected " + std::to_string(decbuf.len()) +
                        " bytes left in entry");
  return error::Error();
}
error::Error RecordDecoder::samples(const uint8_t *rec, int length,
                                    std::deque<RefSample> &refsamples) {
  tsdbutil::DecBuf decbuf(rec, length);

  if (decbuf.get_byte() != RECORD_SAMPLES)
    return error::Error("invalid record type");
  if (decbuf.len() == 0) return error::Error();

  uint64_t base_ref = decbuf.get_BE_uint64();
  int64_t base_time = static_cast<int64_t>(decbuf.get_BE_uint64());
  uint64_t value = decbuf.get_BE_uint64();
  refsamples.emplace_back(base_ref, base_time, base::decode_double(value));
  while (decbuf.len() > 0 && decbuf.error() == NO_ERR) {
    int64_t id_delta = decbuf.get_signed_variant();
    int64_t time_delta = decbuf.get_signed_variant();
    value = decbuf.get_BE_uint64();
    refsamples.emplace_back(
        static_cast<uint64_t>(static_cast<int64_t>(base_ref) + id_delta),
        base_time + time_delta, base::decode_double(value));
  }

  if (decbuf.error() != NO_ERR) return error::Error(decbuf.error_str());
  if (decbuf.len() > 0)
    return error::Error("unexpected " + std::to_string(decbuf.len()) +
                        " bytes left in entry");
  return error::Error();
}

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
// error::Error RecordDecoder::group_samples(const std::vector<uint8_t> &rec,
//                                           std::deque<RefGroupSample> &rgs) {
//   tsdbutil::DecBuf decbuf(&(rec[0]), rec.size());

//   if (decbuf.get_byte() != RECORD_GROUP_SAMPLES)
//     return error::Error("invalid record type");
//   if (decbuf.len() == 0) return error::Error();

//   while (decbuf.len() > 0 && decbuf.error() == NO_ERR) {
//     uint64_t group_ref = decbuf.get_BE_uint64();
//     uint64_t num_series = decbuf.get_unsigned_variant();
//     rgs.emplace_back(group_ref, decbuf.get_BE_uint64());

//     uint64_t base_ref = decbuf.get_BE_uint64();
//     uint64_t value = decbuf.get_BE_uint64();
//     rgs.back().push_back(base_ref, base::decode_double(value));
//     for (int i = 1; i < num_series; ++i) {
//       int64_t id_delta = decbuf.get_signed_variant();
//       value = decbuf.get_BE_uint64();
//       rgs.back().push_back(
//           static_cast<uint64_t>(static_cast<int64_t>(base_ref) + id_delta),
//           base::decode_double(value));
//     }
//   }

//   if (decbuf.error() != NO_ERR) return error::Error(decbuf.error_str());
//   if (decbuf.len() > 0)
//     return error::Error("unexpected " + std::to_string(decbuf.len()) +
//                         " bytes left in entry");
//   return error::Error();
// }
// error::Error RecordDecoder::group_samples(const uint8_t *rec, int length,
//                                           std::deque<RefGroupSample> &rgs) {
//   tsdbutil::DecBuf decbuf(rec, length);

//   if (decbuf.get_byte() != RECORD_GROUP_SAMPLES)
//     return error::Error("invalid record type");
//   if (decbuf.len() == 0) return error::Error();

//   while (decbuf.len() > 0 && decbuf.error() == NO_ERR) {
//     uint64_t group_ref = decbuf.get_BE_uint64();
//     uint64_t num_series = decbuf.get_unsigned_variant();
//     rgs.emplace_back(group_ref, decbuf.get_BE_uint64());

//     uint64_t base_ref = decbuf.get_BE_uint64();
//     uint64_t value = decbuf.get_BE_uint64();
//     rgs.back().push_back(base_ref, base::decode_double(value));
//     for (int i = 1; i < num_series; ++i) {
//       int64_t id_delta = decbuf.get_signed_variant();
//       value = decbuf.get_BE_uint64();
//       rgs.back().push_back(
//           static_cast<uint64_t>(static_cast<int64_t>(base_ref) + id_delta),
//           base::decode_double(value));
//     }
//   }

//   if (decbuf.error() != NO_ERR) return error::Error(decbuf.error_str());
//   if (decbuf.len() > 0)
//     return error::Error("unexpected " + std::to_string(decbuf.len()) +
//                         " bytes left in entry");
//   return error::Error();
// }

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
error::Error RecordDecoder::tombstones(const std::vector<uint8_t> &rec,
                                       std::deque<Stone> &stones) {
  tsdbutil::DecBuf decbuf(&(rec[0]), rec.size());

  if (decbuf.get_byte() != RECORD_TOMBSTONES)
    return error::Error("invalid record type");

  while (decbuf.len() > 0 && decbuf.error() == NO_ERR) {
    uint64_t ref = decbuf.get_BE_uint64();
    int64_t t1 = decbuf.get_signed_variant();
    int64_t t2 = decbuf.get_signed_variant();
    stones.emplace_back(ref, tombstone::Intervals({{t1, t2}}));
  }

  if (decbuf.error() != NO_ERR) return error::Error(decbuf.error_str());
  if (decbuf.len() > 0)
    return error::Error("unexpected " + std::to_string(decbuf.len()) +
                        " bytes left in entry");
  return error::Error();
}
error::Error RecordDecoder::tombstones(const uint8_t *rec, int length,
                                       std::deque<Stone> &stones) {
  tsdbutil::DecBuf decbuf(rec, length);

  if (decbuf.get_byte() != RECORD_TOMBSTONES)
    return error::Error("invalid record type");

  while (decbuf.len() > 0 && decbuf.error() == NO_ERR) {
    uint64_t ref = decbuf.get_BE_uint64();
    int64_t t1 = decbuf.get_signed_variant();
    int64_t t2 = decbuf.get_signed_variant();
    stones.emplace_back(ref, tombstone::Intervals({{t1, t2}}));
  }

  if (decbuf.error() != NO_ERR) return error::Error(decbuf.error_str());
  if (decbuf.len() > 0)
    return error::Error("unexpected " + std::to_string(decbuf.len()) +
                        " bytes left in entry");
  return error::Error();
}

// error::Error RecordDecoder::group_tombstones(const std::vector<uint8_t> &rec,
//                                              std::deque<Stone> &stones) {
//   tsdbutil::DecBuf decbuf(&(rec[0]), rec.size());

//   if (decbuf.get_byte() != RECORD_GROUP_TOMBSTONES)
//     return error::Error("invalid record type");

//   while (decbuf.len() > 0 && decbuf.error() == NO_ERR) {
//     uint64_t ref = decbuf.get_BE_uint64();
//     int64_t t1 = decbuf.get_signed_variant();
//     int64_t t2 = decbuf.get_signed_variant();
//     stones.emplace_back(ref, tombstone::Intervals({{t1, t2}}));
//   }

//   if (decbuf.error() != NO_ERR) return error::Error(decbuf.error_str());
//   if (decbuf.len() > 0)
//     return error::Error("unexpected " + std::to_string(decbuf.len()) +
//                         " bytes left in entry");
//   return error::Error();
// }
// error::Error RecordDecoder::group_tombstones(const uint8_t *rec, int length,
//                                              std::deque<Stone> &stones) {
//   tsdbutil::DecBuf decbuf(rec, length);

//   if (decbuf.get_byte() != RECORD_GROUP_TOMBSTONES)
//     return error::Error("invalid record type");

//   while (decbuf.len() > 0 && decbuf.error() == NO_ERR) {
//     uint64_t ref = decbuf.get_BE_uint64();
//     int64_t t1 = decbuf.get_signed_variant();
//     int64_t t2 = decbuf.get_signed_variant();
//     stones.emplace_back(ref, tombstone::Intervals({{t1, t2}}));
//   }

//   if (decbuf.error() != NO_ERR) return error::Error(decbuf.error_str());
//   if (decbuf.len() > 0)
//     return error::Error("unexpected " + std::to_string(decbuf.len()) +
//                         " bytes left in entry");
//   return error::Error();
// }

}  // namespace tsdbutil
}  // namespace tsdb