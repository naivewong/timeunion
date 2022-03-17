#include "tsdbutil/RecordEncoder.hpp"

namespace tsdb {
namespace tsdbutil {

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
// Series appends the encoded series to b and returns the resulting slice.
void RecordEncoder::series(const std::deque<RefSeries> &refseries,
                           std::vector<uint8_t> &rec) {
  tsdbutil::EncBuf encbuf(10 * refseries.size());
  encbuf.put_byte(RECORD_SERIES);

  for (const RefSeries &r : refseries) {
    encbuf.put_BE_uint64(r.ref);
    encbuf.put_unsigned_variant(r.lset.size());

    for (const label::Label &l : r.lset) {
      encbuf.put_uvariant_string(l.label);
      encbuf.put_uvariant_string(l.value);
    }
  }

  rec.insert(rec.end(), encbuf.b.begin(), encbuf.b.begin() + encbuf.index);
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
// void RecordEncoder::group_series(const std::deque<RefGroupSeries> &rgs,
//                                  std::vector<uint8_t> &rec) {
//   int size = 0;
//   for (const RefGroupSeries &g : rgs) size += 18 + 10 * g.series.size();
//   tsdbutil::EncBuf encbuf(size);
//   encbuf.put_byte(RECORD_GROUP_SERIES);

//   for (const RefGroupSeries &g : rgs) {
//     encbuf.put_BE_uint64(g.group_ref);
//     encbuf.put_unsigned_variant(g.series.size());
//     for (const RefSeries &r : g.series) {
//       encbuf.put_BE_uint64(r.ref);
//       encbuf.put_unsigned_variant(r.lset.size());

//       for (const label::Label &l : r.lset) {
//         encbuf.put_uvariant_string(l.label);
//         encbuf.put_uvariant_string(l.value);
//       }
//     }
//   }

//   rec.insert(rec.end(), encbuf.b.begin(), encbuf.b.begin() + encbuf.index);
// }
// void RecordEncoder::group_series(const RefGroupSeries &rgs,
//                                  std::vector<uint8_t> &rec) {
//   tsdbutil::EncBuf encbuf(18 + 10 * rgs.series.size());
//   encbuf.put_byte(RECORD_GROUP_SERIES);
//   encbuf.put_BE_uint64(rgs.group_ref);
//   encbuf.put_unsigned_variant(rgs.series.size());
//   for (const RefSeries &r : rgs.series) {
//     encbuf.put_BE_uint64(r.ref);
//     encbuf.put_unsigned_variant(r.lset.size());

//     for (const label::Label &l : r.lset) {
//       encbuf.put_uvariant_string(l.label);
//       encbuf.put_uvariant_string(l.value);
//     }
//   }
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
// Samples appends the encoded samples to b and returns the resulting slice.
void RecordEncoder::samples(const std::deque<RefSample> &refsamples,
                            std::vector<uint8_t> &rec) {
  if (refsamples.empty()) return;
  tsdbutil::EncBuf encbuf(10 * refsamples.size());
  encbuf.put_byte(RECORD_SAMPLES);

  encbuf.put_BE_uint64(refsamples[0].ref);
  encbuf.put_BE_uint64(static_cast<uint64_t>(refsamples[0].t));
  encbuf.put_BE_uint64(base::encode_double(refsamples[0].v));
  for (int i = 1; i < refsamples.size(); ++i) {
    encbuf.put_signed_variant(static_cast<int64_t>(refsamples[i].ref) -
                              static_cast<int64_t>(refsamples.front().ref));
    encbuf.put_signed_variant(refsamples[i].t - refsamples.front().t);
    encbuf.put_BE_uint64(base::encode_double(refsamples[i].v));
  }

  rec.insert(rec.end(), encbuf.b.begin(), encbuf.b.begin() + encbuf.index);
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
// void RecordEncoder::group_samples(const std::deque<RefGroupSample> &rgs,
//                                   std::vector<uint8_t> &rec) {
//   if (rgs.empty()) return;
//   int size = 0;
//   for (const RefGroupSample &g : rgs) size += 10 * g.samples.size() + 26;
//   tsdbutil::EncBuf encbuf(size);
//   encbuf.put_byte(RECORD_GROUP_SAMPLES);

//   for (const RefGroupSample &g : rgs) {
//     encbuf.put_BE_uint64(g.group_ref);
//     encbuf.put_unsigned_variant(g.samples.size());
//     encbuf.put_BE_uint64(g.timestamp);

//     encbuf.put_BE_uint64(g.ids[0]);
//     encbuf.put_BE_uint64(base::encode_double(g.samples[0]));
//     for (int i = 1; i < g.samples.size(); ++i) {
//       encbuf.put_signed_variant(static_cast<int64_t>(g.ids[i]) -
//                                 static_cast<int64_t>(g.ids.front()));
//       encbuf.put_BE_uint64(base::encode_double(g.samples[i]));
//     }
//   }

//   rec.insert(rec.end(), encbuf.b.begin(), encbuf.b.begin() + encbuf.index);
// }
// void RecordEncoder::group_samples(const RefGroupSample &rgs,
//                                   std::vector<uint8_t> &rec) {
//   tsdbutil::EncBuf encbuf(10 * rgs.samples.size() + 18);
//   encbuf.put_byte(RECORD_GROUP_SAMPLES);

//   encbuf.put_BE_uint64(rgs.group_ref);
//   encbuf.put_unsigned_variant(rgs.samples.size());
//   encbuf.put_BE_uint64(rgs.timestamp);

//   encbuf.put_BE_uint64(rgs.ids[0]);
//   encbuf.put_BE_uint64(base::encode_double(rgs.samples[0]));
//   for (int i = 1; i < rgs.samples.size(); ++i) {
//     encbuf.put_signed_variant(static_cast<int64_t>(rgs.ids[i]) -
//                               static_cast<int64_t>(rgs.ids.front()));
//     encbuf.put_BE_uint64(base::encode_double(rgs.samples[i]));
//   }

//   rec.insert(rec.end(), encbuf.b.begin(), encbuf.b.begin() + encbuf.index);
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
// Tombstones appends the encoded tombstones to b and returns the resulting
// slice.
void RecordEncoder::tombstones(const std::deque<Stone> &stones,
                               std::vector<uint8_t> &rec) {
  tsdbutil::EncBuf encbuf(10 * stones.size());
  encbuf.put_byte(RECORD_TOMBSTONES);

  for (const Stone &s : stones) {
    for (const tombstone::Interval &i : s.itvls) {
      encbuf.put_BE_uint64(s.ref);
      encbuf.put_signed_variant(i.min_time);
      encbuf.put_signed_variant(i.max_time);
    }
  }

  rec.insert(rec.end(), encbuf.b.begin(), encbuf.b.begin() + encbuf.index);
}
// void RecordEncoder::group_tombstones(const std::deque<Stone> &stones,
//                                      std::vector<uint8_t> &rec) {
//   tsdbutil::EncBuf encbuf(10 * stones.size());
//   encbuf.put_byte(RECORD_GROUP_TOMBSTONES);

//   for (const Stone &s : stones) {
//     for (const tombstone::Interval &i : s.itvls) {
//       encbuf.put_BE_uint64(s.ref);
//       encbuf.put_signed_variant(i.min_time);
//       encbuf.put_signed_variant(i.max_time);
//     }
//   }
//   rec.insert(rec.end(), encbuf.b.begin(), encbuf.b.begin() + encbuf.index);
// }

}  // namespace tsdbutil
}  // namespace tsdb