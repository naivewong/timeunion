#ifndef FILEUTILS_H
#define FILEUTILS_H

// #include "head/GroupMemSeries.hpp"
#include "head/MemSeries.hpp"
#include "label/Label.hpp"
#include "tombstone/Interval.hpp"

namespace tsdb {
namespace tsdbutil {

// const int ErrNotFound = 1;

std::string filepath_join(const std::string& f1, const std::string& f2);

bool is_number(const std::string& s);

std::pair<int64_t, int64_t> clamp_interval(int64_t a, int64_t b, int64_t mint,
                                           int64_t maxt);

class Stone {
 public:
  uint64_t ref;
  tombstone::Intervals itvls;

  Stone() = default;
  Stone(uint64_t ref, const tombstone::Intervals& itvls)
      : ref(ref), itvls(itvls) {}
};

// RefSeries is the series labels with the series ID.
class RefSeries {
 public:
  uint64_t ref;
  label::Labels lset;

  RefSeries() = default;
  RefSeries(uint64_t ref, const label::Labels& lset) : ref(ref), lset(lset) {}
};

// class RefGroupSeries {
//  public:
//   uint64_t group_ref;
//   std::deque<RefSeries> series;

//   RefGroupSeries() = default;
//   RefGroupSeries(uint64_t group_ref) : group_ref(group_ref) {}
//   RefGroupSeries(uint64_t group_ref, const std::deque<RefSeries> &series)
//       : group_ref(group_ref), series(series) {}

//   void push_back(uint64_t ref, const label::Labels &lset) {
//     series.emplace_back(ref, lset);
//   }
//   void push_back(const RefSeries &rs) { series.push_back(rs); }
// };

// RefSample is a timestamp/value pair associated with a reference to a series.
class RefSample {
 public:
  uint64_t ref;
  int64_t t;
  double v;
  int64_t txn;
  // TODO(Alec), decide whether to add MemSeries.
  std::shared_ptr<head::MemSeries> series = nullptr;
  head::MemSeries* series_ptr = nullptr;

  RefSample() = default;
  RefSample(uint64_t ref, int64_t t, double v) : ref(ref), t(t), v(v) {}
  RefSample(uint64_t ref, int64_t t, double v,
            const std::shared_ptr<head::MemSeries>& series)
      : ref(ref), t(t), v(v), series(series), series_ptr(nullptr) {}
  RefSample(uint64_t ref, int64_t t, double v, head::MemSeries* series)
      : ref(ref), t(t), v(v), series(nullptr), series_ptr(series) {}
  RefSample(uint64_t ref, int64_t t, double v, int64_t txn,
            head::MemSeries* series)
      : ref(ref), t(t), v(v), series(nullptr), txn(txn), series_ptr(series) {}
  // std::pair<bool, bool> append(int64_t timestamp, double value) {
  //   if (series)
  //     return series->append(timestamp, value);
  //   else
  //     return series_ptr->append(timestamp, value);
  // }
  // base::MutexLock& get_lock() {
  //   if (series)
  //     return series->mutex_;
  //   else
  //     return series_ptr->mutex_;
  // }
};

class MMapRefSample {
 public:
  uint64_t ref;
  int64_t t;
  double v;
  int64_t txn;
  // TODO(Alec), decide whether to add MemSeries.
  std::shared_ptr<head::MMapMemSeries> series = nullptr;
  head::MMapMemSeries* series_ptr = nullptr;

  MMapRefSample() = default;
  MMapRefSample(uint64_t ref, int64_t t, double v) : ref(ref), t(t), v(v) {}
  MMapRefSample(uint64_t ref, int64_t t, double v,
                const std::shared_ptr<head::MMapMemSeries>& series)
      : ref(ref), t(t), v(v), series(series), series_ptr(nullptr) {}
  MMapRefSample(uint64_t ref, int64_t t, double v, head::MMapMemSeries* series)
      : ref(ref), t(t), v(v), series(nullptr), series_ptr(series) {}
  MMapRefSample(uint64_t ref, int64_t t, double v, int64_t txn,
                head::MMapMemSeries* series)
      : ref(ref), t(t), v(v), series(nullptr), txn(txn), series_ptr(series) {}
};

class MMapRefGroupSample {
 public:
  uint64_t ref;
  // If this is empty, it represents a full insertion.
  // First bit of 32-bit size set to 1.
  std::vector<int> slots;
  int64_t t;
  std::vector<double> v;
  int64_t txn;

  head::MMapMemGroup* group_ptr = nullptr;

  MMapRefGroupSample() = default;
  MMapRefGroupSample(uint64_t ref, const std::vector<int>& slots, int64_t t,
                     const std::vector<double>& v, int64_t txn,
                     head::MMapMemGroup* p)
      : ref(ref), slots(slots), t(t), v(v), txn(txn), group_ptr(p) {}
  MMapRefGroupSample(uint64_t ref, int64_t t, const std::vector<double>& v,
                     int64_t txn, head::MMapMemGroup* p)
      : ref(ref), t(t), v(v), txn(txn), group_ptr(p) {}
};

// class RefGroupSample {
//  public:
//   uint64_t group_ref;
//   int64_t timestamp;
//   std::deque<uint64_t> ids;
//   std::deque<double> samples;
//   std::shared_ptr<head::GroupMemSeries> series;

//   RefGroupSample() = default;
//   RefGroupSample(uint64_t group_ref, int64_t timestamp)
//       : group_ref(group_ref), timestamp(timestamp) {}
//   RefGroupSample(uint64_t group_ref, int64_t timestamp,
//                  const std::deque<RefSample> &samples)
//       : group_ref(group_ref), timestamp(timestamp) {
//     for (const RefSample &s : samples) {
//       ids.push_back(s.ref);
//       this->samples.push_back(s.v);
//     }
//   }
//   RefGroupSample(uint64_t group_ref, int64_t timestamp,
//                  const std::shared_ptr<head::GroupMemSeries> &series)
//       : group_ref(group_ref), timestamp(timestamp), series(series) {}
//   RefGroupSample(uint64_t group_ref, int64_t timestamp,
//                  const std::shared_ptr<head::GroupMemSeries> &series,
//                  const std::deque<uint64_t> &ids,
//                  const std::deque<double> &samples)
//       : group_ref(group_ref),
//         timestamp(timestamp),
//         series(series),
//         ids(ids),
//         samples(samples) {}

//   void push_back(uint64_t ref, double v) {
//     ids.push_back(ref);
//     samples.push_back(v);
//   }
//   void push_back(const RefSample &sample) {
//     ids.push_back(sample.ref);
//     samples.push_back(sample.v);
//   }
// };

// WAL specific record.
typedef uint8_t RECORD_ENTRY_TYPE;
extern const RECORD_ENTRY_TYPE RECORD_INVALID;
extern const RECORD_ENTRY_TYPE RECORD_SERIES;
extern const RECORD_ENTRY_TYPE RECORD_SAMPLES;
extern const RECORD_ENTRY_TYPE RECORD_TOMBSTONES;
extern const RECORD_ENTRY_TYPE RECORD_GROUP_SERIES;
extern const RECORD_ENTRY_TYPE RECORD_GROUP_SAMPLES;
extern const RECORD_ENTRY_TYPE RECORD_GROUP_TOMBSTONES;

}  // namespace tsdbutil
}  // namespace tsdb

#endif