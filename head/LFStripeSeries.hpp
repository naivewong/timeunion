#ifndef LFSTRIPESERIES_H
#define LFSTRIPESERIES_H

#include <unordered_set>
#include <vector>

#include "base/Atomic.hpp"
#include "base/Mutex.hpp"
#include "head/MemSeries.hpp"
#include "head/SeriesHashmap.hpp"

namespace tsdb {
namespace head {

// class LFSeriesHashmap {
//  public:
//   std::unordered_map<uint64_t, std::list<std::shared_ptr<MemSeries>>> map;

//  public:
//   LFSeriesHashmap() = default;

//   std::shared_ptr<MemSeries> get(uint64_t hash, const label::Labels &lset);
//   void set(uint64_t hash, const std::shared_ptr<MemSeries> &s);

//   // Return the iterator on hash or the next iterator if this one being
//   deleted. std::unordered_map<uint64_t,
//   std::list<std::shared_ptr<MemSeries>>>::iterator del(uint64_t hash, const
//   label::Labels &lset); std::unordered_map<uint64_t,
//   std::list<std::shared_ptr<MemSeries>>>::iterator del(uint64_t hash,
//   uint64_t ref);
// };

class LFStripeSeries {
 public:
  std::vector<std::unordered_map<uint64_t, std::shared_ptr<MemSeries>>>
      series;                         // Index by mod series ref.
  std::vector<SeriesHashmap> hashes;  // Index by mod hash.
  std::vector<base::AtomicInt>
      locks;  // To align cache line (multiples of 64 bytes)

  LFStripeSeries();

  // gc garbage collects old chunks that are strictly before mint and removes
  // series entirely that have no chunks left. return <set of removed series,
  // number of removed chunks>
  std::pair<std::unordered_set<uint64_t>, int> gc(int64_t min_time);

  std::shared_ptr<MemSeries> get_by_id(uint64_t ref);
  std::shared_ptr<MemSeries> get_by_hash(uint64_t hash,
                                         const label::Labels &lset);

  MemSeries *get_ptr_by_id(uint64_t ref);
  MemSeries *get_ptr_by_hash(uint64_t hash, const label::Labels &lset);

  void read_lock(int pos);
  void read_unlock(int pos);
  void write_lock(int pos);
  void write_unlock(int pos);

  // Return <MemSeries, if the series being set>.
  std::pair<std::shared_ptr<MemSeries>, bool> get_or_set(
      uint64_t hash, const std::shared_ptr<MemSeries> &s);
};

}  // namespace head
}  // namespace tsdb

#endif