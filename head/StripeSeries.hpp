#ifndef STRIPESERIES_H
#define STRIPESERIES_H

#include <unordered_set>
#include <vector>

#include "base/Mutex.hpp"
#include "head/MemSeries.hpp"
#include "head/SeriesHashmap.hpp"
#include "index/MemPostings.hpp"

namespace tsdb {
namespace head {

class MMapHeadWithTrie;

// StripeSeries locks modulo ranges of IDs and hashes to reduce lock contention.
// The locks are padded to not be on the same cache line. Filling the padded
// space with the maps was profiled to be slower – likely due to the additional
// pointer dereferences.
class StripeSeries {
 public:
  std::vector<std::unordered_map<uint64_t, std::shared_ptr<MemSeries>>>
      series;                         // Index by mod series ref.
  std::vector<SeriesHashmap> hashes;  // Index by mod hash.
  std::vector<base::PadRWMutexLock>
      locks;  // To align cache line (multiples of 64 bytes)

  StripeSeries();

  // gc garbage collects old chunks that are strictly before mint and removes
  // series entirely that have no chunks left. return <set of removed series,
  // number of removed chunks>
  // std::pair<std::unordered_set<uint64_t>, int> gc(int64_t min_time);

  std::shared_ptr<MemSeries> get_by_id(uint64_t ref);
  std::shared_ptr<MemSeries> get_by_hash(uint64_t hash,
                                         const label::Labels& lset);

  MemSeries* get_ptr_by_id(uint64_t ref);
  MemSeries* get_ptr_by_hash(uint64_t hash, const label::Labels& lset);

  // Return <MemSeries, if the series being set>.
  std::pair<std::shared_ptr<MemSeries>, bool> get_or_set(
      uint64_t hash, const std::shared_ptr<MemSeries>& s);
};

class MMapStripeSeries {
 public:
  std::vector<std::unordered_map<uint64_t, MMapMemSeries*>>
      series;                             // Index by mod series ref.
  std::vector<MMapSeriesHashmap> hashes;  // Index by mod hash.
  std::vector<base::PadRWMutexLock>
      locks;  // To align cache line (multiples of 64 bytes)

  MMapStripeSeries();
  ~MMapStripeSeries();

  MMapMemSeries* get_ptr_by_id(uint64_t ref);
  MMapMemSeries* get_ptr_by_hash(uint64_t hash, const label::Labels& lset);

  int purge_time(int64_t timestamp, index::MemPostingsWithTrie* postings);

#if USE_MMAP_LABELS
  std::pair<MMapMemSeries*, bool> get_or_set(MMapHeadWithTrie* head,
                                             uint64_t hash, uint64_t id,
                                             const label::Labels& lset,
                                             bool alloc_mmap_slot,
                                             int64_t mmap_labels_idx);
#else
  std::pair<MMapMemSeries*, bool> get_or_set(MMapHeadWithTrie* head,
                                             uint64_t hash, uint64_t id,
                                             const label::Labels& lset,
                                             bool alloc_mmap_slot);
#endif
};

class MMapStripeGroups {
 public:
  std::vector<std::unordered_map<uint64_t, MMapMemGroup*>>
      series;                            // Index by mod series ref.
  std::vector<MMapGroupHashmap> hashes;  // Index by mod hash.
  std::vector<base::PadRWMutexLock>
      locks;  // To align cache line (multiples of 64 bytes)

  MMapStripeGroups();
  ~MMapStripeGroups();

  MMapMemGroup* get_ptr_by_id(uint64_t ref);
  MMapMemGroup* get_ptr_by_hash(uint64_t hash, const label::Labels& lset);

  int purge_time(int64_t timestamp, index::MemPostingsWithTrie* postings);

#if USE_MMAP_LABELS
  std::pair<MMapMemGroup*, bool> get_or_set(MMapHeadWithTrie* head,
                                            uint64_t hash, uint64_t id,
                                            const label::Labels& group_lset,
                                            bool alloc_mmap_slot,
                                            int64_t mmap_labels_idx);
#else
  std::pair<MMapMemGroup*, bool> get_or_set(MMapHeadWithTrie* head,
                                            uint64_t hash, uint64_t id,
                                            const label::Labels& group_lset,
                                            bool alloc_mmap_slot);
#endif
};

}  // namespace head
}  // namespace tsdb

#endif