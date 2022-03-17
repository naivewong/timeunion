#include "head/StripeSeries.hpp"

#include <iostream>

#include "head/HeadUtils.hpp"
#include "head/MMapHeadWithTrie.hpp"

namespace tsdb {
namespace head {

StripeSeries::StripeSeries()
    : series(STRIPE_SIZE), hashes(STRIPE_SIZE), locks(STRIPE_SIZE) {}

// gc garbage collects old chunks that are strictly before mint and removes
// series entirely that have no chunks left. return <set of removed series,
// number of removed chunks>
// std::pair<std::unordered_set<uint64_t>, int> StripeSeries::gc(
//     int64_t min_time) {
//   // Run through all series and truncate old chunks. Mark those with no
//   // chunks left as deleted and store their ID.
//   std::unordered_set<uint64_t> rm_series;
//   int rm_chunks = 0;
//   for (int i = 0; i < STRIPE_SIZE; ++i) {
//     base::PadRWLockGuard lock_i(locks[i], 1);

//     // Iterate over all series sharing the same hash mod.
//     std::shared_ptr<MemSeries> temp;
//     std::unordered_map<
//         uint64_t, std::list<std::shared_ptr<MemSeries>>>::iterator hashmap_it
//         = hashes[i].map.begin();
//     while (hashmap_it != hashes[i].map.end()) {
//       std::list<std::shared_ptr<MemSeries>>::iterator serieslist_it =
//           hashmap_it->second.begin();
//       uint64_t hash = hashmap_it->first;
//       while (serieslist_it != hashmap_it->second.end()) {
//         // base::MutexLockGuard series_lock((*serieslist_it)->mutex_);
//         auto cur_it = serieslist_it->get();
//         cur_it->write_lock();
//         rm_chunks += (*serieslist_it)->truncate_chunk_before(min_time);

//         if (!(*serieslist_it)->chunks.empty() ||
//             (*serieslist_it)->pending_commit) {
//           ++serieslist_it;
//           (*serieslist_it)->write_unlock();
//           continue;
//         }
//         // Since serieslist_it will be removed, we increment it here.
//         std::list<std::shared_ptr<MemSeries>>::iterator temp_series =
//             serieslist_it;
//         ++serieslist_it;

//         // This makes sure the series_ will not be deconstructed when its
//         lock
//         // being held. And provide ref for deleting series.
//         temp = (*temp_series);

//         // The series is gone entirely. We need to keep the series lock
//         // and make sure we have acquired the stripe locks for hash and ID of
//         // the series alike. If we don't hold them all, there's a very small
//         // chance that a series receives samples again while we are half-way
//         // into deleting it.
//         int j = static_cast<int>((*temp_series)->ref & STRIPE_MASK);
//         if (i != j) {
//           base::PadRWLockGuard lock_j(locks[j], 1);
//           rm_series.insert((*temp_series)->ref);
//           hashmap_it = hashes[i].del(hash, (*temp_series)->labels);
//           series[j].erase(series[j].find(temp->ref));
//         } else {
//           rm_series.insert((*temp_series)->ref);
//           hashmap_it = hashes[i].del(hash, (*temp_series)->labels);
//           series[j].erase(series[j].find(temp->ref));
//         }
//         if (hashmap_it == hashes[i].map.end() || hashmap_it->first != hash) {
//           cur_it->write_unlock();
//           break;
//         }
//         cur_it->write_unlock();
//       }
//       if (hashmap_it == hashes[i].map.end())
//         break;
//       else if (hashmap_it->first == hash)
//         ++hashmap_it;
//     }
//   }
//   return {rm_series, rm_chunks};
// }

std::shared_ptr<MemSeries> StripeSeries::get_by_id(uint64_t ref) {
  uint64_t i = ref & STRIPE_MASK;
  std::unordered_map<uint64_t, std::shared_ptr<MemSeries>>::iterator r;
  base::PadRWLockGuard lock_i(locks[i], 0);
  if ((r = series[i].find(ref)) == series[i].end()) return nullptr;
  return r->second;
}

std::shared_ptr<MemSeries> StripeSeries::get_by_hash(
    uint64_t hash, const label::Labels& lset) {
  uint64_t i = hash & STRIPE_MASK;
  base::PadRWLockGuard lock_i(locks[i], 0);
  return hashes[i].get(hash, lset);
}

MemSeries* StripeSeries::get_ptr_by_id(uint64_t ref) {
  uint64_t i = ref & STRIPE_MASK;
  std::unordered_map<uint64_t, std::shared_ptr<MemSeries>>::iterator r;
  base::PadRWLockGuard lock_i(locks[i], 0);
  if ((r = series[i].find(ref)) == series[i].end()) return nullptr;
  return r->second.get();
}

MemSeries* StripeSeries::get_ptr_by_hash(uint64_t hash,
                                         const label::Labels& lset) {
  uint64_t i = hash & STRIPE_MASK;
  base::PadRWLockGuard lock_i(locks[i], 0);
  return hashes[i].get(hash, lset).get();
}

// Return <MemSeries, if the series being set>.
std::pair<std::shared_ptr<MemSeries>, bool> StripeSeries::get_or_set(
    uint64_t hash, const std::shared_ptr<MemSeries>& s) {
  uint64_t i = hash & STRIPE_MASK;
  std::shared_ptr<MemSeries> r;
  {
    base::PadRWLockGuard lock_i(locks[i], 1);
    if ((r = hashes[i].get(hash, s->labels))) return {r, false};
    hashes[i].set(hash, s);
  }

  i = s->ref & STRIPE_MASK;
  {
    base::PadRWLockGuard lock_i(locks[i], 1);
    series[i][s->ref] = s;
  }
  return {s, true};
}

/******************************
 *       MMapStripeSeries     *
 ******************************/
MMapStripeSeries::MMapStripeSeries()
    : series(STRIPE_SIZE), hashes(STRIPE_SIZE), locks(STRIPE_SIZE) {}

MMapStripeSeries::~MMapStripeSeries() {
  for (auto& m : series) {
    for (auto& item : m) delete item.second;
  }
}

MMapMemSeries* MMapStripeSeries::get_ptr_by_id(uint64_t ref) {
  uint64_t i = ref & STRIPE_MASK;
  std::unordered_map<uint64_t, MMapMemSeries*>::iterator r;
  base::PadRWLockGuard lock_i(locks[i], 0);
  if ((r = series[i].find(ref)) == series[i].end()) return nullptr;
  return r->second;
}

MMapMemSeries* MMapStripeSeries::get_ptr_by_hash(uint64_t hash,
                                                 const label::Labels& lset) {
  uint64_t i = hash & STRIPE_MASK;
  base::PadRWLockGuard lock_i(locks[i], 0);
  return hashes[i].get(hash, lset);
}

#if USE_MMAP_LABELS
std::pair<MMapMemSeries*, bool> MMapStripeSeries::get_or_set(
    MMapHeadWithTrie* head, uint64_t hash, uint64_t id,
    const label::Labels& lset, bool alloc_mmap_slot, int64_t mmap_labels_idx)
#else
std::pair<MMapMemSeries*, bool> MMapStripeSeries::get_or_set(
    MMapHeadWithTrie* head, uint64_t hash, uint64_t id,
    const label::Labels& lset, bool alloc_mmap_slot)
#endif
{
  uint64_t i = hash & STRIPE_MASK;
  MMapMemSeries* r;
  {
    base::PadRWLockGuard lock_i(locks[i], 1);
    if ((r = hashes[i].get(hash, lset))) return {r, false};
#if USE_MMAP_LABELS
    r = new MMapMemSeries(lset, id, head->xor_array_.get(), alloc_mmap_slot,
                          head->mmap_labels_.get(), mmap_labels_idx);
#else
    r = new MMapMemSeries(lset, id, head->xor_array_.get(), alloc_mmap_slot);
#endif
    hashes[i].set(hash, r);
  }

  i = r->ref & STRIPE_MASK;
  {
    base::PadRWLockGuard lock_i(locks[i], 1);
    series[i][r->ref] = r;
  }
  return {r, true};
}

int MMapStripeSeries::purge_time(int64_t timestamp,
                                 index::MemPostingsWithTrie* postings) {
  int count = 0;
  std::vector<uint64_t> purge_ids;
  label::Labels lset;
  for (size_t i = 0; i < hashes.size(); i++) {
    purge_ids.clear();
    {
      base::PadRWLockGuard lock_i(locks[i], 1);
      hashes[i].purge_time(timestamp, &purge_ids);
    }
    count += purge_ids.size();
    for (uint64_t id : purge_ids) {
      uint64_t idx = id & STRIPE_MASK;
      base::PadRWLockGuard lock_i(locks[idx], 1);

      MMapMemSeries* s = series[idx][id];
      s->write_lock();

      // Clean postings.
      lset.clear();
#if USE_MMAP_LABELS
      s->labels_->get_labels(s->labels_idx_, lset);
#else
      lset.insert(lset.end(), s->labels.begin(), s->labels.end());
#endif

      // TODO(Alec), remove labels.
      s->xor_array_->delete_slot(s->slot_);
      s->write_unlock();

      // Clean postings.
      postings->write_lock();
      for (auto& l : lset) postings->del(id, l);
      postings->del(id, label::ALL_POSTINGS_KEYS);
      postings->unlock();

      // Clean series.
      series[idx].erase(id);
    }
  }
  return count;
}

/******************************
 *       MMapStripeGroups     *
 ******************************/
MMapStripeGroups::MMapStripeGroups()
    : series(STRIPE_SIZE), hashes(STRIPE_SIZE), locks(STRIPE_SIZE) {}

MMapStripeGroups::~MMapStripeGroups() {
  for (auto& m : series) {
    for (auto& item : m) delete item.second;
  }
}

MMapMemGroup* MMapStripeGroups::get_ptr_by_id(uint64_t ref) {
  uint64_t i = ref & STRIPE_MASK;
  std::unordered_map<uint64_t, MMapMemGroup*>::iterator r;
  base::PadRWLockGuard lock_i(locks[i], 0);
  if ((r = series[i].find(ref)) == series[i].end()) return nullptr;
  return r->second;
}

MMapMemGroup* MMapStripeGroups::get_ptr_by_hash(uint64_t hash,
                                                const label::Labels& lset) {
  uint64_t i = hash & STRIPE_MASK;
  base::PadRWLockGuard lock_i(locks[i], 0);
  return hashes[i].get(hash, lset);
}

#if USE_MMAP_LABELS
std::pair<MMapMemGroup*, bool> MMapStripeGroups::get_or_set(
    MMapHeadWithTrie* head, uint64_t hash, uint64_t id,
    const label::Labels& group_lset, bool alloc_mmap_slot,
    int64_t mmap_labels_idx)
#else
std::pair<MMapMemGroup*, bool> MMapStripeGroups::get_or_set(
    MMapHeadWithTrie* head, uint64_t hash, uint64_t id,
    const label::Labels& group_lset, bool alloc_mmap_slot)
#endif
{
  uint64_t i = hash & STRIPE_MASK;
  MMapMemGroup* r = nullptr;
  {
    base::PadRWLockGuard lock_i(locks[i], 1);
    if ((r = hashes[i].get(hash, group_lset))) return {r, false};
#if USE_MMAP_LABELS
    r = new MMapMemGroup(group_lset, id, head->group_xor_array_.get(),
                         alloc_mmap_slot, head->mmap_group_labels_.get(),
                         mmap_labels_idx);
#else
    r = new MMapMemGroup(group_lset, id, head->group_xor_array_.get(),
                         alloc_mmap_slot);
#endif
    hashes[i].set(hash, r);
  }

  i = r->ref & STRIPE_MASK;
  {
    base::PadRWLockGuard lock_i(locks[i], 1);
    series[i][r->ref] = r;
  }
  return {r, true};
}

int MMapStripeGroups::purge_time(int64_t timestamp,
                                 index::MemPostingsWithTrie* postings) {
  int count = 0;
  std::vector<uint64_t> purge_ids;
  label::Labels group_lset;
  std::vector<label::Labels> individual_lsets;
  for (size_t i = 0; i < hashes.size(); i++) {
    purge_ids.clear();
    {
      base::PadRWLockGuard lock_i(locks[i], 1);
      hashes[i].purge_time(timestamp, &purge_ids);
    }
    count += purge_ids.size();
    for (uint64_t id : purge_ids) {
      uint64_t idx = id & STRIPE_MASK;
      base::PadRWLockGuard lock_i(locks[idx], 1);

      MMapMemGroup* s = series[idx][id];
      s->write_lock();

      // Clean postings.
      group_lset.clear();
      individual_lsets.clear();
#if USE_MMAP_LABELS
      uint32_t no_op_idx;
      s->labels_->get_labels(s->group_labels_idx_, group_lset, &no_op_idx);
      for (size_t j = 0; j < s->individual_labels_idx_.size(); j++) {
        individual_lsets.emplace_back();
        s->labels_->get_labels(s->individual_labels_idx_[j],
                               individual_lsets.back(), &no_op_idx);
      }
#else
      group_lset = s->group_labels_;
      individual_lsets = s->individual_labels_;
#endif

      // TODO(Alec), remove labels.
      s->xor_array_->delete_slot(s->time_slot_);
      for (size_t j = 0; j < s->value_slots_.size(); j++)
        s->xor_array_->delete_slot(s->value_slots_[j]);
      s->write_unlock();

      // Clean postings.
      postings->write_lock();
      for (auto& l : group_lset) postings->del(id, l);
      for (auto& lset : individual_lsets) {
        for (auto& l : lset) postings->del(id, l);
      }
      postings->del(id, label::ALL_POSTINGS_KEYS);
      postings->unlock();

      // Clean series.
      series[idx].erase(id);
    }
  }
  return count;
}

}  // namespace head
}  // namespace tsdb