#include "head/LFStripeSeries.hpp"

#include <iostream>

#include "head/HeadUtils.hpp"

namespace tsdb {
namespace head {

LFStripeSeries::LFStripeSeries()
    : series(STRIPE_SIZE), hashes(STRIPE_SIZE), locks(STRIPE_SIZE) {}

void LFStripeSeries::read_lock(int pos) {
  bool b = true;
  int x;
  while (b) {
    x = locks[pos].get();
    if (x == -1) continue;
    if (locks[pos].cas(x, x + 1)) b = false;
  }
}

void LFStripeSeries::read_unlock(int pos) { locks[pos].decrement(); }

void LFStripeSeries::write_lock(int pos) {
  bool b = true;
  int x;
  while (b) {
    x = locks[pos].get();
    if (x > 0) continue;
    if (locks[pos].cas(0, -1)) b = false;
  }
}

void LFStripeSeries::write_unlock(int pos) { locks[pos].increment(); }

std::pair<std::unordered_set<uint64_t>, int> LFStripeSeries::gc(
    int64_t min_time) {
  // Run through all series and truncate old chunks. Mark those with no
  // chunks left as deleted and store their ID.
  std::unordered_set<uint64_t> rm_series;
  int rm_chunks = 0;
  for (int i = 0; i < STRIPE_SIZE; ++i) {
    write_lock(i);

    // Iterate over all series sharing the same hash mod.
    std::shared_ptr<MemSeries> temp;
    std::unordered_map<
        uint64_t, std::list<std::shared_ptr<MemSeries>>>::iterator hashmap_it =
        hashes[i].map.begin();
    while (hashmap_it != hashes[i].map.end()) {
      std::list<std::shared_ptr<MemSeries>>::iterator serieslist_it =
          hashmap_it->second.begin();
      uint64_t hash = hashmap_it->first;
      while (serieslist_it != hashmap_it->second.end()) {
        // base::MutexLockGuard series_lock((*serieslist_it)->mutex_);
        auto cur_it = serieslist_it->get();
        cur_it->write_lock();
        rm_chunks += (*serieslist_it)->truncate_chunk_before(min_time);

        if (!(*serieslist_it)->chunks.empty() ||
            (*serieslist_it)->pending_commit) {
          ++serieslist_it;
          cur_it->write_unlock();
          continue;
        }
        // Since serieslist_it will be removed, we increment it here.
        std::list<std::shared_ptr<MemSeries>>::iterator temp_series =
            serieslist_it;
        ++serieslist_it;

        // This makes sure the series_ will not be deconstructed when its lock
        // being held. And provide ref for deleting series.
        temp = (*temp_series);

        // The series is gone entirely. We need to keep the series lock
        // and make sure we have acquired the stripe locks for hash and ID of
        // the series alike. If we don't hold them all, there's a very small
        // chance that a series receives samples again while we are half-way
        // into deleting it.
        int j = static_cast<int>((*temp_series)->ref & STRIPE_MASK);
        if (i != j) {
          write_lock(j);
          rm_series.insert((*temp_series)->ref);
          hashmap_it = hashes[i].del(hash, (*temp_series)->labels);
          series[j].erase(series[j].find(temp->ref));
          write_unlock(j);
        } else {
          rm_series.insert((*temp_series)->ref);
          hashmap_it = hashes[i].del(hash, (*temp_series)->labels);
          series[j].erase(series[j].find(temp->ref));
        }
        if (hashmap_it == hashes[i].map.end() || hashmap_it->first != hash) {
          cur_it->write_unlock();
          break;
        }
        cur_it->write_unlock();
      }
      if (hashmap_it == hashes[i].map.end())
        break;
      else if (hashmap_it->first == hash)
        ++hashmap_it;
    }
    write_unlock(i);
  }
  return {rm_series, rm_chunks};
}

std::shared_ptr<MemSeries> LFStripeSeries::get_by_id(uint64_t ref) {
  uint64_t i = ref & STRIPE_MASK;
  std::unordered_map<uint64_t, std::shared_ptr<MemSeries>>::iterator r;
  read_lock(i);

  if ((r = series[i].find(ref)) == series[i].end()) {
    read_unlock(i);
    return nullptr;
  }
  read_unlock(i);
  return r->second;
}

std::shared_ptr<MemSeries> LFStripeSeries::get_by_hash(
    uint64_t hash, const label::Labels& lset) {
  uint64_t i = hash & STRIPE_MASK;
  read_lock(i);
  std::shared_ptr<MemSeries> r = hashes[i].get(hash, lset);
  read_unlock(i);
  return r;
}

MemSeries* LFStripeSeries::get_ptr_by_id(uint64_t ref) {
  uint64_t i = ref & STRIPE_MASK;
  std::unordered_map<uint64_t, std::shared_ptr<MemSeries>>::iterator r;
  read_lock(i);

  if ((r = series[i].find(ref)) == series[i].end()) {
    read_unlock(i);
    return nullptr;
  }
  MemSeries* result = r->second.get();
  read_unlock(i);
  return result;
}

MemSeries* LFStripeSeries::get_ptr_by_hash(uint64_t hash,
                                           const label::Labels& lset) {
  uint64_t i = hash & STRIPE_MASK;
  read_lock(i);
  MemSeries* r = hashes[i].get(hash, lset).get();
  read_unlock(i);
  return r;
}

// Return <MemSeries, if the series being set>.
std::pair<std::shared_ptr<MemSeries>, bool> LFStripeSeries::get_or_set(
    uint64_t hash, const std::shared_ptr<MemSeries>& s) {
  uint64_t i = hash & STRIPE_MASK;
  std::shared_ptr<MemSeries> r;
  {
    write_lock(i);
    if ((r = hashes[i].get(hash, s->labels))) {
      write_unlock(i);
      return {r, false};
    }
    hashes[i].set(hash, s);
    write_unlock(i);
  }

  i = s->ref & STRIPE_MASK;
  {
    write_lock(i);
    series[i][s->ref] = s;
    write_unlock(i);
  }
  return {s, true};
}

}  // namespace head
}  // namespace tsdb