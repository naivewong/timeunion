#include "head/SeriesHashmap.hpp"

namespace tsdb {
namespace head {

/******************************
 *       SeriesHashmap     *
 ******************************/
std::shared_ptr<MemSeries> SeriesHashmap::get(uint64_t hash,
                                              const label::Labels& lset) {
  auto it1 = map.find(hash);
  if (it1 == map.end())
    return nullptr;
  else {
    auto it2 = std::find_if(it1->second.begin(), it1->second.end(),
                            [&lset](const std::shared_ptr<MemSeries>& s) {
                              return label::lbs_compare(s->labels, lset) == 0;
                            });
    if (it2 == it1->second.end())
      return nullptr;
    else
      return *it2;
  }
}

void SeriesHashmap::set(uint64_t hash, const std::shared_ptr<MemSeries>& s) {
  auto it1 = map.find(hash);
  if (it1 == map.end())
    map.insert({hash, {s}});
  else {
    auto it2 =
        std::find_if(it1->second.begin(), it1->second.end(),
                     [&s](const std::shared_ptr<MemSeries>& s1) {
                       return label::lbs_compare(s->labels, s1->labels) == 0;
                     });
    if (it2 == it1->second.end())
      it1->second.push_back(s);
    else
      *it2 = s;
  }
}

std::unordered_map<uint64_t, std::list<std::shared_ptr<MemSeries>>>::iterator
SeriesHashmap::del(uint64_t hash, const label::Labels& lset) {
  auto it1 = map.find(hash);
  if (it1 != map.end())
    it1->second.remove_if([&lset](const std::shared_ptr<MemSeries>& s) {
      return label::lbs_compare(s->labels, lset) == 0;
    });
  if (it1->second.empty()) return map.erase(it1);
  return it1;
}

std::unordered_map<uint64_t, std::list<std::shared_ptr<MemSeries>>>::iterator
SeriesHashmap::del(uint64_t hash, uint64_t ref) {
  auto it1 = map.find(hash);
  if (it1 != map.end())
    it1->second.remove_if(
        [&ref](const std::shared_ptr<MemSeries>& s) { return s->ref == ref; });
  if (it1->second.empty()) return map.erase(it1);
  return it1;
}

/******************************
 *       MMapSeriesHashmap    *
 ******************************/
MMapMemSeries* MMapSeriesHashmap::get(uint64_t hash,
                                      const label::Labels& lset) {
  auto it1 = map.find(hash);
  if (it1 == map.end())
    return nullptr;
  else {
    auto it2 = std::find_if(it1->second.begin(), it1->second.end(),
                            [&lset](const MMapMemSeries* s) {
#if USE_MMAP_LABELS
                              label::Labels tmp_lset;
                              s->labels_->get_labels(s->labels_idx_, tmp_lset);
                              return label::lbs_compare(tmp_lset, lset) == 0;
#else
                              return label::lbs_compare(s->labels, lset) == 0;
#endif
                            });
    if (it2 == it1->second.end())
      return nullptr;
    else
      return *it2;
  }
}

void MMapSeriesHashmap::set(uint64_t hash, MMapMemSeries* s) {
  auto it1 = map.find(hash);
  if (it1 == map.end())
    map.insert({hash, {s}});
  else {
    auto it2 = std::find_if(
        it1->second.begin(), it1->second.end(), [&s](const MMapMemSeries* s1) {
#if USE_MMAP_LABELS
          label::Labels tmp_lset1, tmp_lset2;
          s->labels_->get_labels(s->labels_idx_, tmp_lset1);
          s1->labels_->get_labels(s1->labels_idx_, tmp_lset2);
          return label::lbs_compare(tmp_lset1, tmp_lset2) == 0;
#else
                      return label::lbs_compare(s->labels, s1->labels) == 0;
#endif
        });
    if (it2 == it1->second.end())
      it1->second.push_back(s);
    else
      *it2 = s;
  }
}

std::unordered_map<uint64_t, std::list<MMapMemSeries*>>::iterator
MMapSeriesHashmap::del(uint64_t hash, const label::Labels& lset) {
  auto it1 = map.find(hash);
  if (it1 != map.end())
    it1->second.remove_if([&lset](const MMapMemSeries* s) {
#if USE_MMAP_LABELS
      label::Labels tmp_lset;
      s->labels_->get_labels(s->labels_idx_, tmp_lset);
      return label::lbs_compare(tmp_lset, lset) == 0;
#else
      return label::lbs_compare(s->labels, lset) == 0;
#endif
    });
  if (it1->second.empty()) return map.erase(it1);
  return it1;
}

std::unordered_map<uint64_t, std::list<MMapMemSeries*>>::iterator
MMapSeriesHashmap::del(uint64_t hash, uint64_t ref) {
  auto it1 = map.find(hash);
  if (it1 != map.end())
    it1->second.remove_if(
        [&ref](const MMapMemSeries* s) { return s->ref == ref; });
  if (it1->second.empty()) return map.erase(it1);
  return it1;
}

void MMapSeriesHashmap::purge_time(int64_t timestamp,
                                   std::vector<uint64_t>* ids) {
  for (auto& hash_item : map) {
    auto it = hash_item.second.begin();
    while (it != hash_item.second.end()) {
      if ((*it)->global_max_time_ <= timestamp) {
        ids->push_back((*it)->ref);
        it = hash_item.second.erase(it);
      } else
        it++;
    }
  }
}

/******************************
 *       MMapGroupHashmap     *
 ******************************/
MMapMemGroup* MMapGroupHashmap::get(uint64_t hash, const label::Labels& lset) {
  auto it1 = map.find(hash);
  if (it1 == map.end())
    return nullptr;
  else {
    uint32_t no_op_idx;
    auto it2 = std::find_if(
        it1->second.begin(), it1->second.end(), [&](const MMapMemGroup* s) {
#if USE_MMAP_LABELS
          label::Labels tmp_lset;
          s->labels_->get_labels(s->group_labels_idx_, tmp_lset, &no_op_idx);
          return label::lbs_compare(tmp_lset, lset) == 0;
#else
                              return label::lbs_compare(s->group_labels_, lset) == 0;
#endif
        });
    if (it2 == it1->second.end())
      return nullptr;
    else
      return *it2;
  }
}

void MMapGroupHashmap::set(uint64_t hash, MMapMemGroup* s) {
  auto it1 = map.find(hash);
  if (it1 == map.end())
    map.insert({hash, {s}});
  else {
    uint32_t no_op_idx;
    auto it2 = std::find_if(
        it1->second.begin(), it1->second.end(),
        [&s, &no_op_idx](const MMapMemGroup* s1) {
#if USE_MMAP_LABELS
          label::Labels tmp_lset1, tmp_lset2;
          s->labels_->get_labels(s->group_labels_idx_, tmp_lset1, &no_op_idx);
          s1->labels_->get_labels(s1->group_labels_idx_, tmp_lset2, &no_op_idx);
          return label::lbs_compare(tmp_lset1, tmp_lset2) == 0;
#else
          return label::lbs_compare(s->group_labels_, s1->group_labels_) == 0;
#endif
        });
    if (it2 == it1->second.end())
      it1->second.push_back(s);
    else
      *it2 = s;
  }
}

std::unordered_map<uint64_t, std::list<MMapMemGroup*>>::iterator
MMapGroupHashmap::del(uint64_t hash, const label::Labels& lset) {
  auto it1 = map.find(hash);
  if (it1 != map.end())
    it1->second.remove_if([&lset](const MMapMemGroup* s) {
#if USE_MMAP_LABELS
      uint32_t no_op_idx;
      label::Labels tmp_lset;
      s->labels_->get_labels(s->group_labels_idx_, tmp_lset, &no_op_idx);
      return label::lbs_compare(tmp_lset, lset) == 0;
#else
      return label::lbs_compare(s->group_labels_, lset) == 0;
#endif
    });
  if (it1->second.empty()) return map.erase(it1);
  return it1;
}

std::unordered_map<uint64_t, std::list<MMapMemGroup*>>::iterator
MMapGroupHashmap::del(uint64_t hash, uint64_t ref) {
  auto it1 = map.find(hash);
  if (it1 != map.end())
    it1->second.remove_if(
        [&ref](const MMapMemGroup* s) { return s->ref == ref; });
  if (it1->second.empty()) return map.erase(it1);
  return it1;
}

void MMapGroupHashmap::purge_time(int64_t timestamp,
                                  std::vector<uint64_t>* ids) {
  for (auto& hash_item : map) {
    auto it = hash_item.second.begin();
    while (it != hash_item.second.end()) {
      if ((*it)->global_max_time_ <= timestamp) {
        ids->push_back((*it)->ref);
        it = hash_item.second.erase(it);
      } else
        it++;
    }
  }
}

}  // namespace head
}  // namespace tsdb