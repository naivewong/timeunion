#include "index/MemPostings.hpp"

#include <boost/bind.hpp>
#include <functional>
#include <limits>

#include "index/EmptyPostings.hpp"
#include "index/VectorPostings.hpp"

namespace tsdb {
namespace index {

void sort_slice(std::deque<uint64_t> *d, base::WaitGroup *wg) {
  std::sort(d->begin(), d->end());
  wg->done();
}

MemPostings::MemPostings(bool ordered) : mutex_(), m(), ordered(ordered) {}

// Will get a const reference of deque list.
// Like slice in Go.
std::unique_ptr<PostingsInterface> MemPostings::get(const std::string &label,
                                                    const std::string &value) {
  {
    base::RWLockGuard mutex(mutex_, 0);
    auto v_map = m.find(label);
    if (v_map != m.end()) {
      auto v_list = v_map->second.find(value);
      if (v_list != v_map->second.end())
        return std::unique_ptr<PostingsInterface>(
            new ListPostings(&(v_list->second)));
    }
  }
  return nullptr;
}

std::unique_ptr<PostingsInterface> MemPostings::all() {
  return get(label::ALL_POSTINGS_KEYS.label, label::ALL_POSTINGS_KEYS.value);
}

label::Labels MemPostings::sorted_keys() {
  label::Labels ls;
  {
    base::RWLockGuard mutex(mutex_, 0);
    for (auto const &v_map : m) {
      for (auto const &v : v_map.second) {
        ls.emplace_back(v_map.first, v.first);
      }
    }
  }
  std::sort(ls.begin(), ls.end());
  return ls;
}

// Used under lock.
void MemPostings::add(uint64_t id, const label::Label &l) {
  if (ordered)
    binary_insert<uint64_t>(&(m[l.label][l.value]), id);
  else {
    m[l.label][l.value].push_back(id);
  }
}

void MemPostings::add(uint64_t id, const label::Labels &ls) {
  base::RWLockGuard mutex(mutex_, 1);
  for (const label::Label &l : ls) add(id, l);
  add(id, label::ALL_POSTINGS_KEYS);
}

void MemPostings::del(const std::set<uint64_t> &deleted) {
  std::vector<std::reference_wrapper<const std::string>> keys;
  keys.reserve(m.size());
  {
    // Collect all keys relevant for deletion once. New keys added afterwards
    // can by definition not be affected by any of the given deletes.
    base::RWLockGuard mutex(mutex_, 0);
    for (auto const &p : m) keys.push_back(p.first);
  }
  for (auto const &key : keys) {
    std::vector<std::reference_wrapper<const std::string>> values;
    values.reserve(m[key.get()].size());
    {
      base::RWLockGuard mutex(mutex_, 0);
      for (auto const &p : m[key.get()]) values.push_back(p.first);
    }

    // For each posting we first analyse whether the postings list is affected
    // by the deletes. If yes, we actually reallocate a new postings list.
    for (auto const &value : values) {
      // Only lock for processing one postings list so we don't block reads for
      // too long.
      base::RWLockGuard mutex(mutex_, 1);

      bool found = false;
      for (auto const &ref : m[key.get()][value.get()]) {
        if (deleted.find(ref) != deleted.end()) {
          found = true;
          break;
        }
      }
      if (!found) continue;

      std::deque<uint64_t> v;
      for (auto i : m[key.get()][value.get()]) {
        if (deleted.find(i) == deleted.end()) v.push_back(i);
      }

      if (v.empty())
        m[key.get()].erase(m[key.get()].find(value.get()));
      else
        m[key.get()][value.get()] = v;
    }
    {
      base::RWLockGuard mutex(mutex_, 1);
      if (m[key.get()].empty()) m.erase(m.find(key.get()));
    }
  }
}

void MemPostings::del(const std::unordered_set<uint64_t> &deleted) {
  std::vector<std::reference_wrapper<const std::string>> keys;
  keys.reserve(m.size());
  {
    // Collect all keys relevant for deletion once. New keys added afterwards
    // can by definition not be affected by any of the given deletes.
    base::RWLockGuard mutex(mutex_, 0);
    for (auto const &p : m) keys.push_back(p.first);
  }
  for (auto const &key : keys) {
    std::vector<std::reference_wrapper<const std::string>> values;
    values.reserve(m[key.get()].size());
    {
      base::RWLockGuard mutex(mutex_, 0);
      for (auto const &p : m[key.get()]) values.push_back(p.first);
    }

    // For each posting we first analyse whether the postings list is affected
    // by the deletes. If yes, we actually reallocate a new postings list.
    for (auto const &value : values) {
      // Only lock for processing one postings list so we don't block reads for
      // too long.
      base::RWLockGuard mutex(mutex_, 1);

      bool found = false;
      for (auto const &ref : m[key.get()][value.get()]) {
        if (deleted.find(ref) != deleted.end()) {
          found = true;
          break;
        }
      }
      if (!found) continue;

      std::deque<uint64_t> v;
      for (auto i : m[key.get()][value.get()]) {
        if (deleted.find(i) == deleted.end()) v.push_back(i);
      }

      if (v.empty())
        m[key.get()].erase(m[key.get()].find(value.get()));
      else
        m[key.get()][value.get()] = v;
    }
    {
      base::RWLockGuard mutex(mutex_, 1);
      if (m[key.get()].empty()) m.erase(m.find(key.get()));
    }
  }
}

void MemPostings::clear() {
  base::RWLockGuard mutex(mutex_, 1);
  m.clear();
}

void MemPostings::iter(const boost::function<void(const label::Label &,
                                                  const ListPostings &)> &f) {
  base::RWLockGuard mutex(mutex_, 0);
  for (auto const &v_map : m) {
    for (auto const &v_list : v_map.second) {
      f(label::Label(v_map.first, v_list.first),
        ListPostings(&(v_list.second)));
    }
  }
}

// Used under lock.
int MemPostings::size() {
  int size_ = 0;
  for (auto const &v_map : m) size_ += v_map.second.size();
  return size_;
}

// This ThreadPool can be shared at the same time.
// Use base::WaitGroup to ensure all sortings are finished.
void MemPostings::ensure_order(base::ThreadPool *pool) {
  base::RWLockGuard mutex(mutex_, 1);
  for (auto &v_map : m) {
    for (auto &v_list : v_map.second) {
      wg.add(1);
      pool->run(boost::bind(&sort_slice, &(v_list.second), &wg));
    }
  }

  wg.wait();
}

// This ThreadPool can be shared at the same time.
// Use base::WaitGroup to ensure all sortings are finished.
void MemPostings::ensure_order(const std::shared_ptr<base::ThreadPool> &pool) {
  base::RWLockGuard mutex(mutex_, 1);
  for (auto &v_map : m) {
    for (auto &v_list : v_map.second) {
      wg.add(1);
      pool->run(boost::bind(&sort_slice, &(v_list.second), &wg));
    }
  }

  wg.wait();
}

/* ================================================ */
/* MemPostingsWithTrie                              */
/* ================================================ */
std::unique_ptr<PostingsInterface> MemPostingsWithTrie::get(
    const std::string &label, const std::string &value) {
  std::string tmp = label + label::HEAD_LABEL_SEP + value;
  base::RWLockGuard mutex(mutex_, 0);
  uint32_t result = trie_.exactMatchSearch<uint32_t>(tmp.c_str());
  if (result != std::numeric_limits<uint32_t>::max())
    return std::unique_ptr<PostingsInterface>(
        new VectorPtrPostings(&(lists_[result])));

  return nullptr;
}

void MemPostingsWithTrie::add(uint64_t id, const label::Label &l) {
  std::string tmp = l.label + label::HEAD_LABEL_SEP + l.value;
  uint32_t result = trie_.exactMatchSearch<uint32_t>(tmp.c_str());
  if (result != std::numeric_limits<uint32_t>::max())
    lists_[result].push_back(id);
  else {
    lists_.emplace_back();
    lists_.back().push_back(id);
    trie_.update(tmp.c_str(), tmp.size(), lists_.size() - 1);
  }
}

void MemPostingsWithTrie::add(uint64_t id, const label::Labels &ls) {
  base::RWLockGuard mutex(mutex_, 1);
  for (const label::Label &l : ls) add(id, l);
  add(id, label::ALL_POSTINGS_KEYS);
}

void MemPostingsWithTrie::del(uint64_t id, const label::Label &l) {
  std::string tmp = l.label + label::HEAD_LABEL_SEP + l.value;
  uint32_t result = trie_.exactMatchSearch<uint32_t>(tmp.c_str());
  if (result != std::numeric_limits<uint32_t>::max()) {
    auto it = lists_[result].begin();
    while (it != lists_[result].end()) {
      if (*it == id) {
        lists_[result].erase(it);
        break;
      }
      it++;
    }
  }
}

}  // namespace index
}  // namespace tsdb