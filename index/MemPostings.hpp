#ifndef MEMPOSTINGS_H
#define MEMPOSTINGS_H

#include <stdint.h>

#include <boost/function.hpp>
#include <deque>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "base/Mutex.hpp"
#include "base/ThreadPool.hpp"
#include "base/WaitGroup.hpp"
#include "index/ListPostings.hpp"
#include "label/Label.hpp"
#include "third_party/cedarpp.h"

namespace tsdb {
namespace index {

void sort_slice(std::deque<uint64_t> *d, base::WaitGroup *wg);

class MemPostings {
 private:
  base::RWMutexLock mutex_;
  std::unordered_map<std::string,
                     std::unordered_map<std::string, std::deque<uint64_t>>>
      m;

  // NOTE(Alec): maybe no need to be ordered for Head, because the id is in
  // ascending order.
  bool ordered;
  base::WaitGroup wg;

 public:
  MemPostings(bool ordered = false);

  // Will get a const reference of deque list.
  // Like slice in Go.
  std::unique_ptr<PostingsInterface> get(const std::string &label,
                                         const std::string &value);

  std::unique_ptr<PostingsInterface> all();

  label::Labels sorted_keys();

  // Used under lock.
  void add(uint64_t id, const label::Label &l);

  void add(uint64_t id, const label::Labels &ls);

  void del(const std::set<uint64_t> &deleted);

  void del(const std::unordered_set<uint64_t> &deleted);

  void iter(const boost::function<void(const label::Label &,
                                       const ListPostings &)> &f);

  void clear();

  // Used under lock.
  int size();

  // This ThreadPool can be shared at the same time.
  void ensure_order(base::ThreadPool *pool);

  // This ThreadPool can be shared at the same time.
  void ensure_order(const std::shared_ptr<base::ThreadPool> &pool);
};

class MemPostingsWithTrie {
 private:
  base::RWMutexLock mutex_;
  cedar::da<uint32_t> trie_;
  std::vector<std::vector<uint64_t>> lists_;

 public:
  // TODO(Alec): safe get.
  std::unique_ptr<PostingsInterface> get(const std::string &label,
                                         const std::string &value);

  // Used under lock.
  void add(uint64_t id, const label::Label &l);

  void add(uint64_t id, const label::Labels &ls);

  void del(uint64_t id, const label::Label &l);

  void write_lock() { mutex_.write_lock(); }
  void read_lock() { mutex_.read_lock(); }
  void unlock() { mutex_.unlock(); }

  int size() {
    base::RWLockGuard mutex(mutex_, 0);
    return trie_.num_keys();
  }

  std::vector<uint64_t> TEST_get_list(int i) {
    if (i < lists_.size()) return lists_[i];
    return {};
  }
};

}  // namespace index
}  // namespace tsdb

#endif