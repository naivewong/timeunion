#include "tombstone/Interval.hpp"

namespace tsdb {
namespace tombstone {

// Will not check order of min_time and max_time.
// It's user's responsibility.
Interval::Interval(const std::initializer_list<int64_t> &list) {
  if (list.size() == 2) {
    min_time = *(list.begin());
    max_time = *(list.begin() + 1);
  }
}

Interval::Interval(int64_t min_time, int64_t max_time)
    : min_time(min_time), max_time(max_time) {}

bool Interval::in_bounds(int64_t t) const {
  return t <= max_time && t >= min_time;
}

// If (*this) is subrange of itvs.
bool Interval::is_subrange(const Intervals &itvs) const {
  for (auto const &interval : itvs) {
    if (interval.in_bounds(min_time) && interval.in_bounds(max_time))
      return true;
  }
  return false;
}

bool is_subrange(int64_t min_time, int64_t max_time, const Intervals &itvs) {
  for (auto const &interval : itvs) {
    if (interval.in_bounds(min_time) && interval.in_bounds(max_time))
      return true;
  }
  return false;
}

void itvls_add(Intervals &itvs, const Interval &itvl) {
  for (std::list<Interval>::iterator i = itvs.begin(); i != itvs.end(); i++) {
    // itvs[i].min      itvs[i].max itvl
    //      |---------------------| |---------|
    // The above two intervals can be concatenated together
    if ((*i).in_bounds(itvl.min_time) || (*i).in_bounds(itvl.min_time - 1)) {
      std::list<Interval>::iterator cur = std::next(i, 1);
      if ((*i).max_time < itvl.max_time) (*i).max_time = itvl.max_time;
      while (cur != itvs.end()) {
        if ((*cur).min_time > itvl.max_time + 1) break;
        ++cur;
      }
      if ((*i).max_time < (*(std::prev(cur, 1))).max_time)
        (*i).max_time = (*(std::prev(cur, 1))).max_time;
      itvs.erase(std::next(i, 1), cur);
      return;
    } else if ((*i).in_bounds(itvl.max_time) ||
               (*i).in_bounds(itvl.max_time + 1)) {
      // if(itvl.min_time < (*i).max_time)
      (*i).min_time = itvl.min_time;
      return;
    }
    if ((*i).min_time > itvl.min_time) {
      itvs.insert(i, itvl);
      return;
    }
  }
  itvs.push_back(itvl);
}

bool intervals_equal(const Intervals &itvls1, const Intervals &itvls2) {
  if (itvls1.size() != itvls2.size()) return false;
  Intervals::const_iterator it1 = itvls1.cbegin();
  Intervals::const_iterator it2 = itvls2.cbegin();
  while (it1 != itvls1.cend()) {
    if (it1->min_time != it2->min_time || it1->max_time != it2->max_time)
      return false;
    ++it1;
    ++it2;
  }
  return true;
}

}  // namespace tombstone
}  // namespace tsdb