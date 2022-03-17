#ifndef INTERVAL_H
#define INTERVAL_H

#include <stdint.h>

#include <initializer_list>
#include <list>

namespace tsdb {
namespace tombstone {

class Interval;

typedef std::list<Interval> Intervals;

// Will not check order of min_time and max_time.
// It's user's responsibility.
class Interval {
 public:
  int64_t min_time;
  int64_t max_time;

  Interval() = default;

  Interval(const std::initializer_list<int64_t> &list);

  Interval(int64_t min_time, int64_t max_time);

  bool in_bounds(int64_t t) const;

  // If (*this) is subrange of itvs.
  bool is_subrange(const Intervals &itvs) const;
};

bool is_subrange(int64_t min_time, int64_t max_time, const Intervals &itvs);

void itvls_add(Intervals &itvs, const Interval &itvl);

bool intervals_equal(const Intervals &itvls1, const Intervals &itvls2);

}  // namespace tombstone
}  // namespace tsdb

#endif