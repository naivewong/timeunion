#ifndef STATS_TIMER_H
#define STATS_TIMER_H

#include <deque>

#include "base/TimeStamp.hpp"

namespace tsdb {
namespace tsdbutil {

class Timer {
 private:
  std::string name;
  base::TimeStamp created;
  base::TimeStamp start;
  double duration;  // seconds.

 public:
  Timer(const std::string& name)
      : name(name), created(base::TimeStamp::now()), duration(0) {}

  void start() { created = base::TimeStamp::now(); }
  void stop() {
    duration += base::timeDifference(base::TimeStamp::now(), start);
  }
  double duration() { return duration; }
  std::string timer_str() { return name + ": " + std::to_string(duration); }
};

typedef std::deque<Timer> Timers;

}  // namespace tsdbutil
}  // namespace tsdb

#endif