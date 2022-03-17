#ifndef STATS_TIMERGROUP_H
#define STATS_TIMERGROUP_H

#include <unordered_map>

#include "tsdbutil/stats/Timer.hpp"

namespace tsdb {
namespace tsdbutil {

// A TimerGroup represents a group of timers relevant to a single query.
class TimerGroup {
 private:
  std::unordered_map<std::string, Timer> timers;

 public:
  Timer get(const std::string& name) {
    if (timers.find(name) == timers.end()) timers[name] = Timer(name);
    return timers[name];
  }
};

}  // namespace tsdbutil
}  // namespace tsdb

#endif