#ifndef TIMESTAMP_H
#define TIMESTAMP_H
#include <stdint.h>
#include <sys/time.h>

#include <boost/operators.hpp>
#include <string>

namespace tsdb {
namespace base {

class TimeStamp : public boost::equality_comparable<TimeStamp>,
                  public boost::less_than_comparable<TimeStamp> {
 public:
  ///
  /// Constucts an invalid Timestamp.
  ///
  TimeStamp() : microSecondsSinceEpoch_(0) {}

  ///
  /// Constucts a Timestamp at specific time
  ///
  /// @param microSecondsSinceEpoch
  explicit TimeStamp(int64_t microSecondsSinceEpochArg)
      : microSecondsSinceEpoch_(microSecondsSinceEpochArg) {}

  void swap(TimeStamp &that) {
    std::swap(microSecondsSinceEpoch_, that.microSecondsSinceEpoch_);
  }

  // default copy/assignment/dtor are Okay
  // can return a local buffer because string(char buf[])
  std::string toString() const {
    char buf[32] = {0};
    int64_t seconds = microSecondsSinceEpoch_ / kMicroSecondsPerSecond;
    int64_t microseconds = microSecondsSinceEpoch_ % kMicroSecondsPerSecond;
    // align to the left
    sprintf(buf, "%ld.%-6ld", seconds, microseconds);
    return buf;
  }

  std::string toFormattedString(bool showMicroseconds = true) const {
    char buf[64] = {0};
    time_t seconds =
        static_cast<time_t>(microSecondsSinceEpoch_ / kMicroSecondsPerSecond);
    struct tm tm_time;
    gmtime_r(&seconds, &tm_time);

    if (showMicroseconds) {
      int microseconds =
          static_cast<int>(microSecondsSinceEpoch_ % kMicroSecondsPerSecond);
      snprintf(buf, sizeof(buf), "%4d%02d%02d %02d:%02d:%02d.%-6d",
               tm_time.tm_year + 1900, tm_time.tm_mon + 1, tm_time.tm_mday,
               tm_time.tm_hour, tm_time.tm_min, tm_time.tm_sec, microseconds);
    } else {
      snprintf(buf, sizeof(buf), "%4d%02d%02d %02d:%02d:%02d",
               tm_time.tm_year + 1900, tm_time.tm_mon + 1, tm_time.tm_mday,
               tm_time.tm_hour, tm_time.tm_min, tm_time.tm_sec);
    }
    return buf;
  }

  bool valid() const { return microSecondsSinceEpoch_ > 0; }

  // for internal usage.
  int64_t microSecondsSinceEpoch() const { return microSecondsSinceEpoch_; }
  time_t secondsSinceEpoch() const {
    return static_cast<time_t>(microSecondsSinceEpoch_ /
                               kMicroSecondsPerSecond);
  }

  ///
  /// Get time of now.
  ///
  static TimeStamp now() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    int64_t seconds = tv.tv_sec;
    return TimeStamp(seconds * kMicroSecondsPerSecond + tv.tv_usec);
  }

  static TimeStamp invalid() { return TimeStamp(); }

  static TimeStamp fromUnixTime(time_t t) { return fromUnixTime(t, 0); }

  static TimeStamp fromUnixTime(time_t t, int microseconds) {
    return TimeStamp(static_cast<int64_t>(t) * kMicroSecondsPerSecond +
                     microseconds);
  }

  static const int kMicroSecondsPerSecond = 1000 * 1000;
  static const int MilliSecondsPerSecond = 1000;

 private:
  int64_t microSecondsSinceEpoch_;
};

inline bool operator<(TimeStamp lhs, TimeStamp rhs) {
  return lhs.microSecondsSinceEpoch() < rhs.microSecondsSinceEpoch();
}

inline bool operator==(TimeStamp lhs, TimeStamp rhs) {
  return lhs.microSecondsSinceEpoch() == rhs.microSecondsSinceEpoch();
}

///
/// Gets time difference of two timestamps, result in seconds.
///
/// @param high, low
/// @return (high-low) in seconds
/// @c double has 52-bit precision, enough for one-microsecond
/// resolution for next 100 years.
inline double timeDifference(TimeStamp high, TimeStamp low) {
  int64_t diff = high.microSecondsSinceEpoch() - low.microSecondsSinceEpoch();
  return static_cast<double>(diff) / TimeStamp::kMicroSecondsPerSecond;
}

inline double timeDifference_Milli(TimeStamp high, TimeStamp low) {
  int64_t diff = high.microSecondsSinceEpoch() - low.microSecondsSinceEpoch();
  return static_cast<double>(diff) / TimeStamp::MilliSecondsPerSecond;
}

///
/// Add @c seconds to given timestamp.
///
/// @return timestamp+seconds as Timestamp
///
inline TimeStamp addTime(TimeStamp timestamp, double seconds) {
  int64_t delta =
      static_cast<int64_t>(seconds * TimeStamp::kMicroSecondsPerSecond);
  return TimeStamp(timestamp.microSecondsSinceEpoch() + delta);
}

}  // namespace base
}  // namespace tsdb

#endif