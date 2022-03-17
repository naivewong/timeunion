#ifndef TOMBSTONEREADERINTERFACE_H
#define TOMBSTONEREADERINTERFACE_H

#include <boost/function.hpp>

#include "base/Error.hpp"
#include "tombstone/Interval.hpp"

namespace tsdb {
namespace tombstone {

class TombstoneReaderInterface {
 public:
  typedef boost::function<void(uint64_t, const Intervals &)> IterFunc;
  typedef boost::function<error::Error(uint64_t, const Intervals &)>
      ErrIterFunc;

  // NOTICE, may throw std::out_of_range.
  virtual const Intervals &get(uint64_t ref) const = 0;
  virtual void iter(const IterFunc &f) const = 0;
  virtual error::Error iter(const ErrIterFunc &f) const = 0;

  // Number of Interval
  virtual uint64_t total() const = 0;

  virtual void add_interval(uint64_t ref, const Interval &itvl){};

  virtual ~TombstoneReaderInterface() = default;
};

}  // namespace tombstone
}  // namespace tsdb

#endif