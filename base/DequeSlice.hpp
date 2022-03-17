#ifndef DEQUESLICE_H
#define DEQUESLICE_H

#include <deque>

namespace tsdb {
namespace base {

template <typename T>
class DequeSlice {
 public:
  const std::deque<T> &deque_;
  typename std::deque<T>::const_iterator begin;
  typename std::deque<T>::const_iterator last;
  typename std::deque<T>::const_iterator cur;

  DequeSlice(const std::deque<T> &deque_)
      : deque_(deque_),
        begin(deque_.cbegin()),
        last(deque_.cend() - 1),
        cur(deque_.cbegin()) {}
};

}  // namespace base
}  // namespace tsdb

#endif