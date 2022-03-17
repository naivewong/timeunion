#ifndef BUFFER_H
#define BUFFER_H
#include <string.h>  // memcpy
// #ifndef MUDUO_STD_STRING
#include <string>
// #endif
#include <boost/noncopyable.hpp>
#include <boost/scoped_ptr.hpp>

namespace tsdb {
namespace base {

const int TestBuffer = 10;
const int SmallBuffer = 4000;
const int LargeBuffer = 4000 * 1000;

template <int SIZE>
class FixedBuffer : boost::noncopyable {
 private:
  char *cur_;
  char data_[SIZE];
  const char *end() const { return data_ + sizeof data_; }

 public:
  FixedBuffer() : cur_(data_) {}

  void append(const char * /*restrict*/ buf, size_t len) {
    if ((size_t)(avail()) > len) {
      memcpy(cur_, buf, len);
      cur_ += len;
    } else {
      memcpy(cur_, buf, avail() - 1);
      cur_ += avail();
    }
  }

  const char *data() const { return data_; }

  int length() const { return (int)(cur_ - data_); }

  char *current() { return cur_; }

  int avail() const { return (int)(end() - cur_); }

  void add(size_t len) { cur_ += len; }

  void reset() { cur_ = data_; }

  void bzero() { memset(data_, 0, sizeof(data_)); }

  std::string toString() const { return std::string(data_, length()); }

  // StringPiece toStringPiece() const { return StringPiece(data_, length()); }
};

}  // namespace base
}  // namespace tsdb
#endif