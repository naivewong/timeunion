#ifndef LOGSTREAM_H
#define LOGSTREAM_H
#include "base/FixedBuffer.hpp"

namespace tsdb {
namespace base {

class LogStream : boost::noncopyable {
  typedef LogStream self;

 public:
  typedef FixedBuffer<SmallBuffer> Buffer;

  self &operator<<(bool v) {
    buffer_.append(v ? "1" : "0", 1);
    return *this;
  }

  self &operator<<(short);
  self &operator<<(unsigned short);
  self &operator<<(int);
  self &operator<<(unsigned int);
  self &operator<<(long);
  self &operator<<(unsigned long);
  self &operator<<(long long);
  self &operator<<(unsigned long long);

  self &operator<<(const void *);

  self &operator<<(float v) {
    *this << static_cast<double>(v);
    return *this;
  }
  self &operator<<(double);
  // self& operator<<(long double);

  self &operator<<(char v) {
    if (v == 0) {
      *this << "0";
      return *this;
    }
    buffer_.append(&v, 1);
    return *this;
  }

  // self& operator<<(signed char);
  // self& operator<<(unsigned char);

  self &operator<<(const char *str) {
    if (str) {
      buffer_.append(str, strlen(str));
    } else {
      buffer_.append("(null)", 6);
    }
    return *this;
  }

  // self& operator<<(const unsigned char* str){
  //     return operator<<(reinterpret_cast<const char*>(str));
  // }

  // #ifndef MUDUO_STD_STRING
  self &operator<<(const std::string &v) {
    buffer_.append(v.c_str(), v.size());
    return *this;
  }
  // #endif

  // self& operator<<(const StringPiece& v)
  // {
  // buffer_.append(v.data(), v.size());
  // return *this;
  // }

  // self& operator<<(const Buffer& v){
  //     *this << v.toStringPiece();
  //     return *this;
  // }

  void append(const char *data, int len) { buffer_.append(data, len); }
  const Buffer &buffer() const { return buffer_; }
  void resetBuffer() { buffer_.reset(); }

 private:
  // void staticCheck();

  template <typename T>
  void formatInteger(T);

  Buffer buffer_;

  static const int kMaxNumericSize = 64;
};

// class Fmt{
//     public:
//         template<typename T>
//         Fmt(const char* fmt, T val);

//         const char* data() const { return buf_; }
//         int length() const { return length_; }

//     private:
//         char buf_[32];
//         int length_;
// };

// inline LogStream& operator<<(LogStream& s, const Fmt& fmt){
//     s.append(fmt.data(), fmt.length());
//     return s;
// }

}  // namespace base
}  // namespace tsdb

#endif