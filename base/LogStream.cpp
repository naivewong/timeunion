#include "base/LogStream.hpp"

#include <stdint.h>

#include <algorithm>

namespace tsdb {
namespace base {

char digits[] = "0123456789ABCDEF";

template <typename T>
size_t convert(char *buf, T val) {
  size_t length = 0;
  std::string temp;
  T temp_val = val;
  if (temp_val < 0) {
    *buf++ = '-';
    temp += '-';
  }
  while (temp_val > 0) {
    // *buf++ = digits[temp_val % 10];
    temp += digits[temp_val % 10];
    temp_val /= 10;
    ++length;
  }
  reverse(temp.begin(), temp.end());
  memcpy(buf, temp.c_str(), length);
  if (val < 0) ++length;
  return length;
}

size_t convertHEX(char *buf, uintptr_t val) {
  size_t length = 0;
  std::string temp;
  while (val > 0) {
    temp += digits[val % 16];
    val /= 16;
    ++length;
  }
  reverse(temp.begin(), temp.end());
  memcpy(buf, temp.c_str(), length);
  return length;
}

template <typename T>
void LogStream::formatInteger(T val) {
  if (buffer_.avail() >= kMaxNumericSize) {
    size_t add_length = convert(buffer_.current(), val);
    buffer_.add(add_length);
  }
}

LogStream &LogStream::operator<<(short val) {
  *this << (int)(val);
  return *this;
}

LogStream &LogStream::operator<<(unsigned short val) {
  *this << (unsigned int)(val);
  return *this;
}

LogStream &LogStream::operator<<(int val) {
  formatInteger(val);
  return *this;
}

LogStream &LogStream::operator<<(unsigned int val) {
  formatInteger(val);
  return *this;
}

LogStream &LogStream::operator<<(long val) {
  formatInteger(val);
  return *this;
}

LogStream &LogStream::operator<<(unsigned long val) {
  formatInteger(val);
  return *this;
}

LogStream &LogStream::operator<<(long long val) {
  formatInteger(val);
  return *this;
}

LogStream &LogStream::operator<<(unsigned long long val) {
  formatInteger(val);
  return *this;
}

LogStream &LogStream::operator<<(const void *p) {
  if (buffer_.avail() >= kMaxNumericSize) {
    char *buf = buffer_.current();
    *buf++ = '0';
    *buf++ = 'x';
    size_t add_length = convertHEX(buf, (uintptr_t)(p));
    buffer_.add(add_length + 2);
  }
  return *this;
}

// g自动选择e或者f，并且不会输出无意义的0
LogStream &LogStream::operator<<(double val) {
  if (buffer_.avail() >= kMaxNumericSize) {
    int add_length = snprintf(buffer_.current(), kMaxNumericSize, "%.12g", val);
    buffer_.add(add_length);
  }
  return *this;
}

}  // namespace base
}  // namespace tsdb