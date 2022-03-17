#include "base/Rune.hpp"

namespace tsdb {
namespace base {

// Rune and size.
Rune decode_rune(const std::string &s, int pos) {
  int i = 1;
  int32_t rune = s[pos];
  if (s[pos] & 0b11000000 == 0b11000000) {
    while (pos + i < s.length() && (s[pos + i] & 0b11000000 == 0b10000000)) {
      rune <<= 8;
      rune += s[pos + i];
      ++i;
    }
  }
  return Rune(rune, i);
}
Rune decode_rune(const char *s, int size, int pos) {
  int i = 1;
  int32_t rune = s[pos];
  if (s[pos] & 0b11000000 == 0b11000000) {
    while (pos + i < size && (s[pos + i] & 0b11000000 == 0b10000000)) {
      rune <<= 8;
      rune += s[pos + i];
      ++i;
    }
  }
  return Rune(rune, i);
}
Rune decode_rune(const std::vector<char> &s, int pos) {
  int i = 1;
  int32_t rune = s[pos];
  if (s[pos] & 0b11000000 == 0b11000000) {
    while (pos + i < s.size() && (s[pos + i] & 0b11000000 == 0b10000000)) {
      rune <<= 8;
      rune += s[pos + i];
      ++i;
    }
  }
  return Rune(rune, i);
}

int encode(std::string &s, const Rune &rune) {
  s += rune.get_str();
  return rune.size;
}
int encode(char *s, const Rune &rune) {
  std::string r = rune.get_str();
  for (int i = 0; i < rune.size; ++i) s[i] = r[i];
  return rune.size;
}
int encode(std::vector<char> &s, int pos, const Rune &rune) {
  std::string r = rune.get_str();
  for (int i = 0; i < rune.size; ++i) s[i] = r[i];
  return rune.size;
}
int encode(std::vector<char> &s, const Rune &rune) {
  std::string r = rune.get_str();
  for (int i = 0; i < rune.size; ++i) s.push_back(r[i]);
  return rune.size;
}

}  // namespace base
}  // namespace tsdb