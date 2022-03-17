#ifndef BASE_RUNE_H
#define BASE_RUNE_H

#include <stdint.h>

#include <string>
#include <vector>

#include "base/Endian.hpp"

namespace tsdb {
namespace base {

class Rune {
 public:
  int32_t rune;
  int size;

  Rune() : rune(0), size(0) {}
  Rune(int32_t rune, int size) : rune(rune), size(size) {}
  bool is_char() { return size == 1; }
  bool is_space() {
    if (size != 1) return false;
    char r = static_cast<char>(rune & 0xff);
    return r == ' ' || r == '\t' || r == '\n' || r == '\r';
  }
  bool is_end_of_line() {
    if (size != 1) return false;
    char r = static_cast<char>(rune & 0xff);
    return r == '\r' || r == '\n';
  }
  bool is_alpha() {
    if (size != 1) return false;
    char r = static_cast<char>(rune & 0xff);
    return r == '_' || ('a' <= r && r <= 'z') || ('A' <= r && r <= 'Z');
  }
  bool is_digit() {
    if (size != 1) return false;
    char r = static_cast<char>(rune & 0xff);
    return '0' <= r && r <= '9';
  }
  bool is_alpha_numeric() { return is_alpha() || is_digit(); }
  int digit_val() {
    if (size != 1) return 16;
    char ch = static_cast<char>(rune & 0xff);
    if ('0' <= ch && ch <= '9')
      return static_cast<int>(ch - '0');
    else if ('a' <= ch && ch <= 'f')
      return static_cast<int>(ch - 'a' + 10);
    else if ('A' <= ch && ch <= 'F')
      return static_cast<int>(ch - 'A' + 10);
    return 16;  // Larger than any legal digit val.
  }
  std::string get_str() const {
    if (size > 0) {
      std::string s(size, '\0');
      for (int i = 0; i < size; ++i) s[i] = rune >> ((size - i - 1) * 8);
      return s;
    }
    return std::string();
  }
  std::string get_hex_str() const { return n2hexstr(rune); }

  std::pair<int32_t, int> get_rune() { return {rune, size}; }

  bool is_valid_utf8() {
    int num;
    unsigned int cp;
    int i = 0;
    unsigned char byte = (rune >> ((size - i - 1) * 8)) & 0xff;
    while (i < size) {
      if ((byte & 0x80) == 0x00) {
        // U+0000 to U+007F
        cp = (byte & 0x7F);
        num = 1;
      } else if ((byte & 0xE0) == 0xC0) {
        // U+0080 to U+07FF
        cp = (byte & 0x1F);
        num = 2;
      } else if ((byte & 0xF0) == 0xE0) {
        // U+0800 to U+FFFF
        cp = (byte & 0x0F);
        num = 3;
      } else if ((byte & 0xF8) == 0xF0) {
        // U+10000 to U+10FFFF
        cp = (byte & 0x07);
        num = 4;
      } else
        return false;

      ++i;
      byte = (rune >> ((size - i - 1) * 8)) & 0xff;
      for (int j = 1; j < num; ++j) {
        if ((byte & 0xC0) != 0x80) return false;
        cp = (cp << 6) | (byte & 0x3F);
        ++i;
        byte = (rune >> ((size - i - 1) * 8)) & 0xff;
      }

      if ((cp > 0x10FFFF) || ((cp >= 0xD800) && (cp <= 0xDFFF)) ||
          ((cp <= 0x007F) && (num != 1)) ||
          ((cp >= 0x0080) && (cp <= 0x07FF) && (num != 2)) ||
          ((cp >= 0x0800) && (cp <= 0xFFFF) && (num != 3)) ||
          ((cp >= 0x10000) && (cp <= 0x1FFFFF) && (num != 4)))
        return false;
    }

    return true;
  }

  bool eof() { return size == -1; }

  operator bool() const { return size > 0; }
  bool operator==(const Rune &r) { return rune == r.rune; }
  bool operator!=(const Rune &r) { return rune != r.rune; }
  bool operator==(char c) {
    return size == 1 && static_cast<char>(rune & 0xff) == c;
  }
  bool operator!=(char c) {
    return size != 1 || static_cast<char>(rune & 0xff) != c;
  }

  static Rune max_rune() { return Rune(1114111, 3); }  // '\U0010FFFF'.
  static Rune max_ascii() { return Rune(127, 1); }     // '\u007F'.
  static Rune max_latin1() { return Rune(255, 1); }    // '\u00FF'.
};

// Rune and size.
Rune decode_rune(const std::string &s, int pos = 0);
Rune decode_rune(const char *s, int size, int pos = 0);
Rune decode_rune(const std::vector<char> &s, int pos = 0);

int encode(std::string &s, const Rune &rune);
int encode(char *s, const Rune &rune);
int encode(std::vector<char> &s, int pos, const Rune &rune);
int encode(std::vector<char> &s, const Rune &rune);

}  // namespace base
}  // namespace tsdb

#endif