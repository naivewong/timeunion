#ifndef MMAPSLICE_H
#define MMAPSLICE_H

#include <boost/iostreams/device/mapped_file.hpp>

#include "tsdbutil/ByteSlice.hpp"

namespace tsdb {
namespace tsdbutil {

class MMapSlice : public ByteSlice {
 private:
  boost::iostreams::mapped_file_source file;
  int len_;

 public:
  MMapSlice(const std::string &path) {
    try {
      file.open(path);
      if (file.is_open())
        len_ = file.size();
      else
        len_ = -1;
    } catch (const std::exception &e) {
      len_ = -1;  // -1 len_ means error opening
    }
  }

  int len() const { return len_; }

  std::pair<const uint8_t *, int> range(int begin, int end) const {
    if (len_ == -1 || end <= begin)
      return std::make_pair<const uint8_t *, int>(nullptr, 0);
    if (begin < 0) begin = 0;
    if (end > len_) end = len_;
    return std::make_pair<const uint8_t *, int>(
        reinterpret_cast<const uint8_t *>(file.data()) + begin, end - begin);
  }

  ~MMapSlice() {
    if (len_ >= 0) file.close();
  }
};

}  // namespace tsdbutil
}  // namespace tsdb

#endif