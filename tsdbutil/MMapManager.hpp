#pragma once
#include <boost/iostreams/device/mapped_file.hpp>

namespace tsdb {
namespace tsdbutil {

class MMapManager {
 private:
  boost::iostreams::mapped_file file_;

 public:
  MMapManager(const std::string& name, bool new_file, int new_file_size = 0) {
    boost::iostreams::mapped_file_params params;
    params.path = name;
    if (new_file) params.new_file_size = new_file_size;
    params.flags = boost::iostreams::mapped_file::mapmode::readwrite;
    file_.open(params);
  }
  ~MMapManager() { file_.close(); }

  char* data() { return file_.data(); }
  size_t size() { return file_.size(); }
};

}  // namespace tsdbutil
}  // namespace tsdb