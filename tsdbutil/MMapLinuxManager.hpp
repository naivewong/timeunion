#pragma once

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#include <fstream>

namespace tsdb {
namespace tsdbutil {

class MMapLinuxManager {
 private:
  int fd_;
  char* addr_;
  size_t length_;
  bool err_;

 public:
  MMapLinuxManager(const std::string& name, int new_file_size = 0)
      : err_(false) {
    fd_ = open(name.c_str(), O_RDWR | O_CREAT, (mode_t)0600);
    if (fd_ < 0) {
      printf("MMapLinuxManager cannot open file: %s\n", strerror(errno));
      err_ = true;
      return;
    }
    if (new_file_size > 0) {
      lseek(fd_, new_file_size - 1, SEEK_SET);
      write(fd_, "\0", 1);
      length_ = new_file_size;
    } else {
      length_ = lseek(fd_, 0, SEEK_END);
      lseek(fd_, 0, SEEK_SET);
    }

    addr_ = reinterpret_cast<char*>(
        mmap(NULL, length_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0));
    if (addr_ == MAP_FAILED) {
      printf("MMapLinuxManager cannot mmap: %s\n", strerror(errno));
      err_ = true;
    }
  }

  ~MMapLinuxManager() {
    msync(addr_, length_, MS_SYNC);
    munmap(addr_, length_);
    close(fd_);
  }

  // User should use it in-bound.
  void sync(size_t off, size_t length) { msync(addr_ + off, length, MS_SYNC); }

  char* data() { return addr_; }
  size_t size() { return length_; }
};

}  // namespace tsdbutil
}  // namespace tsdb