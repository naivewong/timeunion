#include "util/io_posix.h"

#include <algorithm>
#include <errno.h>
#include <fcntl.h>
#include <future>
#include <linux/falloc.h>
#include <linux/fs.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <sys/syscall.h>
#include <sys/sysmacros.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>

#include "leveldb/status.h"

namespace leveldb {

bool PosixWrite(int fd, const char* buf, size_t nbyte) {
  const size_t kLimit1Gb = 1UL << 30;

  const char* src = buf;
  size_t left = nbyte;

  while (left != 0) {
    size_t bytes_to_write = std::min(left, kLimit1Gb);

    ssize_t done = write(fd, src, bytes_to_write);
    if (done < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    left -= done;
    src += done;
  }
  return true;
}

bool PosixPositionedWrite(int fd, const char* buf, size_t nbyte, off_t offset) {
  const size_t kLimit1Gb = 1UL << 30;

  const char* src = buf;
  size_t left = nbyte;

  while (left != 0) {
    size_t bytes_to_write = std::min(left, kLimit1Gb);

    ssize_t done = pwrite(fd, src, bytes_to_write, offset);
    if (done < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    left -= done;
    offset += done;
    src += done;
  }

  return true;
}

/*
 * PosixDirectory
 */

PosixDirectory::~PosixDirectory() { close(fd_); }

Status PosixDirectory::Fsync() {
#ifndef OS_AIX
  if (fsync(fd_) == -1) {
    return IOError("While fsync", "a directory", errno);
  }
#endif
  return Status::OK();
}

/*
 * PosixRandomRWFile
 */

PosixRandomRWFile::PosixRandomRWFile(const std::string& fname, int fd)
    : filename_(fname), fd_(fd) {}

PosixRandomRWFile::~PosixRandomRWFile() {
  if (fd_ >= 0) {
    Close();
  }
}

Status PosixRandomRWFile::Write(uint64_t offset, const Slice& data) {
  const char* src = data.data();
  size_t nbytes = data.size();
  if (!PosixPositionedWrite(fd_, src, nbytes, static_cast<off_t>(offset))) {
    return IOError("While write random read/write file at offset " +
                       std::to_string(offset),
                   filename_, errno);
  }

  return Status::OK();
}

Status PosixRandomRWFile::Read(uint64_t offset, size_t n, Slice* result,
                               char* scratch) const {
  size_t left = n;
  char* ptr = scratch;
  while (left > 0) {
    ssize_t done = pread(fd_, ptr, left, offset);
    if (done < 0) {
      // error while reading from file
      if (errno == EINTR) {
        // read was interrupted, try again.
        continue;
      }
      return IOError("While reading random read/write file offset " +
                         std::to_string(offset) + " len " + std::to_string(n),
                     filename_, errno);
    } else if (done == 0) {
      // Nothing more to read
      break;
    }

    // Read `done` bytes
    ptr += done;
    offset += done;
    left -= done;
  }

  *result = Slice(scratch, n - left);
  return Status::OK();
}

Status PosixRandomRWFile::Flush() { return Status::OK(); }

Status PosixRandomRWFile::Sync() {
  if (fdatasync(fd_) < 0) {
    return IOError("While fdatasync random read/write file", filename_, errno);
  }
  return Status::OK();
}

Status PosixRandomRWFile::Fsync() {
  if (fsync(fd_) < 0) {
    return IOError("While fsync random read/write file", filename_, errno);
  }
  return Status::OK();
}

Status PosixRandomRWFile::Close() {
  if (close(fd_) < 0) {
    return IOError("While close random read/write file", filename_, errno);
  }
  fd_ = -1;
  return Status::OK();
}

}  // namespace leveldb