#pragma once

#include "leveldb/env.h"
#include "leveldb/status.h"

namespace leveldb {

class PosixDirectory : public Directory {
 public:
  explicit PosixDirectory(int fd) : fd_(fd) {}
  ~PosixDirectory();
  virtual Status Fsync() override;

 private:
  int fd_;
};

class PosixRandomRWFile : public RandomRWFile {
 public:
  explicit PosixRandomRWFile(const std::string& fname, int fd);
  virtual ~PosixRandomRWFile();

  virtual Status Write(uint64_t offset, const Slice& data) override;

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const override;

  virtual Status Flush() override;
  virtual Status Sync() override;
  virtual Status Fsync() override;
  virtual Status Close() override;

 private:
  const std::string filename_;
  int fd_;
};

static std::string IOErrorMsg(const std::string& context,
                              const std::string& file_name) {
  if (file_name.empty()) {
    return context;
  }
  return context + ": " + file_name;
}

static Status IOError(const std::string& context, const std::string& file_name,
                      int err_number) {
  switch (err_number) {
    case ENOSPC:
      return Status::NoSpace(IOErrorMsg(context, file_name),
                             strerror(err_number));
    case ESTALE:
      return Status::IOError("kStaleFile");
    case ENOENT:
      return Status::PathNotFound(IOErrorMsg(context, file_name),
                                  strerror(err_number));
    default:
      return Status::IOError(IOErrorMsg(context, file_name),
                             strerror(err_number));
  }
}

}  // namespace leveldb