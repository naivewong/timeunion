//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.

#pragma once
#include <atomic>
#include <thread>

#include "leveldb/cloud/cloud_env_options.h"
#include "leveldb/env.h"
#include "leveldb/status.h"

#include "cloud/cloud_env_impl.h"

namespace leveldb {

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"

// An implementation of Env that forwards all calls to another Env.
// May be useful to clients who wish to override just part of the
// functionality of another Env.
class CloudEnvWrapper : public CloudEnvImpl {
 public:
  // Initialize an EnvWrapper that delegates all calls to *t
  explicit CloudEnvWrapper(const CloudEnvOptions& options, Env* t,
                           const std::shared_ptr<Logger>& l)
      : CloudEnvImpl(options, t, l) {
    cloud_env_options.log_type = LogType::kLogNone;
    cloud_env_options.cloud_type = CloudType::kCloudNone;
    notsup_ = Status::NotSupported();
  }

  virtual ~CloudEnvWrapper();

  virtual Status EmptyBucket(const std::string& /*bucket*/,
                             const std::string& /*path_prefix*/) override {
    return notsup_;
  }
  virtual Status NewSequentialFileCloud(const std::string& bucket_prefix,
                                        const std::string& fname,
                                        std::unique_ptr<SequentialFile>* result,
                                        const EnvOptions& options) override {
    return notsup_;
  }
  virtual Status SaveDbid(const std::string& bucket_name,
                          const std::string& dbid,
                          const std::string& dirname) override {
    return notsup_;
  }
  virtual Status GetPathForDbid(const std::string& bucket_prefix,
                                const std::string& dbid,
                                std::string* dirname) override {
    return notsup_;
  }
  virtual Status GetDbidList(const std::string& bucket_prefix,
                             DbidList* dblist) override {
    return notsup_;
  }
  virtual Status DeleteDbid(const std::string& bucket_prefix,
                            const std::string& dbid) override {
    return notsup_;
  }

  // Ability to read a file directly from cloud storage
  virtual Status NewSequentialFileCloud(const std::string& fname,
                                        std::unique_ptr<SequentialFile>* result,
                                        const EnvOptions& options) {
    return notsup_;
  }

  // The following text is boilerplate that forwards all methods to base_env
  Status NewSequentialFile(const std::string& f, SequentialFile** r,
                           const EnvOptions& options = EnvOptions()) override {
    return base_env_->NewSequentialFile(f, r, options);
  }
  Status NewRandomAccessFile(
      const std::string& f, RandomAccessFile** r,
      const EnvOptions& options = EnvOptions()) override {
    return base_env_->NewRandomAccessFile(f, r, options);
  }
  Status NewWritableFile(const std::string& f, WritableFile** r,
                         const EnvOptions& options = EnvOptions()) override {
    return base_env_->NewWritableFile(f, r, options);
  }
  Status NewRandomRWFile(const std::string& fname,
                         std::unique_ptr<RandomRWFile>* result,
                         const EnvOptions& options = EnvOptions()) override {
    return base_env_->NewRandomRWFile(fname, result, options);
  }
  virtual Status NewDirectory(const std::string& name,
                              std::unique_ptr<Directory>* result) override {
    return base_env_->NewDirectory(name, result);
  }
  bool FileExists(const std::string& f) override {
    return base_env_->FileExists(f);
  }
  Status GetChildren(const std::string& dir,
                     std::vector<std::string>* r) override {
    return base_env_->GetChildren(dir, r);
  }
  Status GetChildrenFileAttributes(
      const std::string& dir, std::vector<FileAttributes>* result) override {
    return base_env_->GetChildrenFileAttributes(dir, result);
  }
  Status DeleteFile(const std::string& f) override {
    return base_env_->DeleteFile(f);
  }
  Status CreateDir(const std::string& d) override {
    return base_env_->CreateDir(d);
  }
  Status CreateDirIfMissing(const std::string& d) override {
    return base_env_->CreateDirIfMissing(d);
  }
  Status DeleteDir(const std::string& d) override {
    return base_env_->DeleteDir(d);
  }
  Status GetFileSize(const std::string& f, uint64_t* s) override {
    return base_env_->GetFileSize(f, s);
  }

  Status GetFileModificationTime(const std::string& fname,
                                 uint64_t* file_mtime) override {
    return base_env_->GetFileModificationTime(fname, file_mtime);
  }

  Status RenameFile(const std::string& s, const std::string& t) override {
    return base_env_->RenameFile(s, t);
  }

  Status LockFile(const std::string& f, FileLock** l) override {
    return base_env_->LockFile(f, l);
  }

  Status UnlockFile(FileLock* l) override { return base_env_->UnlockFile(l); }

  void Schedule(void (*function)(void* arg), void* arg) override {
    base_env_->Schedule(function, arg);
  }

  void StartThread(void (*f)(void*), void* a) override {
    base_env_->StartThread(f, a);
  }
  virtual Status GetTestDirectory(std::string* path) override {
    return base_env_->GetTestDirectory(path);
  }
  virtual Status NewLogger(const std::string& fname, Logger** result) override {
    return base_env_->NewLogger(fname, result);
  }
  uint64_t NowMicros() override { return base_env_->NowMicros(); }
  void SleepForMicroseconds(int micros) override {
    base_env_->SleepForMicroseconds(micros);
  }

  Status ListObjects(const std::string& bucket_name_prefix,
                     const std::string& bucket_object_prefix,
                     BucketObjectMetadata* meta) override {
    return notsup_;
  }
  Status DeleteObject(const std::string& bucket_name_prefix,
                      const std::string& bucket_object_path) override {
    return notsup_;
  }
  Status ExistsObject(const std::string& bucket_name_prefix,
                      const std::string& bucket_object_path) override {
    return notsup_;
  }
  Status GetObjectSize(const std::string& bucket_name_prefix,
                       const std::string& bucket_object_path,
                       uint64_t* size) override {
    return notsup_;
  }
  Status CopyObject(const std::string& bucket_name_prefix_src,
                    const std::string& bucket_object_path_src,
                    const std::string& bucket_name_prefix_dest,
                    const std::string& bucket_object_path_dest) override {
    return notsup_;
  }
  Status GetObject(const std::string& bucket_name_prefix,
                   const std::string& bucket_object_path,
                   const std::string& local_path) override {
    return notsup_;
  }
  Status PutObject(const std::string& local_path,
                   const std::string& bucket_name_prefix,
                   const std::string& bucket_object_path) override {
    return notsup_;
  }
  Status DeleteCloudFileFromDest(const std::string& path) override {
    return notsup_;
  }

 private:
  Status notsup_;
  std::string empty_;
};

#pragma GCC diagnostic pop
}  // namespace leveldb
