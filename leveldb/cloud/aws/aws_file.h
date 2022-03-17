//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
#pragma once
// #ifdef USE_AWS

#include "db/filename.h"
#include <aws/core/Aws.h>
#include <aws/core/utils/DateTime.h>
#include <aws/core/utils/Outcome.h>
#include <aws/core/utils/crypto/CryptoStream.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3Errors.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/CopyObjectResult.h>
#include <aws/s3/model/CreateBucketConfiguration.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/CreateBucketResult.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectResult.h>
#include <aws/s3/model/GetBucketVersioningRequest.h>
#include <aws/s3/model/GetBucketVersioningResult.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/GetObjectResult.h>
#include <aws/s3/model/HeadBucketRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/HeadObjectResult.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/ListObjectsResult.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/PutObjectResult.h>
#include <aws/s3/model/ServerSideEncryption.h>
#include <chrono>
#include <fstream>
#include <iostream>

#include "leveldb/env.h"
#include "leveldb/status.h"

#include "third_party/thread_pool.h"

#include "cloud/aws/aws_env.h"
#include "cloud/filename.h"

namespace leveldb {
inline Aws::String ToAwsString(const std::string& s) {
  return Aws::String(s.data(), s.size());
}

class CloudCache;
class S3ReadableFile : virtual public SequentialFile,
                       virtual public RandomAccessFile {
 public:
  S3ReadableFile(AwsEnv* env, const std::string& bucket_prefix,
                 const std::string& fname, uint64_t size,
                 const EnvOptions& options);

  void read_helper(char* scratch, uint64_t offset, size_t n, uint64_t* size,
                   int* st, WaitGroup* wg) const;

  // sequential access, read data at current offset in file
  virtual Status Read(size_t n, Slice* result, char* scratch) override;

  // random access, read data from specified offset in file
  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const override;

  virtual Status Skip(uint64_t n) override;

  virtual size_t GetUniqueId(char* id, size_t max_size) const override;

 private:
  AwsEnv* env_;
  std::string fname_;
  std::string fname_without_epoch_;
  Aws::String s3_bucket_;
  Aws::String s3_object_;
  uint64_t offset_;
  uint64_t file_size_;

  CloudCache* ccache_;
  bool is_vl_;

  EnvOptions options_;
};

// Appends to a file in S3.
class S3WritableFile : public WritableFile {
 private:
  AwsEnv* env_;
  std::string fname_;
  std::string tmp_file_;
  Status status_;
  std::unique_ptr<WritableFile> local_file_;
  std::string bucket_prefix_;
  std::string cloud_fname_;
  bool is_manifest_;

  EnvOptions options_;

 public:
  // create S3 bucket
  static Status CreateBucketInS3(
      std::shared_ptr<AwsS3ClientWrapper> client,
      const std::string& bucket_prefix,
      const Aws::S3::Model::BucketLocationConstraint& location);

  // bucket exists and we can access it
  static Status BucketExistsInS3(
      std::shared_ptr<AwsS3ClientWrapper> client,
      const std::string& bucket_prefix,
      const Aws::S3::Model::BucketLocationConstraint& location);

  S3WritableFile(AwsEnv* env, const std::string& local_fname,
                 const std::string& bucket_prefix,
                 const std::string& cloud_fname, const EnvOptions& options,
                 const CloudEnvOptions cloud_env_options);

  virtual ~S3WritableFile();

  virtual Status Append(const Slice& data) override {
    assert(status_.ok());
    // write to temporary file
    return local_file_->Append(data);
  }

  // Status PositionedAppend(const Slice& data, uint64_t offset) override {
  //   return local_file_->PositionedAppend(data, offset);
  // }
  // Status Truncate(uint64_t size) override {
  //   return local_file_->Truncate(size);
  // }
  // Status Fsync() override { return local_file_->Fsync(); }
  // bool IsSyncThreadSafe() const override {
  //   return local_file_->IsSyncThreadSafe();
  // }
  // bool use_direct_io() const override { return local_file_->use_direct_io();
  // } size_t GetRequiredBufferAlignment() const override {
  //   return local_file_->GetRequiredBufferAlignment();
  // }
  // uint64_t GetFileSize() override { return local_file_->GetFileSize(); }
  // size_t GetUniqueId(char* id, size_t max_size) const override {
  //   return local_file_->GetUniqueId(id, max_size);
  // }
  // Status InvalidateCache(size_t offset, size_t length) override {
  //   return local_file_->InvalidateCache(offset, length);
  // }
  // Status RangeSync(uint64_t offset, uint64_t nbytes) override {
  //   return local_file_->RangeSync(offset, nbytes);
  // }
  // Status Allocate(uint64_t offset, uint64_t len) override {
  //   return local_file_->Allocate(offset, len);
  // }

  virtual Status Flush() override {
    assert(status_.ok());
    return local_file_->Flush();
  }

  virtual Status Sync() override;

  virtual Status status() { return status_; }

  virtual Status Close() override;
};

// Appends to a string in S3.
class S3StringWritableFile : public WritableFile {
 private:
  AwsEnv* env_;
  std::string fname_;
  std::string tmp_file_;
  Status status_;
  std::shared_ptr<Aws::StringStream> input_data_;
  uint64_t write_size_;
  std::string bucket_prefix_;
  std::string cloud_fname_;
  bool is_manifest_;

  EnvOptions options_;

 public:
  // create S3 bucket
  static Status CreateBucketInS3(
      std::shared_ptr<AwsS3ClientWrapper> client,
      const std::string& bucket_prefix,
      const Aws::S3::Model::BucketLocationConstraint& location);

  // bucket exists and we can access it
  static Status BucketExistsInS3(
      std::shared_ptr<AwsS3ClientWrapper> client,
      const std::string& bucket_prefix,
      const Aws::S3::Model::BucketLocationConstraint& location);

  S3StringWritableFile(AwsEnv* env, const std::string& bucket_prefix,
                       const std::string& cloud_fname,
                       const EnvOptions& options,
                       const CloudEnvOptions cloud_env_options);

  virtual ~S3StringWritableFile();

  virtual Status Append(const Slice& data) override {
    assert(status_.ok());
    input_data_->write(data.data(), data.size());
    write_size_ += data.size();
    return status_;
  }

  virtual Status Flush() override {
    assert(status_.ok());
    return status_;
  }

  virtual Status Sync() override { return status_; }

  virtual Status status() { return status_; }

  virtual Status Close() override;
};

}  // namespace leveldb

// #endif /* USE_AWS */
