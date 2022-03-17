//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
#pragma once

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

#include "cloud/Filename.hpp"
#include "cloud/Slice.hpp"
#include "cloud/Status.hpp"

namespace tsdb {
namespace cloud {

class AwsS3ClientWrapper;
class S3Wrapper;
class S3ReadableFile {
 public:
  S3ReadableFile(S3Wrapper* env, const std::string& bucket_prefix,
                 const std::string& fname, uint64_t size);

  Status read_helper(char* scratch, uint64_t offset, size_t n,
                     uint64_t* size) const;

  // sequential access, read data at current offset in file
  Status Read(size_t n, Slice* result, char* scratch);

  // random access, read data from specified offset in file
  Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const;

  Status Skip(uint64_t n);

 private:
  S3Wrapper* env_;
  std::string fname_;
  std::string fname_without_epoch_;
  Aws::String s3_bucket_;
  Aws::String s3_object_;
  uint64_t offset_;
  uint64_t file_size_;
};

// Appends to a file in S3.
class S3WritableFile {
 private:
  S3Wrapper* env_;
  std::string fname_;
  std::string tmp_file_;
  Status status_;
  std::string bucket_prefix_;
  std::string cloud_fname_;
  bool closed_ = false;

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

  S3WritableFile(S3Wrapper* env, const std::string& local_fname,
                 const std::string& bucket_prefix,
                 const std::string& cloud_fname);

  ~S3WritableFile();

  Status status() { return status_; }

  Status Close();
};

}  // namespace cloud.
}  // namespace tsdb.