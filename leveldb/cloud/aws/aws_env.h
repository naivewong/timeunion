//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//

#pragma once
#include <algorithm>
#include <iostream>
#include <stdio.h>
#include <time.h>

#include "port/sys_time.h"

#include "cloud/cloud_env_impl.h"

// #ifdef USE_AWS

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/utils/Outcome.h>
#include <aws/kinesis/KinesisClient.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/BucketLocationConstraint.h>
#include <aws/transfer/TransferManager.h>
#include <chrono>
#include <list>
#include <unordered_map>

namespace leveldb {

extern int put_request_count;
extern int get_request_count;

class S3ReadableFile;

class AwsS3ClientWrapper {
 public:
  AwsS3ClientWrapper(
      std::shared_ptr<Aws::S3::S3Client> client,
      std::shared_ptr<CloudRequestCallback> cloud_request_callback);

  Aws::S3::Model::ListObjectsOutcome ListObjects(
      const Aws::S3::Model::ListObjectsRequest& request);

  Aws::S3::Model::CreateBucketOutcome CreateBucket(
      const Aws::S3::Model::CreateBucketRequest& request);

  Aws::S3::Model::HeadBucketOutcome HeadBucket(
      const Aws::S3::Model::HeadBucketRequest& request);

  Aws::S3::Model::DeleteObjectOutcome DeleteObject(
      const Aws::S3::Model::DeleteObjectRequest& request);

  Aws::S3::Model::CopyObjectOutcome CopyObject(
      const Aws::S3::Model::CopyObjectRequest& request);

  Aws::S3::Model::GetObjectOutcome GetObject(
      const Aws::S3::Model::GetObjectRequest& request);

  Aws::S3::Model::PutObjectOutcome PutObject(
      const Aws::S3::Model::PutObjectRequest& request, uint64_t size_hint = 0);

  Aws::S3::Model::HeadObjectOutcome HeadObject(
      const Aws::S3::Model::HeadObjectRequest& request);

  const std::shared_ptr<Aws::S3::S3Client>& GetClient() const {
    return client_;
  }

 private:
  std::shared_ptr<Aws::S3::S3Client> client_;
  std::shared_ptr<CloudRequestCallback> cloud_request_callback_;
};

namespace detail {
struct JobHandle;
}  // namespace detail

//
// The S3 environment for rocksdb. This class overrides all the
// file/dir access methods and delegates all other methods to the
// default posix environment.
//
// When a SST file is written and closed, it is uploaded synchronusly
// to AWS-S3. The local copy of the sst file is either deleted immediately
// or kept depending on a configuration parameter called keep_local_sst_files.
// If the local copy of the sst file is around, then future reads are served
// from the local sst file. If the local copy of the sst file is not found
// locally, then every read request to portions of that file is translated to
// a range-get request from the corresponding AWS-S3 file-object.
//
// When a WAL file or MANIFEST file is written, every write is synchronously
// written to a Kinesis stream.
//
// If you access multiple rocksdb-cloud instances, create a separate instance
// of AwsEnv for each of those rocksdb-cloud instances. This is required because
// the cloud-configuration needed to operate on an individual instance of
// rocksdb
// is associated with a specific instance of AwsEnv. All AwsEnv internally share
// Env::Posix() for sharing common resources like background threads, etc.
//
class AwsEnv : public CloudEnvImpl {
 public:
  // A factory method for creating S3 envs
  static Status NewAwsEnv(Env* env, const CloudEnvOptions& env_options,
                          const std::shared_ptr<Logger>& info_log,
                          CloudEnv** cenv);

  static Status TEST_NewAwsEnv(Env* env, const CloudEnvOptions& env_options,
                               const std::shared_ptr<Logger>& info_log,
                               AwsEnv** cenv);

  virtual ~AwsEnv();

  // We cannot invoke Aws::ShutdownAPI from the destructor because there could
  // be
  // multiple AwsEnv's ceated by a process and Aws::ShutdownAPI should be called
  // only once by the entire process when all AwsEnvs are destroyed.
  static void Shutdown() { Aws::ShutdownAPI(Aws::SDKOptions()); }

  // If you do not specify a region, then S3 buckets are created in the
  // standard-region which might not satisfy read-your-own-writes. So,
  // explicitly make the default region be us-west-2.
  static constexpr const char* default_region = "us-west-2";

  virtual Status PrefetchFile(const std::string& fname, bool remap) override;

  virtual Status NewSequentialFile(
      const std::string& fname, SequentialFile** result,
      const EnvOptions& options = EnvOptions()) override;

  virtual Status NewSequentialFileCloud(const std::string& bucket,
                                        const std::string& fname,
                                        std::unique_ptr<SequentialFile>* result,
                                        const EnvOptions& options) override;

  virtual Status NewRandomAccessFile(
      const std::string& fname, RandomAccessFile** result,
      const EnvOptions& options = EnvOptions()) override;

  virtual Status NewWritableFile(
      const std::string& fname, WritableFile** result,
      const EnvOptions& options = EnvOptions()) override;

  virtual Status NewStringWritableFile(
      const std::string& fname, WritableFile** result,
      const EnvOptions& options = EnvOptions()) override;

  virtual Status NewDirectory(const std::string& name,
                              std::unique_ptr<Directory>* result) override;

  virtual bool FileExists(const std::string& fname) override;

  virtual Status GetChildren(const std::string& path,
                             std::vector<std::string>* result) override;

  virtual Status DeleteFile(const std::string& fname) override;

  virtual Status CreateDir(const std::string& name) override;

  virtual Status CreateDirIfMissing(const std::string& name) override;

  virtual Status DeleteDir(const std::string& name) override;

  virtual Status GetFileSize(const std::string& fname, uint64_t* size) override;

  virtual Status GetFileModificationTime(const std::string& fname,
                                         uint64_t* file_mtime) override;

  virtual Status RenameFile(const std::string& src,
                            const std::string& target) override;

  virtual Status LockFile(const std::string& fname, FileLock** lock) override;

  virtual Status UnlockFile(FileLock* lock) override;

  virtual Status NewLogger(const std::string& fname, Logger** result) override;

  virtual void Schedule(void (*function)(void* arg), void* arg) override {
    base_env_->Schedule(function, arg);
  }

  virtual void StartThread(void (*function)(void* arg), void* arg) override {
    base_env_->StartThread(function, arg);
  }

  virtual Status GetTestDirectory(std::string* path) override {
    return base_env_->GetTestDirectory(path);
  }

  virtual uint64_t NowMicros() override { return base_env_->NowMicros(); }

  virtual void SleepForMicroseconds(int micros) override {
    base_env_->SleepForMicroseconds(micros);
  }

  virtual Status EmptyBucket(const std::string& bucket,
                             const std::string& path) override;

  std::string GetWALCacheDir();

  // The S3 client
  std::shared_ptr<AwsS3ClientWrapper> s3client_;

  // AWS's utility to help out with uploading and downloading S3 file
  std::shared_ptr<Aws::Transfer::TransferManager> awsTransferManager_;

  // Saves and retrieves the dbid->dirname mapping in S3
  Status SaveDbid(const std::string& bucket_name, const std::string& dbid,
                  const std::string& dirname) override;
  Status GetPathForDbid(const std::string& bucket, const std::string& dbid,
                        std::string* dirname) override;
  Status GetDbidList(const std::string& bucket, DbidList* dblist) override;
  Status DeleteDbid(const std::string& bucket,
                    const std::string& dbid) override;
  Status ListObjects(const std::string& bucket_name,
                     const std::string& bucket_object,
                     BucketObjectMetadata* meta) override;
  Status DeleteObject(const std::string& bucket_name,
                      const std::string& bucket_object_path) override;
  Status ExistsObject(const std::string& bucket_name,
                      const std::string& bucket_object_path) override;
  Status GetObjectSize(const std::string& bucket_name,
                       const std::string& bucket_object_path,
                       uint64_t* filesize) override;
  Status CopyObject(const std::string& bucket_name_src,
                    const std::string& bucket_object_path_src,
                    const std::string& bucket_name_dest,
                    const std::string& bucket_object_path_dest) override;
  Status GetObject(const std::string& bucket_name,
                   const std::string& bucket_object_path,
                   const std::string& local_path) override;
  Status PutObject(const std::string& local_path,
                   const std::string& bucket_name,
                   const std::string& bucket_object_path) override;
  Status PutObjectWithString(const std::string& input,
                             const std::string& bucket_name,
                             const std::string& bucket_object_path);
  Status PutObjectWithString(const std::shared_ptr<Aws::StringStream>& input,
                             uint64_t size, const std::string& bucket_name,
                             const std::string& bucket_object_path);
  Status DeleteCloudFileFromDest(const std::string& fname) override;

  void RemoveFileFromDeletionQueue(const std::string& filename);

  void TEST_SetFileDeletionDelay(std::chrono::seconds delay) {
    std::lock_guard<std::mutex> lk(files_to_delete_mutex_);
    file_deletion_delay_ = delay;
  }

  // Delete the specified path from S3
  Status DeletePathInS3(const std::string& bucket, const std::string& fname);

 private:
  //
  // The AWS credentials are specified to the constructor via
  // access_key_id and secret_key.
  //
  explicit AwsEnv(Env* underlying_env, const CloudEnvOptions& cloud_options,
                  const std::shared_ptr<Logger>& info_log = nullptr);

  struct GetObjectResult {
    bool success{false};
    Aws::Client::AWSError<Aws::S3::S3Errors> error;  // if success == false
    size_t objectSize{0};
  };

  GetObjectResult DoGetObject(const Aws::String& bucket, const Aws::String& key,
                              const std::string& destination);
  GetObjectResult DoGetObjectWithTransferManager(
      const Aws::String& bucket, const Aws::String& key,
      const std::string& destination);

  struct PutObjectResult {
    bool success{false};
    Aws::Client::AWSError<Aws::S3::S3Errors> error;  // if success == false
  };

  PutObjectResult DoPutObject(const std::string& filename,
                              const Aws::String& bucket, const Aws::String& key,
                              uint64_t sizeHint);

  PutObjectResult DoPutObjectWithString(const std::string& input,
                                        const Aws::String& bucket,
                                        const Aws::String& key);

  PutObjectResult DoPutObjectWithString(
      const std::shared_ptr<Aws::StringStream>& input, uint64_t size,
      const Aws::String& bucket, const Aws::String& key);

  PutObjectResult DoPutObjectWithTransferManager(const std::string& filename,
                                                 const Aws::String& bucket,
                                                 const Aws::String& key,
                                                 uint64_t sizeHint);

  // The pathname that contains a list of all db's inside a bucket.
  static constexpr const char* dbid_registry_ = "/.rockset/dbid/";

  Status create_bucket_status_;

  std::mutex files_to_delete_mutex_;
  std::chrono::seconds file_deletion_delay_ = std::chrono::hours(1);
  std::unordered_map<std::string, std::shared_ptr<detail::JobHandle>>
      files_to_delete_;

  Aws::S3::Model::BucketLocationConstraint bucket_location_;

  Status status();

  // Validate options
  Status CheckOption(const EnvOptions& options);

  // Return the list of children of the specified path
  Status GetChildrenFromS3(const std::string& path, const std::string& bucket,
                           std::vector<std::string>* result);

  // If metadata, size or modtime is non-nullptr, returns requested data
  Status HeadObject(const std::string& bucket, const std::string& path,
                    Aws::Map<Aws::String, Aws::String>* metadata = nullptr,
                    uint64_t* size = nullptr, uint64_t* modtime = nullptr);

  Status NewS3ReadableFile(const std::string& bucket, const std::string& fname,
                           std::unique_ptr<S3ReadableFile>* result,
                           const EnvOptions& options);

  // Save IDENTITY file to S3. Update dbid registry.
  Status SaveIdentitytoS3(const std::string& localfile,
                          const std::string& target_idfile);

  // Converts a local pathname to an object name in the src bucket
  std::string srcname(const std::string& localname);

  // Converts a local pathname to an object name in the dest bucket
  std::string destname(const std::string& localname);
};

}  // namespace leveldb

// #endif  // USE_AWS
