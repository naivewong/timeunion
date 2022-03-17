#ifndef CLOUD_S3_HPP
#define CLOUD_S3_HPP

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/AWSError.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/core/client/RetryStrategy.h>
#include <aws/core/utils/Outcome.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/BucketLocationConstraint.h>
#include <aws/transfer/TransferManager.h>

#include <functional>
#include <iostream>

#include "base/Logging.hpp"
#include "cloud/Slice.hpp"
#include "cloud/Status.hpp"

namespace tsdb {
namespace cloud {

class S3ReadableFile;
class S3WritableFile;

/*
 * The information about all objects stored in a cloud bucket
 */
class BucketObjectMetadata {
 public:
  // list of all pathnames
  std::vector<std::string> pathnames;
};

inline Aws::String ToAwsString(const std::string& s) {
  return Aws::String(s.data(), s.size());
}

enum class CloudRequestOpType {
  kReadOp,
  kWriteOp,
  kListOp,
  kCreateOp,
  kDeleteOp,
  kCopyOp,
  kInfoOp
};
using CloudRequestCallback =
    std::function<void(CloudRequestOpType, uint64_t, uint64_t, bool)>;

// Type of AWS access credentials
enum class AwsAccessType {
  kUndefined,  // Use AWS SDK's default credential chain
  kSimple,
  kInstance,
  kTaskRole,
  kEnvironment,
  kConfig,
  kAnonymous,
};

// Credentials needed to access AWS cloud service
class AwsCloudAccessCredentials {
 public:
  // functions to support AWS credentials
  //
  // Initialize AWS credentials using a config file
  void InitializeConfig(const std::string& aws_config_file);

  // test if valid AWS credentials are present
  Status HasValid() const;
  // Get AWSCredentialsProvider to supply to AWS API calls when required (e.g.
  // to create S3Client)
  Status GetCredentialsProvider(
      std::shared_ptr<Aws::Auth::AWSCredentialsProvider>* result) const;

 private:
  AwsAccessType GetAccessType() const;
  Status CheckCredentials(const AwsAccessType& aws_type) const;

 public:
  std::string access_key_id;
  std::string secret_key;
  std::string config_file;
  AwsAccessType type{AwsAccessType::kUndefined};
};

class BucketOptions {
 private:
  std::string bucket_;  // The suffix for the bucket name
  std::string
      prefix_;  // The prefix for the bucket name.  Defaults to "rockset."
  std::string object_;  // The object path for the bucket
  std::string region_;  // The region for the bucket
  std::string name_;    // The name of the bucket (prefix_ + bucket_)
 public:
  BucketOptions();
  // Sets the name of the bucket to be the new bucket name.
  // If prefix is specified, the new bucket name will be [prefix][bucket]
  // If no prefix is specified, the bucket name will use the existing prefix
  void SetBucketName(const std::string& bucket, const std::string& prefix = "");
  const std::string& GetBucketName() const { return name_; }
  const std::string& GetObjectPath() const { return object_; }
  void SetObjectPath(const std::string& object) { object_ = object; }
  const std::string& GetRegion() const { return region_; }
  void SetRegion(const std::string& region) { region_ = region; }

  bool IsValid() const {
    if (object_.empty() || name_.empty()) {
      return false;
    } else {
      return true;
    }
  }
};

inline bool operator==(const BucketOptions& lhs, const BucketOptions& rhs) {
  if (lhs.IsValid() && rhs.IsValid()) {
    return ((lhs.GetBucketName() == rhs.GetBucketName()) &&
            (lhs.GetObjectPath() == rhs.GetObjectPath()) &&
            (lhs.GetRegion() == rhs.GetRegion()));
  } else {
    return false;
  }
}
inline bool operator!=(const BucketOptions& lhs, const BucketOptions& rhs) {
  return !(lhs == rhs);
}

class S3Options {
 public:
  BucketOptions src_bucket;
  BucketOptions dest_bucket;

  bool empty = true;

  // Access credentials
  AwsCloudAccessCredentials credentials;

  // if non-null, will be called *after* every cloud operation with some basic
  // information about the operation. Use this to instrument your calls to the
  // cloud.
  // parameters: (op, size, latency in microseconds, is_success)
  std::shared_ptr<CloudRequestCallback> cloud_request_callback = nullptr;

  // If false, it will not attempt to create cloud bucket if it doesn't exist.
  // Default: true
  bool create_bucket_if_missing = true;

  // request timeout for requests from the cloud storage. A value of 0
  // means the default timeout assigned by the underlying cloud storage.
  uint64_t request_timeout_ms = 0;

  // If true, we will use AWS TransferManager instead of Put/Get operaations to
  // download and upload S3 files.
  // Default: false
  bool use_aws_transfer_manager = false;

  bool keep_local_file = true;

  // If true, enables server side encryption. If used with encryption_key_id in
  // S3 mode uses AWS KMS. Otherwise, uses S3 server-side encryption where
  // key is automatically created by Amazon.
  // Default: false
  bool server_side_encryption = false;

  // If non-empty, uses the key ID for encryption.
  // Default: empty
  std::string encryption_key_id;

  // Sets result based on the value of name or alt in the environment
  // Returns true if the name/alt exists in the environment, false otherwise
  static bool GetNameFromEnvironment(const char* name, const char* alt,
                                     std::string* result);
};

//
// Ability to configure retry policies for the AWS client
//
class AwsRetryStrategy : public Aws::Client::RetryStrategy {
 public:
  AwsRetryStrategy() {
    default_strategy_ = std::make_shared<Aws::Client::DefaultRetryStrategy>();
    LOG_INFO << "[aws] Configured custom retry policy";
  }

  ~AwsRetryStrategy() override {}

  // Returns true if the error can be retried given the error and the number of
  // times already tried.
  bool ShouldRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors>& error,
                   long attemptedRetries) const override {
    auto ce = error.GetErrorType();
    const Aws::String errmsg = error.GetMessage();
    const Aws::String exceptionMsg = error.GetExceptionName();
    std::string err(errmsg.c_str(), errmsg.size());
    std::string emsg(exceptionMsg.c_str(), exceptionMsg.size());

    // Internal errors are unknown errors and we try harder to fix them
    //
    if (ce == Aws::Client::CoreErrors::INTERNAL_FAILURE ||
        ce == Aws::Client::CoreErrors::UNKNOWN ||
        err.find("try again") != std::string::npos) {
      if (attemptedRetries <= internal_failure_num_retries_) {
        LOG_INFO << "[aws] Encountered retriable failure: " << err << " (code "
                 << static_cast<int>(ce) << ", http "
                 << static_cast<int>(error.GetResponseCode()) << "). "
                 << "Exception " << emsg << ". retry attempt "
                 << attemptedRetries << " is lesser than max retries "
                 << internal_failure_num_retries_ << ". Retrying...";
        return true;
      }
      LOG_INFO << "[aws] Encountered retriable failure: " << err << " (code "
               << static_cast<int>(ce) << ", http "
               << static_cast<int>(error.GetResponseCode()) << "). Exception "
               << emsg << ". retry attempt " << attemptedRetries
               << " exceeds max retries " << internal_failure_num_retries_
               << ". Aborting...";
      return false;
    }
    LOG_WARN << "[aws] Encountered S3 failure " << err << " (code "
             << static_cast<int>(ce) << ", http "
             << static_cast<int>(error.GetResponseCode()) << "). Exception "
             << emsg << " retry attempt " << attemptedRetries << " max retries "
             << internal_failure_num_retries_
             << ". Using default retry policy...";
    return default_strategy_->ShouldRetry(error, attemptedRetries);
  }

  // Calculates the time in milliseconds the client should sleep before
  // attempting another request based on the error and attemptedRetries count.
  long CalculateDelayBeforeNextRetry(
      const Aws::Client::AWSError<Aws::Client::CoreErrors>& error,
      long attemptedRetries) const override {
    return default_strategy_->CalculateDelayBeforeNextRetry(error,
                                                            attemptedRetries);
  }

 private:
  // The default strategy implemented by AWS client
  std::shared_ptr<Aws::Client::RetryStrategy> default_strategy_;

  // The number of times an internal-error failure should be retried
  const int internal_failure_num_retries_{10};
};

class AwsCloudOptions {
 public:
  static Status GetClientConfiguration(
      const S3Options& options, const std::string& region,
      Aws::Client::ClientConfiguration* config) {
    config->connectTimeoutMs = 30000;
    config->requestTimeoutMs = 600000;

    // Setup how retries need to be done
    config->retryStrategy = std::make_shared<AwsRetryStrategy>();
    if (options.request_timeout_ms != 0) {
      config->requestTimeoutMs = options.request_timeout_ms;
    }

    config->region = ToAwsString(region);
    return Status::OK();
  }
};

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

class S3Wrapper {
 public:
  S3Wrapper(const S3Options& options);

  Status status() { return create_bucket_status_; }

  // The S3 client
  std::shared_ptr<AwsS3ClientWrapper> s3client_ = nullptr;

  // AWS's utility to help out with uploading and downloading S3 file
  std::shared_ptr<Aws::Transfer::TransferManager> awsTransferManager_ = nullptr;

  // We cannot invoke Aws::ShutdownAPI from the destructor because there could
  // be
  // multiple AwsEnv's ceated by a process and Aws::ShutdownAPI should be called
  // only once by the entire process when all AwsEnvs are destroyed.
  static void Shutdown() { Aws::ShutdownAPI(Aws::SDKOptions()); }

  // The SrcBucketName identifies the cloud storage bucket and
  // GetSrcObjectPath specifies the path inside that bucket
  // where data files reside. The specified bucket is used in
  // a readonly mode by the associated DBCloud instance.
  const std::string& GetSrcBucketName() const {
    return options_.src_bucket.GetBucketName();
  }
  const std::string& GetSrcObjectPath() const {
    return options_.src_bucket.GetObjectPath();
  }
  bool HasSrcBucket() const { return options_.src_bucket.IsValid(); }

  // The DestBucketName identifies the cloud storage bucket and
  // GetDestObjectPath specifies the path inside that bucket
  // where data files reside. The associated DBCloud instance
  // writes newly created files to this bucket.
  const std::string& GetDestBucketName() const {
    return options_.dest_bucket.GetBucketName();
  }
  const std::string& GetDestObjectPath() const {
    return options_.dest_bucket.GetObjectPath();
  }

  bool HasDestBucket() const { return options_.dest_bucket.IsValid(); }
  bool SrcMatchesDest() const {
    if (HasSrcBucket() && HasDestBucket()) {
      return options_.src_bucket == options_.dest_bucket;
    } else {
      return false;
    }
  }

  // returns the options used to create this env
  const S3Options& GetCloudEnvOptions() const { return options_; }

  Status NewWritableFile(const std::string& fname,
                         std::unique_ptr<S3WritableFile>* result);

  Status NewRandomAccessFile(const std::string& fname,
                             std::unique_ptr<S3ReadableFile>* result);

  Status FileExists(const std::string& fname);
  Status DeleteFile(const std::string& fname);
  Status GetFileSize(const std::string& fname, uint64_t* size);

  Status ListObjects(const std::string& bucket_name,
                     const std::string& bucket_object,
                     BucketObjectMetadata* meta);
  Status DeleteObject(const std::string& bucket_name,
                      const std::string& bucket_object_path);
  Status ExistsObject(const std::string& bucket_name,
                      const std::string& bucket_object_path);
  Status GetObjectSize(const std::string& bucket_name,
                       const std::string& bucket_object_path,
                       uint64_t* filesize);
  Status CopyObject(const std::string& bucket_name_src,
                    const std::string& bucket_object_path_src,
                    const std::string& bucket_name_dest,
                    const std::string& bucket_object_path_dest);
  Status GetObject(const std::string& bucket_name,
                   const std::string& bucket_object_path,
                   const std::string& local_path);
  Status PutObject(const std::string& local_path,
                   const std::string& bucket_name,
                   const std::string& bucket_object_path);
  Status DeleteCloudFileFromDest(const std::string& fname);

 private:
  S3Options options_;
  Status create_bucket_status_;
  Aws::S3::Model::BucketLocationConstraint bucket_location_;

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

  PutObjectResult DoPutObjectWithTransferManager(const std::string& filename,
                                                 const Aws::String& bucket,
                                                 const Aws::String& key,
                                                 uint64_t sizeHint);

  // Delete the specified path from S3
  Status DeletePathInS3(const std::string& bucket, const std::string& fname);

  // Return the list of children of the specified path
  Status GetChildrenFromS3(const std::string& path, const std::string& bucket,
                           std::vector<std::string>* result);

  // If metadata, size or modtime is non-nullptr, returns requested data
  Status HeadObject(const std::string& bucket, const std::string& path,
                    Aws::Map<Aws::String, Aws::String>* metadata = nullptr,
                    uint64_t* size = nullptr, uint64_t* modtime = nullptr);

  Status NewS3ReadableFile(const std::string& bucket, const std::string& fname,
                           std::unique_ptr<S3ReadableFile>* result);

  // Converts a local pathname to an object name in the src bucket
  std::string srcname(const std::string& localname);

  // Converts a local pathname to an object name in the dest bucket
  std::string destname(const std::string& localname);
};

extern S3Options default_s3_options();
extern S3Options empty_s3_options();

}  // namespace cloud.
}  // namespace tsdb.

#endif