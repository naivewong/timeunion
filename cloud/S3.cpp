#include "cloud/S3.hpp"

#include <assert.h>
#include <unistd.h>

#include <boost/filesystem.hpp>
#include <iostream>
#include <memory>

#include "base/Logging.hpp"
#include "cloud/Filename.hpp"
#include "cloud/S3File.hpp"

namespace tsdb {
namespace cloud {

BucketOptions::BucketOptions() { prefix_ = "rockset."; }

void BucketOptions::SetBucketName(const std::string& bucket,
                                  const std::string& prefix) {
  if (!prefix.empty()) {
    prefix_ = prefix;
  }

  bucket_ = bucket;
  if (bucket_.empty()) {
    name_.clear();
  } else {
    name_ = prefix_ + bucket_;
  }
}

Aws::Utils::Threading::Executor* GetAwsTransferManagerExecutor() {
  static Aws::Utils::Threading::PooledThreadExecutor executor(8);
  return &executor;
}

class CloudRequestCallbackGuard {
 public:
  CloudRequestCallbackGuard(CloudRequestCallback* callback,
                            CloudRequestOpType type, uint64_t size = 0)
      : callback_(callback), type_(type), size_(size), start_(now()) {}

  ~CloudRequestCallbackGuard() {
    if (callback_) {
      (*callback_)(type_, size_, now() - start_, success_);
    }
  }

  void SetSize(uint64_t size) { size_ = size; }
  void SetSuccess(bool success) { success_ = success; }

 private:
  uint64_t now() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::system_clock::now() -
               std::chrono::system_clock::from_time_t(0))
        .count();
  }
  CloudRequestCallback* callback_;
  CloudRequestOpType type_;
  uint64_t size_;
  bool success_{false};
  uint64_t start_;
};

template <typename T>
void SetEncryptionParameters(const S3Options& cloud_env_options,
                             T& put_request) {
  if (cloud_env_options.server_side_encryption) {
    if (cloud_env_options.encryption_key_id.empty()) {
      put_request.SetServerSideEncryption(
          Aws::S3::Model::ServerSideEncryption::AES256);
    } else {
      put_request.SetServerSideEncryption(
          Aws::S3::Model::ServerSideEncryption::aws_kms);
      put_request.SetSSEKMSKeyId(cloud_env_options.encryption_key_id.c_str());
    }
  }
}

bool S3Options::GetNameFromEnvironment(const char* name, const char* alt,
                                       std::string* result) {
  char* value = getenv(name);  // See if name is set in the environment
  if (value == nullptr &&
      alt != nullptr) {   // Not set.  Do we have an alt name?
    value = getenv(alt);  // See if alt is in the environment
  }
  if (value != nullptr) {   // Did we find the either name/alt in the env?
    result->assign(value);  // Yes, update result
    return true;            // And return success
  } else {
    return false;  // No, return not found
  }
}

AwsAccessType AwsCloudAccessCredentials::GetAccessType() const {
  if (type != AwsAccessType::kUndefined) {
    return type;
  } else if (!config_file.empty()) {
    return AwsAccessType::kConfig;
  } else if (!access_key_id.empty() || !secret_key.empty()) {
    return AwsAccessType::kSimple;
  }
  return AwsAccessType::kUndefined;
}

Status AwsCloudAccessCredentials::CheckCredentials(
    const AwsAccessType& aws_type) const {
  if (aws_type == AwsAccessType::kSimple) {
    if ((access_key_id.empty() && getenv("AWS_ACCESS_KEY_ID") == nullptr) ||
        (secret_key.empty() && getenv("AWS_SECRET_ACCESS_KEY") == nullptr)) {
      return Status::InvalidArgument(
          "AWS Credentials require both access ID and secret keys");
    }
  } else if (aws_type == AwsAccessType::kTaskRole) {
    return Status::InvalidArgument(
        "AWS access type: Task Role access is not supported.");
  }
  return Status::OK();
}

void AwsCloudAccessCredentials::InitializeConfig(
    const std::string& aws_config_file) {
  type = AwsAccessType::kConfig;
  config_file = aws_config_file;
}

Status AwsCloudAccessCredentials::HasValid() const {
  AwsAccessType aws_type = GetAccessType();
  Status status = CheckCredentials(aws_type);
  return status;
}

Status AwsCloudAccessCredentials::GetCredentialsProvider(
    std::shared_ptr<Aws::Auth::AWSCredentialsProvider>* result) const {
  result->reset();

  AwsAccessType aws_type = GetAccessType();
  Status status = CheckCredentials(aws_type);
  if (status.ok()) {
    switch (aws_type) {
      case AwsAccessType::kSimple: {
        const char* access_key =
            (access_key_id.empty() ? getenv("AWS_ACCESS_KEY_ID")
                                   : access_key_id.c_str());
        const char* secret =
            (secret_key.empty() ? getenv("AWS_SECRET_ACCESS_KEY")
                                : secret_key.c_str());
        result->reset(
            new Aws::Auth::SimpleAWSCredentialsProvider(access_key, secret));
        break;
      }
      case AwsAccessType::kConfig:
        if (!config_file.empty()) {
          result->reset(new Aws::Auth::ProfileConfigFileAWSCredentialsProvider(
              config_file.c_str()));
        } else {
          result->reset(
              new Aws::Auth::ProfileConfigFileAWSCredentialsProvider());
        }
        break;
      case AwsAccessType::kInstance:
        result->reset(new Aws::Auth::InstanceProfileCredentialsProvider());
        break;
      case AwsAccessType::kAnonymous:
        result->reset(new Aws::Auth::AnonymousAWSCredentialsProvider());
        break;
      case AwsAccessType::kEnvironment:
        result->reset(new Aws::Auth::EnvironmentAWSCredentialsProvider());
        break;
      case AwsAccessType::kUndefined:
        // Use AWS SDK's default credential chain
        result->reset();
        break;
      default:
        status = Status::NotSupported("AWS credentials type not supported");
        break;  // not supported
    }
  }
  return status;
}

AwsS3ClientWrapper::AwsS3ClientWrapper(
    std::shared_ptr<Aws::S3::S3Client> client,
    std::shared_ptr<CloudRequestCallback> cloud_request_callback)
    : client_(std::move(client)),
      cloud_request_callback_(std::move(cloud_request_callback)) {}

Aws::S3::Model::ListObjectsOutcome AwsS3ClientWrapper::ListObjects(
    const Aws::S3::Model::ListObjectsRequest& request) {
  CloudRequestCallbackGuard t(cloud_request_callback_.get(),
                              CloudRequestOpType::kListOp);
  auto outcome = client_->ListObjects(request);
  t.SetSuccess(outcome.IsSuccess());
  return outcome;
}

Aws::S3::Model::CreateBucketOutcome AwsS3ClientWrapper::CreateBucket(
    const Aws::S3::Model::CreateBucketRequest& request) {
  CloudRequestCallbackGuard t(cloud_request_callback_.get(),
                              CloudRequestOpType::kCreateOp);
  return client_->CreateBucket(request);
}

Aws::S3::Model::HeadBucketOutcome AwsS3ClientWrapper::HeadBucket(
    const Aws::S3::Model::HeadBucketRequest& request) {
  CloudRequestCallbackGuard t(cloud_request_callback_.get(),
                              CloudRequestOpType::kInfoOp);
  return client_->HeadBucket(request);
}

Aws::S3::Model::DeleteObjectOutcome AwsS3ClientWrapper::DeleteObject(
    const Aws::S3::Model::DeleteObjectRequest& request) {
  CloudRequestCallbackGuard t(cloud_request_callback_.get(),
                              CloudRequestOpType::kDeleteOp);
  auto outcome = client_->DeleteObject(request);
  t.SetSuccess(outcome.IsSuccess());
  return outcome;
}

Aws::S3::Model::CopyObjectOutcome AwsS3ClientWrapper::CopyObject(
    const Aws::S3::Model::CopyObjectRequest& request) {
  CloudRequestCallbackGuard t(cloud_request_callback_.get(),
                              CloudRequestOpType::kCopyOp);
  auto outcome = client_->CopyObject(request);
  t.SetSuccess(outcome.IsSuccess());
  return outcome;
}

Aws::S3::Model::GetObjectOutcome AwsS3ClientWrapper::GetObject(
    const Aws::S3::Model::GetObjectRequest& request) {
  CloudRequestCallbackGuard t(cloud_request_callback_.get(),
                              CloudRequestOpType::kReadOp);
  auto outcome = client_->GetObject(request);
  if (outcome.IsSuccess()) {
    t.SetSize(outcome.GetResult().GetContentLength());
    t.SetSuccess(true);
  }
  return outcome;
}

Aws::S3::Model::PutObjectOutcome AwsS3ClientWrapper::PutObject(
    const Aws::S3::Model::PutObjectRequest& request, uint64_t size_hint) {
  CloudRequestCallbackGuard t(cloud_request_callback_.get(),
                              CloudRequestOpType::kWriteOp, size_hint);
  auto outcome = client_->PutObject(request);
  t.SetSuccess(outcome.IsSuccess());
  return outcome;
}

Aws::S3::Model::HeadObjectOutcome AwsS3ClientWrapper::HeadObject(
    const Aws::S3::Model::HeadObjectRequest& request) {
  CloudRequestCallbackGuard t(cloud_request_callback_.get(),
                              CloudRequestOpType::kInfoOp);
  auto outcome = client_->HeadObject(request);
  t.SetSuccess(outcome.IsSuccess());
  return outcome;
}

//
// The AWS credentials are specified to the constructor via
// access_key_id and secret_key.
//
S3Wrapper::S3Wrapper(const S3Options& options) : options_(options) {
  Aws::InitAPI(Aws::SDKOptions());
  if (options_.src_bucket.GetRegion().empty() ||
      options_.dest_bucket.GetRegion().empty()) {
    std::string region;
    if (!S3Options::GetNameFromEnvironment("AWS_DEFAULT_REGION",
                                           "aws_default_region", &region)) {
      region = "ap-northeast-1";
    }
    if (options_.src_bucket.GetRegion().empty()) {
      options_.src_bucket.SetRegion(region);
    }
    if (options_.dest_bucket.GetRegion().empty()) {
      options_.dest_bucket.SetRegion(region);
    }
  }

  std::shared_ptr<Aws::Auth::AWSCredentialsProvider> creds;
  create_bucket_status_ = options_.credentials.GetCredentialsProvider(&creds);
  if (!create_bucket_status_.ok()) {
    LOG_INFO << "[aws] NewS3Wrapper - Bad AWS credentials";
  }

  LOG_DEBUG << "      S3Wrapper.src_bucket_name: "
            << options_.src_bucket.GetBucketName();
  LOG_DEBUG << "      S3Wrapper.src_object_path: "
            << options_.src_bucket.GetObjectPath();
  LOG_DEBUG << "      S3Wrapper.src_bucket_region: "
            << options_.src_bucket.GetRegion();
  LOG_DEBUG << "      S3Wrapper.dest_bucket_name: "
            << options_.dest_bucket.GetBucketName();
  LOG_DEBUG << "      S3Wrapper.dest_object_path: "
            << options_.dest_bucket.GetObjectPath();
  LOG_DEBUG << "      S3Wrapper.dest_bucket_region: "
            << options_.dest_bucket.GetRegion();
  LOG_DEBUG << "      S3Wrapper.credentials: "
            << (creds ? "[given]" : "[not given]");

  // TODO: support buckets being in different regions
  if (!SrcMatchesDest() && HasSrcBucket() && HasDestBucket()) {
    if (options_.src_bucket.GetRegion() == options_.dest_bucket.GetRegion()) {
      // alls good
    } else {
      create_bucket_status_ =
          Status::InvalidArgument("Two different regions not supported");
      LOG_ERROR << "[aws] NewS3Wrapper Buckets "
                << options_.src_bucket.GetBucketName() << ", "
                << options_.dest_bucket.GetBucketName()
                << " in two different regions "
                << options_.src_bucket.GetRegion() << ", "
                << options_.dest_bucket.GetRegion() << " is not supported";
      return;
    }
  }
  // create AWS S3 client with appropriate timeouts
  Aws::Client::ClientConfiguration config;
  create_bucket_status_ = AwsCloudOptions::GetClientConfiguration(
      options_, options_.src_bucket.GetRegion(), &config);

  LOG_DEBUG << "S3Wrapper connection to endpoint in region: "
            << config.region.c_str();
  bucket_location_ = Aws::S3::Model::BucketLocationConstraintMapper::
      GetBucketLocationConstraintForName(config.region);

  {
    auto s3client = creds ? std::make_shared<Aws::S3::S3Client>(creds, config)
                          : std::make_shared<Aws::S3::S3Client>(config);

    s3client_ = std::make_shared<AwsS3ClientWrapper>(
        std::move(s3client), options_.cloud_request_callback);
  }

  {
    Aws::Transfer::TransferManagerConfiguration transferManagerConfig(
        GetAwsTransferManagerExecutor());
    transferManagerConfig.s3Client = s3client_->GetClient();
    SetEncryptionParameters(options_, transferManagerConfig.putObjectTemplate);
    SetEncryptionParameters(
        options_, transferManagerConfig.createMultipartUploadTemplate);

    if (options_.use_aws_transfer_manager) {
      awsTransferManager_ =
          Aws::Transfer::TransferManager::Create(transferManagerConfig);
    }
  }

  // create dest bucket if specified
  if (HasDestBucket()) {
    if (S3WritableFile::BucketExistsInS3(s3client_, GetDestBucketName(),
                                         bucket_location_)
            .ok()) {
      LOG_INFO << "[aws] NewAwsEnv Bucket " << GetDestBucketName()
               << " already exists";
    } else if (options_.create_bucket_if_missing) {
      LOG_INFO << "[aws] NewAwsEnv Going to create bucket "
               << GetDestBucketName();
      create_bucket_status_ = S3WritableFile::CreateBucketInS3(
          s3client_, GetDestBucketName(), bucket_location_);
    } else {
      create_bucket_status_ = Status::NotFound(
          "[aws] Bucket not found and create_bucket_if_missing is false");
    }
  }
  if (!create_bucket_status_.ok()) {
    LOG_ERROR << "[aws] NewAwsEnv Unable to create bucket "
              << GetDestBucketName() << " " << create_bucket_status_.ToString();
  }

  if (!create_bucket_status_.ok()) {
    LOG_ERROR << "[aws] NewAwsEnv Unable to create environment "
              << create_bucket_status_.ToString();
  }
}

// create a new file for writing
Status S3Wrapper::NewWritableFile(const std::string& fname,
                                  std::unique_ptr<S3WritableFile>* result) {
  assert(status().ok());
  result->reset();

  std::unique_ptr<S3WritableFile> f(
      // new S3WritableFile(this, fname, GetDestBucketName(), destname(fname)));
      new S3WritableFile(this, fname, GetDestBucketName(), fname));
  Status s = f->status();
  if (!s.ok()) {
    LOG_ERROR << "[s3] NewWritableFile src " << fname << " " << s.ToString();
    return s;
  }
  result->reset(f.release());
  LOG_DEBUG << "[aws] NewWritableFile src " << fname << " " << s.ToString();
  return s;
}

Status S3Wrapper::NewRandomAccessFile(const std::string& fname,
                                      std::unique_ptr<S3ReadableFile>* result) {
  Status st;
  std::unique_ptr<S3ReadableFile> file;
  if (HasDestBucket()) {
    // st = NewS3ReadableFile(GetDestBucketName(), destname(fname), &file);
    st = NewS3ReadableFile(GetDestBucketName(), fname, &file);
  }
  if (!st.ok() && HasSrcBucket()) {
    // st = NewS3ReadableFile(GetSrcBucketName(), srcname(fname), &file);
    st = NewS3ReadableFile(GetSrcBucketName(), fname, &file);
  }
  if (st.ok()) {
    result->reset(file.release());
  }
  LOG_ERROR << "[s3] NewRandomAccessFile file " << fname << " "
            << st.ToString();
  return st;
}

Status S3Wrapper::FileExists(const std::string& fname) {
  assert(status().ok());
  Status st;

  // We read first from local storage and then from cloud storage.
  bool exist = boost::filesystem::exists(fname);
  if (!exist && HasDestBucket()) {
    st = ExistsObject(GetDestBucketName(), destname(fname));
  }
  if (!st.ok() && HasSrcBucket()) {
    st = ExistsObject(GetSrcBucketName(), srcname(fname));
  }
  LOG_DEBUG << "[aws] FileExists path " << fname << " " << st.ToString();
  return st;
}

Status S3Wrapper::DeleteFile(const std::string& fname) {
  Status st;
  if (HasDestBucket()) {
    // add the remote file deletion to the queue
    st = DeleteCloudFileFromDest(basename(fname));
  }
  LOG_DEBUG << "[s3] DeleteFile file " << fname << " " << st.ToString();
  return st;
}

Status S3Wrapper::GetFileSize(const std::string& fname, uint64_t* size) {
  assert(status().ok());
  *size = 0L;

  Status st = Status::NotFound();
  // Get file length from S3
  if (HasDestBucket()) {
    st = HeadObject(GetDestBucketName(), destname(fname), nullptr, size,
                    nullptr);
  }
  if (st.IsNotFound() && HasSrcBucket()) {
    st = HeadObject(GetSrcBucketName(), srcname(fname), nullptr, size, nullptr);
  }

  LOG_DEBUG << "[aws] GetFileSize src " << fname << " " << st.ToString();
  return st;
}

// Returns a list of all objects that start with the specified
// prefix and are stored in the bucket.
Status S3Wrapper::ListObjects(const std::string& bucket_name,
                              const std::string& object_path,
                              BucketObjectMetadata* meta) {
  return GetChildrenFromS3(object_path, bucket_name, &meta->pathnames);
}

// Deletes the specified object from cloud storage
Status S3Wrapper::DeleteObject(const std::string& bucket_name,
                               const std::string& object_path) {
  return DeletePathInS3(bucket_name, object_path);
}

// Delete the specified object from the specified cloud bucket
Status S3Wrapper::ExistsObject(const std::string& bucket_name,
                               const std::string& object_path) {
  return HeadObject(bucket_name, object_path);
}

// Return size of cloud object
Status S3Wrapper::GetObjectSize(const std::string& bucket_name,
                                const std::string& object_path,
                                uint64_t* filesize) {
  return HeadObject(bucket_name, object_path, nullptr, filesize, nullptr);
}

// Copy the specified cloud object from one location in the cloud
// storage to another location in cloud storage
Status S3Wrapper::CopyObject(const std::string& bucket_name_src,
                             const std::string& object_path_src,
                             const std::string& bucket_name_dest,
                             const std::string& object_path_dest) {
  Status st;
  Aws::String src_bucket = ToAwsString(bucket_name_src);
  Aws::String dest_bucket = ToAwsString(bucket_name_dest);

  // The filename is the same as the object name in the bucket
  Aws::String src_object = ToAwsString(object_path_src);
  Aws::String dest_object = ToAwsString(object_path_dest);

  Aws::String src_url = src_bucket + src_object;

  // create copy request
  Aws::S3::Model::CopyObjectRequest request;
  request.SetCopySource(src_url);
  request.SetBucket(dest_bucket);
  request.SetKey(dest_object);

  // execute request
  Aws::S3::Model::CopyObjectOutcome outcome = s3client_->CopyObject(request);
  bool isSuccess = outcome.IsSuccess();
  if (!isSuccess) {
    const Aws::Client::AWSError<Aws::S3::S3Errors>& error = outcome.GetError();
    std::string errmsg(error.GetMessage().c_str());
    LOG_ERROR << "[aws] S3WritableFile src path " << src_url.c_str()
              << " error in copying to " << dest_object.c_str() << " "
              << errmsg;
    return Status::IOError(dest_object.c_str(), errmsg.c_str());
  }
  LOG_ERROR << "[aws] S3WritableFile src path " << src_url.c_str()
            << " copied to " << dest_object.c_str() << " " << st.ToString();
  return st;
}

Status S3Wrapper::DeleteCloudFileFromDest(const std::string& fname) {
  assert(HasDestBucket());
  auto base = basename(fname);
  auto path = GetDestObjectPath() + "/" + base;
  auto st = DeletePathInS3(GetDestBucketName(), path);
  if (!st.ok() && !st.IsNotFound()) {
    LOG_ERROR << "[s3] DeleteFile DeletePathInS3 file " << path << " error "
              << st.ToString();
  }

  return st;
}

Status S3Wrapper::GetObject(const std::string& bucket_name,
                            const std::string& object_path,
                            const std::string& local_destination) {
  std::string tmp_destination = local_destination + ".tmp";

  GetObjectResult result;
  if (options_.use_aws_transfer_manager) {
    result = DoGetObjectWithTransferManager(
        ToAwsString(bucket_name), ToAwsString(object_path), tmp_destination);
  } else {
    result = DoGetObject(ToAwsString(bucket_name), ToAwsString(object_path),
                         tmp_destination);
  }

  if (!result.success) {
    boost::filesystem::remove(tmp_destination);
    const auto& error = result.error;
    std::string errmsg(error.GetMessage().c_str(), error.GetMessage().size());
    LOG_ERROR << "[s3] GetObject " << bucket_name << "/" << object_path
              << " error " << errmsg;
    auto errorType = error.GetErrorType();
    if (errorType == Aws::S3::S3Errors::NO_SUCH_BUCKET ||
        errorType == Aws::S3::S3Errors::NO_SUCH_KEY ||
        errorType == Aws::S3::S3Errors::RESOURCE_NOT_FOUND) {
      return Status::NotFound(std::move(errmsg));
    }
    return Status::IOError(std::move(errmsg));
  }

  // Check if our local file is the same as S3 promised
  uint64_t file_size = boost::filesystem::file_size(tmp_destination);
  Status s;
  if (file_size != result.objectSize) {
    boost::filesystem::remove(tmp_destination);
    s = Status::IOError("Partial download of a file " + local_destination);
    LOG_ERROR << "[s3] GetObject " << bucket_name << "/" << object_path
              << " local size " << file_size << " != cloud size "
              << result.objectSize << ". " << s.ToString();
  }

  if (s.ok()) {
    boost::filesystem::rename(tmp_destination, local_destination);
  }
  LOG_INFO << "[s3] GetObject " << bucket_name << "/" << object_path << " size "
           << file_size;
  return s;
}

S3Wrapper::GetObjectResult S3Wrapper::DoGetObject(
    const Aws::String& bucket, const Aws::String& key,
    const std::string& destination) {
  Aws::S3::Model::GetObjectRequest getObjectRequest;
  getObjectRequest.SetBucket(bucket);
  getObjectRequest.SetKey(key);

  getObjectRequest.SetResponseStreamFactory([destination]() {
    return Aws::New<Aws::FStream>(Aws::Utils::ARRAY_ALLOCATION_TAG, destination,
                                  std::ios_base::out);
  });
  auto getOutcome = s3client_->GetObject(getObjectRequest);
  GetObjectResult result;
  result.success = getOutcome.IsSuccess();
  if (!result.success) {
    result.error = getOutcome.GetError();
  } else {
    result.objectSize = getOutcome.GetResult().GetContentLength();
  }
  return result;
}

S3Wrapper::GetObjectResult S3Wrapper::DoGetObjectWithTransferManager(
    const Aws::String& bucket, const Aws::String& key,
    const std::string& destination) {
  CloudRequestCallbackGuard guard(options_.cloud_request_callback.get(),
                                  CloudRequestOpType::kReadOp);

  auto transferHandle =
      awsTransferManager_->DownloadFile(bucket, key, ToAwsString(destination));

  transferHandle->WaitUntilFinished();

  GetObjectResult result;
  result.success =
      transferHandle->GetStatus() == Aws::Transfer::TransferStatus::COMPLETED;
  if (!result.success) {
    result.error = transferHandle->GetLastError();
  } else {
    result.objectSize = transferHandle->GetBytesTotalSize();
  }

  guard.SetSize(result.objectSize);
  guard.SetSuccess(result.success);
  return result;
}

Status S3Wrapper::PutObject(const std::string& local_file,
                            const std::string& bucket_name,
                            const std::string& object_path) {
  uint64_t fsize = boost::filesystem::file_size(local_file);

  if (fsize == 0) {
    LOG_ERROR << "[s3] PutObject localpath " << local_file
              << " error zero size";
    return Status::IOError(local_file + " Zero size.");
  }

  auto s3_bucket = ToAwsString(bucket_name);
  PutObjectResult result;
  if (options_.use_aws_transfer_manager) {
    result = DoPutObjectWithTransferManager(local_file, s3_bucket,
                                            ToAwsString(object_path), fsize);
  } else {
    result =
        DoPutObject(local_file, s3_bucket, ToAwsString(object_path), fsize);
  }
  Status st;
  if (!result.success) {
    auto error = result.error;
    std::string errmsg(error.GetMessage().c_str(), error.GetMessage().size());
    st = Status::IOError(local_file, errmsg);
    LOG_ERROR << "[s3] PutObject " << bucket_name << "/" << object_path
              << ", size " << fsize << ", ERROR " << errmsg;
  } else {
    LOG_ERROR << "[s3] PutObject " << bucket_name << "/" << object_path
              << ", size " << fsize << ", OK";
  }

  return st;
}

S3Wrapper::PutObjectResult S3Wrapper::DoPutObject(const std::string& filename,
                                                  const Aws::String& bucket,
                                                  const Aws::String& key,
                                                  uint64_t sizeHint) {
  auto inputData = Aws::MakeShared<Aws::FStream>(
      key.c_str(), filename.c_str(), std::ios_base::in | std::ios_base::out);

  Aws::S3::Model::PutObjectRequest putRequest;
  putRequest.SetBucket(bucket);
  putRequest.SetKey(key);
  putRequest.SetBody(inputData);
  SetEncryptionParameters(options_, putRequest);

  auto putOutcome = s3client_->PutObject(putRequest, sizeHint);
  PutObjectResult result;
  result.success = putOutcome.IsSuccess();
  if (!result.success) {
    result.error = putOutcome.GetError();
  }
  return result;
}

S3Wrapper::PutObjectResult S3Wrapper::DoPutObjectWithTransferManager(
    const std::string& filename, const Aws::String& bucket,
    const Aws::String& key, uint64_t sizeHint) {
  CloudRequestCallbackGuard guard(options_.cloud_request_callback.get(),
                                  CloudRequestOpType::kWriteOp, sizeHint);

  auto transferHandle = awsTransferManager_->UploadFile(
      ToAwsString(filename), bucket, key, Aws::DEFAULT_CONTENT_TYPE,
      Aws::Map<Aws::String, Aws::String>());

  transferHandle->WaitUntilFinished();

  PutObjectResult result;
  result.success =
      transferHandle->GetStatus() == Aws::Transfer::TransferStatus::COMPLETED;
  if (!result.success) {
    result.error = transferHandle->GetLastError();
  }

  guard.SetSuccess(result.success);
  return result;
}

//
// Delete the specified path from S3
//
Status S3Wrapper::DeletePathInS3(const std::string& bucket,
                                 const std::string& fname) {
  assert(status().ok());
  Status st;

  // The filename is the same as the object name in the bucket
  Aws::String object = ToAwsString(fname);

  // create request
  Aws::S3::Model::DeleteObjectRequest request;
  request.SetBucket(ToAwsString(bucket));
  request.SetKey(object);

  Aws::S3::Model::DeleteObjectOutcome outcome =
      s3client_->DeleteObject(request);
  bool isSuccess = outcome.IsSuccess();
  if (!isSuccess) {
    const Aws::Client::AWSError<Aws::S3::S3Errors>& error = outcome.GetError();
    std::string errmsg(error.GetMessage().c_str());
    Aws::S3::S3Errors s3err = error.GetErrorType();
    if (s3err == Aws::S3::S3Errors::NO_SUCH_BUCKET ||
        s3err == Aws::S3::S3Errors::NO_SUCH_KEY ||
        s3err == Aws::S3::S3Errors::RESOURCE_NOT_FOUND) {
      st = Status::NotFound(fname, errmsg.c_str());
    } else {
      st = Status::IOError(fname, errmsg.c_str());
    }
  }

  LOG_INFO << "[s3] DeleteFromS3 " << bucket << "/" << fname << ", status "
           << st.ToString();

  return st;
}

//
// Appends the names of all children of the specified path from S3
// into the result set.
//
Status S3Wrapper::GetChildrenFromS3(const std::string& path,
                                    const std::string& bucket,
                                    std::vector<std::string>* result) {
  assert(status().ok());

  // S3 paths don't start with '/'
  auto prefix = ltrim_if(path, '/');
  // S3 paths better end with '/', otherwise we might also get a list of files
  // in a directory for which our path is a prefix
  prefix = ensure_ends_with_pathsep(std::move(prefix));
  // the starting object marker
  Aws::String marker;
  bool loop = true;

  // get info of bucket+object
  while (loop) {
    Aws::S3::Model::ListObjectsRequest request;
    request.SetBucket(ToAwsString(bucket));
    request.SetMaxKeys(50);
    request.SetPrefix(ToAwsString(prefix));
    request.SetMarker(marker);

    Aws::S3::Model::ListObjectsOutcome outcome =
        s3client_->ListObjects(request);
    bool isSuccess = outcome.IsSuccess();
    if (!isSuccess) {
      const Aws::Client::AWSError<Aws::S3::S3Errors>& error =
          outcome.GetError();
      std::string errmsg(error.GetMessage().c_str());
      Aws::S3::S3Errors s3err = error.GetErrorType();
      if (s3err == Aws::S3::S3Errors::NO_SUCH_BUCKET ||
          s3err == Aws::S3::S3Errors::NO_SUCH_KEY ||
          s3err == Aws::S3::S3Errors::RESOURCE_NOT_FOUND) {
        LOG_ERROR << "[s3] GetChildren dir " << path
                  << " does not exist: " << errmsg;
        return Status::NotFound(path, errmsg.c_str());
      }
      return Status::IOError(path, errmsg.c_str());
    }
    const Aws::S3::Model::ListObjectsResult& res = outcome.GetResult();
    const Aws::Vector<Aws::S3::Model::Object>& objs = res.GetContents();
    for (auto o : objs) {
      const Aws::String& key = o.GetKey();
      // Our path should be a prefix of the fetched value
      std::string keystr(key.c_str(), key.size());
      assert(keystr.find(prefix) == 0);
      if (keystr.find(prefix) != 0) {
        return Status::IOError("Unexpected result from AWS S3: " + keystr);
      }
      auto fname = keystr.substr(prefix.size());
      result->push_back(fname);
    }

    // If there are no more entries, then we are done.
    if (!res.GetIsTruncated()) {
      break;
    }
    // The new starting point
    marker = res.GetNextMarker();
    if (marker.empty()) {
      // If response does not include the NextMaker and it is
      // truncated, you can use the value of the last Key in the response
      // as the marker in the subsequent request because all objects
      // are returned in alphabetical order
      marker = objs.back().GetKey();
    }
  }
  return Status::OK();
}

Status S3Wrapper::HeadObject(const std::string& bucket, const std::string& path,
                             Aws::Map<Aws::String, Aws::String>* metadata,
                             uint64_t* size, uint64_t* modtime) {
  Aws::S3::Model::HeadObjectRequest request;
  request.SetBucket(ToAwsString(bucket));
  request.SetKey(ToAwsString(path));

  auto outcome = s3client_->HeadObject(request);
  bool isSuccess = outcome.IsSuccess();
  if (!isSuccess) {
    const auto& error = outcome.GetError();
    Aws::S3::S3Errors s3err = error.GetErrorType();
    auto errMessage = error.GetMessage();
    if (s3err == Aws::S3::S3Errors::NO_SUCH_BUCKET ||
        s3err == Aws::S3::S3Errors::NO_SUCH_KEY ||
        s3err == Aws::S3::S3Errors::RESOURCE_NOT_FOUND) {
      return Status::NotFound(path, errMessage.c_str());
    }
    return Status::IOError(path, errMessage.c_str());
  }
  auto& res = outcome.GetResult();
  if (metadata != nullptr) {
    *metadata = res.GetMetadata();
  }
  if (size != nullptr) {
    *size = res.GetContentLength();
  }
  if (modtime != nullptr) {
    *modtime = res.GetLastModified().Millis();
  }
  return Status::OK();
}

Status S3Wrapper::NewS3ReadableFile(const std::string& bucket,
                                    const std::string& fname,
                                    std::unique_ptr<S3ReadableFile>* result) {
  // First, check if the file exists and also find its size. We use size in
  // S3ReadableFile to make sure we always read the valid ranges of the file
  uint64_t size;
  Status st = HeadObject(bucket, fname, nullptr, &size, nullptr);
  if (!st.ok()) {
    return st;
  }
  result->reset(new S3ReadableFile(this, bucket, fname, size));
  return Status::OK();
}

//
// prepends the configured src object path name
//
std::string S3Wrapper::srcname(const std::string& localname) {
  assert(options_.src_bucket.IsValid());
  return options_.src_bucket.GetObjectPath() + "/" + basename(localname);
}

//
// prepends the configured dest object path name
//
std::string S3Wrapper::destname(const std::string& localname) {
  assert(options_.dest_bucket.IsValid());
  return options_.dest_bucket.GetObjectPath() + "/" + basename(localname);
}

S3Options default_s3_options() {
  S3Options options;
  options.src_bucket.SetBucketName("cloud-db-examples.alec", "rockset.");
  options.src_bucket.SetObjectPath("/tmp");
  options.src_bucket.SetRegion("ap-northeast-1");
  options.dest_bucket.SetBucketName("cloud-db-examples.alec", "rockset.");
  options.dest_bucket.SetObjectPath("/tmp");
  options.dest_bucket.SetRegion("ap-northeast-1");
  options.empty = false;
  return options;
}

S3Options empty_s3_options() { return S3Options(); }

}  // namespace cloud.
}  // namespace tsdb.