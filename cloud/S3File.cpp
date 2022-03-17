#include "cloud/S3File.hpp"

#include <boost/filesystem.hpp>
#include <cassert>
#include <cinttypes>
#include <fstream>
#include <iostream>

#include "base/Logging.hpp"
#include "cloud/S3.hpp"

namespace tsdb {
namespace cloud {

/******************** Readablefile ******************/

S3ReadableFile::S3ReadableFile(S3Wrapper* env, const std::string& bucket,
                               const std::string& fname, uint64_t file_size)
    : env_(env), fname_(fname), offset_(0), file_size_(file_size) {
  LOG_INFO << "[s3] S3ReadableFile opening file " << fname_;
  s3_bucket_ = ToAwsString(bucket);
  s3_object_ = ToAwsString(fname_);

  fname_without_epoch_ = RemoveEpoch(fname_);
}

// sequential access, read data at current offset in file
Status S3ReadableFile::Read(size_t n, Slice* result, char* scratch) {
  LOG_DEBUG << "[s3] S3ReadableFile reading " << fname_ << " " << n;
  Status s = Read(offset_, n, result, scratch);

  // If the read successfully returned some data, then update
  // offset_
  if (s.ok()) {
    offset_ += result->size();
  }
  return s;
}

Status S3ReadableFile::read_helper(char* scratch, uint64_t offset, size_t n,
                                   uint64_t* size) const {
  // create a range read request
  // Ranges are inclusive, so we can't read 0 bytes; read 1 instead and
  // drop it later.
  size_t rangeLen = (n != 0 ? n : 1);
  char buffer[512];
  int ret = snprintf(buffer, sizeof(buffer), "bytes=%" PRIu64 "-%" PRIu64,
                     offset, offset + rangeLen - 1);
  if (ret < 0) {
    LOG_ERROR << "[s3] S3ReadableFile vsnprintf error " << fname_ << " offset "
              << offset << " rangelen " << rangeLen;
    return Status::IOError("S3ReadableFile vsnprintf ", fname_.c_str());
  }
  Aws::String range(buffer);

  // set up S3 request to read this range
  Aws::S3::Model::GetObjectRequest request;
  request.SetBucket(s3_bucket_);
  request.SetKey(s3_object_);
  request.SetRange(range);

  Aws::S3::Model::GetObjectOutcome outcome =
      env_->s3client_->GetObject(request);
  bool isSuccess = outcome.IsSuccess();
  if (!isSuccess) {
    const Aws::Client::AWSError<Aws::S3::S3Errors>& error = outcome.GetError();
    std::string errmsg(error.GetMessage().c_str(), error.GetMessage().size());
    Aws::S3::S3Errors s3err = error.GetErrorType();
    if (s3err == Aws::S3::S3Errors::NO_SUCH_BUCKET ||
        s3err == Aws::S3::S3Errors::NO_SUCH_KEY ||
        s3err == Aws::S3::S3Errors::RESOURCE_NOT_FOUND ||
        errmsg.find("Response code: 404") != std::string::npos) {
      LOG_ERROR << "[s3] S3ReadableFile error in reading not-existent "
                << fname_ << " " << errmsg;
      return Status::NotFound(fname_, errmsg.c_str());
    }
    LOG_ERROR << "[s3] S3ReadableFile error in reading " << fname_ << " "
              << offset << " " << buffer << " " << error.GetMessage().c_str();
    return Status::IOError(fname_, errmsg.c_str());
  }
  // const Aws::S3::Model::GetObjectResult& res = outcome.GetResult();

  // extract data payload
  Aws::IOStream& body = outcome.GetResult().GetBody();
  if (n != 0) {
    body.read(scratch, n);
    *size = body.gcount();
    assert(*size <= n);
  }
  return Status::OK();
}

// random access, read data from specified offset in file
Status S3ReadableFile::Read(uint64_t offset, size_t n, Slice* result,
                            char* scratch) const {
  LOG_DEBUG << "[s3] S3ReadableFile reading " << fname_ << " at offset "
            << offset << " size " << n;

  *result = Slice();

  if (offset >= file_size_) {
    LOG_DEBUG << "[s3] S3ReadableFile reading " << fname_ << " at offset "
              << offset << " filesize " << file_size_ << ". Nothing to do";
    return Status::OK();
  }

  // trim size if needed
  if (offset + n > file_size_) {
    n = file_size_ - offset;
    LOG_DEBUG << "[s3] S3ReadableFile reading " << fname_ << " at offset "
              << offset << " trimmed size " << n;
  }

  Status s;
  uint64_t size = 0;
  s = read_helper(scratch, offset, n, &size);
  if (!s.ok()) return s;
  *result = Slice(scratch, size);

  LOG_DEBUG << "[s3] S3ReadableFile file " << fname_ << " filesize "
            << file_size_ << " read " << size << " bytes";
  return Status::OK();
}

Status S3ReadableFile::Skip(uint64_t n) {
  LOG_DEBUG << "[s3] S3ReadableFile file " << fname_ << " skip " << n;
  // Update offset_ so that it does not go beyond filesize
  offset_ += n;
  if (offset_ > file_size_) {
    offset_ = file_size_;
  }
  return Status::OK();
}

/******************** Writablefile ******************/

Status S3WritableFile::BucketExistsInS3(
    std::shared_ptr<AwsS3ClientWrapper> client, const std::string& bucket,
    const Aws::S3::Model::BucketLocationConstraint& location) {
  Aws::S3::Model::HeadBucketRequest request;
  request.SetBucket(Aws::String(bucket.c_str(), bucket.size()));
  Aws::S3::Model::HeadBucketOutcome outcome = client->HeadBucket(request);
  return outcome.IsSuccess() ? Status::OK() : Status::NotFound();
}

//
// Create bucket in S3 if it does not already exist.
//
Status S3WritableFile::CreateBucketInS3(
    std::shared_ptr<AwsS3ClientWrapper> client, const std::string& bucket,
    const Aws::S3::Model::BucketLocationConstraint& location) {
  // specify region for the bucket
  Aws::S3::Model::CreateBucketConfiguration conf;
  if (location != Aws::S3::Model::BucketLocationConstraint::NOT_SET) {
    // only set the location constraint if it's not not set
    conf.SetLocationConstraint(location);
  }

  // create bucket
  Aws::S3::Model::CreateBucketRequest request;
  request.SetBucket(Aws::String(bucket.c_str(), bucket.size()));
  request.SetCreateBucketConfiguration(conf);
  Aws::S3::Model::CreateBucketOutcome outcome = client->CreateBucket(request);
  bool isSuccess = outcome.IsSuccess();
  if (!isSuccess) {
    const Aws::Client::AWSError<Aws::S3::S3Errors>& error = outcome.GetError();
    std::string errmsg(error.GetMessage().c_str());
    Aws::S3::S3Errors s3err = error.GetErrorType();
    if (s3err != Aws::S3::S3Errors::BUCKET_ALREADY_EXISTS &&
        s3err != Aws::S3::S3Errors::BUCKET_ALREADY_OWNED_BY_YOU) {
      return Status::IOError(bucket.c_str(), errmsg.c_str());
    }
  }
  return Status::OK();
}

S3WritableFile::S3WritableFile(S3Wrapper* env, const std::string& local_fname,
                               const std::string& bucket_prefix,
                               const std::string& cloud_fname)
    : env_(env),
      fname_(local_fname),
      bucket_prefix_(bucket_prefix),
      cloud_fname_(cloud_fname),
      closed_(false) {
  LOG_DEBUG << "[s3] S3WritableFile bucket " << bucket_prefix
            << " opened local file " << fname_ << " cloud file " << cloud_fname;

  auto* file_to_open = &fname_;
  if (!boost::filesystem::exists(fname_) ||
      !boost::filesystem::is_regular_file(fname_)) {
    status_ = Status::NotFound("file not exists");
    LOG_ERROR << "local file " << fname_ << " does not exist";
  }
}

S3WritableFile::~S3WritableFile() {
  if (!closed_) {
    Close();
  }
}

Status S3WritableFile::Close() {
  if (closed_) {  // already closed
    return status_;
  }

  // upload sst file to S3.
  status_ = env_->PutObject(fname_, bucket_prefix_, cloud_fname_);
  if (!status_.ok()) {
    LOG_ERROR << "[s3] S3WritableFile closing PutObject failed on local file "
              << fname_;
    return status_;
  }

  // delete local file
  if (!env_->GetCloudEnvOptions().keep_local_file) {
    bool removed = boost::filesystem::remove(fname_);
    if (!removed) {
      LOG_ERROR << "[s3] S3WritableFile closing delete failed on local file "
                << fname_;
      status_ = Status::IOError();
      return status_;
    }
  }
  LOG_DEBUG << "[s3] S3WritableFile closed file " << fname_;
  return Status::OK();
}

}  // namespace cloud.
}  // namespace tsdb.