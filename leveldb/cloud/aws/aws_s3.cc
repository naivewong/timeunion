//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
// This file defines an AWS-S3 environment for rocksdb.
// A directory maps to an an zero-size object in an S3 bucket
// A sst file maps to an object in that S3 bucket.
//
// #ifdef USE_AWS

#include <cassert>
#include <cinttypes>
#include <fstream>
#include <iostream>

#include "util/coding.h"
#include "util/stderr_logger.h"
#include "util/string_util.h"

#include "base/Logging.hpp"
#include "cloud/aws/aws_env.h"
#include "cloud/aws/aws_file.h"
#include "cloud/cloud_cache.h"

namespace leveldb {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"

/******************** Readablefile ******************/

S3ReadableFile::S3ReadableFile(AwsEnv* env, const std::string& bucket,
                               const std::string& fname, uint64_t file_size,
                               const EnvOptions& options)
    : env_(env),
      fname_(fname),
      offset_(0),
      file_size_(file_size),
      ccache_(nullptr),
      is_vl_(false),
      options_(options) {
  Log(options.info_log, "[s3] S3ReadableFile opening file %s", fname_.c_str());
  s3_bucket_ = ToAwsString(bucket);
  s3_object_ = ToAwsString(fname_);

  fname_without_epoch_ = RemoveEpoch(fname_);
  // printf("S3ReadableFile:%s use_cache:%d\n", fname.c_str(),
  // options.aws_use_cloud_cache);
  if (options.aws_use_cloud_cache && options.ccache) {
    ccache_ = options.ccache.get();
    ccache_->add_file(fname_without_epoch_, file_size);
  }
  if (fname_without_epoch_.size() > 0 &&
      fname_without_epoch_[fname_without_epoch_.size() - 1] == 'v')
    is_vl_ = true;
}

// sequential access, read data at current offset in file
Status S3ReadableFile::Read(size_t n, Slice* result, char* scratch) {
  Log(options_.info_log, "[s3] S3ReadableFile reading %s %ld", fname_.c_str(),
      n);
  Status s = Read(offset_, n, result, scratch);

  // If the read successfully returned some data, then update
  // offset_
  if (s.ok()) {
    offset_ += result->size();
  }
  return s;
}

void S3ReadableFile::read_helper(char* scratch, uint64_t offset, size_t n,
                                 uint64_t* size, int* st, WaitGroup* wg) const {
  // create a range read request
  // Ranges are inclusive, so we can't read 0 bytes; read 1 instead and
  // drop it later.
  size_t rangeLen = (n != 0 ? n : 1);
  char buffer[512];
  int ret = snprintf(buffer, sizeof(buffer), "bytes=%" PRIu64 "-%" PRIu64,
                     offset, offset + rangeLen - 1);
  if (ret < 0) {
    Log(options_.info_log,
        "[s3] S3ReadableFile vsnprintf error %s offset %" PRIu64
        " rangelen %"
        "zu"
        "\n",
        fname_.c_str(), offset, rangeLen);
    if (st) *st = 1;
    if (wg) wg->Done();
    return;
    // return Status::IOError("S3ReadableFile vsnprintf ", fname_.c_str());
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
      Log(options_.info_log,
          "[s3] S3ReadableFile error in reading not-existent %s %s",
          fname_.c_str(), errmsg.c_str());
      if (s3err == Aws::S3::S3Errors::NO_SUCH_BUCKET)
        printf("s3err == Aws::S3::S3Errors::NO_SUCH_BUCKET\n");
      else if (s3err == Aws::S3::S3Errors::NO_SUCH_KEY)
        printf("s3err == Aws::S3::S3Errors::NO_SUCH_KEY\n");
      else if (s3err == Aws::S3::S3Errors::RESOURCE_NOT_FOUND)
        printf("s3err == Aws::S3::S3Errors::RESOURCE_NOT_FOUND\n");
      else if (errmsg.find("Response code: 404") != std::string::npos)
        printf("errmsg.find(\"Response code: 404\") != std::string::npos\n");

      if (st) *st = 2;
      if (wg) wg->Done();
      return;
      // return Status::NotFound(fname_, errmsg.c_str());
    }
    Log(options_.info_log,
        "[s3] S3ReadableFile error in reading %s %" PRIu64 " %s %s",
        fname_.c_str(), offset, buffer, error.GetMessage().c_str());
    if (st) *st = 3;
    if (wg) wg->Done();
    return;
    // return Status::IOError(fname_, errmsg.c_str());
  }
  // const Aws::S3::Model::GetObjectResult& res = outcome.GetResult();

  // extract data payload
  Aws::IOStream& body = outcome.GetResult().GetBody();
  if (n != 0) {
    body.read(scratch, n);
    *size = body.gcount();
    assert(*size <= n);
  }
  if (st) *st = 0;
  if (wg) wg->Done();
  return;
  // return Status::OK();
}

// random access, read data from specified offset in file
Status S3ReadableFile::Read(uint64_t offset, size_t n, Slice* result,
                            char* scratch) const {
  Log(options_.info_log,
      "[s3] S3ReadableFile reading %s at offset %" PRIu64
      " size %"
      "zu",
      fname_.c_str(), offset, n);

  *result = Slice();

  if (offset >= file_size_) {
    Log(options_.info_log,
        "[s3] S3ReadableFile reading %s at offset %" PRIu64 " filesize %" PRIu64
        "."
        " Nothing to do",
        fname_.c_str(), offset, file_size_);
    return Status::OK();
  }

  // trim size if needed
  if (offset + n > file_size_) {
    n = file_size_ - offset;
    Log(options_.info_log,
        "[s3] S3ReadableFile reading %s at offset %" PRIu64
        " trimmed size %"
        "zu",
        fname_.c_str(), offset, n);
  }

  Status s;
  uint64_t size = 0;
  if (ccache_) {
    bool is_in_compaction = ccache_->in_compaction(fname_without_epoch_);
    if (!is_in_compaction) {
      // Update cache stat.
      ccache_->add_cache_stat(n, !is_vl_);
    }

    Cache::Handle* h = ccache_->lookup(fname_, offset, n);
    if (h) {
      // printf("%s off:%lu n:%lu charge:%lu\n", fname_.c_str(), offset, n,
      // ccache_->page_cache_->GetCharge(h));
      size = n;
      memcpy(scratch, reinterpret_cast<char*>(ccache_->value(h)), n);
      ccache_->release(h);
    } else {
      int st = 0;
      read_helper(scratch, offset, n, &size, &st, nullptr);
      if (st != 0)
        printf("error st:%d %s off:%lu n:%lu\n", st, fname_.c_str(), offset, n);
      if (!is_in_compaction) {
        char* buf = new char[n];
        memcpy(buf, scratch, n);
        ccache_->insert(
            fname_, offset, n, buf, n,
            [](const Slice&, void* value) { delete[](char*)(value); });
      }
    }
  } else {
    // int st = 0;
    read_helper(scratch, offset, n, &size, nullptr, nullptr);
    // if (st != 0)
    //   return Status::IOError("S3ReadableFile", fname_.c_str());
  }
  *result = Slice(scratch, size);

  Log(options_.info_log,
      "[s3] S3ReadableFile file %s filesize %" PRIu64 " read %" PRIu64 " bytes",
      fname_.c_str(), file_size_, size);
  return Status::OK();
}

Status S3ReadableFile::Skip(uint64_t n) {
  Log(options_.info_log, "[s3] S3ReadableFile file %s skip %" PRIu64,
      fname_.c_str(), n);
  // Update offset_ so that it does not go beyond filesize
  offset_ += n;
  if (offset_ > file_size_) {
    offset_ = file_size_;
  }
  return Status::OK();
}

size_t S3ReadableFile::GetUniqueId(char* id, size_t max_size) const {
  // If this is an SST file name, then it can part of the persistent cache.
  // We need to generate a unique id for the cache.
  // If it is not a sst file, then nobody should be using this id.
  uint64_t file_number;
  FileType file_type;
  ParseFileName(RemoveEpoch(basename(fname_)), &file_number, &file_type);
  if (max_size >= kMaxVarint64Length && file_number > 0) {
    char* rid = id;
    rid = EncodeVarint64(rid, file_number);
    return static_cast<size_t>(rid - id);
  }
  return 0;
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

S3WritableFile::S3WritableFile(AwsEnv* env, const std::string& local_fname,
                               const std::string& bucket_prefix,
                               const std::string& cloud_fname,
                               const EnvOptions& options,
                               const CloudEnvOptions cloud_env_options)
    : env_(env),
      fname_(local_fname),
      bucket_prefix_(bucket_prefix),
      cloud_fname_(cloud_fname),
      options_(options) {
  auto fname_no_epoch = RemoveEpoch(fname_);
  // Is this a manifest file?
  is_manifest_ = IsManifestFile(fname_no_epoch);
  assert(IsSstFile(fname_no_epoch) || IsSstValueLog(fname_no_epoch) ||
         is_manifest_);

  Log(options.info_log,
      "[s3] S3WritableFile bucket %s opened local file %s "
      "cloud file %s manifest %d",
      bucket_prefix.c_str(), fname_.c_str(), cloud_fname.c_str(), is_manifest_);

  auto* file_to_open = &fname_;
  auto local_env = env_->GetBaseEnv();
  Status s;
  if (is_manifest_) {
    bool found = local_env->FileExists(fname_);
    if (found) {
      // Manifest exists. Instead of overwriting the MANIFEST (which could be
      // bad if we crash mid-write), write to the temporary file and do an
      // atomic rename on Sync() (Sync means we have a valid data in the
      // MANIFEST, so we can crash after it)
      tmp_file_ = fname_ + ".tmp";
      file_to_open = &tmp_file_;
    }
  }

  WritableFile* f;
  s = local_env->NewWritableFile(*file_to_open, &f, options);
  if (!s.ok()) {
    Log(options.info_log, "[s3] NewWritableFile src %s %s", fname_.c_str(),
        s.ToString().c_str());
    status_ = s;
  }
  local_file_.reset(f);
}

S3WritableFile::~S3WritableFile() {
  if (local_file_ != nullptr) {
    Close();
  }
}

Status S3WritableFile::Close() {
  if (local_file_ == nullptr) {  // already closed
    return status_;
  }
  Log(options_.info_log, "[s3] S3WritableFile closing %s", fname_.c_str());
  assert(status_.ok());

  // close local file
  Status st = local_file_->Close();
  if (!st.ok()) {
    Log(options_.info_log, "[s3] S3WritableFile closing error on local %s\n",
        fname_.c_str());
    return st;
  }
  local_file_.reset();

  if (is_manifest_) {
    // MANIFEST is made durable in each Sync() call, no need to re-upload it
    return Status::OK();
  }

  // upload sst file to S3, but first remove from deletion queue if it's in
  // there
  uint64_t fileNumber;
  FileType type;
  bool ok __attribute__((unused)) =
      ParseFileName(RemoveEpoch(basename(fname_)), &fileNumber, &type);
  assert(ok && (type == kTableFile));
  env_->RemoveFileFromDeletionQueue(basename(fname_));
  status_ = env_->PutObject(fname_, bucket_prefix_, cloud_fname_);
  if (!status_.ok()) {
    Log(options_.info_log,
        "[s3] S3WritableFile closing PutObject failed on local file %s",
        fname_.c_str());
    return status_;
  }

  // printf("S3WritableFile::Close %s\n", fname_.c_str());
  // delete local file
  // TODO(Alec): may need to keep the sst files when specify keep_sst_levels.
  if (!env_->GetCloudEnvOptions().keep_local_sst_files) {
    status_ = env_->GetBaseEnv()->DeleteFile(fname_);
    if (!status_.ok()) {
      Log(options_.info_log,
          "[s3] S3WritableFile closing delete failed on local file %s",
          fname_.c_str());
      return status_;
    }
  }
  Log(options_.info_log, "[s3] S3WritableFile closed file %s", fname_.c_str());
  return Status::OK();
}

// Sync a file to stable storage
Status S3WritableFile::Sync() {
  if (local_file_ == nullptr) {
    return status_;
  }
  assert(status_.ok());

  // sync local file
  Status stat = local_file_->Sync();

  if (stat.ok() && !tmp_file_.empty()) {
    assert(is_manifest_);
    // We are writing to the temporary file. On a first sync we need to rename
    // the file to the real filename.
    stat = env_->GetBaseEnv()->RenameFile(tmp_file_, fname_);
    // Note: this is not thread safe, but we know that manifest writes happen
    // from the same thread, so we are fine.
    tmp_file_.clear();
  }

  // We copy MANIFEST to S3 on every Sync()
  if (is_manifest_ && stat.ok()) {
    // printf("%s %s %s\n", fname_.c_str(), bucket_prefix_.c_str(),
    // cloud_fname_.c_str());
    stat = env_->PutObject(fname_, bucket_prefix_, cloud_fname_);

    if (stat.ok()) {
      Log(options_.info_log,
          "[s3] S3WritableFile made manifest %s durable to "
          "bucket %s bucketpath %s.",
          fname_.c_str(), bucket_prefix_.c_str(), cloud_fname_.c_str());
    } else {
      Log(options_.info_log,
          "[s3] S3WritableFile failed to make manifest %s durable to "
          "bucket %s bucketpath %s. %s",
          fname_.c_str(), bucket_prefix_.c_str(), cloud_fname_.c_str(),
          stat.ToString().c_str());
    }
  }
  return stat;
}

/******************** S3StringWritableFile ******************/

Status S3StringWritableFile::BucketExistsInS3(
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
Status S3StringWritableFile::CreateBucketInS3(
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

S3StringWritableFile::S3StringWritableFile(
    AwsEnv* env, const std::string& bucket_prefix,
    const std::string& cloud_fname, const EnvOptions& options,
    const CloudEnvOptions cloud_env_options)
    : env_(env),
      write_size_(0),
      bucket_prefix_(bucket_prefix),
      cloud_fname_(cloud_fname),
      options_(options) {
  input_data_ = Aws::MakeShared<Aws::StringStream>(
      "PutObjectInputStream", std::stringstream::in | std::stringstream::out |
                                  std::stringstream::binary);
  // LOG_DEBUG << cloud_fname_;
}

S3StringWritableFile::~S3StringWritableFile() {
  if (input_data_ != nullptr) {
    Close();
  }
}

Status S3StringWritableFile::Close() {
  if (input_data_ == nullptr) {  // already closed
    return status_;
  }

  status_ = env_->PutObjectWithString(input_data_, write_size_, bucket_prefix_,
                                      cloud_fname_);
  if (!status_.ok()) {
    Log(options_.info_log,
        "[s3] S3StringWritableFile closing PutObjectWithString failed on size "
        "%lu",
        write_size_);
    printf(
        "[s3] S3StringWritableFile %s closing PutObjectWithString failed on "
        "size %lu %s\n",
        cloud_fname_.c_str(), write_size_, status_.ToString().c_str());
    return status_;
  }
  input_data_.reset();

  Log(options_.info_log, "[s3] S3StringWritableFile closed string on size %lu",
      write_size_);
  printf("[s3] S3StringWritableFile %s closed string on size %lu\n",
         cloud_fname_.c_str(), write_size_);
  return Status::OK();
}

#pragma GCC diagnostic pop
}  // namespace leveldb

// #endif /* USE_AWS */
