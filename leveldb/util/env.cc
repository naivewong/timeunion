// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/env.h"

#include <cstdarg>

#include "leveldb/options.h"

#include "util/random.h"

// This workaround can be removed when leveldb::Env::DeleteFile is removed.
// See env.h for justification.
#if defined(_WIN32) && defined(LEVELDB_DELETEFILE_UNDEFINED)
#undef DeleteFile
#endif

namespace leveldb {

EnvOptions::EnvOptions(const Options& options) {
  info_log = options.info_log;
  posix_use_cloud_cache = options.posix_use_cloud_cache;
  aws_use_cloud_cache = options.aws_use_cloud_cache;
  ccache = options.ccache;
}

Env::Env() = default;

Env::~Env() = default;

Status Env::NewAppendableFile(const std::string& fname, WritableFile** result,
                              const EnvOptions& options) {
  return Status::NotSupported("NewAppendableFile", fname);
}

Status Env::GetChildrenFileAttributes(const std::string& dir,
                                      std::vector<FileAttributes>* result) {
  assert(result != nullptr);
  std::vector<std::string> child_fnames;
  Status s = GetChildren(dir, &child_fnames);
  if (!s.ok()) {
    return s;
  }
  result->resize(child_fnames.size());
  size_t result_size = 0;
  for (size_t i = 0; i < child_fnames.size(); ++i) {
    const std::string path = dir + "/" + child_fnames[i];
    if (!(s = GetFileSize(path, &(*result)[result_size].size_bytes)).ok()) {
      if (!FileExists(path)) {
        // The file may have been deleted since we listed the directory
        continue;
      }
      return s;
    }
    (*result)[result_size].name = std::move(child_fnames[i]);
    result_size++;
  }
  result->resize(result_size);
  return Status::OK();
}

std::string Env::GenerateUniqueId() {
  std::string uuid_file = "/proc/sys/kernel/random/uuid";

  bool s = FileExists(uuid_file);
  if (s) {
    std::string uuid;
    Status st = ReadFileToString(this, uuid_file, &uuid);
    if (st.ok()) {
      return uuid;
    }
  }
  // Could not read uuid_file - generate uuid using "nanos-random"
  Random64 r(time(nullptr));
  uint64_t random_uuid_portion =
      r.Uniform(std::numeric_limits<uint64_t>::max());
  uint64_t nanos_uuid_portion = NowMicros() * 1000;
  char uuid2[200];
  snprintf(uuid2, 200, "%lx-%lx", (unsigned long)nanos_uuid_portion,
           (unsigned long)random_uuid_portion);
  return uuid2;
}

Status Env::RemoveDir(const std::string& dirname) { return DeleteDir(dirname); }
Status Env::DeleteDir(const std::string& dirname) { return RemoveDir(dirname); }

Status Env::RemoveFile(const std::string& fname) { return DeleteFile(fname); }
Status Env::DeleteFile(const std::string& fname) { return RemoveFile(fname); }

SequentialFile::~SequentialFile() = default;

RandomAccessFile::~RandomAccessFile() = default;

WritableFile::~WritableFile() = default;

Logger::~Logger() = default;

FileLock::~FileLock() = default;

void Log(Logger* info_log, const char* format, ...) {
  if (info_log != nullptr) {
    std::va_list ap;
    va_start(ap, format);
    info_log->Logv(format, ap);
    va_end(ap);
  }
}

void LogFlush(Logger* info_log) {
  if (info_log) {
    info_log->Flush();
  }
}

static void Logv(Logger* info_log, const char* format, va_list ap) {
  if (info_log && info_log->GetInfoLogLevel() <= InfoLogLevel::INFO_LEVEL) {
    info_log->Logv(InfoLogLevel::INFO_LEVEL, format, ap);
  }
}

void Logger::Logv(const InfoLogLevel log_level, const char* format,
                  va_list ap) {
  static const char* kInfoLogLevelNames[5] = {"DEBUG", "INFO", "WARN", "ERROR",
                                              "FATAL"};
  if (log_level < log_level_) {
    return;
  }

  if (log_level == InfoLogLevel::INFO_LEVEL) {
    // Doesn't print log level if it is INFO level.
    // This is to avoid unexpected performance regression after we add
    // the feature of log level. All the logs before we add the feature
    // are INFO level. We don't want to add extra costs to those existing
    // logging.
    Logv(format, ap);
  } else if (log_level == InfoLogLevel::HEADER_LEVEL) {
    LogHeader(format, ap);
  } else {
    char new_format[500];
    snprintf(new_format, sizeof(new_format) - 1, "[%s] %s",
             kInfoLogLevelNames[log_level], format);
    Logv(new_format, ap);
  }
}

static void Logv(const InfoLogLevel log_level, Logger* info_log,
                 const char* format, va_list ap) {
  if (info_log && info_log->GetInfoLogLevel() <= log_level) {
    if (log_level == InfoLogLevel::HEADER_LEVEL) {
      info_log->LogHeader(format, ap);
    } else {
      info_log->Logv(log_level, format, ap);
    }
  }
}

void Log(const InfoLogLevel log_level, Logger* info_log, const char* format,
         ...) {
  va_list ap;
  va_start(ap, format);
  Logv(log_level, info_log, format, ap);
  va_end(ap);
}

static void Headerv(Logger* info_log, const char* format, va_list ap) {
  if (info_log) {
    info_log->LogHeader(format, ap);
  }
}

void Header(Logger* info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Headerv(info_log, format, ap);
  va_end(ap);
}

static void Debugv(Logger* info_log, const char* format, va_list ap) {
  if (info_log && info_log->GetInfoLogLevel() <= InfoLogLevel::DEBUG_LEVEL) {
    info_log->Logv(InfoLogLevel::DEBUG_LEVEL, format, ap);
  }
}

void Debug(Logger* info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Debugv(info_log, format, ap);
  va_end(ap);
}

static void Infov(Logger* info_log, const char* format, va_list ap) {
  if (info_log && info_log->GetInfoLogLevel() <= InfoLogLevel::INFO_LEVEL) {
    info_log->Logv(InfoLogLevel::INFO_LEVEL, format, ap);
  }
}

void Info(Logger* info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Infov(info_log, format, ap);
  va_end(ap);
}

static void Warnv(Logger* info_log, const char* format, va_list ap) {
  if (info_log && info_log->GetInfoLogLevel() <= InfoLogLevel::WARN_LEVEL) {
    info_log->Logv(InfoLogLevel::WARN_LEVEL, format, ap);
  }
}

void Warn(Logger* info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Warnv(info_log, format, ap);
  va_end(ap);
}

static void Errorv(Logger* info_log, const char* format, va_list ap) {
  if (info_log && info_log->GetInfoLogLevel() <= InfoLogLevel::ERROR_LEVEL) {
    info_log->Logv(InfoLogLevel::ERROR_LEVEL, format, ap);
  }
}

void Error(Logger* info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Errorv(info_log, format, ap);
  va_end(ap);
}

static void Fatalv(Logger* info_log, const char* format, va_list ap) {
  if (info_log && info_log->GetInfoLogLevel() <= InfoLogLevel::FATAL_LEVEL) {
    info_log->Logv(InfoLogLevel::FATAL_LEVEL, format, ap);
  }
}

void Fatal(Logger* info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Fatalv(info_log, format, ap);
  va_end(ap);
}

void LogFlush(const std::shared_ptr<Logger>& info_log) {
  LogFlush(info_log.get());
}

void Log(const InfoLogLevel log_level, const std::shared_ptr<Logger>& info_log,
         const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Logv(log_level, info_log.get(), format, ap);
  va_end(ap);
}

void Header(const std::shared_ptr<Logger>& info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Headerv(info_log.get(), format, ap);
  va_end(ap);
}

void Debug(const std::shared_ptr<Logger>& info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Debugv(info_log.get(), format, ap);
  va_end(ap);
}

void Info(const std::shared_ptr<Logger>& info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Infov(info_log.get(), format, ap);
  va_end(ap);
}

void Warn(const std::shared_ptr<Logger>& info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Warnv(info_log.get(), format, ap);
  va_end(ap);
}

void Error(const std::shared_ptr<Logger>& info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Errorv(info_log.get(), format, ap);
  va_end(ap);
}

void Fatal(const std::shared_ptr<Logger>& info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Fatalv(info_log.get(), format, ap);
  va_end(ap);
}

void Log(const std::shared_ptr<Logger>& info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Logv(info_log.get(), format, ap);
  va_end(ap);
}

static Status DoWriteStringToFile(Env* env, const Slice& data,
                                  const std::string& fname, bool should_sync) {
  WritableFile* file;
  Status s = env->NewWritableFile(fname, &file);
  if (!s.ok()) {
    return s;
  }
  s = file->Append(data);
  if (s.ok() && should_sync) {
    s = file->Sync();
  }
  if (s.ok()) {
    s = file->Close();
  }
  delete file;  // Will auto-close if we did not close above
  if (!s.ok()) {
    env->RemoveFile(fname);
  }
  return s;
}

Status WriteStringToFile(Env* env, const Slice& data,
                         const std::string& fname) {
  return DoWriteStringToFile(env, data, fname, false);
}

Status WriteStringToFileSync(Env* env, const Slice& data,
                             const std::string& fname) {
  return DoWriteStringToFile(env, data, fname, true);
}

Status ReadFileToString(Env* env, const std::string& fname, std::string* data) {
  data->clear();
  SequentialFile* file;
  Status s = env->NewSequentialFile(fname, &file);
  if (!s.ok()) {
    return s;
  }
  static const int kBufferSize = 8192;
  char* space = new char[kBufferSize];
  while (true) {
    Slice fragment;
    s = file->Read(kBufferSize, &fragment, space);
    if (!s.ok()) {
      break;
    }
    data->append(fragment.data(), fragment.size());
    if (fragment.empty()) {
      break;
    }
  }
  delete[] space;
  delete file;
  return s;
}

EnvWrapper::~EnvWrapper() {}

}  // namespace leveldb
