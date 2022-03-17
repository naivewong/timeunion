#include "base/Logging.hpp"

#include <errno.h>

namespace tsdb {
namespace base {

const char *LogLevelName[Logger::NUM_LOG_LEVELS] = {
    "[TRACE] ", "[DEBUG] ", "[INFO]  ", "[WARN]  ", "[ERROR] ", "[FATAL] ",
};

Logger::LogLevel g_logLevel = Logger::TRACE;

__thread char t_errnobuf[512];

const char *strerror_tl(int savedErrno) {
  // int strerror_r(int errnum, char *buf, size_t buflen);
  auto x = strerror_r(savedErrno, t_errnobuf, sizeof t_errnobuf);
  (void)(x);
  return t_errnobuf;
}

void defaultOutput(const char *msg, int len) {
  size_t n = fwrite(msg, 1, len, stdout);
  // FIXME check n
  // (void)n;
  size_t remain = len - n;
  while (remain > 0) {
    size_t x = ::fwrite(msg + n, sizeof(char), remain, stdout);
    if (x == 0) {
      int err = ferror(stdout);
      if (err) {
        fprintf(stderr, "Logging.cpp: defaultOutput() failed %s\n",
                strerror(err));
      }
      break;
    }
    remain = remain - x;
    n += x;
  }
}

void defaultFlush() { fflush(stdout); }

//为Logger定义全局output和flush函数
Logger::OutputFunc g_output = defaultOutput;
Logger::FlushFunc g_flush = defaultFlush;

inline LogStream &operator<<(LogStream &s, const Logger::SourceFile &v) {
  s.append(v.data_, v.size_);
  return s;
}

Logger::Impl::Impl(LogLevel level, int savedErrno, const SourceFile &file,
                   int line)
    : time_(TimeStamp::now()),
      stream_(),
      level_(level),
      line_(line),
      basename_(file) {
  formatTime();
  // CurrentThread::tid();
  // stream_ << T(CurrentThread::tidString(), CurrentThread::tidStringLength());
  stream_ << LogLevelName[level];
  if (savedErrno != 0) {
    stream_ << strerror_tl(savedErrno) << " (errno=" << savedErrno << ") ";
  }
}

void Logger::Impl::formatTime() {}

void Logger::Impl::finish() {
  stream_ << " - " << basename_ << ':' << line_ << '\n';
}

Logger::Logger(SourceFile file, int line) : impl_(INFO, 0, file, line) {}

Logger::Logger(SourceFile file, int line, LogLevel level, const char *func)
    : impl_(level, 0, file, line) {
  impl_.stream_ << func << ' ';
}

Logger::Logger(SourceFile file, int line, LogLevel level)
    : impl_(level, 0, file, line) {}

Logger::Logger(SourceFile file, int line, bool toAbort)
    : impl_(toAbort ? FATAL : ERROR, errno, file, line) {}

Logger::~Logger() {
  impl_.finish();
  const LogStream::Buffer &buf(stream().buffer());  //初始化引用
  //析构时调用全局output
  g_output(buf.data(), buf.length());
  if (impl_.level_ == FATAL) {
    g_flush();
    abort();
  }
}

void Logger::setLogLevel(Logger::LogLevel level) { g_logLevel = level; }

void Logger::setOutput(OutputFunc out) { g_output = out; }

void Logger::setFlush(FlushFunc flush) { g_flush = flush; }

}  // namespace base
}  // namespace tsdb