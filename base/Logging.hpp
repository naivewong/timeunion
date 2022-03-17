#ifndef LOGGING_H
#define LOGGING_H
#include "base/LogStream.hpp"
#include "base/TimeStamp.hpp"

namespace tsdb {
namespace base {

class Logger {
 public:
  enum LogLevel {
    TRACE,
    DEBUG,
    INFO,
    WARN,
    ERROR,
    FATAL,
    NUM_LOG_LEVELS,
  };

  class SourceFile {
   public:
    template <int N>
    // 如果没有const就不能传递const引用进来
    inline SourceFile(const char (&arr)[N]) : data_(arr), size_(N - 1) {
      const char *slash = strrchr(data_, '/');  // builtin function
      if (slash) {
        data_ = slash + 1;
        size_ -= static_cast<int>(data_ - arr);
      }
      // cout << sizeof(arr) << endl;
      // cout << "inline\n";
    }

    explicit SourceFile(const char *filename) : data_(filename) {
      const char *slash = strrchr(filename, '/');
      if (slash) {
        data_ = slash + 1;
      }
      size_ = static_cast<int>(strlen(data_));
      // cout << sizeof(filename) << endl;
      // cout << "not inline\n";
    }

    const char *data_;
    int size_;
  };

  Logger(SourceFile file, int line);
  Logger(SourceFile file, int line, LogLevel level);
  Logger(SourceFile file, int line, LogLevel level, const char *func);
  Logger(SourceFile file, int line, bool toAbort);
  ~Logger();

  LogStream &stream() { return impl_.stream_; }
  //设置为static因为g_logLevel是定义在Logging.cpp的全局变量
  static LogLevel logLevel();
  static void setLogLevel(LogLevel level);

  typedef void (*OutputFunc)(const char *msg, int len);
  typedef void (*FlushFunc)();

  //设置为static因为g_output, g_flush是定义在Logging.cpp的全局变量
  static void setOutput(OutputFunc);
  static void setFlush(FlushFunc);

 private:
  class Impl {
   public:
    typedef Logger::LogLevel LogLevel;  //去掉不影响
    Impl(LogLevel level, int old_errno, const SourceFile &file, int line);
    void formatTime();
    void finish();

    TimeStamp time_;
    LogStream stream_;
    LogLevel level_;
    int line_;
    SourceFile basename_;
  };

  Impl impl_;
};

extern Logger::LogLevel g_logLevel;

inline Logger::LogLevel Logger::logLevel() { return g_logLevel; }

//
// CAUTION: do not write:
//
// if (good)
//   LOG_INFO << "Good news";
// else
//   LOG_WARN << "Bad news";
//
// this expends to
//
// if (good)
//   if (logging_INFO)
//     logInfoStream << "Good news";
//   else
//     logWarnStream << "Bad news";
//
#define LOG_TRACE                                                       \
  if (::tsdb::base::Logger::logLevel() <= ::tsdb::base::Logger::TRACE)  \
  ::tsdb::base::Logger(__FILE__, __LINE__, ::tsdb::base::Logger::TRACE, \
                       __func__)                                        \
      .stream()
#define LOG_DEBUG                                                       \
  if (::tsdb::base::Logger::logLevel() <= ::tsdb::base::Logger::DEBUG)  \
  ::tsdb::base::Logger(__FILE__, __LINE__, ::tsdb::base::Logger::DEBUG, \
                       __func__)                                        \
      .stream()
#define LOG_INFO                                                      \
  if (::tsdb::base::Logger::logLevel() <= ::tsdb::base::Logger::INFO) \
  ::tsdb::base::Logger(__FILE__, __LINE__).stream()
#define LOG_WARN \
  ::tsdb::base::Logger(__FILE__, __LINE__, ::tsdb::base::Logger::WARN).stream()
#define LOG_ERROR \
  ::tsdb::base::Logger(__FILE__, __LINE__, ::tsdb::base::Logger::ERROR).stream()
#define LOG_FATAL \
  ::tsdb::base::Logger(__FILE__, __LINE__, ::tsdb::base::Logger::FATAL).stream()
#define LOG_SYSERR ::tsdb::base::Logger(__FILE__, __LINE__, false).stream()
#define LOG_SYSFATAL ::tsdb::base::Logger(__FILE__, __LINE__, true).stream()

const char *strerror_tl(int savedErrno);

}  // namespace base
}  // namespace tsdb

#endif