#include "port/port_stdcxx.h"

namespace leveldb {
namespace port {

namespace CurrentThread {
thread_local uint64_t cached_id = 0;
uint64_t pid() {
  if (cached_id == 0)
#if defined(OS_AIX)
    cached_id = syscall(__NR_gettid);
#else
    cached_id = (uint64_t)(pthread_self());
#endif
  return cached_id;
}
}  // namespace CurrentThread

}  // namespace port
}  // namespace leveldb