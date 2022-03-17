#include <unistd.h>

#include <atomic>
#include <iostream>
#include <thread>

#include "base/Condition.hpp"
#include "gtest/gtest.h"

namespace tsdb {
namespace base {

class BaseTest : public testing::Test {};

TEST_F(BaseTest, TestCond1) {
  MutexLock m;
  Condition cond(m);
  std::atomic<bool> finished(false);

  std::thread t([&]() {
    sleep(1);
    m.lock();
    if (!finished)
      cond.wait();
    else
      m.unlock();
    std::cout << "t finish" << std::endl;
  });
  t.detach();

  finished = true;
  cond.notify();
  sleep(2);
  std::cout << "main finish" << std::endl;
}

TEST_F(BaseTest, TestCond2) {
  MutexLock m;
  Condition cond(m);
  bool finished = false;

  std::thread t([&]() {
    while (true) {
      {
        MutexLockGuard lock(m);
        if (finished) {
          cond.notify();
          break;
        }
      }
      sleep(1);
      std::cout << "t loop" << std::endl;
    }
  });
  t.detach();

  sleep(4);
  std::cout << "main finish" << std::endl;
  m.lock();
  finished = true;
  cond.wait();
}

}  // namespace base
}  // namespace tsdb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}