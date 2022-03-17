#ifndef INITAPPENDER_H
#define INITAPPENDER_H

#include "db/AppenderInterface.hpp"
#include "head/Head.hpp"
#include "head/HeadUtils.hpp"

namespace tsdb {
namespace head {

class InitAppender : public db::AppenderInterface {
 private:
  std::unique_ptr<db::AppenderInterface> app;
  Head *head;

 public:
  InitAppender(Head *head) : head(head) {}

  std::pair<uint64_t, leveldb::Status> add(const label::Labels &lset, int64_t t,
                                           double v) {
    if (app) return app->add(lset, t, v);
    head->init_time(t);
    app = head->head_appender();
    return app->add(lset, t, v);
  }

  leveldb::Status add_fast(uint64_t ref, int64_t t, double v) {
    if (!app) return leveldb::Status::NotFound("no appender");
    return app->add_fast(ref, t, v);
  }

  leveldb::Status commit() {
    if (!app) return leveldb::Status::OK();
    return app->commit();
  }

  leveldb::Status rollback() {
    if (!app) return leveldb::Status::OK();
    return app->rollback();
  }

  ~InitAppender() {
    // LOG_DEBUG << "~InitAppender()";
  }
};

}  // namespace head
}  // namespace tsdb

#endif