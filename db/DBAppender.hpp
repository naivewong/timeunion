#ifndef DBAPPENDER_H
#define DBAPPENDER_H

#include "db/AppenderInterface.hpp"
#include "db/DB.hpp"
#include "leveldb/status.h"

namespace tsdb {
namespace db {

// DBAppender wraps the DB's head appender and triggers compactions on commit
// if necessary.
class DBAppender : public AppenderInterface {
 private:
  std::unique_ptr<db::AppenderInterface> app;
  db::DB *db;

 public:
  DBAppender(std::unique_ptr<db::AppenderInterface> &&app, db::DB *db)
      : app(std::move(app)), db(db) {}

  std::pair<uint64_t, leveldb::Status> add(const label::Labels &lset, int64_t t,
                                           double v) {
    return app->add(lset, t, v);
  }

  leveldb::Status add_fast(uint64_t ref, int64_t t, double v) {
    return app->add_fast(ref, t, v);
  }

  leveldb::Status commit() {
    leveldb::Status err = app->commit();
    // We could just run this check every few minutes practically. But for
    // benchmarks and high frequency use cases this is the safer way.
    if (db->head()->MaxTime() - db->head()->MinTime() >
        db->head()->chunk_range / 2 * 3) {
      db->compact_channel()->send(0);
    }
    return err;
  }

  leveldb::Status rollback() { return app->rollback(); }

  ~DBAppender() {}
};

}  // namespace db
}  // namespace tsdb

#endif