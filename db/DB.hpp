#pragma once

#include <unordered_map>

#include "base/Channel.hpp"
#include "base/Error.hpp"
#include "base/Logging.hpp"
#include "base/Mutex.hpp"
#include "base/ThreadPool.hpp"
#include "block/BlockInterface.hpp"
#include "cloud/S3.hpp"
#include "db/AppenderInterface.hpp"
#include "db/DBUtils.hpp"
#include "head/Head.hpp"
#include "head/MMapHeadWithTrie.hpp"
#include "leveldb/db.h"
#include "querier/QuerierInterface.hpp"
#include "third_party/httplib.h"
#include "third_party/ulid.hpp"

namespace leveldb {
class DBQuerier;
}

namespace tsdb {
namespace db {

class DB {
 private:
  std::string dir_;

  leveldb::DB* db_;
  std::unique_ptr<head::HeadType> head_;

  httplib::Server server_;

  // std::shared_ptr<base::ThreadPool> pool_;
  error::Error err_;

  // Used for HTTP requests.
  std::unique_ptr<db::AppenderInterface> cached_appender_;
  leveldb::DBQuerier* cached_querier_;

  void init_http_json_server();
  void init_http_proto_server();

 public:
  DB(const std::string& dir, leveldb::DB* db);

  std::string dir() { return dir_; }

  head::HeadType* head() { return head_.get(); }

  error::Error error() { return err_; }

  std::unique_ptr<db::AppenderInterface> appender() {
    return head_->appender(false);
  }

  leveldb::DBQuerier* querier(int64_t mint, int64_t maxt) {
    return db_->Querier(head_.get(), mint, maxt);
  }

  void purge_time(int64_t);

  void print_level(bool hex = false, bool print_stats = false) {
    db_->PrintLevel(hex, print_stats);
  }

  ~DB();
};

}  // namespace db
}  // namespace tsdb
