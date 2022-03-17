#include "db/db_impl.h"
#include "db/db_querier.h"
#include <boost/filesystem.hpp>
#include <gperftools/profiler.h>

#include "leveldb/cache.h"
#include "leveldb/cloud/db_cloud.h"

#include "third_party/thread_pool.h"

#include "chunk/XORChunk.hpp"
#include "cloud/cloud_cache.h"
#include "gtest/gtest.h"
#include "head/Head.hpp"
#include "head/MMapHeadWithTrie.hpp"
#include "label/EqualMatcher.hpp"
#include "label/Label.hpp"

namespace leveldb {

std::vector<std::vector<std::string>> devops(
    {{"usage_user", "usage_system", "usage_idle", "usage_nice", "usage_iowait",
      "usage_irq", "usage_softirq", "usage_steal", "usage_guest",
      "usage_guest_nice"},
     {"reads", "writes", "read_bytes", "write_bytes", "read_time", "write_time",
      "io_time"},
     {"total", "free", "used", "used_percent", "inodes_total", "inodes_free",
      "inodes_used"},
     {"boot_time", "interrupts", "context_switches", "processes_forked",
      "disk_pages_in", "disk_pages_out"},
     {"total", "available", "used", "free", "cached", "buffered",
      "used_percent", "available_percent", "buffered_percent"},
     {"bytes_sent", "bytes_recv", "packets_sent", "packets_recv", "err_in",
      "err_out", "drop_in", "drop_out"},
     {"accepts", "active", "handled", "reading", "requests", "waiting",
      "writing"},
     {"numbackends", "xact_commit", "xact_rollback", "blks_read", "blks_hit",
      "tup_returned", "tup_fetched", "tup_inserted", "tup_updated",
      "tup_deleted", "conflicts", "temp_files", "temp_bytes", "deadlocks",
      "blk_read_time", "blk_write_time"},
     {"uptime_in_seconds",
      "total_connections_received",
      "expired_keys",
      "evicted_keys",
      "keyspace_hits",
      "keyspace_misses",
      "instantaneous_ops_per_sec",
      "instantaneous_input_kbps",
      "instantaneous_output_kbps",
      "connected_clients",
      "used_memory",
      "used_memory_rss",
      "used_memory_peak",
      "used_memory_lua",
      "rdb_changes_since_last_save",
      "sync_full",
      "sync_partial_ok",
      "sync_partial_err",
      "pubsub_channels",
      "pubsub_patterns",
      "latest_fork_usec",
      "connected_slaves",
      "master_repl_offset",
      "repl_backlog_active",
      "repl_backlog_size",
      "repl_backlog_histlen",
      "mem_fragmentation_ratio",
      "used_cpu_sys",
      "used_cpu_user",
      "used_cpu_sys_children",
      "used_cpu_user_children"}});
std::vector<std::string> devops_names({"cpu_", "diskio_", "disk_", "kernel_",
                                       "mem_", "net_", "nginx_", "postgres_",
                                       "redis_"});

std::unordered_map<std::string, bool> query_types({{"1-1-1", true},
                                                   {"1-1-12", true},
                                                   {"1-8-1", true},
                                                   {"5-1-1", true},
                                                   {"5-1-12", true},
                                                   {"5-8-1", true},
                                                   {"double-groupby-1", false},
                                                   {"high-cpu-1", false},
                                                   {"high-cpu-all", false},
                                                   {"lastpoint", true}});

class HeadLogTest : public testing::Test {
 public:
  void _clean_files(const std::string& dir, const std::string& prefix) {
    std::vector<std::string> to_remove;
    boost::filesystem::path p(dir);
    boost::filesystem::directory_iterator end_itr;
    for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
      if (boost::filesystem::is_regular_file(itr->path())) {
        std::string current_file = itr->path().filename().string();
        if ((current_file.size() - prefix.size() > 5 &&
             memcmp(current_file.c_str() + prefix.size(), "array", 5) == 0) ||
            (current_file.size() - prefix.size() > 5 &&
             memcmp(current_file.c_str() + prefix.size(), "ninfo", 5) == 0) ||
            (current_file.size() - prefix.size() > 5 &&
             memcmp(current_file.c_str() + prefix.size(), "block", 5) == 0) ||
            (current_file.size() - prefix.size() > 4 &&
             memcmp(current_file.c_str() + prefix.size(), "tail", 4) == 0)) {
          to_remove.push_back(itr->path().string());
        }
      }
    }

    for (const auto& name : to_remove) {
      std::cout << "clean " << name << std::endl;
      boost::filesystem::remove(name);
    }
  }

  void clean_mmap_arrays(const std::string& dir) {
    std::vector<std::string> to_remove;
    boost::filesystem::path p(dir);
    boost::filesystem::directory_iterator end_itr;
    for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
      if (boost::filesystem::is_regular_file(itr->path())) {
        std::string current_file = itr->path().filename().string();
        if ((current_file.size() > 9 &&
             memcmp(current_file.c_str(), "8intarray", 9) == 0) ||
            (current_file.size() > 10 &&
             memcmp(current_file.c_str(), "8gintarray", 10) == 0) ||
            (current_file.size() > 11 &&
             memcmp(current_file.c_str(), "8floatarray", 11) == 0) ||
            (current_file.size() > 12 &&
             memcmp(current_file.c_str(), "8gfloatarray", 12) == 0)) {
          to_remove.push_back(itr->path().string());
        }
      }
    }

    for (const auto& name : to_remove) {
      std::cout << "clean " << name << std::endl;
      boost::filesystem::remove(name);
    }
  }

  void clean_logs(const std::string& dir) {
    std::vector<std::string> to_remove;
    boost::filesystem::path p(dir);
    boost::filesystem::directory_iterator end_itr;
    for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
      if (boost::filesystem::is_regular_file(itr->path())) {
        std::string current_file = itr->path().filename().string();
        if (current_file.size() > tsdb::head::HEAD_SAMPLES_LOG_NAME.size() &&
            memcmp(current_file.c_str(),
                   tsdb::head::HEAD_SAMPLES_LOG_NAME.c_str(),
                   tsdb::head::HEAD_SAMPLES_LOG_NAME.size()) == 0) {
          to_remove.push_back(itr->path().string());
        } else if (current_file == "head.log")
          to_remove.push_back(itr->path().string());
      }
    }

    for (const auto& name : to_remove) {
      std::cout << "clean " << name << std::endl;
      boost::filesystem::remove(name);
    }
    boost::filesystem::remove(dir + "/mmap_arrays_safe");
    boost::filesystem::remove(dir + "/mmap_labels_safe");
  }

  void clean_mmap_labels(const std::string& dir) {
    std::vector<std::string> to_remove;
    boost::filesystem::path p(dir);
    boost::filesystem::directory_iterator end_itr;
    for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
      if (boost::filesystem::is_regular_file(itr->path())) {
        std::string current_file = itr->path().filename().string();
        if ((current_file.size() > 6 &&
             memcmp(current_file.c_str(), "labels", 6) == 0) ||
            (current_file.size() > 7 &&
             memcmp(current_file.c_str(), "glabels", 7) == 0)) {
          to_remove.push_back(itr->path().string());
        }
      }
    }

    for (const auto& name : to_remove) {
      std::cout << "clean " << name << std::endl;
      boost::filesystem::remove(name);
    }
  }

  void clean_files(const std::string& dir) {
    _clean_files(dir, "sym_");
    _clean_files(dir, "ln_");
    _clean_files(dir, "lv_");
    clean_mmap_arrays(dir);
    clean_mmap_labels(dir);
    clean_logs(dir);
  }
};

TEST_F(HeadLogTest, Test1) {
  clean_files("/tmp");

  int num_ts = 1000;
  {
    ::tsdb::head::MMapHeadWithTrie h(0, "/tmp", "/tmp", nullptr);
    for (int i = 0; i < num_ts; i++) {
      ::tsdb::label::Labels lset;
      for (int j = 0; j < 10; j++)
        lset.emplace_back(
            "label" + std::to_string(j),
            "label" + std::to_string(j) + "_" + std::to_string(i));
      lset.emplace_back("label_all", "label_all");

      auto app = h.appender();
      app->add(lset, 0, 0);
      ASSERT_TRUE(app->commit().ok());
    }

    // add a group.
    std::vector<int> slots;
    uint64_t gid;
    auto app = h.appender();
    app->add({{"group", "1"}, {"label_all", "value_all"}},
             {{{"a", "b"}}, {{"c", "d"}}}, 0, std::vector<double>(2, 0), &gid,
             &slots);
    ASSERT_EQ((uint64_t)(num_ts + 1) | 0x8000000000000000, gid);
    ASSERT_EQ(std::vector<int>({0, 1}), slots);
    ASSERT_TRUE(app->commit().ok());

    auto p = h.postings("label_all", "label_all");
    ASSERT_TRUE(p.second);
    uint64_t id = 1;
    while (p.first->next()) {
      if ((p.first->at() >> 63) == 0) {
        ASSERT_EQ(id, p.first->at());
        ::tsdb::label::Labels tmp_lset;
        std::string s;
        ASSERT_TRUE(h.series(id, tmp_lset, &s));

        ::tsdb::label::Labels lset;
        for (int j = 0; j < 10; j++)
          lset.emplace_back(
              "label" + std::to_string(j),
              "label" + std::to_string(j) + "_" + std::to_string(id - 1));
        lset.emplace_back("label_all", "label_all");
        ASSERT_EQ(lset, tmp_lset);
      } else {
        ASSERT_EQ(gid, p.first->at());
        ::tsdb::label::Labels tmp_lset;
        std::vector<::tsdb::label::Labels> tmp_lsets;
        std::string s;
        ::tsdb::label::EqualMatcher m1("group", "1");
        ::tsdb::label::EqualMatcher m2("a", "b");
        std::vector<::tsdb::label::MatcherInterface*> matchers({&m1, &m2});
        std::vector<int> tmp_slots;
        ASSERT_TRUE(
            h.group(gid, matchers, &tmp_slots, &tmp_lset, &tmp_lsets, &s));
        ASSERT_EQ(slots, tmp_slots);
        ASSERT_EQ(
            ::tsdb::label::Labels({{"group", "1"}, {"label_all", "value_all"}}),
            tmp_lset);
        ASSERT_EQ(std::vector<::tsdb::label::Labels>({{{"a", "b"}}}),
                  tmp_lsets);
      }
      id++;
    }
    ASSERT_EQ(num_ts + 1, id);
  }

  // Reopen.
  {
    ::tsdb::head::MMapHeadWithTrie h(num_ts, "/tmp", "/tmp", nullptr);
    auto p = h.postings("label_all", "label_all");
    ASSERT_TRUE(p.second);
    uint64_t id = 1;
    while (p.first->next()) {
      ASSERT_EQ(id, p.first->at());
      ::tsdb::label::Labels tmp_lset;
      std::string s;
      ASSERT_TRUE(h.series(id, tmp_lset, &s));

      ::tsdb::label::Labels lset;
      for (int j = 0; j < 10; j++)
        lset.emplace_back(
            "label" + std::to_string(j),
            "label" + std::to_string(j) + "_" + std::to_string(id - 1));
      lset.emplace_back("label_all", "label_all");
      ASSERT_EQ(lset, tmp_lset);
      id++;
    }
    ASSERT_EQ(num_ts + 1, id);

    for (int i = num_ts; i < num_ts * 2; i++) {
      ::tsdb::label::Labels lset;
      for (int j = 0; j < 10; j++)
        lset.emplace_back(
            "label" + std::to_string(j),
            "label" + std::to_string(j) + "_" + std::to_string(i));
      lset.emplace_back("label_all", "label_all");

      auto app = h.appender();
      app->add(lset, 0, 0);
      app->commit();
    }

    p = h.postings("label_all", "label_all");
    ASSERT_TRUE(p.second);
    id = 1;
    while (p.first->next()) {
      if ((p.first->at() >> 63) == 0) {
        ASSERT_EQ(id, p.first->at());
        ::tsdb::label::Labels tmp_lset;
        std::string s;
        ASSERT_TRUE(h.series(id, tmp_lset, &s));

        ::tsdb::label::Labels lset;
        for (int j = 0; j < 10; j++)
          lset.emplace_back(
              "label" + std::to_string(j),
              "label" + std::to_string(j) + "_" + std::to_string(id - 1));
        lset.emplace_back("label_all", "label_all");
        ASSERT_EQ(lset, tmp_lset);
      } else {
        ASSERT_EQ((uint64_t)(num_ts + 1) | 0x8000000000000000, p.first->at());
        ::tsdb::label::Labels tmp_lset;
        std::vector<::tsdb::label::Labels> tmp_lsets;
        std::string s;
        ::tsdb::label::EqualMatcher m1("group", "1");
        ::tsdb::label::EqualMatcher m2("a", "b");
        std::vector<::tsdb::label::MatcherInterface*> matchers({&m1, &m2});
        std::vector<int> tmp_slots;
        ASSERT_TRUE(h.group((uint64_t)(num_ts + 1) | 0x8000000000000000,
                            matchers, &tmp_slots, &tmp_lset, &tmp_lsets, &s));
        ASSERT_EQ(std::vector<int>({0, 1}), tmp_slots);
        ASSERT_EQ(
            ::tsdb::label::Labels({{"group", "1"}, {"label_all", "value_all"}}),
            tmp_lset);
        ASSERT_EQ(std::vector<::tsdb::label::Labels>({{{"a", "b"}}}),
                  tmp_lsets);
      }
      id++;
    }
    ASSERT_EQ(2 * num_ts + 1, id);
  }
}

class TSDBTest : public testing::Test {
 public:
  ::tsdb::chunk::XORChunk* new_xor_chunk1(int64_t t, int num, int* remain,
                                          int64_t off = 0) {
    ::tsdb::chunk::XORChunk* c = new ::tsdb::chunk::XORChunk();
    auto app = c->appender();
    for (int i = 0; i < num; i++) {
      // if ((t + i * 1000) / PARTITION_LENGTH * PARTITION_LENGTH != t /
      // PARTITION_LENGTH * PARTITION_LENGTH) {
      //   *remain = num - i;
      //   app->append(std::numeric_limits<int64_t>::max(), 0);
      //   return c;
      // }
      app->append(t + i * 1000, t + i * 1000 + off);
    }
    // if (num != tuple_size)
    //   app->append(std::numeric_limits<int64_t>::max(), 0);
    *remain = 0;
    return c;
  }

  void set_parameters(int num_ts_, int tuple_size_, int num_tuple_) {
    num_ts = num_ts_;
    tuple_size = tuple_size_;
    num_tuple = num_tuple_;
    MEM_TUPLE_SIZE = tuple_size_;
  }

  void set_db(DB* db) { head_->set_db(db); }

  void insert_tuple_with_labels(DB* db, int64_t st, int64_t off = 0) {
    for (int i = 0; i < num_ts; i++) {
      ::tsdb::label::Labels lset;
      for (int j = 0; j < 10; j++)
        lset.emplace_back(
            "label" + std::to_string(j),
            "label" + std::to_string(j) + "_" + std::to_string(i));
      lset.emplace_back("label_all", "label_all");

      auto app = head_->appender();
      auto r = app->add(lset, 0, 0);
      if (!r.second.ok())
        std::cout << "TSDBTest::insert_tuple_with_labels failed" << std::endl;
      app->rollback();  // Clear the fake sample.

      std::string key;
      encodeKey(&key, "", i + 1, st);
      int remain;
      ::tsdb::chunk::XORChunk* c = new_xor_chunk1(st, tuple_size, &remain, off);
      std::string data;
      PutVarint64(&data, (tuple_size - remain - 1) * 1000);
      PutVarint32(&data, c->size());
      data.append(reinterpret_cast<const char*>(c->bytes()), c->size());
      ASSERT_TRUE(db->Put(WriteOptions(), key, Slice(data)).ok());
      delete c;
      if (remain != 0) {
        key.clear();
        encodeKey(&key, "", i + 1, st + (tuple_size - remain) * 1000);
        c = new_xor_chunk1(st + (tuple_size - remain) * 1000, remain, &remain,
                           off);
        // ASSERT_TRUE(db->Put(WriteOptions(), key, Slice(reinterpret_cast<const
        // char*>(c->bytes()), c->size())).ok());
        data.clear();
        PutVarint64(&data, (remain - 1) * 1000);
        PutVarint32(&data, c->size());
        data.append(reinterpret_cast<const char*>(c->bytes()), c->size());
        ASSERT_TRUE(db->Put(WriteOptions(), key, Slice(data)).ok());
        delete c;
      }
    }
  }

  void insert_tuple_without_labels(DB* db, int64_t st, int64_t off = 0) {
    for (int i = 0; i < num_ts; i++) {
      std::string key;
      encodeKey(&key, "", i + 1, st);
      int remain;
      ::tsdb::chunk::XORChunk* c = new_xor_chunk1(st, tuple_size, &remain, off);
      std::string data;
      PutVarint64(&data, (tuple_size - remain - 1) * 1000);
      PutVarint32(&data, c->size());
      data.append(reinterpret_cast<const char*>(c->bytes()), c->size());
      ASSERT_TRUE(db->Put(WriteOptions(), key, Slice(data)).ok());
      delete c;
      if (remain != 0) {
        key.clear();
        encodeKey(&key, "", i + 1, st + (tuple_size - remain) * 1000);
        c = new_xor_chunk1(st + (tuple_size - remain) * 1000, remain, &remain,
                           off);
        // ASSERT_TRUE(db->Put(WriteOptions(), key, Slice(reinterpret_cast<const
        // char*>(c->bytes()), c->size())).ok());
        data.clear();
        PutVarint64(&data, (remain - 1) * 1000);
        PutVarint32(&data, c->size());
        data.append(reinterpret_cast<const char*>(c->bytes()), c->size());
        ASSERT_TRUE(db->Put(WriteOptions(), key, Slice(data)).ok());
        delete c;
      }
    }
  }

  void head_add(int64_t st, int64_t off = 0, bool write_flush_mark = true) {
    auto app = head_->appender(write_flush_mark);
    for (int i = 0; i < num_ts; i++) {
      ::tsdb::label::Labels lset;
      for (int j = 0; j < 10; j++)
        lset.emplace_back(
            "label" + std::to_string(j),
            "label" + std::to_string(j) + "_" + std::to_string(i));
      lset.emplace_back("label_all", "label_all");

      auto r = app->add(lset, 0, 0 + off);
      if (!r.second.ok())
        std::cout << "TSDBTest::insert_tuple_with_labels failed" << std::endl;
      else if (r.first != i + 1)
        std::cout << "TSDBTest::insert_tuple_with_labels wrong id exp:" << i + 1
                  << " got:" << r.first << std::endl;

      for (int k = 1; k < tuple_size; k++)
        app->add_fast(i + 1, st + k * 1000, st + k * 1000 + off);
    }

    // add a group.
    std::vector<int> slots;
    uint64_t gid;
    app->add({{"group", "1"}, {"label_all", "label_all"}},
             {{{"a", "b"}}, {{"c", "d"}}}, 0 + off, {0.0 + off, 0.0 + off},
             &gid, &slots);
    if (((uint64_t)(num_ts + 1) | 0x8000000000000000) != gid)
      std::cout << "TSDBTest::insert_tuple_with_labels wrong gid" << std::endl;

    for (int k = 1; k < tuple_size; k++)
      app->add(gid, st + k * 1000,
               {(double)(st + k * 1000 + off), (double)(st + k * 1000 + off)});
    app->commit();
  }

  void head_add1(int64_t st, int64_t interval, int num_labels,
                 bool write_flush_mark = true) {
    auto app = head_->appender(write_flush_mark);
    for (int i = 0; i < num_ts; i++) {
      ::tsdb::label::Labels lset;
      for (int j = 0; j < num_labels; j++)
        lset.emplace_back(
            "label" + std::to_string(j),
            "label" + std::to_string(j) + "_" + std::to_string(i));
      lset.emplace_back("label_all", "label_all");

      auto r = app->add(lset, 0, 0);
      if (!r.second.ok())
        std::cout << "TSDBTest::insert_tuple_with_labels failed" << std::endl;
      else if (r.first != i + 1)
        std::cout << "TSDBTest::insert_tuple_with_labels wrong id exp:" << i + 1
                  << " got:" << r.first << std::endl;

      for (int k = 1; k < tuple_size; k++)
        app->add_fast(i + 1, st + k * interval * 1000,
                      st + k * interval * 1000);
    }
    app->commit();
  }

  void head_add_random(int64_t st, int64_t interval,
                       bool write_flush_mark = true) {
    auto app = head_->appender(write_flush_mark);
    int64_t fluc;
    for (int i = 0; i < num_ts; i++) {
      ::tsdb::label::Labels lset;
      for (int j = 0; j < 20; j++)
        lset.emplace_back(
            "label" + std::to_string(j),
            "label" + std::to_string(j) + "_" + std::to_string(i));
      lset.emplace_back("label_all", "label_all");

      auto r = app->add(lset, 0, 0);
      if (!r.second.ok())
        std::cout << "TSDBTest::insert_tuple_with_labels failed" << std::endl;
      else if (r.first != i + 1)
        std::cout << "TSDBTest::insert_tuple_with_labels wrong id exp:" << i + 1
                  << " got:" << r.first << std::endl;
      // app->rollback(); // Clear the fake sample.

      fluc = rand() % 200;
      for (int k = 1; k < tuple_size; k++)
        app->add_fast(i + 1, st + k * interval * 1000 + fluc,
                      st + k * interval * 1000 + fluc);
    }
    app->commit();
  }

  void head_add_fast(int64_t st, int64_t off = 0,
                     bool write_flush_mark = true) {
    auto app = head_->appender(write_flush_mark);
    for (int i = 0; i < num_ts; i++) {
      for (int k = 0; k < tuple_size; k++)
        app->add_fast(i + 1, st + k * 1000, st + k * 1000 + off);
    }

    for (int k = 0; k < tuple_size; k++)
      app->add((uint64_t)(num_ts + 1) | 0x8000000000000000, st + k * 1000,
               {(double)(st + k * 1000 + off), (double)(st + k * 1000 + off)});
    app->commit();
  }

  void head_add_fast_partial1(int64_t st, int step, int64_t off = 0,
                              bool write_flush_mark = true) {
    auto app = head_->appender(write_flush_mark);
    for (int i = 0; i < num_ts; i += step) {
      for (int k = 0; k < tuple_size; k++)
        app->add_fast(i + 1, st + k * 1000, st + k * 1000 + off);
    }

    for (int k = 0; k < tuple_size; k++)
      app->add((uint64_t)(num_ts + 1) | 0x8000000000000000, st + k * 1000,
               {(double)(st + k * 1000 + off), (double)(st + k * 1000 + off)});
    app->commit();
  }

  void head_add_fast_partial2(int64_t st, int step, int64_t off = 0,
                              bool write_flush_mark = true) {
    auto app = head_->appender(write_flush_mark);
    for (int i = 0; i < num_ts; i += step) {
      for (int k = 0; k < tuple_size; k++)
        app->add_fast(i + 1, st + k * 1000 + 500, st + k * 1000 + off);
    }

    for (int k = 0; k < tuple_size; k++)
      app->add((uint64_t)(num_ts + 1) | 0x8000000000000000, st + k * 1000 + 500,
               {(double)(st + k * 1000 + off), (double)(st + k * 1000 + off)});
    app->commit();
  }

  void head_add_fast_random(int64_t st, int64_t interval,
                            bool write_flush_mark = true) {
    int64_t fluc;
    auto app = head_->appender(write_flush_mark);
    for (int i = 0; i < num_ts; i++) {
      fluc = rand() % 200;
      for (int k = 0; k < tuple_size; k++)
        app->add_fast(i + 1, st + k * interval * 1000 + fluc,
                      st + k * interval * 1000 + fluc);
    }
    app->commit();
  }

  void head_add_fast1(int64_t st, int64_t interval,
                      bool write_flush_mark = true) {
    auto app = head_->appender(write_flush_mark);
    for (int i = 0; i < num_ts; i++) {
      for (int k = 0; k < tuple_size; k++)
        app->add_fast(i + 1, st + k * interval * 1000,
                      st + k * interval * 1000);
    }
    app->commit();
  }

  void setup(const std::string& log_dir, const std::string& idx_dir,
             DB* db = nullptr) {
    ::tsdb::head::MMapHeadWithTrie* p = head_.release();
    delete p;
    head_.reset(new ::tsdb::head::MMapHeadWithTrie(0, log_dir, idx_dir, db));
  }

  void load_devops_labels1(std::vector<tsdb::label::Labels>* lsets) {
    char instance[64];
    int current_instance = 0;
    std::ifstream file("../test/devops100000.txt");
    std::string line;
    int num_lines = num_ts / 100;
    int cur_line = 0;
    int ts_counter = 1;

    std::vector<std::string> items, names, values;
    for (size_t round = 0; round < devops_names.size(); round++) {
      while (cur_line < num_lines) {
        getline(file, line);

        size_t pos_start = 0, pos_end, delim_len = 1;
        std::string token;
        items.clear();
        while ((pos_end = line.find(",", pos_start)) != std::string::npos) {
          token = line.substr(pos_start, pos_end - pos_start);
          pos_start = pos_end + delim_len;
          items.push_back(token);
        }
        items.push_back(line.substr(pos_start));

        names.clear();
        values.clear();
        for (size_t i = 1; i < items.size(); i++) {
          pos_end = items[i].find("=");
          names.push_back(items[i].substr(0, pos_end));
          values.push_back(items[i].substr(pos_end + 1));
        }

        for (size_t i = 0; i < devops[round].size(); i++) {
          tsdb::label::Labels lset;
          for (size_t j = 0; j < names.size(); j++)
            lset.emplace_back(names[j], values[j]);
          lset.emplace_back("__name__", devops_names[round] + devops[round][i]);
          std::sort(lset.begin(), lset.end());

          lsets->push_back(std::move(lset));

          ts_counter++;
        }
        cur_line++;
      }
      for (int i = 0; i < 100000 - cur_line; i++) getline(file, line);
      cur_line = 0;
    }
  }

  void load_devops_labels2(std::vector<tsdb::label::Labels>* lsets) {
    char instance[64];
    int current_instance = 0;
    std::ifstream file("../test/devops100000.txt");
    std::string line;
    int num_lines = num_ts / 100;
    int cur_line = 0;
    int ts_counter = 0;

    std::vector<std::string> items, names, values;
    for (size_t round = 0; round < devops_names.size(); round++) {
      while (cur_line < num_lines) {
        getline(file, line);

        size_t pos_start = 0, pos_end, delim_len = 1;
        std::string token;
        items.clear();
        while ((pos_end = line.find(",", pos_start)) != std::string::npos) {
          token = line.substr(pos_start, pos_end - pos_start);
          pos_start = pos_end + delim_len;
          items.push_back(token);
        }
        items.push_back(line.substr(pos_start));

        names.clear();
        values.clear();
        for (size_t i = 1; i < items.size(); i++) {
          pos_end = items[i].find("=");
          names.push_back(items[i].substr(0, pos_end));
          if (items[i].substr(0, pos_end) == "hostname")
            values.push_back(items[i].substr(pos_end + 1));
          else
            values.push_back(items[i].substr(pos_end + 1) + "_" +
                             std::to_string(ts_counter));
        }

        for (size_t i = 0; i < devops[round].size(); i++) {
          tsdb::label::Labels lset;
          for (size_t j = 0; j < names.size(); j++)
            lset.emplace_back(names[j], values[j]);
          lset.emplace_back("__name__", devops_names[round] + devops[round][i]);
          std::sort(lset.begin(), lset.end());

          lsets->push_back(std::move(lset));

          ts_counter++;
        }
        cur_line++;
      }
      for (int i = 0; i < 100000 - cur_line; i++) getline(file, line);
      cur_line = 0;
    }
  }

  void load_devops_group_labels1(
      std::vector<std::vector<tsdb::label::Labels>>* lsets) {
    char instance[64];
    int current_instance = 0;
    std::ifstream file("../test/devops100000.txt");
    std::string line;
    int num_lines = num_ts / 100;
    int cur_line = 0;
    int ts_counter = 1;

    for (int i = 0; i < num_lines; i++) lsets->emplace_back();

    std::vector<std::string> items, names, values;
    for (size_t round = 0; round < devops_names.size(); round++) {
      while (cur_line < num_lines) {
        getline(file, line);

        size_t pos_start = 0, pos_end, delim_len = 1;
        std::string token;
        items.clear();
        while ((pos_end = line.find(",", pos_start)) != std::string::npos) {
          token = line.substr(pos_start, pos_end - pos_start);
          pos_start = pos_end + delim_len;
          items.push_back(token);
        }
        items.push_back(line.substr(pos_start));

        names.clear();
        values.clear();
        for (size_t i = 1; i < items.size(); i++) {
          pos_end = items[i].find("=");
          // remove host
          if (items[i].substr(0, pos_end) == "hostname") continue;
          names.push_back(items[i].substr(0, pos_end));
          values.push_back(items[i].substr(pos_end + 1));
        }

        for (size_t i = 0; i < devops[round].size(); i++) {
          tsdb::label::Labels lset;
          for (size_t j = 0; j < names.size(); j++)
            lset.emplace_back(names[j], values[j]);
          lset.emplace_back("__name__", devops_names[round] + devops[round][i]);
          std::sort(lset.begin(), lset.end());

          lsets->at(cur_line).push_back(std::move(lset));

          ts_counter++;
        }
        cur_line++;
      }
      for (int i = 0; i < 100000 - cur_line; i++) getline(file, line);
      cur_line = 0;
    }
  }

  void load_devops_group_labels2(
      std::vector<std::vector<tsdb::label::Labels>>* lsets) {
    char instance[64];
    int current_instance = 0;
    std::ifstream file("../test/devops100000.txt");
    std::string line;
    int num_lines = num_ts / 100;
    int cur_line = 0;
    int ts_counter = 0;

    for (int i = 0; i < num_lines; i++) lsets->emplace_back();

    std::vector<std::string> items, names, values;
    for (size_t round = 0; round < devops_names.size(); round++) {
      while (cur_line < num_lines) {
        getline(file, line);

        size_t pos_start = 0, pos_end, delim_len = 1;
        std::string token;
        items.clear();
        while ((pos_end = line.find(",", pos_start)) != std::string::npos) {
          token = line.substr(pos_start, pos_end - pos_start);
          pos_start = pos_end + delim_len;
          items.push_back(token);
        }
        items.push_back(line.substr(pos_start));

        names.clear();
        values.clear();
        for (size_t i = 1; i < items.size(); i++) {
          pos_end = items[i].find("=");
          // remove host
          if (items[i].substr(0, pos_end) == "hostname") continue;
          names.push_back(items[i].substr(0, pos_end));
          values.push_back(items[i].substr(pos_end + 1) + "_" +
                           std::to_string(ts_counter));
        }

        for (size_t i = 0; i < devops[round].size(); i++) {
          tsdb::label::Labels lset;
          for (size_t j = 0; j < names.size(); j++)
            lset.emplace_back(names[j], values[j]);
          lset.emplace_back("__name__", devops_names[round] + devops[round][i]);
          std::sort(lset.begin(), lset.end());

          lsets->at(cur_line).push_back(std::move(lset));

          ts_counter++;
        }
        cur_line++;
      }
      for (int i = 0; i < 100000 - cur_line; i++) getline(file, line);
      cur_line = 0;
    }
  }

  int num_ts;
  int tuple_size;
  int num_tuple;
  std::unique_ptr<::tsdb::head::MMapHeadWithTrie> head_;
};

TEST_F(TSDBTest, TestMergeGroups) {
  DecodedGroup* g1 = new DecodedGroup(0, 100, {1, 3, 5, 7, 9}, {0, 40, 80, 100},
                                      {
                                          {1.1, 1.1, 1.1, 1.1},
                                          {2.1, 2.1, 2.1, 2.1},
                                          {3.1, 3.1, 3.1, 3.1},
                                          {4.1, 4.1, 4.1, 4.1},
                                          {5.1, 5.1, 5.1, 5.1},
                                      });
  DecodedGroup* g2 =
      new DecodedGroup(50, 150, {1, 2, 6, 7, 9}, {50, 80, 100, 150},
                       {
                           {1.2, 1.2, 1.2, 1.2},
                           {2.2, 2.2, 2.2, 2.2},
                           {3.2, 3.2, 3.2, 3.2},
                           {4.2, 4.2, 4.2, 4.2},
                           {5.2, 5.2, 5.2, 5.2},
                       });
  DecodedGroup* g3 =
      new DecodedGroup(100, 200, {3, 4, 5, 8, 9}, {100, 120, 150, 200},
                       {
                           {1.3, 1.3, 1.3, 1.3},
                           {2.3, 2.3, 2.3, 2.3},
                           {3.3, 3.3, 3.3, 3.3},
                           {4.3, 4.3, 4.3, 4.3},
                           {5.3, 5.3, 5.3, 5.3},
                       });
  DecodedGroup* g4 =
      new DecodedGroup(300, 400, {3, 4, 5, 8, 9}, {300, 340, 370, 400},
                       {
                           {1.4, 1.4, 1.4, 1.4},
                           {2.4, 2.4, 2.4, 2.4},
                           {3.4, 3.4, 3.4, 3.4},
                           {4.4, 4.4, 4.4, 4.4},
                           {5.4, 5.4, 5.4, 5.4},
                       });

  double mv = std::numeric_limits<double>::max();
  std::vector<DecodedGroup*> groups({g1, g2, g3, g4});
  merge_overlapping_groups(&groups);
  ASSERT_EQ(2, groups.size());
  ASSERT_EQ(0, groups[0]->mint_);
  ASSERT_EQ(200, groups[0]->maxt_);
  ASSERT_EQ(std::vector<int>({1, 2, 3, 4, 5, 6, 7, 8, 9}), groups[0]->slots_);
  ASSERT_EQ(std::vector<int64_t>({0, 40, 50, 80, 100, 120, 150, 200}),
            groups[0]->timestamps_);
  ASSERT_EQ(std::vector<double>({1.1, 1.1, 1.2, 1.2, 1.2, mv, 1.2, mv}),
            groups[0]->values_[0]);
  ASSERT_EQ(std::vector<double>({mv, mv, 2.2, 2.2, 2.2, mv, 2.2, mv}),
            groups[0]->values_[1]);
  ASSERT_EQ(std::vector<double>({2.1, 2.1, mv, 2.1, 1.3, 1.3, 1.3, 1.3}),
            groups[0]->values_[2]);
  ASSERT_EQ(std::vector<double>({mv, mv, mv, mv, 2.3, 2.3, 2.3, 2.3}),
            groups[0]->values_[3]);
  ASSERT_EQ(std::vector<double>({3.1, 3.1, mv, 3.1, 3.3, 3.3, 3.3, 3.3}),
            groups[0]->values_[4]);
  ASSERT_EQ(std::vector<double>({mv, mv, 3.2, 3.2, 3.2, mv, 3.2, mv}),
            groups[0]->values_[5]);
  ASSERT_EQ(std::vector<double>({4.1, 4.1, 4.2, 4.2, 4.2, mv, 4.2, mv}),
            groups[0]->values_[6]);
  ASSERT_EQ(std::vector<double>({mv, mv, mv, mv, 4.3, 4.3, 4.3, 4.3}),
            groups[0]->values_[7]);
  ASSERT_EQ(std::vector<double>({5.1, 5.1, 5.2, 5.2, 5.3, 5.3, 5.3, 5.3}),
            groups[0]->values_[8]);

  ASSERT_EQ(300, groups[1]->mint_);
  ASSERT_EQ(400, groups[1]->maxt_);
  ASSERT_EQ(std::vector<int>({3, 4, 5, 8, 9}), groups[1]->slots_);
  ASSERT_EQ(std::vector<int64_t>({300, 340, 370, 400}), groups[1]->timestamps_);
  ASSERT_EQ(std::vector<double>({1.4, 1.4, 1.4, 1.4}), groups[1]->values_[0]);
  ASSERT_EQ(std::vector<double>({2.4, 2.4, 2.4, 2.4}), groups[1]->values_[1]);
  ASSERT_EQ(std::vector<double>({3.4, 3.4, 3.4, 3.4}), groups[1]->values_[2]);
  ASSERT_EQ(std::vector<double>({4.4, 4.4, 4.4, 4.4}), groups[1]->values_[3]);
  ASSERT_EQ(std::vector<double>({5.4, 5.4, 5.4, 5.4}), groups[1]->values_[4]);

  merge_nonoverlapping_groups(groups[0], groups[1]);
  ASSERT_EQ(0, groups[0]->mint_);
  ASSERT_EQ(400, groups[0]->maxt_);
  ASSERT_EQ(std::vector<int>({1, 2, 3, 4, 5, 6, 7, 8, 9}), groups[0]->slots_);
  ASSERT_EQ(std::vector<int64_t>(
                {0, 40, 50, 80, 100, 120, 150, 200, 300, 340, 370, 400}),
            groups[0]->timestamps_);
  ASSERT_EQ(std::vector<double>(
                {1.1, 1.1, 1.2, 1.2, 1.2, mv, 1.2, mv, mv, mv, mv, mv}),
            groups[0]->values_[0]);
  ASSERT_EQ(
      std::vector<double>({mv, mv, 2.2, 2.2, 2.2, mv, 2.2, mv, mv, mv, mv, mv}),
      groups[0]->values_[1]);
  ASSERT_EQ(std::vector<double>(
                {2.1, 2.1, mv, 2.1, 1.3, 1.3, 1.3, 1.3, 1.4, 1.4, 1.4, 1.4}),
            groups[0]->values_[2]);
  ASSERT_EQ(std::vector<double>(
                {mv, mv, mv, mv, 2.3, 2.3, 2.3, 2.3, 2.4, 2.4, 2.4, 2.4}),
            groups[0]->values_[3]);
  ASSERT_EQ(std::vector<double>(
                {3.1, 3.1, mv, 3.1, 3.3, 3.3, 3.3, 3.3, 3.4, 3.4, 3.4, 3.4}),
            groups[0]->values_[4]);
  ASSERT_EQ(
      std::vector<double>({mv, mv, 3.2, 3.2, 3.2, mv, 3.2, mv, mv, mv, mv, mv}),
      groups[0]->values_[5]);
  ASSERT_EQ(std::vector<double>(
                {4.1, 4.1, 4.2, 4.2, 4.2, mv, 4.2, mv, mv, mv, mv, mv}),
            groups[0]->values_[6]);
  ASSERT_EQ(std::vector<double>(
                {mv, mv, mv, mv, 4.3, 4.3, 4.3, 4.3, 4.4, 4.4, 4.4, 4.4}),
            groups[0]->values_[7]);
  ASSERT_EQ(std::vector<double>(
                {5.1, 5.1, 5.2, 5.2, 5.3, 5.3, 5.3, 5.3, 5.4, 5.4, 5.4, 5.4}),
            groups[0]->values_[8]);
}

TEST_F(TSDBTest, TestMergeIterator) {
  boost::filesystem::remove_all("/tmp/testmergeiter");
  Options options;
  options.create_if_missing = true;
  options.write_buffer_size = 10 << 14;  // 10KB.
  options.max_file_size = 10 << 10;      // 10KB.

  DB* db;
  ASSERT_TRUE(DB::Open(options, "/tmp/testmergeiter", &db).ok());
  set_parameters(10, 16, 300);
  setup("/tmp/testmergeiter", "/tmp/testmergeiter");
  set_db(db);

  ASSERT_TRUE(db->Put(WriteOptions(), "hello", "world1").ok());
  reinterpret_cast<DBImpl*>(db)->TEST_CompactMemTable();
  ASSERT_TRUE(db->Put(WriteOptions(), "hello", "world2").ok());
  reinterpret_cast<DBImpl*>(db)->TEST_CompactMemTable();

  Iterator* it = reinterpret_cast<DBImpl*>(db)->TEST_merge_iterator();
  it->SeekToFirst();
  ASSERT_TRUE(it->Valid());
  std::cout << it->value().ToString() << std::endl;
  it->Next();
  ASSERT_TRUE(it->Valid());
  std::cout << it->value().ToString() << std::endl;
}

TEST_F(TSDBTest, TestOverlapL1) {
  boost::filesystem::remove_all("/tmp/tsdb_test1");
  Options options;
  options.create_if_missing = true;
  options.write_buffer_size = 10 << 14;  // 10KB.
  options.max_file_size = 10 << 10;      // 10KB.

  DB* db;
  ASSERT_TRUE(DB::Open(options, "/tmp/tsdb_test1", &db).ok());
  set_parameters(5, 8, 300);
  setup("/tmp/tsdb_test1", "/tmp/tsdb_test1");
  set_db(db);

  head_add(0);
  reinterpret_cast<DBImpl*>(db)->TEST_CompactMemTable();
  head_add_fast(PARTITION_LENGTH);
  reinterpret_cast<DBImpl*>(db)->TEST_CompactMemTable();
  head_add_fast(PARTITION_LENGTH * 2);
  reinterpret_cast<DBImpl*>(db)->TEST_CompactMemTable();
  sleep(1);
  db->PrintLevel();
  head_add_fast(0, 1);
  reinterpret_cast<DBImpl*>(db)->TEST_CompactMemTable();

  sleep(3);
  db->PrintLevel();

  DBQuerier* q = db->Querier(head_.get(), 0, 2000000);
  std::vector<::tsdb::label::MatcherInterface*> matchers(
      {new ::tsdb::label::EqualMatcher("label_all", "label_all")});
  std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss = q->select(matchers);
  int tsid = 1;
  tsid = 1;
  while (ss->next()) {
    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

    if (series->type() == tsdb::querier::kTypeSeries) {
      ASSERT_EQ(tsid, ss->current_tsid());
      ::tsdb::label::Labels lset;
      for (int j = 0; j < 10; j++)
        lset.emplace_back(
            "label" + std::to_string(j),
            "label" + std::to_string(j) + "_" + std::to_string(tsid - 1));
      lset.emplace_back("label_all", "label_all");
      ASSERT_EQ(lset, series->labels());

      std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
          series->iterator();
      int i = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000), p.first);
        ASSERT_EQ((double)(i * 1000) + (i <= 8 ? 1 : 0), p.second);
        i++;
        if (i == 8) i = 1800;
      }
      ASSERT_EQ(i, 1808);
    } else {
      ASSERT_EQ(tsid | 0x8000000000000000, ss->current_tsid());
      ::tsdb::label::Labels lset;
      ASSERT_TRUE(series->next());
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "label_all"}, {"a", "b"}}),
                lset);
      std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
          series->iterator();
      int i = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000), p.first);
        ASSERT_EQ((double)(i * 1000) + (i <= 8 ? 1 : 0), p.second);
        i++;
        if (i == 8) i = 1800;
      }
      ASSERT_EQ(i, 1808);

      ASSERT_TRUE(series->next());
      lset.clear();
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "label_all"}, {"c", "d"}}),
                lset);
      it = series->iterator();
      i = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000), p.first);
        ASSERT_EQ((double)(i * 1000) + (i <= 8 ? 1 : 0), p.second);
        i++;
        if (i == 8) i = 1800;
      }
      ASSERT_EQ(i, 1808);

      ASSERT_FALSE(series->next());
    }

    tsid++;
  }
  ASSERT_EQ(num_ts + 2, tsid);
}

TEST_F(TSDBTest, TestOverlapL2) {
  boost::filesystem::remove_all("/tmp/tsdb_test1");
  Options options;
  options.create_if_missing = true;
  options.write_buffer_size = 10 << 14;  // 10KB.
  options.max_file_size = 10 << 10;      // 10KB.

  DB* db;
  ASSERT_TRUE(DB::Open(options, "/tmp/tsdb_test1", &db).ok());
  set_parameters(5, 8, 300);
  setup("/tmp/tsdb_test1", "/tmp/tsdb_test1");
  set_db(db);

  head_add(0);
  reinterpret_cast<DBImpl*>(db)->TEST_CompactMemTable();
  head_add_fast(PARTITION_LENGTH);
  reinterpret_cast<DBImpl*>(db)->TEST_CompactMemTable();
  head_add_fast(PARTITION_LENGTH * 2);
  reinterpret_cast<DBImpl*>(db)->TEST_CompactMemTable();
  head_add_fast(PARTITION_LENGTH * 3);
  reinterpret_cast<DBImpl*>(db)->TEST_CompactMemTable();
  head_add_fast(PARTITION_LENGTH * 4);
  reinterpret_cast<DBImpl*>(db)->TEST_CompactMemTable();
  head_add_fast(PARTITION_LENGTH * 5);
  reinterpret_cast<DBImpl*>(db)->TEST_CompactMemTable();
  head_add_fast(PARTITION_LENGTH * 6);
  reinterpret_cast<DBImpl*>(db)->TEST_CompactMemTable();
  head_add_fast(PARTITION_LENGTH * 7);
  reinterpret_cast<DBImpl*>(db)->TEST_CompactMemTable();
  head_add_fast(PARTITION_LENGTH * 8);
  reinterpret_cast<DBImpl*>(db)->TEST_CompactMemTable();
  head_add_fast(PARTITION_LENGTH * 9);
  reinterpret_cast<DBImpl*>(db)->TEST_CompactMemTable();
  head_add_fast(PARTITION_LENGTH * 10);
  reinterpret_cast<DBImpl*>(db)->TEST_CompactMemTable();
  sleep(3);
  db->PrintLevel();

  head_add_fast(0, 1);
  reinterpret_cast<DBImpl*>(db)->TEST_CompactMemTable();
  sleep(3);
  db->PrintLevel();

  // Trigger L2 merge.
  head_add_fast(0, 2);
  reinterpret_cast<DBImpl*>(db)->TEST_CompactMemTable();
  sleep(3);
  db->PrintLevel();

  head_add_fast(0, 3);
  reinterpret_cast<DBImpl*>(db)->TEST_CompactMemTable();

  sleep(5);
  db->PrintLevel();

  DBQuerier* q = db->Querier(head_.get(), 0, 2000000);
  std::vector<::tsdb::label::MatcherInterface*> matchers(
      {new ::tsdb::label::EqualMatcher("label_all", "label_all")});
  std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss = q->select(matchers);
  int tsid = 1;
  tsid = 1;
  while (ss->next()) {
    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

    if (series->type() == tsdb::querier::kTypeSeries) {
      ASSERT_EQ(tsid, ss->current_tsid());
      ::tsdb::label::Labels lset;
      for (int j = 0; j < 10; j++)
        lset.emplace_back(
            "label" + std::to_string(j),
            "label" + std::to_string(j) + "_" + std::to_string(tsid - 1));
      lset.emplace_back("label_all", "label_all");
      ASSERT_EQ(lset, series->labels());

      std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
          series->iterator();
      int i = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000), p.first);
        ASSERT_EQ((double)(i * 1000) + (i <= 8 ? 3 : 0), p.second);
        i++;
        if (i == 8) i = 1800;
      }
      ASSERT_EQ(i, 1808);
    } else {
      ASSERT_EQ(tsid | 0x8000000000000000, ss->current_tsid());
      ::tsdb::label::Labels lset;
      ASSERT_TRUE(series->next());
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "label_all"}, {"a", "b"}}),
                lset);
      std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
          series->iterator();
      int i = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000), p.first);
        ASSERT_EQ((double)(i * 1000) + (i <= 8 ? 3 : 0), p.second);
        i++;
        if (i == 8) i = 1800;
      }
      ASSERT_EQ(i, 1808);

      ASSERT_TRUE(series->next());
      lset.clear();
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "label_all"}, {"c", "d"}}),
                lset);
      it = series->iterator();
      i = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000), p.first);
        ASSERT_EQ((double)(i * 1000) + (i <= 8 ? 3 : 0), p.second);
        i++;
        if (i == 8) i = 1800;
      }
      ASSERT_EQ(i, 1808);

      ASSERT_FALSE(series->next());
    }

    tsid++;
  }
  ASSERT_EQ(num_ts + 2, tsid);
}

TEST_F(TSDBTest, Test1) {
  boost::filesystem::remove_all("/tmp/tsdb_test1");

  Options options;
  options.create_if_missing = true;
  options.write_buffer_size = 10 << 13;  // 10KB.
  options.max_file_size = 10 << 10;      // 10KB.

  DB* db;
  ASSERT_TRUE(DB::Open(options, "/tmp/tsdb_test1", &db).ok());
  set_parameters(10, 16, 200);

  setup("/tmp/tsdb_test1", "/tmp/tsdb_test1");

  insert_tuple_with_labels(db, 0);

  for (int tuple = 1; tuple < num_tuple; tuple++) {
    insert_tuple_without_labels(db, tuple * tuple_size * 1000);
  }

  sleep(2);
  db->PrintLevel();

  DBQuerier* q = db->Querier(head_.get(), 0, 1800000);
  std::vector<::tsdb::label::MatcherInterface*> matchers(
      {new ::tsdb::label::EqualMatcher("label0", "label0_0")});
  std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss = q->select(matchers);
  int tsid = 1;
  while (ss->next()) {
    ASSERT_EQ(tsid, ss->current_tsid());
    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

    ::tsdb::label::Labels lset;
    for (int j = 0; j < 10; j++)
      lset.emplace_back(
          "label" + std::to_string(j),
          "label" + std::to_string(j) + "_" + std::to_string(tsid - 1));
    lset.emplace_back("label_all", "label_all");
    ASSERT_EQ(lset, series->labels());

    std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
        series->iterator();
    int i = 0;
    while (it->next()) {
      auto p = it->at();
      ASSERT_EQ((int64_t)(i * 1000), p.first);
      ASSERT_EQ((double)(i * 1000), p.second);
      i++;
    }
    ASSERT_EQ(i, 1801);

    tsid++;
  }
  ASSERT_EQ(2, tsid);

  // close and reopen to test log.
  delete q;
  delete db;
  ASSERT_TRUE(DB::Open(options, "/tmp/tsdb_test1", &db).ok());
  db->PrintLevel();
  q = db->Querier(head_.get(), 0, 1800000);
  matchers = std::vector<::tsdb::label::MatcherInterface*>(
      {new ::tsdb::label::EqualMatcher("label_all", "label_all")});
  ss.reset(q->select(matchers).release());
  tsid = 1;
  while (ss->next()) {
    ASSERT_EQ(tsid, ss->current_tsid());
    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

    ::tsdb::label::Labels lset;
    for (int j = 0; j < 10; j++)
      lset.emplace_back(
          "label" + std::to_string(j),
          "label" + std::to_string(j) + "_" + std::to_string(tsid - 1));
    lset.emplace_back("label_all", "label_all");
    ASSERT_EQ(lset, series->labels());

    std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
        series->iterator();
    int i = 0;
    while (it->next()) {
      auto p = it->at();
      ASSERT_EQ((int64_t)(i * 1000), p.first);
      ASSERT_EQ((double)(i * 1000), p.second);
      i++;
    }
    ASSERT_EQ(i, 1801);

    tsid++;
  }
  ASSERT_EQ(num_ts + 1, tsid);

  // Add new samples.
  for (int tuple = num_tuple; tuple < num_tuple * 2; tuple++) {
    insert_tuple_without_labels(db, tuple * tuple_size * 1000);
  }
  delete q;
  q = db->Querier(head_.get(), 0, 4000000);
  matchers = std::vector<::tsdb::label::MatcherInterface*>(
      {new ::tsdb::label::EqualMatcher("label_all", "label_all")});
  ss.reset(q->select(matchers).release());
  tsid = 1;
  while (ss->next()) {
    ASSERT_EQ(tsid, ss->current_tsid());
    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

    ::tsdb::label::Labels lset;
    for (int j = 0; j < 10; j++)
      lset.emplace_back(
          "label" + std::to_string(j),
          "label" + std::to_string(j) + "_" + std::to_string(tsid - 1));
    lset.emplace_back("label_all", "label_all");
    ASSERT_EQ(lset, series->labels());

    std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
        series->iterator();
    int i = 0;
    while (it->next()) {
      auto p = it->at();
      ASSERT_EQ((int64_t)(i * 1000), p.first);
      ASSERT_EQ((double)(i * 1000), p.second);
      i++;
    }
    ASSERT_EQ(i, 4001);

    tsid++;
  }
  ASSERT_EQ(num_ts + 1, tsid);
  delete q;
  delete db;
}

TEST_F(TSDBTest, Test1_Head) {
  boost::filesystem::remove_all("/tmp/tsdb_test1");
  Options options;
  options.create_if_missing = true;
  options.write_buffer_size = 10 << 13;  // 10KB.
  options.max_file_size = 10 << 10;      // 10KB.

  DB* db;
  Status s = DB::Open(options, "/tmp/tsdb_test1", &db);
  // std::cout << s.ToString() << std::endl;
  ASSERT_TRUE(s.ok());
  set_parameters(10, 16, 200);
  setup("/tmp/tsdb_test1", "/tmp/tsdb_test1");
  set_db(db);

  head_add(0);
  for (int tuple = 1; tuple < num_tuple; tuple++) {
    head_add_fast(tuple * tuple_size * 1000);
  }

  sleep(5);
  db->PrintLevel();

  DBQuerier* q = db->Querier(head_.get(), 16000, 1800000);
  std::vector<::tsdb::label::MatcherInterface*> matchers(
      {new ::tsdb::label::EqualMatcher("label0", "label0_0")});
  std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss = q->select(matchers);
  int tsid = 1;
  while (ss->next()) {
    ASSERT_EQ(tsid, ss->current_tsid());
    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

    ::tsdb::label::Labels lset;
    for (int j = 0; j < 10; j++)
      lset.emplace_back(
          "label" + std::to_string(j),
          "label" + std::to_string(j) + "_" + std::to_string(tsid - 1));
    lset.emplace_back("label_all", "label_all");
    ASSERT_EQ(lset, series->labels());

    std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
        series->iterator();
    int i = 16;
    while (it->next()) {
      auto p = it->at();
      ASSERT_EQ((int64_t)(i * 1000), p.first);
      ASSERT_EQ((double)(i * 1000), p.second);
      i++;
    }
    ASSERT_EQ(i, 1801);

    tsid++;
  }
  ASSERT_EQ(2, tsid);

  // close and reopen to test log.
  delete q;
  delete db;
  ASSERT_TRUE(DB::Open(options, "/tmp/tsdb_test1", &db).ok());
  db->PrintLevel();
  q = db->Querier(head_.get(), 0, 1800000);
  matchers = std::vector<::tsdb::label::MatcherInterface*>(
      {new ::tsdb::label::EqualMatcher("label_all", "label_all")});
  ss.reset(q->select(matchers).release());
  tsid = 1;
  while (ss->next()) {
    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

    if (series->type() == tsdb::querier::kTypeSeries) {
      ASSERT_EQ(tsid, ss->current_tsid());
      ::tsdb::label::Labels lset;
      for (int j = 0; j < 10; j++)
        lset.emplace_back(
            "label" + std::to_string(j),
            "label" + std::to_string(j) + "_" + std::to_string(tsid - 1));
      lset.emplace_back("label_all", "label_all");
      ASSERT_EQ(lset, series->labels());

      std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
          series->iterator();
      int i = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000), p.first);
        ASSERT_EQ((double)(i * 1000), p.second);
        i++;
      }
      ASSERT_EQ(i, 1801);
    } else {
      ASSERT_EQ(tsid | 0x8000000000000000, ss->current_tsid());
      ::tsdb::label::Labels lset;
      ASSERT_TRUE(series->next());
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "label_all"}, {"a", "b"}}),
                lset);
      std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
          series->iterator();
      int i = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000), p.first);
        ASSERT_EQ((double)(i * 1000), p.second);
        i++;
      }
      ASSERT_EQ(i, 1801);

      ASSERT_TRUE(series->next());
      lset.clear();
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "label_all"}, {"c", "d"}}),
                lset);
      it = series->iterator();
      i = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000), p.first);
        ASSERT_EQ((double)(i * 1000), p.second);
        i++;
      }
      ASSERT_EQ(i, 1801);

      ASSERT_FALSE(series->next());
    }

    tsid++;
  }
  ASSERT_EQ(num_ts + 2, tsid);
  delete q;
  delete db;
}

TEST_F(TSDBTest, Test1_Merge_Log) {
  boost::filesystem::remove_all("/tmp/tsdb_test1");
  Options options;
  options.create_if_missing = true;
  options.write_buffer_size = 10 << 13;  // 10KB.
  options.max_file_size = 10 << 10;      // 10KB.
  options.use_log = false;

  tsdb::head::MAX_HEAD_SAMPLES_LOG_SIZE = 96 * 1024;

  DB* db;
  Status s = DB::Open(options, "/tmp/tsdb_test1", &db);
  ASSERT_TRUE(s.ok());
  set_parameters(10, 16, 200);
  setup("/tmp/tsdb_test1", "/tmp/tsdb_test1", db);

  head_add(0, 0, false);
  for (int tuple = 1; tuple < num_tuple; tuple++) {
    head_add_fast(tuple * tuple_size * 1000, 0, false);
  }

  sleep(5);
  db->PrintLevel();

  DBQuerier* q = db->Querier(head_.get(), 0, 10000000000);
  std::vector<::tsdb::label::MatcherInterface*> matchers(
      {new ::tsdb::label::EqualMatcher("label0", "label0_0")});
  std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss = q->select(matchers);
  int tsid = 1;
  while (ss->next()) {
    ASSERT_EQ(tsid, ss->current_tsid());
    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

    ::tsdb::label::Labels lset;
    for (int j = 0; j < 10; j++)
      lset.emplace_back(
          "label" + std::to_string(j),
          "label" + std::to_string(j) + "_" + std::to_string(tsid - 1));
    lset.emplace_back("label_all", "label_all");
    ASSERT_EQ(lset, series->labels());

    std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
        series->iterator();
    int i = 0;
    while (it->next()) {
      auto p = it->at();
      ASSERT_EQ((int64_t)(i * 1000), p.first);
      ASSERT_EQ((double)(i * 1000), p.second);
      i++;
    }
    ASSERT_EQ(i, tuple_size * num_tuple);

    tsid++;
  }
  ASSERT_EQ(2, tsid);

  // close and reopen to test log.
  delete q;
  delete db;
  ASSERT_TRUE(DB::Open(options, "/tmp/tsdb_test1", &db).ok());
  setup("/tmp/tsdb_test1", "/tmp/tsdb_test1", db);

  db->PrintLevel();
  q = db->Querier(head_.get(), 0, 10000000000);
  matchers = std::vector<::tsdb::label::MatcherInterface*>(
      {new ::tsdb::label::EqualMatcher("label_all", "label_all")});
  ss.reset(q->select(matchers).release());
  tsid = 1;
  while (ss->next()) {
    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

    if (series->type() == tsdb::querier::kTypeSeries) {
      ASSERT_EQ(tsid, ss->current_tsid());
      ::tsdb::label::Labels lset;
      for (int j = 0; j < 10; j++)
        lset.emplace_back(
            "label" + std::to_string(j),
            "label" + std::to_string(j) + "_" + std::to_string(tsid - 1));
      lset.emplace_back("label_all", "label_all");
      ASSERT_EQ(lset, series->labels());

      std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
          series->iterator();
      int i = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000), p.first);
        ASSERT_EQ((double)(i * 1000), p.second);
        i++;
      }
      ASSERT_EQ(i, tuple_size * num_tuple);
    } else {
      ASSERT_EQ(tsid | 0x8000000000000000, ss->current_tsid());
      ::tsdb::label::Labels lset;
      ASSERT_TRUE(series->next());
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "label_all"}, {"a", "b"}}),
                lset);
      std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
          series->iterator();
      int i = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000), p.first);
        ASSERT_EQ((double)(i * 1000), p.second);
        i++;
      }
      ASSERT_EQ(i, tuple_size * num_tuple);

      ASSERT_TRUE(series->next());
      lset.clear();
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "label_all"}, {"c", "d"}}),
                lset);
      it = series->iterator();
      i = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000), p.first);
        ASSERT_EQ((double)(i * 1000), p.second);
        i++;
      }
      ASSERT_EQ(i, tuple_size * num_tuple);

      ASSERT_FALSE(series->next());
    }

    tsid++;
  }
  ASSERT_EQ(num_ts + 2, tsid);
  delete q;
  delete db;
}

TEST_F(TSDBTest, Test1_Head_Cloud) {
  std::string dbpath = "/tmp/tsdb_test1";
  boost::filesystem::remove_all(dbpath);
  Options options;
  options.create_if_missing = true;
  options.write_buffer_size = 10 << 13;  // 10KB.
  options.max_file_size = 10 << 10;      // 10KB.

  std::string region = "ap-northeast-1";
  std::string bucket_prefix = "rockset.";
  std::string bucket_suffix = "cloud-db-examples.alec";

  CloudEnvOptions cloud_env_options;
  std::unique_ptr<CloudEnv> cloud_env;
  cloud_env_options.src_bucket.SetBucketName(bucket_suffix, bucket_prefix);
  cloud_env_options.dest_bucket.SetBucketName(bucket_suffix, bucket_prefix);
  cloud_env_options.keep_local_sst_files = true;
  CloudEnv* cenv;
  const std::string bucketName = bucket_suffix + bucket_prefix;
  Status s = CloudEnv::NewAwsEnv(Env::Default(), bucket_suffix, dbpath, region,
                                 bucket_suffix, dbpath, region,
                                 cloud_env_options, nullptr, &cenv);
  // NewLRUCache(256*1024*1024));
  ASSERT_TRUE(s.ok());
  cloud_env.reset(cenv);

  DBCloud* db;
  options.env = cloud_env.get();
  s = DBCloud::Open(options, dbpath, &db);
  // std::cout << s.ToString() << std::endl;
  ASSERT_TRUE(s.ok());
  set_parameters(10, 16, 200);
  setup("/tmp/tsdb_test1", "/tmp/tsdb_test1");
  set_db(reinterpret_cast<DB*>(db));

  head_add(0);
  for (int tuple = 1; tuple < num_tuple; tuple++) {
    head_add_fast(tuple * tuple_size * 1000);
  }

  sleep(2);
  db->PrintLevel();

  DBQuerier* q = db->Querier(head_.get(), 0, 1800000);
  std::vector<::tsdb::label::MatcherInterface*> matchers(
      {new ::tsdb::label::EqualMatcher("label0", "label0_0")});
  std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss = q->select(matchers);
  int tsid = 1;
  while (ss->next()) {
    ASSERT_EQ(tsid, ss->current_tsid());
    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

    ::tsdb::label::Labels lset;
    for (int j = 0; j < 10; j++)
      lset.emplace_back(
          "label" + std::to_string(j),
          "label" + std::to_string(j) + "_" + std::to_string(tsid - 1));
    lset.emplace_back("label_all", "label_all");
    ASSERT_EQ(lset, series->labels());

    std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
        series->iterator();
    int i = 0;
    while (it->next()) {
      auto p = it->at();
      ASSERT_EQ((int64_t)(i * 1000), p.first);
      ASSERT_EQ((double)(i * 1000), p.second);
      i++;
    }
    ASSERT_EQ(i, 1801);

    tsid++;
  }
  ASSERT_EQ(2, tsid);

  // close and reopen to test log.
  delete q;
  delete db;
  ASSERT_TRUE(DBCloud::Open(options, dbpath, &db).ok());
  db->PrintLevel();
  q = db->Querier(head_.get(), 0, 1800000);
  matchers = std::vector<::tsdb::label::MatcherInterface*>(
      {new ::tsdb::label::EqualMatcher("label_all", "label_all")});
  ss.reset(q->select(matchers).release());
  tsid = 1;
  while (ss->next()) {
    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

    if (series->type() == tsdb::querier::kTypeSeries) {
      ASSERT_EQ(tsid, ss->current_tsid());
      ::tsdb::label::Labels lset;
      for (int j = 0; j < 10; j++)
        lset.emplace_back(
            "label" + std::to_string(j),
            "label" + std::to_string(j) + "_" + std::to_string(tsid - 1));
      lset.emplace_back("label_all", "label_all");
      ASSERT_EQ(lset, series->labels());

      std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
          series->iterator();
      int i = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000), p.first);
        ASSERT_EQ((double)(i * 1000), p.second);
        i++;
      }
      ASSERT_EQ(i, 1801);
    } else {
      ASSERT_EQ(tsid | 0x8000000000000000, ss->current_tsid());
      ::tsdb::label::Labels lset;
      ASSERT_TRUE(series->next());
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "label_all"}, {"a", "b"}}),
                lset);
      std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
          series->iterator();
      int i = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000), p.first);
        ASSERT_EQ((double)(i * 1000), p.second);
        i++;
      }
      ASSERT_EQ(i, 1801);

      ASSERT_TRUE(series->next());
      lset.clear();
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "label_all"}, {"c", "d"}}),
                lset);
      it = series->iterator();
      i = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000), p.first);
        ASSERT_EQ((double)(i * 1000), p.second);
        i++;
      }
      ASSERT_EQ(i, 1801);

      ASSERT_FALSE(series->next());
    }

    tsid++;
  }
  ASSERT_EQ(num_ts + 2, tsid);
  delete q;
  delete db;
}

// Trigger L0 compaction.
TEST_F(TSDBTest, Test2) {
  boost::filesystem::remove_all("/tmp/tsdb_test2");
  Options options;
  options.create_if_missing = true;
  options.write_buffer_size = 10 << 14;  // 10KB.
  options.max_file_size = 10 << 10;      // 10KB.

  DB* db;
  ASSERT_TRUE(DB::Open(options, "/tmp/tsdb_test2", &db).ok());
  set_parameters(10, 16, 300);
  setup("/tmp/tsdb_test2", "/tmp/tsdb_test2");
  set_db(db);

  insert_tuple_with_labels(db, 0);

  for (int tuple = 1; tuple < num_tuple; tuple++) {
    insert_tuple_without_labels(db, tuple * tuple_size * 1000);
  }

  sleep(3);
  db->PrintLevel();

  DBQuerier* q = db->Querier(head_.get(), 1600000, 4200000);
  std::vector<::tsdb::label::MatcherInterface*> matchers(
      {new ::tsdb::label::EqualMatcher("label0", "label0_0")});
  std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss = q->select(matchers);
  int tsid = 1;
  while (ss->next()) {
    ASSERT_EQ(tsid, ss->current_tsid());
    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

    ::tsdb::label::Labels lset;
    for (int j = 0; j < 10; j++)
      lset.emplace_back(
          "label" + std::to_string(j),
          "label" + std::to_string(j) + "_" + std::to_string(tsid - 1));
    lset.emplace_back("label_all", "label_all");
    ASSERT_EQ(lset, series->labels());

    std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
        series->iterator();
    int i = 1600;
    while (it->next()) {
      auto p = it->at();
      ASSERT_EQ((int64_t)(i * 1000), p.first);
      ASSERT_EQ((double)(i * 1000), p.second);
      i++;
    }
    ASSERT_EQ(i, 4201);

    tsid++;
  }
  ASSERT_EQ(2, tsid);

  // close and reopen to test log.
  delete q;
  delete db;
  ASSERT_TRUE(DB::Open(options, "/tmp/tsdb_test2", &db).ok());
  sleep(3);
  db->PrintLevel();
  q = db->Querier(head_.get(), 1600000, 4200000);
  matchers = std::vector<::tsdb::label::MatcherInterface*>(
      {new ::tsdb::label::EqualMatcher("label_all", "label_all")});
  ss.reset(q->select(matchers).release());
  tsid = 1;
  while (ss->next()) {
    ASSERT_EQ(tsid, ss->current_tsid());
    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

    ::tsdb::label::Labels lset;
    for (int j = 0; j < 10; j++)
      lset.emplace_back(
          "label" + std::to_string(j),
          "label" + std::to_string(j) + "_" + std::to_string(tsid - 1));
    lset.emplace_back("label_all", "label_all");
    ASSERT_EQ(lset, series->labels());

    std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
        series->iterator();
    int i = 1600;
    while (it->next()) {
      auto p = it->at();
      ASSERT_EQ((int64_t)(i * 1000), p.first);
      ASSERT_EQ((double)(i * 1000), p.second);
      i++;
    }
    ASSERT_EQ(i, 4201);

    tsid++;
  }
  ASSERT_EQ(num_ts + 1, tsid);
  delete q;
  delete db;
}

// Trigger L0 compaction.
TEST_F(TSDBTest, Test2_Head) {
  boost::filesystem::remove_all("/tmp/tsdb_test2");
  Options options;
  options.create_if_missing = true;
  options.write_buffer_size = 10 << 14;  // 10KB.
  options.max_file_size = 10 << 10;      // 10KB.
  // options.use_log = false;

  DB* db;
  ASSERT_TRUE(DB::Open(options, "/tmp/tsdb_test2", &db).ok());
  set_parameters(10, 16, 300);
  setup("/tmp/tsdb_test2", "/tmp/tsdb_test2");
  set_db(db);

  head_add(0, 0, options.use_log);
  for (int tuple = 1; tuple < num_tuple; tuple++) {
    head_add_fast(tuple * tuple_size * 1000, 0, options.use_log);
  }

  sleep(5);
  db->PrintLevel();

  DBQuerier* q = db->Querier(head_.get(), 1600000, 4200000);
  std::vector<::tsdb::label::MatcherInterface*> matchers(
      {new ::tsdb::label::EqualMatcher("label0", "label0_0")});
  std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss = q->select(matchers);
  int tsid = 1;
  while (ss->next()) {
    ASSERT_EQ(tsid, ss->current_tsid());
    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

    ::tsdb::label::Labels lset;
    for (int j = 0; j < 10; j++)
      lset.emplace_back(
          "label" + std::to_string(j),
          "label" + std::to_string(j) + "_" + std::to_string(tsid - 1));
    lset.emplace_back("label_all", "label_all");
    ASSERT_EQ(lset, series->labels());

    std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
        series->iterator();
    int i = 1600;
    while (it->next()) {
      auto p = it->at();
      ASSERT_EQ((int64_t)(i * 1000), p.first);
      ASSERT_EQ((double)(i * 1000), p.second);
      i++;
    }
    ASSERT_EQ(i, 4201);

    tsid++;
  }
  ASSERT_EQ(2, tsid);

  // close and reopen to test log.
  delete q;
  delete db;
  ASSERT_TRUE(DB::Open(options, "/tmp/tsdb_test2", &db).ok());
  sleep(3);
  db->PrintLevel();
  q = db->Querier(head_.get(), 1600000, 4200000);
  matchers = std::vector<::tsdb::label::MatcherInterface*>(
      {new ::tsdb::label::EqualMatcher("label_all", "label_all")});
  ss.reset(q->select(matchers).release());
  tsid = 1;
  while (ss->next()) {
    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

    if (series->type() == tsdb::querier::kTypeSeries) {
      ASSERT_EQ(tsid, ss->current_tsid());
      ::tsdb::label::Labels lset;
      for (int j = 0; j < 10; j++)
        lset.emplace_back(
            "label" + std::to_string(j),
            "label" + std::to_string(j) + "_" + std::to_string(tsid - 1));
      lset.emplace_back("label_all", "label_all");
      ASSERT_EQ(lset, series->labels());

      std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
          series->iterator();
      int i = 1600;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000), p.first);
        ASSERT_DOUBLE_EQ((double)(i * 1000), p.second);
        i++;
      }
      ASSERT_EQ(i, 4201);
    } else {
      ASSERT_EQ(tsid | 0x8000000000000000, ss->current_tsid());
      ::tsdb::label::Labels lset;
      ASSERT_TRUE(series->next());
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "label_all"}, {"a", "b"}}),
                lset);
      std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
          series->iterator();
      int i = 1600;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000), p.first);
        ASSERT_DOUBLE_EQ((double)(i * 1000), p.second);
        i++;
      }
      ASSERT_EQ(i, 4201);

      ASSERT_TRUE(series->next());
      lset.clear();
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "label_all"}, {"c", "d"}}),
                lset);
      it = series->iterator();
      i = 1600;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000), p.first);
        ASSERT_DOUBLE_EQ((double)(i * 1000), p.second);
        i++;
      }
      ASSERT_EQ(i, 4201);

      ASSERT_FALSE(series->next());
    }

    tsid++;
  }
  ASSERT_EQ(num_ts + 2, tsid);
  delete q;
  delete db;
}

// Trigger L1 compaction.
TEST_F(TSDBTest, Test3) {
  boost::filesystem::remove_all("/tmp/tsdb_test3");
  Options options;
  options.create_if_missing = true;
  options.write_buffer_size = 10 << 14;  // 10KB.
  options.max_file_size = 10 << 10;      // 10KB.

  DB* db;
  ASSERT_TRUE(DB::Open(options, "/tmp/tsdb_test3", &db).ok());
  set_parameters(10, 16, 1000);
  setup("/tmp/tsdb_test3", "/tmp/tsdb_test3");
  set_db(db);

  insert_tuple_with_labels(db, 0);

  for (int tuple = 1; tuple < num_tuple; tuple++) {
    insert_tuple_without_labels(db, tuple * tuple_size * 1000);
  }

  sleep(5);
  db->PrintLevel();

  DBQuerier* q = db->Querier(head_.get(), 1600000, 12300000);
  std::vector<::tsdb::label::MatcherInterface*> matchers(
      {new ::tsdb::label::EqualMatcher("label0", "label0_0")});
  std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss = q->select(matchers);
  int tsid = 1;
  while (ss->next()) {
    ASSERT_EQ(tsid, ss->current_tsid());
    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

    ::tsdb::label::Labels lset;
    for (int j = 0; j < 10; j++)
      lset.emplace_back(
          "label" + std::to_string(j),
          "label" + std::to_string(j) + "_" + std::to_string(tsid - 1));
    lset.emplace_back("label_all", "label_all");
    ASSERT_EQ(lset, series->labels());

    std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
        series->iterator();
    int i = 1600;
    while (it->next()) {
      auto p = it->at();
      ASSERT_EQ((int64_t)(i * 1000), p.first);
      ASSERT_EQ((double)(i * 1000), p.second);
      i++;
    }
    ASSERT_EQ(i, 12301);

    tsid++;
  }
  ASSERT_EQ(2, tsid);

  // close and reopen to test log.
  delete q;
  delete db;
  Status s = DB::Open(options, "/tmp/tsdb_test3", &db);
  ASSERT_TRUE(s.ok());
  sleep(3);
  db->PrintLevel();
  q = db->Querier(head_.get(), 1600000, 12300000);
  matchers = std::vector<::tsdb::label::MatcherInterface*>(
      {new ::tsdb::label::EqualMatcher("label_all", "label_all")});
  ss.reset(q->select(matchers).release());
  tsid = 1;
  while (ss->next()) {
    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

    if (series->type() == tsdb::querier::kTypeSeries) {
      ASSERT_EQ(tsid, ss->current_tsid());
      ::tsdb::label::Labels lset;
      for (int j = 0; j < 10; j++)
        lset.emplace_back(
            "label" + std::to_string(j),
            "label" + std::to_string(j) + "_" + std::to_string(tsid - 1));
      lset.emplace_back("label_all", "label_all");
      ASSERT_EQ(lset, series->labels());

      std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
          series->iterator();
      int i = 1600;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000), p.first);
        ASSERT_EQ((double)(i * 1000), p.second);
        i++;
      }
      ASSERT_EQ(i, 12301);
    } else {
      ASSERT_EQ(tsid | 0x8000000000000000, ss->current_tsid());
      ::tsdb::label::Labels lset;
      ASSERT_TRUE(series->next());
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "label_all"}, {"a", "b"}}),
                lset);
      std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
          series->iterator();
      int i = 1600;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000), p.first);
        ASSERT_EQ((double)(i * 1000), p.second);
        i++;
      }
      ASSERT_EQ(i, 12301);

      ASSERT_TRUE(series->next());
      lset.clear();
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "label_all"}, {"c", "d"}}),
                lset);
      it = series->iterator();
      i = 1600;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000), p.first);
        ASSERT_EQ((double)(i * 1000), p.second);
        i++;
      }
      ASSERT_EQ(i, 12301);

      ASSERT_FALSE(series->next());
    }

    tsid++;
  }
  ASSERT_EQ(num_ts + 1, tsid);
  delete q;
  delete db;
}

// Trigger L1 compaction.
TEST_F(TSDBTest, Test3_Head) {
  boost::filesystem::remove_all("/tmp/tsdb_test3");
  Options options;
  options.create_if_missing = true;
  options.write_buffer_size = 10 << 14;  // 10KB.
  options.max_file_size = 10 << 10;      // 10KB.
  options.sample_cache = NewLRUCache(32 * 1024 * 1024);
  // options.max_imm_num = 1;

  std::cout << "cache usage: " << options.sample_cache->GetUsage() << std::endl;

  DB* db;
  ASSERT_TRUE(DB::Open(options, "/tmp/tsdb_test3", &db).ok());
  set_parameters(10, 16, 1000);
  setup("/tmp/tsdb_test3", "/tmp/tsdb_test3");
  set_db(db);

  head_add(0);
  for (int tuple = 1; tuple < num_tuple; tuple++) {
    head_add_fast(tuple * tuple_size * 1000);
  }

  sleep(2);
  db->PrintLevel();

  DBQuerier* q = db->Querier(head_.get(), 1600000, 20000000);
  std::vector<::tsdb::label::MatcherInterface*> matchers(
      {new ::tsdb::label::EqualMatcher("label0", "label0_0")});
  std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss = q->select(matchers);
  int tsid = 1;
  while (ss->next()) {
    ASSERT_EQ(tsid, ss->current_tsid());
    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

    ::tsdb::label::Labels lset;
    for (int j = 0; j < 10; j++)
      lset.emplace_back(
          "label" + std::to_string(j),
          "label" + std::to_string(j) + "_" + std::to_string(tsid - 1));
    lset.emplace_back("label_all", "label_all");
    ASSERT_EQ(lset, series->labels());

    std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
        series->chain_iterator();
    int i = 1600;
    while (it->next()) {
      auto p = it->at();
      ASSERT_EQ((int64_t)(i * 1000), p.first);
      ASSERT_EQ((double)(i * 1000), p.second);
      i++;
    }
    ASSERT_EQ(i, 16000);

    tsid++;
  }
  ASSERT_EQ(2, tsid);

  std::cout << "cache usage: " << options.sample_cache->GetUsage() << std::endl;

  // close and reopen to test log.
  delete q;
  delete db;
  Status s = DB::Open(options, "/tmp/tsdb_test3", &db);
  ASSERT_TRUE(s.ok());
  sleep(3);
  db->PrintLevel();
  q = db->Querier(head_.get(), 1600000, 12300000);
  matchers = std::vector<::tsdb::label::MatcherInterface*>(
      {new ::tsdb::label::EqualMatcher("label_all", "label_all")});
  ss.reset(q->select(matchers).release());
  tsid = 1;
  while (ss->next()) {
    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

    if (series->type() == tsdb::querier::kTypeSeries) {
      ASSERT_EQ(tsid, ss->current_tsid());
      ::tsdb::label::Labels lset;
      for (int j = 0; j < 10; j++)
        lset.emplace_back(
            "label" + std::to_string(j),
            "label" + std::to_string(j) + "_" + std::to_string(tsid - 1));
      lset.emplace_back("label_all", "label_all");
      ASSERT_EQ(lset, series->labels());

      std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
          series->iterator();
      int i = 1600;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000), p.first);
        ASSERT_EQ((double)(i * 1000), p.second);
        i++;
      }
      ASSERT_EQ(i, 12301);
    } else {
      ASSERT_EQ(tsid | 0x8000000000000000, ss->current_tsid());
      ::tsdb::label::Labels lset;
      ASSERT_TRUE(series->next());
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "label_all"}, {"a", "b"}}),
                lset);
      std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
          series->iterator();
      int i = 1600;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000), p.first);
        ASSERT_EQ((double)(i * 1000), p.second);
        i++;
      }
      ASSERT_EQ(i, 12301);

      ASSERT_TRUE(series->next());
      lset.clear();
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "label_all"}, {"c", "d"}}),
                lset);
      it = series->iterator();
      i = 1600;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000), p.first);
        ASSERT_EQ((double)(i * 1000), p.second);
        i++;
      }
      ASSERT_EQ(i, 12301);

      ASSERT_FALSE(series->next());
    }

    tsid++;
  }
  std::cout << "cache usage: " << options.sample_cache->GetUsage() << std::endl;
  ASSERT_EQ(num_ts + 2, tsid);
  delete q;
  delete db;
}

// out-of-order
TEST_F(TSDBTest, Test3_Head_OOO) {
  boost::filesystem::remove_all("/tmp/tsdb_test3");
  Options options;
  options.create_if_missing = true;
  options.write_buffer_size = 10 << 14;  // 10KB.
  options.max_file_size = 10 << 10;      // 10KB.
  // options.sample_cache = NewLRUCache(32 * 1024 * 1024);
  // options.max_imm_num = 1;

  DB* db;
  ASSERT_TRUE(DB::Open(options, "/tmp/tsdb_test3", &db).ok());
  set_parameters(20, 16, 1000);
  setup("/tmp/tsdb_test3", "/tmp/tsdb_test3");
  set_db(db);

  head_add(0);
  for (int tuple = 1; tuple < num_tuple; tuple++) {
    head_add_fast(tuple * tuple_size * 1000);
  }

  int step = 2;
  int64_t off = 12;
  for (int tuple = 0; tuple < num_tuple; tuple++) {
    head_add_fast_partial1(tuple * tuple_size * 1000, step, off);
  }

  sleep(2);
  db->PrintLevel();

  DBQuerier* q = db->Querier(head_.get(), 1600000, 20000000);
  std::vector<::tsdb::label::MatcherInterface*> matchers(
      {new ::tsdb::label::EqualMatcher("label0", "label0_0")});
  std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss = q->select(matchers);
  int tsid = 1;
  while (ss->next()) {
    ASSERT_EQ(tsid, ss->current_tsid());
    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

    ::tsdb::label::Labels lset;
    for (int j = 0; j < 10; j++)
      lset.emplace_back(
          "label" + std::to_string(j),
          "label" + std::to_string(j) + "_" + std::to_string(tsid - 1));
    lset.emplace_back("label_all", "label_all");
    ASSERT_EQ(lset, series->labels());

    std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
        series->iterator();
    int i = 1600;
    while (it->next()) {
      auto p = it->at();
      ASSERT_EQ((int64_t)(i * 1000), p.first);
      if ((tsid - 1) % step == 0)
        ASSERT_EQ((double)(i * 1000 + off), p.second);
      else
        ASSERT_EQ((double)(i * 1000), p.second);
      i++;
    }
    ASSERT_EQ(i, 16000);

    tsid++;
  }
  ASSERT_EQ(2, tsid);

  // close and reopen to test log.
  delete q;
  delete db;
  Status s = DB::Open(options, "/tmp/tsdb_test3", &db);
  ASSERT_TRUE(s.ok());
  sleep(3);
  db->PrintLevel();
  q = db->Querier(head_.get(), 1600000, 12300000);
  matchers = std::vector<::tsdb::label::MatcherInterface*>(
      {new ::tsdb::label::EqualMatcher("label_all", "label_all")});
  ss.reset(q->select(matchers).release());
  tsid = 1;
  while (ss->next()) {
    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

    if (series->type() == tsdb::querier::kTypeSeries) {
      ASSERT_EQ(tsid, ss->current_tsid());
      ::tsdb::label::Labels lset;
      for (int j = 0; j < 10; j++)
        lset.emplace_back(
            "label" + std::to_string(j),
            "label" + std::to_string(j) + "_" + std::to_string(tsid - 1));
      lset.emplace_back("label_all", "label_all");
      ASSERT_EQ(lset, series->labels());

      std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
          series->iterator();
      int i = 1600;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000), p.first);
        if ((tsid - 1) % step == 0)
          ASSERT_EQ((double)(i * 1000 + off), p.second);
        else
          ASSERT_EQ((double)(i * 1000), p.second);
        i++;
      }
      ASSERT_EQ(i, 12301);
    } else {
      ASSERT_EQ(tsid | 0x8000000000000000, ss->current_tsid());
      ::tsdb::label::Labels lset;
      ASSERT_TRUE(series->next());
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "label_all"}, {"a", "b"}}),
                lset);
      std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
          series->iterator();
      int i = 1600;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000), p.first);
        ASSERT_EQ((double)(i * 1000 + off), p.second);
        i++;
      }
      ASSERT_EQ(i, 12301);

      ASSERT_TRUE(series->next());
      lset.clear();
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "label_all"}, {"c", "d"}}),
                lset);
      it = series->iterator();
      i = 1600;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000), p.first);
        ASSERT_EQ((double)(i * 1000 + off), p.second);
        i++;
      }
      ASSERT_EQ(i, 12301);

      ASSERT_FALSE(series->next());
    }

    tsid++;
  }
  ASSERT_EQ(num_ts + 2, tsid);
  delete q;
  delete db;
}

TEST_F(TSDBTest, Test3_Head_OOO2) {
  boost::filesystem::remove_all("/tmp/tsdb_test3");
  Options options;
  options.create_if_missing = true;
  options.write_buffer_size = 10 << 14;  // 10KB.
  options.max_file_size = 10 << 10;      // 10KB.
  // options.sample_cache = NewLRUCache(32 * 1024 * 1024);
  // options.max_imm_num = 1;

  DB* db;
  ASSERT_TRUE(DB::Open(options, "/tmp/tsdb_test3", &db).ok());
  set_parameters(20, 16, 1000);
  setup("/tmp/tsdb_test3", "/tmp/tsdb_test3");
  set_db(db);

  head_add(0);
  for (int tuple = 1; tuple < num_tuple; tuple++) {
    head_add_fast(tuple * tuple_size * 1000);
  }

  int step = 2;
  int64_t off = 12;
  for (int tuple = 0; tuple < num_tuple; tuple++) {
    head_add_fast_partial2(tuple * tuple_size * 1000, step, off);
  }

  sleep(2);
  db->PrintLevel();

  DBQuerier* q = db->Querier(head_.get(), 1600000, 20000000);
  std::vector<::tsdb::label::MatcherInterface*> matchers(
      {new ::tsdb::label::EqualMatcher("label0", "label0_0")});
  std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss = q->select(matchers);
  int tsid = 1;
  while (ss->next()) {
    ASSERT_EQ(tsid, ss->current_tsid());
    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

    ::tsdb::label::Labels lset;
    for (int j = 0; j < 10; j++)
      lset.emplace_back(
          "label" + std::to_string(j),
          "label" + std::to_string(j) + "_" + std::to_string(tsid - 1));
    lset.emplace_back("label_all", "label_all");
    ASSERT_EQ(lset, series->labels());

    std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
        series->iterator();
    int i = 1600;
    bool normal = true;
    while (it->next()) {
      auto p = it->at();
      if ((tsid - 1) % step == 0) {
        if (normal) {
          ASSERT_EQ((int64_t)(i * 1000), p.first);
          ASSERT_EQ((double)(i * 1000), p.second);
        } else {
          ASSERT_EQ((int64_t)(i * 1000 + 500), p.first);
          ASSERT_EQ((double)(i * 1000 + off), p.second);
          i++;
        }
        normal = !normal;
      } else {
        ASSERT_EQ((int64_t)(i * 1000), p.first);
        ASSERT_EQ((double)(i * 1000), p.second);
        i++;
      }
    }
    ASSERT_EQ(i, 16000);

    tsid++;
  }
  ASSERT_EQ(2, tsid);

  // close and reopen to test log.
  delete q;
  delete db;
  Status s = DB::Open(options, "/tmp/tsdb_test3", &db);
  ASSERT_TRUE(s.ok());
  sleep(3);
  db->PrintLevel();
  q = db->Querier(head_.get(), 1600000, 12300000);
  matchers = std::vector<::tsdb::label::MatcherInterface*>(
      {new ::tsdb::label::EqualMatcher("label_all", "label_all")});
  ss.reset(q->select(matchers).release());
  tsid = 1;
  while (ss->next()) {
    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

    if (series->type() == tsdb::querier::kTypeSeries) {
      ASSERT_EQ(tsid, ss->current_tsid());
      ::tsdb::label::Labels lset;
      for (int j = 0; j < 10; j++)
        lset.emplace_back(
            "label" + std::to_string(j),
            "label" + std::to_string(j) + "_" + std::to_string(tsid - 1));
      lset.emplace_back("label_all", "label_all");
      ASSERT_EQ(lset, series->labels());

      std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
          series->iterator();
      int i = 1600;
      bool normal = true;
      while (it->next()) {
        auto p = it->at();
        if ((tsid - 1) % step == 0) {
          if (normal) {
            ASSERT_EQ((int64_t)(i * 1000), p.first);
            ASSERT_EQ((double)(i * 1000), p.second);
          } else {
            ASSERT_EQ((int64_t)(i * 1000 + 500), p.first);
            ASSERT_EQ((double)(i * 1000 + off), p.second);
            i++;
          }
          normal = !normal;
        } else {
          ASSERT_EQ((int64_t)(i * 1000), p.first);
          ASSERT_EQ((double)(i * 1000), p.second);
          i++;
        }
      }
      if ((tsid - 1) % step == 0)
        ASSERT_EQ(i, 12300);
      else
        ASSERT_EQ(i, 12301);
    } else {
      ASSERT_EQ(tsid | 0x8000000000000000, ss->current_tsid());
      ::tsdb::label::Labels lset;
      ASSERT_TRUE(series->next());
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "label_all"}, {"a", "b"}}),
                lset);
      std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
          series->iterator();
      int i = 1600;
      bool normal = true;
      while (it->next()) {
        auto p = it->at();
        if (normal) {
          ASSERT_EQ((int64_t)(i * 1000), p.first);
          ASSERT_EQ((double)(i * 1000), p.second);
        } else {
          ASSERT_EQ((int64_t)(i * 1000 + 500), p.first);
          ASSERT_EQ((double)(i * 1000 + off), p.second);
          i++;
        }
        normal = !normal;
      }
      if ((tsid - 1) % step == 0)
        ASSERT_EQ(i, 12300);
      else
        ASSERT_EQ(i, 12301);

      ASSERT_TRUE(series->next());
      lset.clear();
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "label_all"}, {"c", "d"}}),
                lset);
      it = series->iterator();
      i = 1600;
      normal = true;
      while (it->next()) {
        auto p = it->at();
        if (normal) {
          ASSERT_EQ((int64_t)(i * 1000), p.first);
          ASSERT_EQ((double)(i * 1000), p.second);
        } else {
          ASSERT_EQ((int64_t)(i * 1000 + 500), p.first);
          ASSERT_EQ((double)(i * 1000 + off), p.second);
          i++;
        }
        normal = !normal;
      }
      if ((tsid - 1) % step == 0)
        ASSERT_EQ(i, 12300);
      else
        ASSERT_EQ(i, 12301);

      ASSERT_FALSE(series->next());
    }

    tsid++;
  }
  ASSERT_EQ(num_ts + 2, tsid);
  delete q;
  delete db;
}

TEST_F(TSDBTest, Test3_Head_Purge) {
  boost::filesystem::remove_all("/tmp/tsdb_test3");
  Options options;
  options.create_if_missing = true;
  options.write_buffer_size = 10 << 14;  // 10KB.
  options.max_file_size = 10 << 10;      // 10KB.
  options.sample_cache = NewLRUCache(32 * 1024 * 1024);
  // options.max_imm_num = 1;

  std::cout << "cache usage: " << options.sample_cache->GetUsage() << std::endl;

  DB* db;
  ASSERT_TRUE(DB::Open(options, "/tmp/tsdb_test3", &db).ok());
  set_parameters(10, 16, 1000);
  setup("/tmp/tsdb_test3", "/tmp/tsdb_test3");
  set_db(db);

  head_add(0);
  for (int tuple = 1; tuple < num_tuple; tuple++) {
    auto app = head_->appender(true);
    for (int i = 0; i < num_ts / 2; i++) {
      for (int k = 0; k < tuple_size; k++)
        app->add_fast(i + 1, tuple * tuple_size * 1000 + k * 1000,
                      tuple * tuple_size * 1000 + k * 1000);
    }
    app->commit();
  }

  sleep(2);
  db->PrintLevel();

  db->PurgeTime(num_tuple * tuple_size * 1000 / 2);
  head_->purge_time(num_tuple * tuple_size * 1000 / 2);
  sleep(3);
  db->PrintLevel();

  // close and reopen to test log.
  delete db;
  Status s = DB::Open(options, "/tmp/tsdb_test3", &db);
  ASSERT_TRUE(s.ok());
  sleep(3);
  db->PrintLevel();
  DBQuerier* q = db->Querier(head_.get(), 0, 12300000);
  std::vector<::tsdb::label::MatcherInterface*> matchers =
      std::vector<::tsdb::label::MatcherInterface*>(
          {new ::tsdb::label::EqualMatcher("label_all", "label_all")});
  std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss;
  ss.reset(q->select(matchers).release());
  uint64_t tsid = 1;
  while (ss->next()) {
    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

    ASSERT_EQ(tsdb::querier::kTypeSeries, series->type());
    ASSERT_EQ(tsid, ss->current_tsid());
    ::tsdb::label::Labels lset;
    for (int j = 0; j < 10; j++)
      lset.emplace_back(
          "label" + std::to_string(j),
          "label" + std::to_string(j) + "_" + std::to_string(tsid - 1));
    lset.emplace_back("label_all", "label_all");
    ASSERT_EQ(lset, series->labels());

    std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
        series->iterator();
    int i = 7200;
    while (it->next()) {
      auto p = it->at();
      ASSERT_EQ((int64_t)(i * 1000), p.first);
      ASSERT_EQ((double)(i * 1000), p.second);
      i++;
    }
    ASSERT_EQ(i, 12301);

    tsid++;
  }
  std::cout << "cache usage: " << options.sample_cache->GetUsage() << std::endl;
  ASSERT_EQ(num_ts / 2 + 1, tsid);
  delete q;
  delete db;
}

TEST_F(TSDBTest, Test3_Head_Cloud) {
  std::string dbpath = "/tmp/tsdb_test3";
  boost::filesystem::remove_all(dbpath);
  Options options;
  options.create_if_missing = true;
  options.write_buffer_size = 10 << 14;  // 10KB.
  options.max_file_size = 10 << 10;      // 10KB.
  options.use_log = false;

  tsdb::head::MAX_HEAD_SAMPLES_LOG_SIZE = 512 * 1024;

  std::string region = "ap-northeast-1";
  std::string bucket_prefix = "rockset.";
  std::string bucket_suffix = "cloud-db-examples.alec";

  CloudEnvOptions cloud_env_options;
  std::unique_ptr<CloudEnv> cloud_env;
  cloud_env_options.src_bucket.SetBucketName(bucket_suffix, bucket_prefix);
  cloud_env_options.dest_bucket.SetBucketName(bucket_suffix, bucket_prefix);
  cloud_env_options.keep_local_sst_files = true;
  cloud_env_options.keep_sst_levels =
      2;  // Avoid the file being copied from S3.
  CloudEnv* cenv;
  const std::string bucketName = bucket_suffix + bucket_prefix;
  Status s = CloudEnv::NewAwsEnv(Env::Default(), bucket_suffix, dbpath, region,
                                 bucket_suffix, dbpath, region,
                                 cloud_env_options, nullptr, &cenv);
  // NewLRUCache(256*1024*1024));
  ASSERT_TRUE(s.ok());
  cloud_env.reset(cenv);

  options.aws_use_cloud_cache = true;
  options.ccache = std::shared_ptr<CloudCache>(
      new CloudCache(std::shared_ptr<Cache>(NewLRUCache(1 * 1024 * 1024)),
                     1 * 1024 * 1024 /*pcache size*/, 128 * 1024 /*block size*/,
                     cloud_env->GetBaseEnv()));

  DBCloud* db;
  options.env = cloud_env.get();
  ASSERT_TRUE(DBCloud::Open(options, dbpath, &db).ok());

  set_parameters(10, 16, 1000);
  setup(dbpath, dbpath, db);

  head_add(0, false);
  for (int tuple = 1; tuple < num_tuple; tuple++) {
    head_add_fast(tuple * tuple_size * 1000, false);
  }

  sleep(15);
  db->PrintLevel();

  DBQuerier* q = db->Querier(head_.get(), 1600000, 12300000);
  std::vector<::tsdb::label::MatcherInterface*> matchers(
      {new ::tsdb::label::EqualMatcher("label0", "label0_0")});
  std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss = q->select(matchers);
  int tsid = 1;
  while (ss->next()) {
    ASSERT_EQ(tsid, ss->current_tsid());
    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

    ::tsdb::label::Labels lset;
    for (int j = 0; j < 10; j++)
      lset.emplace_back(
          "label" + std::to_string(j),
          "label" + std::to_string(j) + "_" + std::to_string(tsid - 1));
    lset.emplace_back("label_all", "label_all");
    ASSERT_EQ(lset, series->labels());

    std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
        series->iterator();
    int i = 1600;
    while (it->next()) {
      auto p = it->at();
      ASSERT_EQ((int64_t)(i * 1000), p.first);
      ASSERT_EQ((double)(i * 1000), p.second);
      i++;
    }
    ASSERT_EQ(i, 12301);

    tsid++;
  }
  ASSERT_EQ(2, tsid);

  options.ccache->print_summary(true);
  // close and reopen to test log.
  delete q;
  delete db;
  s = DBCloud::Open(options, dbpath, &db);
  ASSERT_TRUE(s.ok());
  setup(dbpath, dbpath, db);

  sleep(3);
  db->PrintLevel();
  q = db->Querier(head_.get(), 1600000, 12300000);
  matchers = std::vector<::tsdb::label::MatcherInterface*>(
      {new ::tsdb::label::EqualMatcher("label_all", "label_all")});
  ss.reset(q->select(matchers).release());
  tsid = 1;
  while (ss->next()) {
    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

    if (series->type() == tsdb::querier::kTypeSeries) {
      ASSERT_EQ(tsid, ss->current_tsid());
      ::tsdb::label::Labels lset;
      for (int j = 0; j < 10; j++)
        lset.emplace_back(
            "label" + std::to_string(j),
            "label" + std::to_string(j) + "_" + std::to_string(tsid - 1));
      lset.emplace_back("label_all", "label_all");
      ASSERT_EQ(lset, series->labels());

      std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
          series->iterator();
      int i = 1600;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000), p.first);
        ASSERT_EQ((double)(i * 1000), p.second);
        i++;
      }
      ASSERT_EQ(i, 12301);
    } else {
      ASSERT_EQ(tsid | 0x8000000000000000, ss->current_tsid());
      ::tsdb::label::Labels lset;
      ASSERT_TRUE(series->next());
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "label_all"}, {"a", "b"}}),
                lset);
      std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
          series->iterator();
      int i = 1600;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000), p.first);
        ASSERT_EQ((double)(i * 1000), p.second);
        i++;
      }
      ASSERT_EQ(i, 12301);

      ASSERT_TRUE(series->next());
      lset.clear();
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "label_all"}, {"c", "d"}}),
                lset);
      it = series->iterator();
      i = 1600;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000), p.first);
        ASSERT_EQ((double)(i * 1000), p.second);
        i++;
      }
      ASSERT_EQ(i, 12301);

      ASSERT_FALSE(series->next());
    }

    tsid++;
  }
  ASSERT_EQ(num_ts + 2, tsid);
  delete q;
  delete db;
}

void mem_usage(double& vm_usage, double& resident_set) {
  vm_usage = 0.0;
  resident_set = 0.0;
  std::ifstream stat_stream("/proc/self/stat",
                            std::ios_base::in);  // get info from proc directory
  // create some variables to get info
  std::string pid, comm, state, ppid, pgrp, session, tty_nr;
  std::string tpgid, flags, minflt, cminflt, majflt, cmajflt;
  std::string utime, stime, cutime, cstime, priority, nice;
  std::string O, itrealvalue, starttime;
  unsigned long vsize;
  long rss;
  stat_stream >> pid >> comm >> state >> ppid >> pgrp >> session >> tty_nr >>
      tpgid >> flags >> minflt >> cminflt >> majflt >> cmajflt >> utime >>
      stime >> cutime >> cstime >> priority >> nice >> O >> itrealvalue >>
      starttime >> vsize >> rss;  // don't care about the rest
  stat_stream.close();
  long page_size_kb = sysconf(_SC_PAGE_SIZE) /
                      1024;  // for x86-64 is configured to use 2MB pages
  vm_usage = vsize / 1024.0;
  resident_set = rss * page_size_kb;
}

TEST_F(TSDBTest, TestBigData1) {
  std::string dbpath = "/tmp/tsdb_big";
  boost::filesystem::remove_all(dbpath);

  Options options;
  options.use_log = false;
  options.create_if_missing = true;

  DB* db;
  ASSERT_TRUE(DB::Open(options, dbpath, &db).ok());

  set_parameters(1000000, 16, 60);
  setup("/tmp/tsdb_big", "/tmp/tsdb_big", db);

  int64_t last_t = 0, d;
  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;
  Timer timer;
  timer.start();
  int interval = 10;
  head_add1(0, interval, 20, options.use_log);
  d = timer.since_start_nano();
  std::cout << "[Labels Insertion duration (ms)]:" << ((d) / 1000000)
            << " [throughput]:"
            << ((double)(num_ts) * (double)(tuple_size) /
                (double)(d)*1000000000)
            << std::endl;
  mem_usage(vm, rss);
  std::cout << "[After Labels Insertion] Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;

  ::tsdb::head::MMapHeadWithTrie* p = head_.release();
  delete p;
  mem_usage(vm, rss);
  std::cout << "Release head VM:" << (vm / 1024) << "MB RSS:" << (rss / 1024)
            << "MB\n"
            << std::endl;
  delete db;
  mem_usage(vm, rss);
  std::cout << "Release db VM:" << (vm / 1024) << "MB RSS:" << (rss / 1024)
            << "MB\n"
            << std::endl;
  return;

  int sep_period = 10;
  // ProfilerStart("cpu.prof");
  for (int tuple = 1; tuple < num_tuple; tuple++) {
    head_add_fast1(tuple * tuple_size * 1000 * interval, interval,
                   options.use_log);
    std::cout << "tuple: " << tuple << std::endl;
    // if ((tuple + 1) % sep_period == 0) {
    //   d = timer.since_start_nano();
    //   std::cout << "[#tuples] " << tuple << " [st] " << (tuple * tuple_size *
    //   1000) << std::endl; std::cout << d << " " << last_t << std::endl;
    //   std::cout << "[Insertion duration (ms)]:" << ((d - last_t) / 1000000)
    //   << " [throughput]:" << ((double)(num_ts) * (double)(sep_period) *
    //   (double)(tuple_size) / (double)(d - last_t) * 1000000000) << std::endl;
    //   last_t = d;
    //   mem_usage(vm, rss);
    //   std::cout << "Virtual Memory: " << (vm / 1024) << "MB\nResident set
    //   size: " << (rss / 1024) << "MB\n" << std::endl; db->PrintLevel();
    //   // break;
    // }
  }
  // ProfilerStop();
  d = timer.since_start_nano();
  std::cout << "[Total Insertion duration (ms)]:" << (d / 1000000)
            << " [#TS]:" << num_ts << " [#samples]:"
            << (double)(num_ts) * (double)(num_tuple) * (double)(tuple_size)
            << " [throughput]:"
            << ((double)(num_ts) * (double)(num_tuple) * (double)(tuple_size) /
                (double)(d)*1000000000)
            << std::endl;

  sleep(500);
  db->PrintLevel();
  delete db;
}

TEST_F(TSDBTest, TestBigData2) {
  std::string dbpath = "/tmp/tsdb_big";
  boost::filesystem::remove_all(dbpath);

  Options options;
  options.use_log = false;
  options.create_if_missing = true;

  DB* db;
  ASSERT_TRUE(DB::Open(options, dbpath, &db).ok());

  set_parameters(10000, 16, 60);
  setup("/tmp/tsdb_big", "/tmp/tsdb_big", db);

  int64_t last_t = 0, d;
  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;
  Timer timer;
  timer.start();
  int interval = 20;
  head_add_random(0, interval, options.use_log);
  d = timer.since_start_nano();
  std::cout << "[Labels Insertion duration (ms)]:" << ((d) / 1000000)
            << " [throughput]:"
            << ((double)(num_ts) * (double)(tuple_size) /
                (double)(d)*1000000000)
            << std::endl;
  mem_usage(vm, rss);
  std::cout << "[After Labels Insertion] Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;

  int sep_period = 10;
  // ProfilerStart("cpu.prof");
  for (int tuple = 1; tuple < num_tuple; tuple++) {
    head_add_fast_random(tuple * tuple_size * 1000 * interval, interval,
                         options.use_log);
    std::cout << "tuple: " << tuple << std::endl;
    if ((tuple + 1) % sep_period == 0) {
      d = timer.since_start_nano();
      std::cout << "[#tuples] " << tuple << " [st] "
                << (tuple * tuple_size * 1000) << std::endl;
      std::cout << d << " " << last_t << std::endl;
      std::cout << "[Insertion duration (ms)]:" << ((d - last_t) / 1000000)
                << " [throughput]:"
                << ((double)(num_ts) * (double)(sep_period) *
                    (double)(tuple_size) / (double)(d - last_t) * 1000000000)
                << std::endl;
      last_t = d;
      mem_usage(vm, rss);
      std::cout << "Virtual Memory: " << (vm / 1024)
                << "MB\nResident set size: " << (rss / 1024) << "MB\n"
                << std::endl;
      db->PrintLevel();
      // break;
    }
  }
  // ProfilerStop();
  d = timer.since_start_nano();
  std::cout << "[Total Insertion duration (ms)]:" << (d / 1000000)
            << " [#TS]:" << num_ts << " [#samples]:"
            << (double)(num_ts) * (double)(num_tuple) * (double)(tuple_size)
            << " [throughput]:"
            << ((double)(num_ts) * (double)(num_tuple) * (double)(tuple_size) /
                (double)(d)*1000000000)
            << std::endl;

  sleep(500);
  db->PrintLevel();
  delete db;
}

TEST_F(TSDBTest, TestBigData_Cloud) {
  std::string dbpath = "/tmp/tsdb_big";
  boost::filesystem::remove_all(dbpath);
  std::string region = "ap-northeast-1";
  std::string bucket_prefix = "rockset.";
  std::string bucket_suffix = "cloud-db-examples.alec";

  CloudEnvOptions cloud_env_options;
  std::unique_ptr<CloudEnv> cloud_env;
  cloud_env_options.src_bucket.SetBucketName(bucket_suffix, bucket_prefix);
  cloud_env_options.dest_bucket.SetBucketName(bucket_suffix, bucket_prefix);
  cloud_env_options.keep_local_sst_files = true;
  cloud_env_options.keep_sst_levels = 2;
  CloudEnv* cenv;
  const std::string bucketName = bucket_suffix + bucket_prefix;
  Status s = CloudEnv::NewAwsEnv(Env::Default(), bucket_suffix, dbpath, region,
                                 bucket_suffix, dbpath, region,
                                 cloud_env_options, nullptr, &cenv);
  // NewLRUCache(256*1024*1024));
  ASSERT_TRUE(s.ok());
  cloud_env.reset(cenv);

  DBCloud* db;
  Options options;
  options.use_log = false;
  options.create_if_missing = true;
  options.env = cloud_env.get();
  s = DBCloud::Open(options, dbpath, &db);
  // std::cout << s.ToString() << std::endl;
  ASSERT_TRUE(s.ok());

  set_parameters(1000000, 16, 60);
  setup("/tmp/tsdb_big", "/tmp/tsdb_big", db);

  int64_t last_t = 0, d;
  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;
  Timer timer;
  timer.start();
  int interval = 20;
  head_add_random(0, interval, options.use_log);
  d = timer.since_start_nano();
  std::cout << "[Labels Insertion duration (ms)]:" << ((d) / 1000000)
            << " [throughput]:"
            << ((double)(num_ts) * (double)(tuple_size) /
                (double)(d)*1000000000)
            << std::endl;
  mem_usage(vm, rss);
  std::cout << "[After Labels Insertion] Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;

  int sep_period = 10;
  // ProfilerStart("cpu.prof");
  for (int tuple = 1; tuple < num_tuple; tuple++) {
    head_add_fast_random(tuple * tuple_size * 1000 * interval, interval,
                         options.use_log);
    std::cout << "tuple: " << tuple << std::endl;
    if ((tuple + 1) % sep_period == 0) {
      d = timer.since_start_nano();
      std::cout << "[#tuples] " << tuple << " [st] "
                << (tuple * tuple_size * 1000) << std::endl;
      std::cout << d << " " << last_t << std::endl;
      std::cout << "[Insertion duration (ms)]:" << ((d - last_t) / 1000000)
                << " [throughput]:"
                << ((double)(num_ts) * (double)(sep_period) *
                    (double)(tuple_size) / (double)(d - last_t) * 1000000000)
                << std::endl;
      last_t = d;
      mem_usage(vm, rss);
      std::cout << "Virtual Memory: " << (vm / 1024)
                << "MB\nResident set size: " << (rss / 1024) << "MB\n"
                << std::endl;
      db->PrintLevel();
      // break;
    }
  }
  // ProfilerStop();
  d = timer.since_start_nano();
  std::cout << "[Total Insertion duration (ms)]:" << (d / 1000000)
            << " [throughput]:"
            << ((double)(num_ts) * (double)(num_tuple) * (double)(tuple_size) /
                (double)(d)*1000000000)
            << std::endl;

  sleep(500);
  db->PrintLevel();
  delete db;
}

TEST_F(TSDBTest, TmpTest1) {
  int num = 1000000;
  boost::filesystem::remove_all("/tmp/tmp");
  boost::filesystem::create_directory("/tmp/tmp");

  Timer t;
  t.start();
  Env* env = Env::Default();
  for (int i = 0; i < num; i++) {
    WritableFile* f;
    env->NewWritableFile("/tmp/tmp/" + std::to_string(i), &f);
    f->Append("kkpkkpkkpkkpkkp");
    f->Sync();
    f->Close();
    delete f;
    if ((i + 1) % (num / 10) == 0) {
      std::cout << (i + 1) << std::endl;
    }
  }
  std::cout << t.since_start_nano() / 1000000 << std::endl;
  // for (int i = 0; i < num; i++) {
  //   std::string s = "/tmp/tmp/" + std::to_string(i);
  //   FILE* f = fopen(s.c_str(), "w");
  //   fwrite("kkpkkpkkpkkpkkp", 1, 15, f);
  //   fclose(f);
  //   if ((i + 1) % (num / 10) == 0) {
  //     std::cout << (i + 1) << std::endl;
  //   }
  // }

  boost::filesystem::remove_all("/tmp/tmp");
}

TEST_F(TSDBTest, TmpTest2) {
  std::vector<std::vector<uint64_t>> v;
  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;
  for (int i = 0; i < 20000000; i++) {
    std::vector<uint64_t> tmp;
    tmp.push_back(1);
    v.push_back(std::move(tmp));
  }
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;
}

TEST_F(TSDBTest, TestSSTSize) {
  std::string dbpath = "/tmp/tsdb_size";
  boost::filesystem::remove_all(dbpath);

  set_parameters(20000, 16, 50);
  {
    Options options;
    options.create_if_missing = true;
    DB* db;
    ASSERT_TRUE(DB::Open(options, dbpath, &db).ok());

    setup("/tmp/tsdb_size", "/tmp/tsdb_size");
    set_db(db);

    for (int j = 0; j < num_tuple; j++) {
      int64_t st = j * tuple_size * 1000;
      for (int i = 0; i < num_ts; i++) {
        std::string key;
        encodeKey(&key, "", i + 1, st);
        int remain;
        ::tsdb::chunk::XORChunk* c = new_xor_chunk1(st, tuple_size, &remain);
        std::string data;
        PutVarint64(&data, (tuple_size - remain - 1) * 1000);
        data.append(reinterpret_cast<const char*>(c->bytes()), c->size());
        ASSERT_TRUE(db->Put(WriteOptions(), key, Slice(data)).ok());
        delete c;
        if (remain != 0) {
          key.clear();
          encodeKey(&key, "", i + 1, st + (tuple_size - remain) * 1000);
          c = new_xor_chunk1(st + (tuple_size - remain) * 1000, remain,
                             &remain);
          data.clear();
          PutVarint64(&data, (remain - 1) * 1000);
          data.append(reinterpret_cast<const char*>(c->bytes()), c->size());
          ASSERT_TRUE(db->Put(WriteOptions(), key, Slice(data)).ok());
          delete c;
        }
      }
    }
    reinterpret_cast<DBImpl*>(db)->TEST_CompactMemTable();
  }

  {
    Env* env = Env::Default();
    WritableFile* f;
    env->NewWritableFile("/tmp/tsdb_size/test_size", &f);
    for (int i = 0; i < num_ts; i++) {
      int count = 0;
      for (int j = 0; j < num_tuple; j++) {
        int64_t st = j * tuple_size * 1000;
        int remain;
        ::tsdb::chunk::XORChunk* c = new_xor_chunk1(st, tuple_size, &remain);
        std::string data;
        PutVarint32(&data, c->size());
        data.append(reinterpret_cast<const char*>(c->bytes()), c->size());
        f->Append(data);
        delete c;
        if (remain != 0) {
          count++;
          c = new_xor_chunk1(st + (tuple_size - remain) * 1000, remain,
                             &remain);
          data.clear();
          PutVarint64(&data, c->size());
          data.append(reinterpret_cast<const char*>(c->bytes()), c->size());
          f->Append(data);
          delete c;
        }
      }
      for (int j = 0; j < count + num_tuple; j++)
        f->Append("\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0");
    }
    for (int i = 0; i < num_ts; i++) {
      f->Append("\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0");
    }
    delete f;
  }
}

TEST_F(TSDBTest, TestErrorAndStatus) {
  Timer timer;
  timer.start();
  for (int i = 0; i < 1000000000; i++) {
    tsdb::error::Error err;
  }
  std::cout << "Error " << timer.since_start_nano() << "ns" << std::endl;

  timer.start();
  for (int i = 0; i < 1000000000; i++) {
    Status err;
  }
  std::cout << "Status " << timer.since_start_nano() / 1000 << "ns"
            << std::endl;
}

}  // namespace leveldb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}