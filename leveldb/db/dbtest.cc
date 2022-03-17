#include "db/db_impl.h"
#include "db/db_querier.h"
#include "db/version_set.h"
#include <boost/filesystem.hpp>
#include <gperftools/profiler.h>
#include <signal.h>

#include "leveldb/cache.h"
#include "leveldb/cloud/db_cloud.h"

#include "port/port.h"
#include "third_party/rapidjson/document.h"
#include "third_party/rapidjson/rapidjson.h"
#include "third_party/rapidjson/stringbuffer.h"
#include "third_party/rapidjson/writer.h"
#include "third_party/thread_pool.h"

#include "chunk/XORChunk.hpp"
#include "cloud/aws/aws_env.h"
#include "cloud/cloud_cache.h"
#include "head/Head.hpp"
#include "head/HeadAppender.hpp"
#include "head/MMapHeadWithTrie.hpp"
#include "label/EqualMatcher.hpp"
#include "label/Label.hpp"

namespace leveldb {

int _dbtest_out_of_order_percentage = 0;

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
                                                   {"1-1-24", true},
                                                   {"1-1-all", false},
                                                   {"1-8-1", true},
                                                   {"5-1-1", true},
                                                   {"5-1-12", true},
                                                   {"5-1-24", true},
                                                   {"5-1-all", false},
                                                   {"5-8-1", true},
                                                   {"double-groupby-1", false},
                                                   {"high-cpu-1", false},
                                                   {"high-cpu-all", false},
                                                   {"lastpoint", true}});

class TSDBTest {
 public:
  void set_parameters(int num_ts_, int tuple_size_, int num_tuple_) {
    num_ts = num_ts_;
    tuple_size = tuple_size_;
    num_tuple = num_tuple_;
    MEM_TUPLE_SIZE = tuple_size_;
  }

  void set_db(DB* db) { head_->set_db(db); }

  // Simple labels.
  void head_add1(int64_t st, int64_t interval, int num_labels,
                 bool write_flush_mark = true) {
    // auto app = head_->TEST_appender();
    for (int i = 0; i < num_ts; i++) {
      auto app = head_->appender(write_flush_mark);
      ::tsdb::label::Labels lset;
      for (int j = 0; j < num_labels; j++)
        lset.emplace_back(
            "label" + std::to_string(j),
            "label" + std::to_string(j) + "_" + std::to_string(i));
      // lset.emplace_back("label_all", "label_all");

      auto r = app->add(lset, 0, 0);
      if (r.first != i + 1)
        std::cout << "TSDBTest::head_add1 wrong id exp:" << i + 1
                  << " got:" << r.first << std::endl;

      for (int k = 1; k < tuple_size; k++)
        app->add_fast(i + 1, st + k * interval * 1000,
                      st + k * interval * 1000);
      app->commit();
    }
    // app->TEST_commit();
  }

  // Node exporter labels.
  void head_add2(int64_t st, int64_t interval, bool write_flush_mark = true) {
    char instance[64];
    int current_instance = 0;
    std::ifstream file("../test/timeseries.json");
    std::string line;

    for (int i = 0; i < num_ts; i++) {
      auto app = head_->appender(write_flush_mark);

      if (!getline(file, line)) {
        file = std::ifstream("../test/timeseries.json");
        ++current_instance;
        getline(file, line);
      }
      rapidjson::Document d;
      d.Parse(line.c_str());
      if (d.HasParseError()) std::cout << "Cannot parse: " << line << std::endl;
      tsdb::label::Labels lset;
      for (auto& m : d.GetObject()) {
        if (memcmp(m.name.GetString(), "instance", 8) == 0) {
          sprintf(instance, "pc9%06d:9100", current_instance);
          lset.emplace_back(m.name.GetString(), instance);
        }
        lset.emplace_back(m.name.GetString(), m.value.GetString());
      }
      std::sort(lset.begin(), lset.end());

      auto r = app->add(lset, 0, 0);
      if (r.first != i + 1)
        std::cout << "TSDBTest::head_add2 wrong id exp:" << i + 1
                  << " got:" << r.first << std::endl;

      for (int k = 1; k < tuple_size; k++)
        app->add_fast(i + 1, st + k * interval * 1000,
                      st + k * interval * 1000);
      app->commit();
    }
  }

  int load_devops(int num_lines, const std::string& name, int64_t st,
                  int64_t interval, bool write_flush_mark, int ts_counter) {
    std::ifstream file(name);
    std::string line;
    int cur_line = 0;

    std::vector<int64_t> tfluc;
    for (int j = 0; j < tuple_size; j++) tfluc.push_back(rand() % 200);
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
          auto app = head_->appender(write_flush_mark);
          tsdb::label::Labels lset;
          for (size_t j = 0; j < names.size(); j++)
            lset.emplace_back(names[j], values[j]);
          lset.emplace_back("__name__", devops_names[round] + devops[round][i]);
          std::sort(lset.begin(), lset.end());

          auto r = app->add(lset, tfluc[0], tfluc[0]);
          if (r.first != ts_counter) {
            std::cout << tsdb::label::lbs_string(lset) << std::endl;
            std::cout << "TSDBTest::load_devops wrong id exp:" << ts_counter
                      << " got:" << r.first << std::endl;
          }

          for (int k = 1; k < tuple_size; k++)
            app->add_fast(ts_counter, st + k * interval * 1000 + tfluc[k],
                          tfluc[k]);
          app->commit();

          ts_counter++;
        }
        cur_line++;
      }
      for (int i = 0; i < 100000 - cur_line; i++) getline(file, line);
      cur_line = 0;
    }
    return ts_counter;
  }

  // Devops labels.
  void head_add3(int64_t st, int64_t interval, bool write_flush_mark = true) {
    int num_lines = num_ts / 100;
    int tscounter;
    if (num_lines > 100000) {
      tscounter = load_devops(100000, "../test/devops100000.txt", st, interval,
                              write_flush_mark, 1);
      tscounter = load_devops(num_lines - 100000, "../test/devops100000-2.txt",
                              st, interval, write_flush_mark, tscounter);
    } else
      tscounter = load_devops(num_lines, "../test/devops100000.txt", st,
                              interval, write_flush_mark, 1);
    std::cout << "head_add3: " << tscounter << std::endl;
  }

  void head_add4(int64_t st, int64_t interval, bool write_flush_mark = true) {
    std::ifstream file("../test/devops100000.txt");
    std::string line;
    int num_lines = num_ts / 100;
    int cur_line = 0;
    int ts_counter = 1;

    std::vector<int64_t> tfluc;
    for (int j = 0; j < tuple_size; j++) tfluc.push_back(rand() % 200);
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
          auto app = head_->appender(write_flush_mark);
          tsdb::label::Labels lset;
          for (size_t j = 0; j < names.size(); j++)
            lset.emplace_back(names[j], values[j]);
          lset.emplace_back("__name__", devops_names[round] + devops[round][i]);
          std::sort(lset.begin(), lset.end());

          auto r = app->add(lset, tfluc[0], tfluc[0]);
          if (r.first != ts_counter) {
            std::cout << tsdb::label::lbs_string(lset) << std::endl;
            std::cout << "TSDBTest::head_add4 wrong id exp:" << ts_counter
                      << " got:" << r.first << std::endl;
          }

          for (int k = 1; k < tuple_size; k++)
            app->add_fast(ts_counter, st + k * interval * 1000 + tfluc[k],
                          tfluc[k]);
          app->commit();

          ts_counter++;
        }
        cur_line++;
      }
      for (int i = 0; i < 100000 - cur_line; i++) getline(file, line);
      cur_line = 0;
    }
  }

  // Mixed labels
  void head_add5(int64_t st, int64_t interval, bool write_flush_mark = true) {
    std::ifstream file("../test/devops100000.txt");
    std::string line;
    int num_lines = num_ts / 100 / 2;
    int cur_line = 0;
    int ts_counter = 1;

    std::vector<int64_t> tfluc;
    for (int j = 0; j < tuple_size; j++) tfluc.push_back(rand() % 200);
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
          auto app = head_->appender(write_flush_mark);
          tsdb::label::Labels lset;
          for (size_t j = 0; j < names.size(); j++)
            lset.emplace_back(names[j], values[j]);
          lset.emplace_back("__name__", devops_names[round] + devops[round][i]);
          std::sort(lset.begin(), lset.end());

          auto r = app->add(lset, tfluc[0], tfluc[0]);
          if (r.first != ts_counter) {
            std::cout << tsdb::label::lbs_string(lset) << std::endl;
            std::cout << "TSDBTest::head_add5 wrong id exp:" << ts_counter
                      << " got:" << r.first << std::endl;
          }

          for (int k = 1; k < tuple_size; k++)
            app->add_fast(ts_counter, st + k * interval * 1000 + tfluc[k],
                          tfluc[k]);
          app->commit();

          ts_counter++;
        }
        cur_line++;
      }
      for (int i = 0; i < 100000 - cur_line; i++) getline(file, line);
      cur_line = 0;
    }

    for (int i = 0; i < num_ts / 2; i++) {
      auto app = head_->appender(write_flush_mark);
      ::tsdb::label::Labels lset;
      for (int j = 0; j < 20; j++)
        lset.emplace_back(
            "label" + std::to_string(j),
            "label" + std::to_string(j) + "_" + std::to_string(i));

      auto r = app->add(lset, tfluc[0], tfluc[0]);
      if (r.first != ts_counter)
        std::cout << "TSDBTest::head_add5 wrong id exp:" << i + 1
                  << " got:" << r.first << std::endl;

      for (int k = 1; k < tuple_size; k++)
        app->add_fast(ts_counter, st + k * interval * 1000 + tfluc[k],
                      tfluc[k]);
      app->commit();
      ts_counter++;
    }
  }

  int load_group_devops(int num_lines, const std::string& name, int64_t st,
                        int64_t interval, bool write_flush_mark,
                        int ts_counter) {
    std::ifstream file(name);
    std::string line;
    int cur_line = 0;

    std::vector<std::vector<::tsdb::label::Labels>> lsets(num_lines);

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

          lsets[cur_line].push_back(std::move(lset));
        }
        cur_line++;
      }
      for (int i = 0; i < 100000 - cur_line; i++) getline(file, line);
      cur_line = 0;
    }

    uint64_t gid;
    std::vector<int> slots;
    std::vector<int64_t> tfluc;
    for (int j = 0; j < tuple_size; j++) tfluc.push_back(rand() % 200);
    std::vector<::tsdb::label::Labels> glsets;
    for (int i = 0; i < lsets.size(); i++)
      glsets.push_back({{"hostname", std::string("host_") +
                                         std::to_string(ts_counter - 1 + i)}});
    auto app = head_->appender(write_flush_mark);
    for (int i = 0; i < lsets.size(); i++) {
      slots.clear();
      Status s = app->add(glsets[i], lsets[i], tfluc[0],
                          std::vector<double>(lsets[i].size(), tfluc[0]), &gid,
                          &slots);
      if (!s.ok() || gid != (ts_counter | 0x8000000000000000)) {
        std::cout << "TSDBTest::head_add_group1 wrong id exp:" << ts_counter
                  << " got:" << gid << std::endl;
      }

      for (int k = 1; k < tuple_size; k++)
        app->add(ts_counter | 0x8000000000000000,
                 st + k * interval * 1000 + tfluc[k],
                 std::vector<double>(lsets[i].size(), tfluc[k]));
      app->commit();
      ts_counter++;
    }
    return ts_counter;
  }

  void head_add_group1(int64_t st, int64_t interval,
                       bool write_flush_mark = true) {
    int num_lines = num_ts / 100;
    int tscounter;
    if (num_lines > 100000) {
      tscounter = load_group_devops(100000, "../test/devops100000.txt", st,
                                    interval, write_flush_mark, 1);
      tscounter =
          load_group_devops(num_lines - 100000, "../test/devops100000-2.txt",
                            st, interval, write_flush_mark, tscounter);
    } else
      tscounter = load_group_devops(num_lines, "../test/devops100000.txt", st,
                                    interval, write_flush_mark, 1);
    std::cout << "head_add_group1: " << tscounter << std::endl;
  }

  void head_add_group2(int64_t st, int64_t interval,
                       bool write_flush_mark = true) {
    std::ifstream file("../test/devops100000.txt");
    std::string line;
    int num_lines = num_ts / 100;
    int cur_line = 0;
    int ts_counter = 1;

    std::vector<std::vector<::tsdb::label::Labels>> lsets(num_lines);

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

          lsets[cur_line].push_back(std::move(lset));
          ts_counter++;
        }
        cur_line++;
      }
      for (int i = 0; i < 100000 - cur_line; i++) getline(file, line);
      cur_line = 0;
    }

    ts_counter = 1;
    uint64_t gid;
    std::vector<int> slots;
    std::vector<int64_t> tfluc;
    for (int j = 0; j < tuple_size; j++) tfluc.push_back(rand() % 200);
    std::vector<::tsdb::label::Labels> glsets;
    for (int i = 0; i < lsets.size(); i++)
      glsets.push_back(
          {{"hostname", std::string("host_") + std::to_string(i)}});
    auto app = head_->appender(write_flush_mark);
    for (int i = 0; i < lsets.size(); i++) {
      slots.clear();
      Status s = app->add(glsets[i], lsets[i], tfluc[0],
                          std::vector<double>(lsets[i].size(), tfluc[0]), &gid,
                          &slots);
      if (!s.ok() || gid != (ts_counter | 0x8000000000000000)) {
        std::cout << "TSDBTest::head_add_group2 wrong id exp:" << ts_counter
                  << " got:" << gid << std::endl;
      }

      for (int k = 1; k < tuple_size; k++)
        app->add(ts_counter | 0x8000000000000000,
                 st + k * interval * 1000 + tfluc[k],
                 std::vector<double>(lsets[i].size(), tfluc[k]));
      app->commit();
      ts_counter++;
    }
  }

  // Mixed labels.
  void head_add_group3(int64_t st, int64_t interval,
                       bool write_flush_mark = true) {
    std::ifstream file("../test/devops100000.txt");
    std::string line;
    int num_lines = num_ts / 100 / 2;
    int cur_line = 0;
    int ts_counter = 1;

    std::vector<std::vector<::tsdb::label::Labels>> lsets(num_lines);

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

          lsets[cur_line].push_back(std::move(lset));
        }
        cur_line++;
      }
      for (int i = 0; i < 100000 - cur_line; i++) getline(file, line);
      cur_line = 0;
    }

    uint64_t gid;
    std::vector<int> slots;
    std::vector<int64_t> tfluc;
    for (int j = 0; j < tuple_size; j++) tfluc.push_back(rand() % 200);
    std::vector<::tsdb::label::Labels> glsets;
    for (int i = 0; i < lsets.size(); i++)
      glsets.push_back(
          {{"hostname", std::string("host_") + std::to_string(i)}});
    auto app = head_->appender(write_flush_mark);
    for (int i = 0; i < lsets.size(); i++) {
      slots.clear();
      Status s = app->add(glsets[i], lsets[i], tfluc[0],
                          std::vector<double>(lsets[i].size(), tfluc[0]), &gid,
                          &slots);
      if (!s.ok() || gid != (ts_counter | 0x8000000000000000)) {
        std::cout << "TSDBTest::head_add_group3 wrong id exp:" << ts_counter
                  << " got:" << gid << std::endl;
      }

      for (int k = 1; k < tuple_size; k++)
        app->add(ts_counter | 0x8000000000000000,
                 st + k * interval * 1000 + tfluc[k],
                 std::vector<double>(lsets[i].size(), tfluc[k]));
      app->commit();
      ts_counter++;
    }

    for (int i = 0; i < num_ts / 2; i++) {
      auto app = head_->appender(write_flush_mark);
      ::tsdb::label::Labels lset;
      for (int j = 0; j < 20; j++)
        lset.emplace_back(
            "label" + std::to_string(j),
            "label" + std::to_string(j) + "_" + std::to_string(i));

      auto r = app->add(lset, tfluc[0], tfluc[0]);
      if (r.first != ts_counter)
        std::cout << "TSDBTest::head_add_group3 wrong id exp:" << i + 1
                  << " got:" << r.first << std::endl;

      for (int k = 1; k < tuple_size; k++)
        app->add_fast(ts_counter, st + k * interval * 1000 + tfluc[k],
                      tfluc[k]);
      app->commit();
      ts_counter++;
    }
  }

  void head_add_fast1(int64_t st, int64_t interval,
                      bool write_flush_mark = true) {
    // auto app = head_->TEST_appender();
    Timer t;
    // int count = 0;
    int64_t d, last_t;
    t.start();
    auto app = head_->appender(write_flush_mark);
    std::vector<int64_t> tfluc;
    for (int j = 0; j < tuple_size; j++) tfluc.push_back(rand() % 200);
    for (int i = 0; i < num_ts; i++) {
      for (int k = 0; k < tuple_size; k++)
        app->add_fast(i + 1, st + k * interval * 1000 + tfluc[k], tfluc[k]);
      app->commit();
      // count += tuple_size;
      // if (count > 500000) {
      //   d = t.since_start_nano();
      //   LOG_INFO << "Time(ms):" <<
      //   std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count()
      //   << " throughput:" << (double)(count)  / (double)(d - last_t) *
      //   1000000000; last_t = d; count = 0;
      // }
    }
    // app->TEST_commit();
  }

  void head_add_fast_partial(int64_t st, int step, int64_t interval,
                             bool write_flush_mark = true) {
    auto app = head_->appender(write_flush_mark);
    std::vector<int64_t> tfluc;
    for (int j = 0; j < tuple_size; j++) tfluc.push_back(rand() % 200);
    for (int i = 0; i < num_ts; i += step) {
      for (int k = 0; k < tuple_size; k++)
        app->add_fast(i + 1, st + k * interval * 1000 + tfluc[k], tfluc[k]);
      app->commit();
    }
  }

  void head_add_group_fast1(int64_t st, int64_t interval,
                            bool write_flush_mark = true) {
    // auto app = head_->TEST_appender();
    auto app = head_->appender(write_flush_mark);
    std::vector<std::vector<double>> tfluc;
    for (int j = 0; j < tuple_size; j++) tfluc.emplace_back(101, rand() % 200);
    for (int i = 0; i < num_ts / 100; i++) {
      for (int k = 0; k < tuple_size; k++)
        app->add((i + 1) | 0x8000000000000000,
                 st + k * interval * 1000 + (int64_t)(tfluc[k][0]), tfluc[k]);
      app->commit();
    }
    // app->TEST_commit();
  }

  void head_add_group_fast2(int64_t st, int64_t interval,
                            bool write_flush_mark = true) {
    auto app = head_->appender(write_flush_mark);
    std::vector<std::vector<double>> tfluc;
    for (int j = 0; j < tuple_size; j++) tfluc.emplace_back(101, rand() % 200);
    for (int i = 0; i < num_ts / 100 / 2; i++) {
      for (int k = 0; k < tuple_size; k++)
        app->add((i + 1) | 0x8000000000000000,
                 st + k * interval * 1000 + (int64_t)(tfluc[k][0]), tfluc[k]);
      app->commit();
    }
    int ts_counter = num_ts / 100 / 2 + 1;
    for (int i = 0; i < num_ts / 2; i++) {
      for (int k = 0; k < tuple_size; k++)
        app->add_fast(ts_counter, st + k * interval * 1000 + tfluc[k][0],
                      tfluc[k][0]);
      app->commit();
      ts_counter++;
    }
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

  void queryDevOps1(DB* db, int64_t endtime, leveldb::CloudCache* ccache) {
    std::vector<tsdb::label::EqualMatcher> matchers1;
    for (int i = 0; i < 50; i++)
      matchers1.emplace_back("hostname", "host_" + std::to_string(i));
    std::vector<tsdb::label::EqualMatcher> matchers2(
        {tsdb::label::EqualMatcher("__name__", "cpu_usage_user"),
         tsdb::label::EqualMatcher("__name__", "diskio_reads"),
         tsdb::label::EqualMatcher("__name__", "kernel_boot_time"),
         tsdb::label::EqualMatcher("__name__", "mem_total"),
         tsdb::label::EqualMatcher("__name__", "net_bytes_sent")});

    int iteration = 1000;

    // Simple aggregrate (MAX) on one metric for 1 host, every 5 mins for 1
    // hours.
    if (query_types["1-1-1"]) {
      int64_t total_samples = 0;
      int64_t duration = 0;
      for (int round = 0; round < iteration; round++) {
        DBQuerier* q = db->Querier(head_.get(), endtime - 3600000, endtime);
        Timer t;
        t.start();

        std::vector<tsdb::label::MatcherInterface*> matchers(
            {&matchers1[round % 50], &matchers2[0]});
        std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
            q->select(matchers);
        while (ss->next()) {
          std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

          if (series->type() == ::tsdb::querier::kTypeGroup) {
            while (series->next()) {
              std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                  series->chain_iterator();
              while (it->next()) {
                ++total_samples;
              }
            }
          } else if (series->type() == ::tsdb::querier::kTypeSeries) {
            std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                series->chain_iterator();
            while (it->next()) {
              ++total_samples;
            }
          }
        }
        duration += t.since_start_nano();
        delete q;
      }
      std::cout << "[1-1-1] duration(total):" << duration / 1000 << "us "
                << "duration(avg):" << duration / 1000 / iteration << "us "
                << "samples:" << total_samples / iteration << std::endl;
      if (ccache) {
        ccache->print_summary(true);
        ccache->clean();
      }
    }

    // Simple aggregrate (MAX) on one metric for 1 host, every 5 mins for 12
    // hours.
    if (query_types["1-1-12"]) {
      int64_t total_samples = 0;
      int64_t duration = 0;
      for (int round = 0; round < (iteration / 2); round++) {
        DBQuerier* q = db->Querier(head_.get(), endtime - 43200000, endtime);
        Timer t;
        t.start();

        std::vector<tsdb::label::MatcherInterface*> matchers(
            {&matchers1[round % 50], &matchers2[0]});
        std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
            q->select(matchers);
        while (ss->next()) {
          std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

          if (series->type() == ::tsdb::querier::kTypeGroup) {
            while (series->next()) {
              std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                  series->chain_iterator();
              while (it->next()) {
                ++total_samples;
              }
            }
          } else if (series->type() == ::tsdb::querier::kTypeSeries) {
            std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                series->chain_iterator();
            while (it->next()) {
              ++total_samples;
            }
          }
        }
        duration += t.since_start_nano();
        delete q;
      }
      std::cout << "[1-1-12] duration(total):" << duration / 1000 << "us "
                << "duration(avg):" << duration / 1000 / (iteration / 2)
                << "us "
                << "samples:" << total_samples / (iteration / 2) << std::endl;
      if (ccache) {
        ccache->print_summary(true);
        ccache->clean();
      }
    }

    // Simple aggregrate (MAX) on one metric for 8 hosts, every 5 mins for 1
    // hour.
    if (query_types["1-8-1"]) {
      int64_t total_samples = 0;
      int64_t duration = 0;
      int count = 0;
      for (int round = 0; round < iteration; round++) {
        DBQuerier* q = db->Querier(head_.get(), endtime - 3600000, endtime);
        Timer t;
        t.start();

        for (int host = 0; host < 8; host++) {
          std::vector<tsdb::label::MatcherInterface*> matchers(
              {&matchers1[count % 50], &matchers2[0]});
          std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
              q->select(matchers);
          while (ss->next()) {
            std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

            if (series->type() == ::tsdb::querier::kTypeGroup) {
              while (series->next()) {
                std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                    series->chain_iterator();
                while (it->next()) {
                  ++total_samples;
                }
              }
            } else if (series->type() == ::tsdb::querier::kTypeSeries) {
              std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                  series->chain_iterator();
              while (it->next()) {
                ++total_samples;
              }
            }
          }
          count++;
        }
        duration += t.since_start_nano();
        delete q;
      }
      std::cout << "[1-8-1] duration(total):" << duration / 1000 << "us "
                << "duration(avg):" << duration / 1000 / iteration << "us "
                << "samples:" << total_samples / iteration << std::endl;
      if (ccache) {
        ccache->print_summary(true);
        ccache->clean();
      }
    }

    // Simple aggregrate (MAX) on 5 metrics for 1 host, every 5 mins for 1 hour.
    if (query_types["5-1-1"]) {
      int64_t total_samples = 0;
      int64_t duration = 0;
      for (int round = 0; round < iteration; round++) {
        DBQuerier* q = db->Querier(head_.get(), endtime - 3600000, endtime);
        Timer t;
        t.start();

        for (int j = 0; j < 5; j++) {
          std::vector<tsdb::label::MatcherInterface*> matchers(
              {&matchers1[round % 50], &matchers2[j]});
          std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
              q->select(matchers);
          while (ss->next()) {
            std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

            if (series->type() == ::tsdb::querier::kTypeGroup) {
              while (series->next()) {
                std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                    series->chain_iterator();
                while (it->next()) {
                  ++total_samples;
                }
              }
            } else if (series->type() == ::tsdb::querier::kTypeSeries) {
              std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                  series->chain_iterator();
              while (it->next()) {
                ++total_samples;
              }
            }
          }
        }
        duration += t.since_start_nano();
        delete q;
      }
      std::cout << "[5-1-1] duration(total):" << duration / 1000 << "us "
                << "duration(avg):" << duration / 1000 / iteration << "us "
                << "samples:" << total_samples / iteration << std::endl;
      if (ccache) {
        ccache->print_summary(true);
        ccache->clean();
      }
    }

    // Simple aggregrate (MAX) on 5 metrics for 1 host, every 5 mins for 12
    // hour.
    if (query_types["5-1-12"]) {
      int64_t total_samples = 0;
      int64_t duration = 0;
      for (int round = 0; round < (iteration / 2); round++) {
        DBQuerier* q = db->Querier(head_.get(), endtime - 43200000, endtime);
        Timer t;
        t.start();

        for (int j = 0; j < 5; j++) {
          std::vector<tsdb::label::MatcherInterface*> matchers(
              {&matchers1[round % 50], &matchers2[j]});
          std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
              q->select(matchers);
          while (ss->next()) {
            std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

            if (series->type() == ::tsdb::querier::kTypeGroup) {
              while (series->next()) {
                std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                    series->chain_iterator();
                while (it->next()) {
                  ++total_samples;
                }
              }
            } else if (series->type() == ::tsdb::querier::kTypeSeries) {
              std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                  series->chain_iterator();
              while (it->next()) {
                ++total_samples;
              }
            }
          }
        }
        duration += t.since_start_nano();
        delete q;
      }
      std::cout << "[5-1-12] duration(total):" << duration / 1000 << "us "
                << "duration(avg):" << duration / 1000 / (iteration / 2)
                << "us "
                << "samples:" << total_samples / (iteration / 2) << std::endl;
      if (ccache) {
        ccache->print_summary(true);
        ccache->clean();
      }
    }

    // Simple aggregrate (MAX) on 5 metrics for 8 hosts, every 5 mins for 1
    // hour.
    if (query_types["5-8-1"]) {
      int64_t total_samples = 0;
      int64_t duration = 0;
      int count = 0;
      for (int round = 0; round < iteration; round++) {
        DBQuerier* q = db->Querier(head_.get(), endtime - 3600000, endtime);
        Timer t;
        t.start();

        for (int host = 0; host < 8; host++) {
          for (int j = 0; j < 5; j++) {
            std::vector<tsdb::label::MatcherInterface*> matchers(
                {&matchers1[count % 50], &matchers2[j]});
            std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                q->select(matchers);
            while (ss->next()) {
              std::unique_ptr<::tsdb::querier::SeriesInterface> series =
                  ss->at();

              if (series->type() == ::tsdb::querier::kTypeGroup) {
                while (series->next()) {
                  std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                      series->chain_iterator();
                  while (it->next()) {
                    ++total_samples;
                  }
                }
              } else if (series->type() == ::tsdb::querier::kTypeSeries) {
                std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                    series->chain_iterator();
                while (it->next()) {
                  ++total_samples;
                }
              }
            }
          }
          ++count;
        }
        duration += t.since_start_nano();
        delete q;
      }
      std::cout << "[5-8-1] duration(total):" << duration / 1000 << "us "
                << "duration(avg):" << duration / 1000 / iteration << "us "
                << "samples:" << total_samples / iteration << std::endl;
      if (ccache) {
        ccache->print_summary(true);
        ccache->clean();
      }
    }

    // Last reading of a metric of a host.
    if (query_types["lastpoint"]) {
      int64_t total_samples = 0;
      int64_t duration = 0;
      for (int round = 0; round < iteration; round++) {
        DBQuerier* q = db->Querier(head_.get(), endtime, endtime + 1);
        Timer t;
        t.start();

        std::vector<tsdb::label::MatcherInterface*> matchers(
            {&matchers1[0], &matchers2[0]});
        std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
            q->select(matchers);
        while (ss->next()) {
          std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

          if (series->type() == ::tsdb::querier::kTypeGroup) {
            while (series->next()) {
              std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                  series->chain_iterator();
              while (it->next()) {
                ++total_samples;
              }
            }
          } else if (series->type() == ::tsdb::querier::kTypeSeries) {
            std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                series->chain_iterator();
            while (it->next()) {
              ++total_samples;
            }
          }
        }
        duration += t.since_start_nano();
        delete q;
      }
      std::cout << "[lastpoint] duration(total):" << duration / 1000 << "us "
                << "duration(avg):" << duration / 1000 / iteration << "us "
                << "samples:" << total_samples / iteration << std::endl;
      if (ccache) {
        ccache->print_summary(true);
        ccache->clean();
      }
    }
  }

  void queryDevOps2(DB* db, int64_t endtime, leveldb::CloudCache* ccache) {
    std::vector<tsdb::label::EqualMatcher> matchers1;
    for (int i = 0; i < 50; i++)
      matchers1.emplace_back("hostname", "host_" + std::to_string(i));
    std::vector<tsdb::label::EqualMatcher> matchers2(
        {tsdb::label::EqualMatcher("__name__", "cpu_usage_user"),
         tsdb::label::EqualMatcher("__name__", "diskio_reads"),
         tsdb::label::EqualMatcher("__name__", "kernel_boot_time"),
         tsdb::label::EqualMatcher("__name__", "mem_total"),
         tsdb::label::EqualMatcher("__name__", "net_bytes_sent")});

    int iteration = 30;

    // Simple aggregrate (MAX) on one metric for 1 host, every 5 mins for 1
    // hours.
    if (query_types["1-1-1"]) {
      int tmp_put = put_request_count;
      int tmp_get = get_request_count;
      int64_t total_samples = 0;
      int64_t duration = 0;
      for (int round = 0; round < iteration; round++) {
        DBQuerier* q = db->Querier(head_.get(), endtime - 3600000, endtime);
        Timer t;
        t.start();

        std::vector<tsdb::label::MatcherInterface*> matchers(
            {&matchers1[0], &matchers2[0]});
        std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
            q->select(matchers);
        while (ss->next()) {
          std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

          if (series->type() == ::tsdb::querier::kTypeGroup) {
            while (series->next()) {
              std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                  series->chain_iterator();
              while (it->next()) {
                ++total_samples;
              }
            }
          } else if (series->type() == ::tsdb::querier::kTypeSeries) {
            std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                series->chain_iterator();
            while (it->next()) {
              ++total_samples;
            }
          }
        }
        duration += t.since_start_nano();
        delete q;
      }
      std::cout << "[1-1-1] duration(total):" << duration / 1000 << "us "
                << "duration(avg):" << duration / 1000 / iteration << "us "
                << "samples:" << total_samples / iteration << ":"
                << total_samples << std::endl;
      std::cout << "get_count:" << get_request_count - tmp_get
                << " put_count:" << put_request_count - tmp_put << std::endl;
      if (ccache) {
        ccache->print_summary(true);
        ccache->clean();
      }
    }

    // Simple aggregrate (MAX) on one metric for 1 host, every 5 mins for 12
    // hours.
    if (query_types["1-1-12"]) {
      int tmp_put = put_request_count;
      int tmp_get = get_request_count;
      int64_t total_samples = 0;
      int64_t duration = 0;
      for (int round = 0; round < iteration; round++) {
        DBQuerier* q = db->Querier(head_.get(), endtime - 43200000, endtime);
        Timer t;
        t.start();

        std::vector<tsdb::label::MatcherInterface*> matchers(
            {&matchers1[0], &matchers2[0]});
        std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
            q->select(matchers);
        while (ss->next()) {
          std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

          if (series->type() == ::tsdb::querier::kTypeGroup) {
            while (series->next()) {
              std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                  series->chain_iterator();
              while (it->next()) {
                ++total_samples;
              }
            }
          } else if (series->type() == ::tsdb::querier::kTypeSeries) {
            std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                series->chain_iterator();
            while (it->next()) {
              ++total_samples;
            }
          }
        }
        duration += t.since_start_nano();
        delete q;
      }
      std::cout << "[1-1-12] duration(total):" << duration / 1000 << "us "
                << "duration(avg):" << duration / 1000 / iteration << "us "
                << "samples:" << total_samples / iteration << std::endl;
      std::cout << "get_count:" << get_request_count - tmp_get
                << " put_count:" << put_request_count - tmp_put << std::endl;
      if (ccache) {
        ccache->print_summary(true);
        ccache->clean();
      }
    }

    if (query_types["1-1-24"] && endtime - 86400000 > -120000) {
      int tmp_put = put_request_count;
      int tmp_get = get_request_count;
      int64_t total_samples = 0;
      int64_t duration = 0;
      for (int round = 0; round < iteration; round++) {
        DBQuerier* q = db->Querier(head_.get(), endtime - 86400000, endtime);
        Timer t;
        t.start();

        std::vector<tsdb::label::MatcherInterface*> matchers(
            {&matchers1[0], &matchers2[0]});
        std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
            q->select(matchers);
        while (ss->next()) {
          std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

          if (series->type() == ::tsdb::querier::kTypeGroup) {
            while (series->next()) {
              std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                  series->chain_iterator();
              while (it->next()) {
                ++total_samples;
              }
            }
          } else if (series->type() == ::tsdb::querier::kTypeSeries) {
            std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                series->chain_iterator();
            while (it->next()) {
              ++total_samples;
            }
          }
        }
        duration += t.since_start_nano();
        delete q;
      }
      std::cout << "[1-1-24] duration(total):" << duration / 1000 << "us "
                << "duration(avg):" << duration / 1000 / iteration << "us "
                << "samples:" << total_samples / iteration << std::endl;
      std::cout << "get_count:" << get_request_count - tmp_get
                << " put_count:" << put_request_count - tmp_put << std::endl;
      if (ccache) {
        ccache->print_summary(true);
        ccache->clean();
      }
    }

    if (query_types["1-1-all"]) {
      int tmp_put = put_request_count;
      int tmp_get = get_request_count;
      int64_t total_samples = 0;
      int64_t duration = 0;
      for (int round = 0; round < iteration; round++) {
        DBQuerier* q = db->Querier(head_.get(), 0, endtime);
        Timer t;
        t.start();

        std::vector<tsdb::label::MatcherInterface*> matchers(
            {&matchers1[0], &matchers2[0]});
        std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
            q->select(matchers);
        while (ss->next()) {
          std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

          if (series->type() == ::tsdb::querier::kTypeGroup) {
            while (series->next()) {
              std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                  series->chain_iterator();
              while (it->next()) {
                ++total_samples;
              }
            }
          } else if (series->type() == ::tsdb::querier::kTypeSeries) {
            std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                series->chain_iterator();
            while (it->next()) {
              ++total_samples;
            }
          }
        }
        duration += t.since_start_nano();
        delete q;
      }
      std::cout << "[1-1-all] duration(total):" << duration / 1000 << "us "
                << "duration(avg):" << duration / 1000 / iteration << "us "
                << "samples:" << total_samples / iteration << std::endl;
      std::cout << "get_count:" << get_request_count - tmp_get
                << " put_count:" << put_request_count - tmp_put << std::endl;
      if (ccache) {
        ccache->print_summary(true);
        ccache->clean();
      }
    }

    // Simple aggregrate (MAX) on one metric for 8 hosts, every 5 mins for 1
    // hour.
    if (query_types["1-8-1"]) {
      int tmp_put = put_request_count;
      int tmp_get = get_request_count;
      int64_t total_samples = 0;
      int64_t duration = 0;
      for (int round = 0; round < iteration; round++) {
        DBQuerier* q = db->Querier(head_.get(), endtime - 3600000, endtime);
        Timer t;
        t.start();

        for (int host = 0; host < 8; host++) {
          std::vector<tsdb::label::MatcherInterface*> matchers(
              {&matchers1[host], &matchers2[0]});
          std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
              q->select(matchers);
          while (ss->next()) {
            std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

            if (series->type() == ::tsdb::querier::kTypeGroup) {
              while (series->next()) {
                std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                    series->chain_iterator();
                while (it->next()) {
                  ++total_samples;
                }
              }
            } else if (series->type() == ::tsdb::querier::kTypeSeries) {
              std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                  series->chain_iterator();
              while (it->next()) {
                ++total_samples;
              }
            }
          }
        }
        duration += t.since_start_nano();
        delete q;
      }
      std::cout << "[1-8-1] duration(total):" << duration / 1000 << "us "
                << "duration(avg):" << duration / 1000 / iteration << "us "
                << "samples:" << total_samples / iteration << std::endl;
      std::cout << "get_count:" << get_request_count - tmp_get
                << " put_count:" << put_request_count - tmp_put << std::endl;
      if (ccache) {
        ccache->print_summary(true);
        ccache->clean();
      }
    }

    // Simple aggregrate (MAX) on 5 metrics for 1 host, every 5 mins for 1 hour.
    if (query_types["5-1-1"]) {
      int tmp_put = put_request_count;
      int tmp_get = get_request_count;
      int64_t total_samples = 0;
      int64_t duration = 0;
      for (int round = 0; round < iteration; round++) {
        DBQuerier* q = db->Querier(head_.get(), endtime - 3600000, endtime);
        Timer t;
        t.start();

        for (int j = 0; j < 5; j++) {
          std::vector<tsdb::label::MatcherInterface*> matchers(
              {&matchers1[0], &matchers2[j]});
          std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
              q->select(matchers);
          while (ss->next()) {
            std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

            if (series->type() == ::tsdb::querier::kTypeGroup) {
              while (series->next()) {
                std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                    series->chain_iterator();
                while (it->next()) {
                  ++total_samples;
                }
              }
            } else if (series->type() == ::tsdb::querier::kTypeSeries) {
              std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                  series->chain_iterator();
              while (it->next()) {
                ++total_samples;
              }
            }
          }
        }
        duration += t.since_start_nano();
        delete q;
      }
      std::cout << "[5-1-1] duration(total):" << duration / 1000 << "us "
                << "duration(avg):" << duration / 1000 / iteration << "us "
                << "samples:" << total_samples / iteration << std::endl;
      std::cout << "get_count:" << get_request_count - tmp_get
                << " put_count:" << put_request_count - tmp_put << std::endl;
      if (ccache) {
        ccache->print_summary(true);
        ccache->clean();
      }
    }

    // Simple aggregrate (MAX) on 5 metrics for 1 host, every 5 mins for 12
    // hour.
    if (query_types["5-1-12"]) {
      int tmp_put = put_request_count;
      int tmp_get = get_request_count;
      int64_t total_samples = 0;
      int64_t duration = 0;
      for (int round = 0; round < iteration; round++) {
        DBQuerier* q = db->Querier(head_.get(), endtime - 43200000, endtime);
        Timer t;
        t.start();

        for (int j = 0; j < 5; j++) {
          std::vector<tsdb::label::MatcherInterface*> matchers(
              {&matchers1[0], &matchers2[j]});
          std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
              q->select(matchers);
          while (ss->next()) {
            std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

            if (series->type() == ::tsdb::querier::kTypeGroup) {
              while (series->next()) {
                std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                    series->chain_iterator();
                while (it->next()) {
                  ++total_samples;
                }
              }
            } else if (series->type() == ::tsdb::querier::kTypeSeries) {
              std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                  series->chain_iterator();
              while (it->next()) {
                ++total_samples;
              }
            }
          }
        }
        duration += t.since_start_nano();
        delete q;
      }
      std::cout << "[5-1-12] duration(total):" << duration / 1000 << "us "
                << "duration(avg):" << duration / 1000 / iteration << "us "
                << "samples:" << total_samples / iteration << std::endl;
      std::cout << "get_count:" << get_request_count - tmp_get
                << " put_count:" << put_request_count - tmp_put << std::endl;
      if (ccache) {
        ccache->print_summary(true);
        ccache->clean();
      }
    }

    if (query_types["5-1-24"] && endtime - 86400000 > -120000) {
      int tmp_put = put_request_count;
      int tmp_get = get_request_count;
      int64_t total_samples = 0;
      int64_t duration = 0;
      for (int round = 0; round < iteration; round++) {
        DBQuerier* q = db->Querier(head_.get(), endtime - 86400000, endtime);
        Timer t;
        t.start();

        for (int j = 0; j < 5; j++) {
          std::vector<tsdb::label::MatcherInterface*> matchers(
              {&matchers1[0], &matchers2[j]});
          std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
              q->select(matchers);
          while (ss->next()) {
            std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

            if (series->type() == ::tsdb::querier::kTypeGroup) {
              while (series->next()) {
                std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                    series->chain_iterator();
                while (it->next()) {
                  ++total_samples;
                }
              }
            } else if (series->type() == ::tsdb::querier::kTypeSeries) {
              std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                  series->chain_iterator();
              while (it->next()) {
                ++total_samples;
              }
            }
          }
        }
        duration += t.since_start_nano();
        delete q;
      }
      std::cout << "[5-1-24] duration(total):" << duration / 1000 << "us "
                << "duration(avg):" << duration / 1000 / iteration << "us "
                << "samples:" << total_samples / iteration << std::endl;
      std::cout << "get_count:" << get_request_count - tmp_get
                << " put_count:" << put_request_count - tmp_put << std::endl;
      if (ccache) {
        ccache->print_summary(true);
        ccache->clean();
      }
    }

    if (query_types["5-1-all"]) {
      int tmp_put = put_request_count;
      int tmp_get = get_request_count;
      int64_t total_samples = 0;
      int64_t duration = 0;
      for (int round = 0; round < iteration; round++) {
        DBQuerier* q = db->Querier(head_.get(), 0, endtime);
        Timer t;
        t.start();

        for (int j = 0; j < 5; j++) {
          std::vector<tsdb::label::MatcherInterface*> matchers(
              {&matchers1[0], &matchers2[j]});
          std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
              q->select(matchers);
          while (ss->next()) {
            std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

            if (series->type() == ::tsdb::querier::kTypeGroup) {
              while (series->next()) {
                std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                    series->chain_iterator();
                while (it->next()) {
                  ++total_samples;
                }
              }
            } else if (series->type() == ::tsdb::querier::kTypeSeries) {
              std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                  series->chain_iterator();
              while (it->next()) {
                ++total_samples;
              }
            }
          }
        }
        duration += t.since_start_nano();
        delete q;
      }
      std::cout << "[5-1-all] duration(total):" << duration / 1000 << "us "
                << "duration(avg):" << duration / 1000 / iteration << "us "
                << "samples:" << total_samples / iteration << std::endl;
      std::cout << "get_count:" << get_request_count - tmp_get
                << " put_count:" << put_request_count - tmp_put << std::endl;
      if (ccache) {
        ccache->print_summary(true);
        ccache->clean();
      }
    }

    // Simple aggregrate (MAX) on 5 metrics for 8 hosts, every 5 mins for 1
    // hour.
    if (query_types["5-8-1"]) {
      int tmp_put = put_request_count;
      int tmp_get = get_request_count;
      int64_t total_samples = 0;
      int64_t duration = 0;
      for (int round = 0; round < iteration; round++) {
        DBQuerier* q = db->Querier(head_.get(), endtime - 3600000, endtime);
        Timer t;
        t.start();

        for (int host = 0; host < 8; host++) {
          for (int j = 0; j < 5; j++) {
            std::vector<tsdb::label::MatcherInterface*> matchers(
                {&matchers1[host], &matchers2[j]});
            std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                q->select(matchers);
            while (ss->next()) {
              std::unique_ptr<::tsdb::querier::SeriesInterface> series =
                  ss->at();

              if (series->type() == ::tsdb::querier::kTypeGroup) {
                while (series->next()) {
                  std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                      series->chain_iterator();
                  while (it->next()) {
                    ++total_samples;
                  }
                }
              } else if (series->type() == ::tsdb::querier::kTypeSeries) {
                std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                    series->chain_iterator();
                while (it->next()) {
                  ++total_samples;
                }
              }
            }
          }
        }
        duration += t.since_start_nano();
        delete q;
      }
      std::cout << "[5-8-1] duration(total):" << duration / 1000 << "us "
                << "duration(avg):" << duration / 1000 / iteration << "us "
                << "samples:" << total_samples / iteration << std::endl;
      std::cout << "get_count:" << get_request_count - tmp_get
                << " put_count:" << put_request_count - tmp_put << std::endl;
      if (ccache) {
        ccache->print_summary(true);
        ccache->clean();
      }
    }

    // Last reading of a metric of a host.
    if (query_types["lastpoint"]) {
      int tmp_put = put_request_count;
      int tmp_get = get_request_count;
      int64_t total_samples = 0;
      int64_t duration = 0;
      for (int round = 0; round < iteration; round++) {
        DBQuerier* q = db->Querier(head_.get(), endtime, endtime + 1000);
        Timer t;
        t.start();

        std::vector<tsdb::label::MatcherInterface*> matchers(
            {&matchers1[0], &matchers2[0]});
        std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
            q->select(matchers);
        while (ss->next()) {
          std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

          if (series->type() == ::tsdb::querier::kTypeGroup) {
            while (series->next()) {
              std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                  series->chain_iterator();
              while (it->next()) {
                ++total_samples;
              }
            }
          } else if (series->type() == ::tsdb::querier::kTypeSeries) {
            std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                series->chain_iterator();
            while (it->next()) {
              ++total_samples;
            }
          }
        }
        duration += t.since_start_nano();
        delete q;
      }
      std::cout << "[lastpoint] duration(total):" << duration / 1000 << "us "
                << "duration(avg):" << duration / 1000 / iteration << "us "
                << "samples:" << total_samples / iteration << ":"
                << total_samples << std::endl;
      std::cout << "get_count:" << get_request_count - tmp_get
                << " put_count:" << put_request_count - tmp_put << std::endl;
      if (ccache) {
        ccache->print_summary(true);
        ccache->clean();
      }
    }
  }

  void setup(const std::string& log_dir, const std::string& idx_dir,
             DB* db = nullptr) {
    ::tsdb::head::MMapHeadWithTrie* p = head_.release();
    delete p;
    head_.reset(new ::tsdb::head::MMapHeadWithTrie(0, log_dir, idx_dir, db));
  }

  int num_ts;
  int tuple_size;
  int num_tuple;
  std::unique_ptr<::tsdb::head::MMapHeadWithTrie> head_;
};

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

void TestBigData1(int num) {
  std::string dbpath = "/tmp/tsdb_big";
  boost::filesystem::remove_all(dbpath);

  Options options;
  options.use_log = false;
  options.create_if_missing = true;
  // options.compression = kNoCompression;
  options.block_size = 16 * 1024;
  // options.write_buffer_size = 64 * 1024 * 1024;

  DB* db;
  if (!DB::Open(options, dbpath, &db).ok()) {
    std::cout << "Cannot open DB" << std::endl;
    exit(-1);
  }

  TSDBTest tsdbtest;
  tsdbtest.set_parameters(num, 32, 30);
  tsdbtest.setup(dbpath, dbpath, db);
  // tsdbtest.setup("", "/tmp/tsdb_big", db);

  int64_t last_t = 0, d;
  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;
  Timer timer;
  timer.start();
  int interval = 45;
  tsdbtest.head_add1(0, interval, 20, options.use_log);
  // tsdbtest.head_add3(0, interval, options.use_log);
  d = timer.since_start_nano();
  std::cout << "[Labels Insertion duration (ms)]:" << ((d) / 1000000)
            << " [throughput]:"
            << ((double)(tsdbtest.num_ts) * (double)(tsdbtest.tuple_size) /
                (double)(d)*1000000000)
            << std::endl;
  // std::cout << "[Labels Insertion duration (ms)]:" << ((d) / 1000000) << "
  // [throughput]:" << ((double)(tsdbtest.num_ts) / (double)(d) * 1000000000) <<
  // std::endl;
  mem_usage(vm, rss);
  std::cout << "[After Labels Insertion] VM:" << (vm / 1024)
            << "MB RSS:" << (rss / 1024) << "MB" << std::endl;

  int sep_period = 5;
  // ProfilerStart("cpu.prof");
  for (int tuple = 1; tuple < tsdbtest.num_tuple; tuple++) {
    tsdbtest.head_add_fast1(tuple * tsdbtest.tuple_size * 1000 * interval,
                            interval, options.use_log);
    if ((tuple + 1) % sep_period == 0) {
      // std::cout << "tuple: " << (tuple + 1) << std::endl;
      d = timer.since_start_nano();
      std::cout << "[#tuples] " << (tuple + 1) << " [st] "
                << (tuple * tsdbtest.tuple_size * 1000) << std::endl;
      std::cout << "[Insertion duration (ms)]:" << ((d - last_t) / 1000000)
                << " [throughput]:"
                << ((double)(tsdbtest.num_ts) * (double)(sep_period) *
                    (double)(tsdbtest.tuple_size) / (double)(d - last_t) *
                    1000000000)
                << std::endl;
      last_t = d;
    }
  }
  // ProfilerStop();
  d = timer.since_start_nano();
  std::cout << "[Total Insertion duration (ms)]:" << (d / 1000000)
            << " [#TS]:" << tsdbtest.num_ts << " [#samples]:"
            << (double)(tsdbtest.num_ts) * (double)(tsdbtest.num_tuple) *
                   (double)(tsdbtest.tuple_size)
            << " [throughput]:"
            << ((double)(tsdbtest.num_ts) * (double)(tsdbtest.num_tuple) *
                (double)(tsdbtest.tuple_size) / (double)(d)*1000000000)
            << std::endl;
  mem_usage(vm, rss);
  std::cout << "VM:" << (vm / 1024) << "MB RSS:" << (rss / 1024) << "MB"
            << std::endl;

  // sleep(100);
  // db->PrintLevel();

  ::tsdb::head::MMapHeadWithTrie* p = tsdbtest.head_.release();
  delete p;
  mem_usage(vm, rss);
  std::cout << "Release head VM:" << (vm / 1024) << "MB RSS:" << (rss / 1024)
            << "MB" << std::endl;
  delete db;
  mem_usage(vm, rss);
  std::cout << "Release db VM:" << (vm / 1024) << "MB RSS:" << (rss / 1024)
            << "MB" << std::endl;
}

void TestBigDataQuery1(int num, int interval = 30, int numtuple = 90) {
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
  if (!s.ok()) {
    std::cout << "Cannot create AwsEnv" << std::endl;
    exit(-1);
  }
  cloud_env.reset(cenv);

  Options options;
  options.create_if_missing = true;
  options.use_log = false;
  options.env = cloud_env.get();
  options.aws_use_cloud_cache = true;
  options.ccache = std::shared_ptr<CloudCache>(
      new CloudCache(std::shared_ptr<Cache>(NewLRUCache(64 * 1024 * 1024)),
                     64 * 1024 * 1024 /*pcache size*/,
                     128 * 1024 /*block size*/, cloud_env->GetBaseEnv()));
  options.max_imm_num = 3;
  options.write_buffer_size = 64 * 1024 * 1024;
  // options.block_cache = NewLRUCache(64 * 1024 * 1024);
  // options.sample_cache = NewLRUCache(64 * 1024 * 1024);

  DBCloud* ldb;
  if (!DBCloud::Open(options, dbpath, &ldb).ok()) {
    std::cout << "Cannot open DB" << std::endl;
    exit(-1);
  }

  TSDBTest tsdbtest;
  tsdbtest.set_parameters(num, 32, numtuple);
  tsdbtest.setup(dbpath, dbpath, ldb);
  // tsdbtest.setup("", "/tmp/tsdb_big", db);

  int64_t last_t = 0, d;
  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;
  Timer timer;
  timer.start();
  // int interval = 30;
  tsdbtest.head_add3(0, interval, options.use_log);
  d = timer.since_start_nano();
  std::cout << "[Labels Insertion duration (ms)]:" << ((d) / 1000000)
            << " [throughput]:"
            << ((double)(tsdbtest.num_ts) * (double)(tsdbtest.tuple_size) /
                (double)(d)*1000000000)
            << std::endl;
  // std::cout << "[Labels Insertion duration (ms)]:" << ((d) / 1000000) << "
  // [throughput]:" << ((double)(tsdbtest.num_ts) / (double)(d) * 1000000000) <<
  // std::endl;
  mem_usage(vm, rss);
  std::cout << "[After Labels Insertion] VM:" << (vm / 1024)
            << "MB RSS:" << (rss / 1024) << "MB" << std::endl;

  int sep_period = 5;
  for (int tuple = 1; tuple < tsdbtest.num_tuple; tuple++) {
    tsdbtest.head_add_fast1(tuple * tsdbtest.tuple_size * 1000 * interval,
                            interval, options.use_log);
    if ((tuple + 1) % sep_period == 0) {
      d = timer.since_start_nano();
      std::cout << "[#tuples] " << (tuple + 1) << " [st] "
                << (tuple * tsdbtest.tuple_size * 1000) << std::endl;
      std::cout << "[Insertion duration (ms)]:" << ((d - last_t) / 1000000)
                << " [throughput]:"
                << ((double)(tsdbtest.num_ts) * (double)(sep_period) *
                    (double)(tsdbtest.tuple_size) / (double)(d - last_t) *
                    1000000000)
                << std::endl;
      last_t = d;
    }
  }

  if (_dbtest_out_of_order_percentage > 0) {
    for (int tuple = 0;
         tuple < tsdbtest.num_tuple * _dbtest_out_of_order_percentage / 100;
         tuple++) {
      tsdbtest.head_add_fast1(tuple * tsdbtest.tuple_size * 1000 * interval,
                              interval, options.use_log);
      if ((tuple + 1) % sep_period == 0) {
        d = timer.since_start_nano();
        std::cout << "[#tuples(ooo)] " << (tuple + 1) << " [st] "
                  << (tuple * tsdbtest.tuple_size * 1000) << std::endl;
        std::cout << "[Insertion duration (ms)(ooo)]:"
                  << ((d - last_t) / 1000000) << " [throughput]:"
                  << ((double)(tsdbtest.num_ts) * (double)(sep_period) *
                      (double)(tsdbtest.tuple_size) / (double)(d - last_t) *
                      1000000000)
                  << std::endl;
        last_t = d;
      }
    }
  }

  d = timer.since_start_nano();
  std::cout << "[Total Insertion duration (ms)]:" << (d / 1000000)
            << " [#TS]:" << tsdbtest.num_ts << " [#samples]:"
            << (double)(tsdbtest.num_ts) * (double)(tsdbtest.num_tuple) *
                   (double)(tsdbtest.tuple_size)
            << " [throughput]:"
            << ((double)(tsdbtest.num_ts) * (double)(tsdbtest.num_tuple) *
                (double)(tsdbtest.tuple_size) / (double)(d)*1000000000)
            << std::endl;

  sleep(200);
  ldb->PrintLevel();

  double vm2, rss2;
  mem_usage(vm2, rss2);
  std::cout << "VM:" << (vm2 / 1024) << "MB RSS:" << (rss2 / 1024) << "MB"
            << std::endl;
  std::cout << "VM(diff):" << ((vm2 - vm) / 1024)
            << " RSS(diff):" << ((rss2 - rss) / 1024) << std::endl;

  // ProfilerStart("dbtest.prof");
  tsdbtest.queryDevOps1(
      ldb,
      (tsdbtest.num_tuple - 1) * tsdbtest.tuple_size * 1000 * interval +
          (tsdbtest.tuple_size - 1) * 1000 * interval,
      options.ccache.get());
  sleep(3);
  tsdbtest.queryDevOps2(
      ldb,
      (tsdbtest.num_tuple - 1) * tsdbtest.tuple_size * 1000 * interval +
          (tsdbtest.tuple_size - 1) * 1000 * interval,
      options.ccache.get());
  sleep(3);
  tsdbtest.queryDevOps2(
      ldb,
      (tsdbtest.num_tuple - 1) * tsdbtest.tuple_size * 1000 * interval +
          (tsdbtest.tuple_size - 1) * 1000 * interval,
      options.ccache.get());
  // ProfilerStop();
  // options.ccache->print_summary(true);
}

void TestBigDataQueryGroup1(int num, int interval = 30, int numtuple = 90) {
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
  if (!s.ok()) {
    std::cout << "Cannot create AwsEnv" << std::endl;
    exit(-1);
  }
  cloud_env.reset(cenv);

  Options options;
  options.create_if_missing = true;
  options.use_log = false;
  options.env = cloud_env.get();
  options.aws_use_cloud_cache = true;
  options.ccache = std::shared_ptr<CloudCache>(
      new CloudCache(std::shared_ptr<Cache>(NewLRUCache(64 * 1024 * 1024)),
                     64 * 1024 * 1024 /*pcache size*/,
                     128 * 1024 /*block size*/, cloud_env->GetBaseEnv()));
  options.max_imm_num = 3;
  options.write_buffer_size = 64 * 1024 * 1024;
  // options.block_cache = NewLRUCache(64 * 1024 * 1024);
  // options.sample_cache = NewLRUCache(64 * 1024 * 1024);

  DBCloud* ldb;
  if (!DBCloud::Open(options, dbpath, &ldb).ok()) {
    std::cout << "Cannot open DB" << std::endl;
    exit(-1);
  }

  TSDBTest tsdbtest;
  tsdbtest.set_parameters(num, 32, numtuple);
  tsdbtest.setup(dbpath, dbpath, ldb);
  // tsdbtest.setup("", "/tmp/tsdb_big", db);

  int64_t last_t = 0, d;
  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;
  Timer timer;
  timer.start();
  // int interval = 30;
  tsdbtest.head_add_group1(0, interval, options.use_log);
  d = timer.since_start_nano();
  std::cout << "[Labels Insertion duration (ms)]:" << ((d) / 1000000)
            << " [throughput]:"
            << ((double)(tsdbtest.num_ts) * (double)(tsdbtest.tuple_size) /
                (double)(d)*1000000000)
            << std::endl;
  // std::cout << "[Labels Insertion duration (ms)]:" << ((d) / 1000000) << "
  // [throughput]:" << ((double)(tsdbtest.num_ts) / (double)(d) * 1000000000) <<
  // std::endl;
  mem_usage(vm, rss);
  std::cout << "[After Labels Insertion] VM:" << (vm / 1024)
            << "MB RSS:" << (rss / 1024) << "MB" << std::endl;

  int sep_period = 5;
  for (int tuple = 1; tuple < tsdbtest.num_tuple; tuple++) {
    tsdbtest.head_add_group_fast1(tuple * tsdbtest.tuple_size * 1000 * interval,
                                  interval, options.use_log);
    if ((tuple + 1) % sep_period == 0) {
      d = timer.since_start_nano();
      std::cout << "[#tuples] " << (tuple + 1) << " [st] "
                << (tuple * tsdbtest.tuple_size * 1000) << std::endl;
      std::cout << "[Insertion duration (ms)]:" << ((d - last_t) / 1000000)
                << " [throughput]:"
                << ((double)(tsdbtest.num_ts) * (double)(sep_period) *
                    (double)(tsdbtest.tuple_size) / (double)(d - last_t) *
                    1000000000)
                << std::endl;
      last_t = d;
    }
  }
  d = timer.since_start_nano();
  std::cout << "[Total Insertion duration (ms)]:" << (d / 1000000)
            << " [#TS]:" << tsdbtest.num_ts << " [#samples]:"
            << (double)(tsdbtest.num_ts) * (double)(tsdbtest.num_tuple) *
                   (double)(tsdbtest.tuple_size)
            << " [throughput]:"
            << ((double)(tsdbtest.num_ts) * (double)(tsdbtest.num_tuple) *
                (double)(tsdbtest.tuple_size) / (double)(d)*1000000000)
            << std::endl;

  sleep(200);
  ldb->PrintLevel();

  double vm2, rss2;
  mem_usage(vm2, rss2);
  std::cout << "VM:" << (vm2 / 1024) << "MB RSS:" << (rss2 / 1024) << "MB"
            << std::endl;
  std::cout << "VM(diff):" << ((vm2 - vm) / 1024)
            << " RSS(diff):" << ((rss2 - rss) / 1024) << std::endl;

  // ProfilerStart("dbtest.prof");
  tsdbtest.queryDevOps2(
      ldb,
      (tsdbtest.num_tuple - 1) * tsdbtest.tuple_size * 1000 * interval +
          (tsdbtest.tuple_size - 1) * 1000 * interval,
      options.ccache.get());
  sleep(3);
  tsdbtest.queryDevOps2(
      ldb,
      (tsdbtest.num_tuple - 1) * tsdbtest.tuple_size * 1000 * interval +
          (tsdbtest.tuple_size - 1) * 1000 * interval,
      options.ccache.get());
  sleep(3);
  tsdbtest.queryDevOps2(
      ldb,
      (tsdbtest.num_tuple - 1) * tsdbtest.tuple_size * 1000 * interval +
          (tsdbtest.tuple_size - 1) * 1000 * interval,
      options.ccache.get());
  // ProfilerStop();
  // options.ccache->print_summary(true);
}

void TestBigDataQueryGroup2(int num) {
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
  if (!s.ok()) {
    std::cout << "Cannot create AwsEnv" << std::endl;
    exit(-1);
  }
  cloud_env.reset(cenv);

  Options options;
  options.create_if_missing = true;
  options.use_log = false;
  options.env = cloud_env.get();
  options.aws_use_cloud_cache = true;
  options.ccache = std::shared_ptr<CloudCache>(
      new CloudCache(std::shared_ptr<Cache>(NewLRUCache(64 * 1024 * 1024)),
                     64 * 1024 * 1024 /*pcache size*/,
                     128 * 1024 /*block size*/, cloud_env->GetBaseEnv()));
  options.max_imm_num = 3;
  options.write_buffer_size = 64 * 1024 * 1024;
  // options.block_cache = NewLRUCache(64 * 1024 * 1024);
  // options.sample_cache = NewLRUCache(64 * 1024 * 1024);

  DBCloud* ldb;
  if (!DBCloud::Open(options, dbpath, &ldb).ok()) {
    std::cout << "Cannot open DB" << std::endl;
    exit(-1);
  }

  TSDBTest tsdbtest;
  tsdbtest.set_parameters(num, 32, 45);
  tsdbtest.setup(dbpath, dbpath, ldb);
  // tsdbtest.setup("", "/tmp/tsdb_big", db);

  int64_t last_t = 0, d;
  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;
  Timer timer;
  timer.start();
  int interval = 30;
  tsdbtest.head_add_group3(0, interval, options.use_log);
  d = timer.since_start_nano();
  std::cout << "[Labels Insertion duration (ms)]:" << ((d) / 1000000)
            << " [throughput]:"
            << ((double)(tsdbtest.num_ts) * (double)(tsdbtest.tuple_size) /
                (double)(d)*1000000000)
            << std::endl;
  // std::cout << "[Labels Insertion duration (ms)]:" << ((d) / 1000000) << "
  // [throughput]:" << ((double)(tsdbtest.num_ts) / (double)(d) * 1000000000) <<
  // std::endl;
  mem_usage(vm, rss);
  std::cout << "[After Labels Insertion] VM:" << (vm / 1024)
            << "MB RSS:" << (rss / 1024) << "MB" << std::endl;

  int sep_period = 5;
  for (int tuple = 1; tuple < tsdbtest.num_tuple; tuple++) {
    tsdbtest.head_add_group_fast2(tuple * tsdbtest.tuple_size * 1000 * interval,
                                  interval, options.use_log);
    if ((tuple + 1) % sep_period == 0) {
      d = timer.since_start_nano();
      std::cout << "[#tuples] " << (tuple + 1) << " [st] "
                << (tuple * tsdbtest.tuple_size * 1000) << std::endl;
      std::cout << "[Insertion duration (ms)]:" << ((d - last_t) / 1000000)
                << " [throughput]:"
                << ((double)(tsdbtest.num_ts) * (double)(sep_period) *
                    (double)(tsdbtest.tuple_size) / (double)(d - last_t) *
                    1000000000)
                << std::endl;
      last_t = d;
    }
  }
  d = timer.since_start_nano();
  std::cout << "[Total Insertion duration (ms)]:" << (d / 1000000)
            << " [#TS]:" << tsdbtest.num_ts << " [#samples]:"
            << (double)(tsdbtest.num_ts) * (double)(tsdbtest.num_tuple) *
                   (double)(tsdbtest.tuple_size)
            << " [throughput]:"
            << ((double)(tsdbtest.num_ts) * (double)(tsdbtest.num_tuple) *
                (double)(tsdbtest.tuple_size) / (double)(d)*1000000000)
            << std::endl;

  sleep(200);
  ldb->PrintLevel();

  double vm2, rss2;
  mem_usage(vm2, rss2);
  std::cout << "VM:" << (vm2 / 1024) << "MB RSS:" << (rss2 / 1024) << "MB"
            << std::endl;
  std::cout << "VM(diff):" << ((vm2 - vm) / 1024)
            << " RSS(diff):" << ((rss2 - rss) / 1024) << std::endl;

  // ProfilerStart("dbtest.prof");
  tsdbtest.queryDevOps2(
      ldb,
      (tsdbtest.num_tuple - 1) * tsdbtest.tuple_size * 1000 * interval +
          (tsdbtest.tuple_size - 1) * 1000 * interval,
      options.ccache.get());
  sleep(3);
  tsdbtest.queryDevOps2(
      ldb,
      (tsdbtest.num_tuple - 1) * tsdbtest.tuple_size * 1000 * interval +
          (tsdbtest.tuple_size - 1) * 1000 * interval,
      options.ccache.get());
  sleep(3);
  tsdbtest.queryDevOps2(
      ldb,
      (tsdbtest.num_tuple - 1) * tsdbtest.tuple_size * 1000 * interval +
          (tsdbtest.tuple_size - 1) * 1000 * interval,
      options.ccache.get());
  // ProfilerStop();
  // options.ccache->print_summary(true);
}

void TestDynamic(int num, int interval = 30, int numtuple = 90) {
  std::string dbpath = "/tmp/tsdb_big";
  boost::filesystem::remove_all(dbpath);
  std::string region = "ap-northeast-1";
  std::string bucket_prefix = "rockset.";
  std::string bucket_suffix = "cloud-db-examples.alec";

  FAST_STORAGE_SIZE = 512 * 1024 * 1024;
  DYNAMIC_PARTITION_LENGTH = true;

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
  if (!s.ok()) {
    std::cout << "Cannot create AwsEnv" << std::endl;
    exit(-1);
  }
  cloud_env.reset(cenv);

  Options options;
  options.create_if_missing = true;
  options.use_log = false;
  // options.env = cloud_env.get();
  options.aws_use_cloud_cache = true;
  options.ccache = std::shared_ptr<CloudCache>(
      new CloudCache(std::shared_ptr<Cache>(NewLRUCache(64 * 1024 * 1024)),
                     64 * 1024 * 1024 /*pcache size*/,
                     128 * 1024 /*block size*/, cloud_env->GetBaseEnv()));
  options.max_imm_num = 3;
  options.write_buffer_size = 64 * 1024 * 1024;
  // options.block_cache = NewLRUCache(64 * 1024 * 1024);
  // options.sample_cache = NewLRUCache(64 * 1024 * 1024);

  // options.write_buffer_size = 64 * 1024;
  // options.max_file_size = 64 * 1024;

  interval = 10;
  numtuple = 20;

  DBCloud* ldb;
  if (!DBCloud::Open(options, dbpath, &ldb).ok()) {
    std::cout << "Cannot open DB" << std::endl;
    exit(-1);
  }

  TSDBTest tsdbtest;
  tsdbtest.set_parameters(num, 32, numtuple);
  tsdbtest.setup(dbpath, dbpath, ldb);
  // tsdbtest.setup("", "/tmp/tsdb_big", db);

  int64_t last_t = 0, d;
  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;

  LOG_INFO << "Time(ms):"
           << std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::high_resolution_clock::now().time_since_epoch())
                  .count()
           << " interval:" << interval << " numtuple:" << tsdbtest.num_tuple;
  LOG_INFO << "Time(ms):"
           << std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::high_resolution_clock::now().time_since_epoch())
                  .count()
           << " PARTITION_LENGTH:" << PARTITION_LENGTH
           << " L2_PARTITION_LENGTH:" << L2_PARTITION_LENGTH;

  Timer timer;
  timer.start();
  tsdbtest.head_add3(0, interval, options.use_log);
  d = timer.since_start_nano();
  std::cout << "[Labels Insertion duration (ms)]:" << ((d) / 1000000)
            << " [throughput]:"
            << ((double)(tsdbtest.num_ts) * (double)(tsdbtest.tuple_size) /
                (double)(d)*1000000000)
            << std::endl;
  // std::cout << "[Labels Insertion duration (ms)]:" << ((d) / 1000000) << "
  // [throughput]:" << ((double)(tsdbtest.num_ts) / (double)(d) * 1000000000) <<
  // std::endl;
  mem_usage(vm, rss);
  std::cout << "[After Labels Insertion] VM:" << (vm / 1024)
            << "MB RSS:" << (rss / 1024) << "MB" << std::endl;

  int sep_period = 5;
  for (int tuple = 1; tuple < tsdbtest.num_tuple; tuple++) {
    tsdbtest.head_add_fast1(tuple * tsdbtest.tuple_size * 1000 * interval,
                            interval, options.use_log);
    if ((tuple + 1) % sep_period == 0) {
      d = timer.since_start_nano();
      std::cout << "[#tuples] " << (tuple + 1) << " [st] "
                << (tuple * tsdbtest.tuple_size * 1000) << std::endl;
      std::cout << "[Insertion duration (ms)]:" << ((d - last_t) / 1000000)
                << " [throughput]:"
                << ((double)(tsdbtest.num_ts) * (double)(sep_period) *
                    (double)(tsdbtest.tuple_size) / (double)(d - last_t) *
                    1000000000)
                << std::endl;
      last_t = d;

      LOG_INFO
          << "before sleep Time(ms):"
          << std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::high_resolution_clock::now().time_since_epoch())
                 .count();
      sleep(80);
      LOG_INFO
          << "after sleep Time(ms):"
          << std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::high_resolution_clock::now().time_since_epoch())
                 .count();
    }
  }

  sleep(80);
  int64_t last_st = tsdbtest.tuple_size * tsdbtest.num_tuple * 1000 * interval;
  interval = 60;
  // tsdbtest.num_tuple = 40;
  LOG_INFO << "Time(ms):"
           << std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::high_resolution_clock::now().time_since_epoch())
                  .count()
           << " interval:" << interval << " numtuple:" << tsdbtest.num_tuple
           << " PARTITION_LENGTH:" << PARTITION_LENGTH
           << " L2_PARTITION_LENGTH:" << L2_PARTITION_LENGTH;
  for (int tuple = 0; tuple < tsdbtest.num_tuple; tuple++) {
    tsdbtest.head_add_fast1(
        last_st + tuple * tsdbtest.tuple_size * 1000 * interval, interval,
        options.use_log);
    if ((tuple + 1) % sep_period == 0) {
      d = timer.since_start_nano();
      std::cout << "[#tuples] " << (tuple + 1) << " [st] "
                << (tuple * tsdbtest.tuple_size * 1000) << std::endl;
      std::cout << "[Insertion duration (ms)]:" << ((d - last_t) / 1000000)
                << " [throughput]:"
                << ((double)(tsdbtest.num_ts) * (double)(sep_period) *
                    (double)(tsdbtest.tuple_size) / (double)(d - last_t) *
                    1000000000)
                << std::endl;
      last_t = d;

      LOG_INFO
          << "before sleep Time(ms):"
          << std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::high_resolution_clock::now().time_since_epoch())
                 .count();
      sleep(80);
      LOG_INFO
          << "after sleep Time(ms):"
          << std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::high_resolution_clock::now().time_since_epoch())
                 .count();
    }
  }

  sleep(80);
  last_st += tsdbtest.tuple_size * tsdbtest.num_tuple * 1000 * interval;
  interval = 10;
  tsdbtest.num_tuple = 60;
  LOG_INFO << "Time(ms):"
           << std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::high_resolution_clock::now().time_since_epoch())
                  .count()
           << " interval:" << interval << " numtuple:" << tsdbtest.num_tuple
           << " PARTITION_LENGTH:" << PARTITION_LENGTH
           << " L2_PARTITION_LENGTH:" << L2_PARTITION_LENGTH;
  for (int tuple = 0; tuple < tsdbtest.num_tuple; tuple++) {
    tsdbtest.head_add_fast1(
        last_st + tuple * tsdbtest.tuple_size * 1000 * interval, interval,
        options.use_log);
    if ((tuple + 1) % sep_period == 0) {
      d = timer.since_start_nano();
      std::cout << "[#tuples] " << (tuple + 1) << " [st] "
                << (tuple * tsdbtest.tuple_size * 1000) << std::endl;
      std::cout << "[Insertion duration (ms)]:" << ((d - last_t) / 1000000)
                << " [throughput]:"
                << ((double)(tsdbtest.num_ts) * (double)(sep_period) *
                    (double)(tsdbtest.tuple_size) / (double)(d - last_t) *
                    1000000000)
                << std::endl;
      last_t = d;

      LOG_INFO
          << "before sleep Time(ms):"
          << std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::high_resolution_clock::now().time_since_epoch())
                 .count();
      sleep(80);
      LOG_INFO
          << "after sleep Time(ms):"
          << std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::high_resolution_clock::now().time_since_epoch())
                 .count();
    }
  }

  d = timer.since_start_nano();
  std::cout << "[Total Insertion duration (ms)]:" << (d / 1000000)
            << " [#TS]:" << tsdbtest.num_ts << " [#samples]:"
            << (double)(tsdbtest.num_ts) * (double)(tsdbtest.num_tuple) *
                   (double)(tsdbtest.tuple_size)
            << " [throughput]:"
            << ((double)(tsdbtest.num_ts) * (double)(tsdbtest.num_tuple) *
                (double)(tsdbtest.tuple_size) / (double)(d)*1000000000)
            << std::endl;

  sleep(80);
  ldb->PrintLevel();

  LOG_INFO << "Time(ms):"
           << std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::high_resolution_clock::now().time_since_epoch())
                  .count()
           << " PARTITION_LENGTH:" << PARTITION_LENGTH
           << " L2_PARTITION_LENGTH:" << L2_PARTITION_LENGTH;

  double vm2, rss2;
  mem_usage(vm2, rss2);
  std::cout << "VM:" << (vm2 / 1024) << "MB RSS:" << (rss2 / 1024) << "MB"
            << std::endl;
  std::cout << "VM(diff):" << ((vm2 - vm) / 1024)
            << " RSS(diff):" << ((rss2 - rss) / 1024) << std::endl;
}

}  // namespace leveldb

int main(int argc, char* argv[]) {
  struct sigaction act;
  // ignore SIGPIPE
  act.sa_handler = SIG_IGN;
  sigemptyset(&act.sa_mask);
  act.sa_flags = 0;
  sigaction(SIGPIPE, &act, NULL);

  if (argc < 2) {
    std::cout << "Need at least two arguments" << std::endl;
    exit(-1);
  }
  int num = std::stoi(argv[1]);
  // leveldb::PARTITION_LENGTH = 1500000;
  if (argc == 4)
    leveldb::TestBigDataQuery1(num, std::stoi(argv[2]), std::stoi(argv[3]));
  // leveldb::TestBigDataQueryGroup1(num, std::stoi(argv[2]),
  // std::stoi(argv[3])); leveldb::TestDynamic(num, std::stoi(argv[2]),
  // std::stoi(argv[3]));
  else
    leveldb::TestBigDataQuery1(num);
  // leveldb::TestBigDataQueryGroup1(num);
  // leveldb::TestDynamic(num);
}