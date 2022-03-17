#include "head/MMapHeadWithTrie.hpp"

#include <algorithm>
#include <boost/bind.hpp>
#include <boost/filesystem.hpp>
#include <iostream>
#include <limits>
#include <thread>

#include "head/HeadAppender.hpp"
#include "head/HeadSeriesSet.hpp"
#include "head/HeadUtils.hpp"
#include "head/InitAppender.hpp"
#include "index/VectorPostings.hpp"
#include "leveldb/db/log_reader.h"
#include "leveldb/db/log_writer.h"
#include "leveldb/db/partition_index.h"
#include "leveldb/third_party/thread_pool.h"
#include "querier/QuerierUtils.hpp"
#include "tombstone/MemTombstones.hpp"
#include "tsdbutil/RecordDecoder.hpp"
#include "tsdbutil/RecordEncoder.hpp"

// namespace leveldb {
// void mem_usage(double& vm_usage, double& resident_set);
// }

namespace tsdb {
namespace head {

MMapHeadWithTrie::MMapHeadWithTrie(uint64_t last_series_id,
                                   const std::string& dir,
                                   const std::string& idx_dir,
                                   ::leveldb::DB* db)
    : cur_samples_log_seq_(0),
      cur_flushes_log_seq_(0),
      active_samples_logs_(0),
      dir_(dir),
      idx_dir_(idx_dir),
      last_series_id(last_series_id),
      db_(db),
      bg_log_clean_stop_signal_(bg_log_clean_stop_mutex_),
      bg_log_clean_stopped_(false) {
  min_time.getAndSet(std::numeric_limits<int64_t>::max());
  max_time.getAndSet(std::numeric_limits<int64_t>::min());

  if (db_) db_->SetHead(this);

#if USE_PERSISTENT_CEDAR
  symbols_.reset(new pcedar::da<char>(idx_dir, "sym_"));
  label_names_.reset(new pcedar::da<char>(idx_dir, "ln_"));
  label_values_.reset(new pcedar::da<char>(idx_dir, "lv_"));
#else
  symbols_.reset(new cedar::da<char>());
  label_names_.reset(new cedar::da<char>());
  label_values_.reset(new cedar::da<char>());
#endif

  // if (!boost::filesystem::exists(idx_dir + "/mmap_arrays_safe")) {
  MMapXORChunk::remove_all(leveldb::MEM_TUPLE_SIZE, idx_dir);

  MMapIntArray::remove_all(leveldb::MEM_TUPLE_SIZE, idx_dir, "g");
  MMapGroupFloatArray::remove_all(leveldb::MEM_TUPLE_SIZE, idx_dir);
  // }

  xor_array_.reset(
      new MMapXORChunk(idx_dir, leveldb::MEM_TUPLE_SIZE, MMAP_ARRAY_SLOTS));

  group_xor_array_.reset(new MMapGroupXORChunk(idx_dir, leveldb::MEM_TUPLE_SIZE,
                                               MMAP_ARRAY_SLOTS, "g"));

#if USE_MMAP_LABELS
  if (!boost::filesystem::exists(idx_dir + "/mmap_labels_safe")) {
    MMapLabels::remove_all(idx_dir);
    MMapGroupLabels::remove_all(idx_dir);
  }

  mmap_labels_.reset(new MMapLabels(idx_dir));
  mmap_group_labels_.reset(new MMapGroupLabels(idx_dir));
#endif
  series_ = std::unique_ptr<MMapStripeSeries>(new MMapStripeSeries());
  groups_ = std::unique_ptr<MMapStripeGroups>(new MMapStripeGroups());
  posting_list = std::unique_ptr<index::MemPostingsWithTrie>(
      new index::MemPostingsWithTrie());
#if USE_MMAP_LABELS
  if (!idx_dir_.empty() &&
      boost::filesystem::exists(idx_dir + "/mmap_labels_safe")) {
    Timer t;
    t.start();
    recover_index_from_mmap();
    LOG_DEBUG << "[recover_index_from_mmap()] duration:" << t.since_start_nano()
              << " nano seconds";
    t.start();
    recover_group_index_from_mmap();
    LOG_DEBUG << "[recover_group_index_from_mmap()] duration:"
              << t.since_start_nano() << " nano seconds";

    // Init index log.
    if (!dir_.empty()) {
      uint64_t fsize = 0;
      ::leveldb::Env* env = ::leveldb::Env::Default();
      ::leveldb::WritableFile* f;
      leveldb::Status s =
          env->NewAppendableFile(dir_ + "/" + HEAD_LOG_NAME, &f);
      if (!s.ok()) {
        delete f;
        LOG_ERROR << "Cannot init index log: " << s.ToString();
      } else {
        env->GetFileSize(dir_ + "/" + HEAD_LOG_NAME, &fsize);

        log_file_.reset(f);
        log_writer_.reset(new ::leveldb::log::Writer(f, fsize));
      }
    }
  } else if (!dir_.empty()) {
    Timer t;
    t.start();
    recover_index_from_log();
    LOG_DEBUG << "[recover_index_from_log()] duration:" << t.since_start_nano()
              << " nano seconds";
  }
#else
  if (!dir_.empty()) {
    Timer t;
    t.start();
    recover_index_from_log();
    LOG_DEBUG << "[recover_index_from_log()] duration:" << t.since_start_nano()
              << " nano seconds";
  }
#endif

  if (!dir_.empty()) {
    Timer t;
    t.start();
    recover_samples_from_log();
    LOG_DEBUG << "[recover_samples_from_log()] duration:"
              << t.since_start_nano() << " nano seconds";
  }

  boost::filesystem::remove(idx_dir + "/mmap_arrays_safe");
  boost::filesystem::remove(idx_dir + "/mmap_labels_safe");

  // Start the thread for periodical log cleaning.
  std::thread bg_log_cleaning([this]() {
    while (true) {
      {
        base::MutexLockGuard lock(bg_log_clean_stop_mutex_);
        if (bg_log_clean_stopped_) {
          bg_log_clean_stop_signal_.notify();
          break;
        }
      }

      if (active_samples_logs_ > HEAD_SAMPLES_LOG_CLEANING_THRES) {
        int current_active_samples_logs = active_samples_logs_;
        this->clean_samples_logs();
        if (active_samples_logs_ >= current_active_samples_logs) sleep(20);
      }
      sleep(1);
    }
  });
  bg_log_cleaning.detach();
}

MMapHeadWithTrie::~MMapHeadWithTrie() {
  // bg_log_clean_stop_mutex_.lock();
  // bg_log_clean_stopped_ = true;
  // bg_log_clean_stop_signal_.wait();

  double vm, rss;
  // leveldb::mem_usage(vm, rss);
  // LOG_DEBUG << "VM:" << (vm / 1024) << "MB RSS:" << (rss / 1024) << "MB";
  MMapXORChunk* p1 = xor_array_.release();
  MMapGroupXORChunk* p2 = group_xor_array_.release();
  if (p1) delete p1;
  // leveldb::mem_usage(vm, rss);
  // LOG_DEBUG << "Release time_array_ VM:" << (vm / 1024) << "MB RSS:" << (rss
  // / 1024) << "MB";
  if (p2) delete p2;
    // leveldb::mem_usage(vm, rss);
    // LOG_DEBUG << "Release value_array_ VM:" << (vm / 1024) << "MB RSS:" <<
    // (rss / 1024) << "MB";

    // FILE* f = fopen((idx_dir_ + "/mmap_arrays_safe").c_str(), "w");
    // fclose(f);
    // LOG_DEBUG << "create safe mark for mmap arrays";

#if USE_MMAP_LABELS
  MMapLabels* l1 = mmap_labels_.release();
  if (l1) delete l1;
  // leveldb::mem_usage(vm, rss);
  // LOG_DEBUG << "Release mmap_labels_ VM:" << (vm / 1024) << "MB RSS:" << (rss
  // / 1024) << "MB";
  MMapGroupLabels* l2 = mmap_group_labels_.release();
  if (l2) delete l2;
    // leveldb::mem_usage(vm, rss);
    // LOG_DEBUG << "Release mmap_group_labels_ VM:" << (vm / 1024) << "MB RSS:"
    // << (rss / 1024) << "MB";
#endif
  FILE* f = fopen((idx_dir_ + "/mmap_labels_safe").c_str(), "w");
  fclose(f);
  LOG_DEBUG << "create safe mark for mmap labels";

#if USE_PERSISTENT_CEDAR
  pcedar::da<char>* ln = label_names_.release();
  if (ln) delete ln;
  // leveldb::mem_usage(vm, rss);
  // LOG_DEBUG << "Release label_names_ VM:" << (vm / 1024) << "MB RSS:" << (rss
  // / 1024) << "MB";
  pcedar::da<char>* lv = label_values_.release();
  if (lv) delete lv;
    // leveldb::mem_usage(vm, rss);
    // LOG_DEBUG << "Release label_values_ VM:" << (vm / 1024) << "MB RSS:" <<
    // (rss / 1024) << "MB";
#else
  cedar::da<char>* ln = label_names_.release();
  if (ln) delete ln;
  // leveldb::mem_usage(vm, rss);
  // LOG_DEBUG << "Release label_names_ VM:" << (vm / 1024) << "MB RSS:" << (rss
  // / 1024) << "MB";
  cedar::da<char>* lv = label_values_.release();
  if (lv) delete lv;
    // leveldb::mem_usage(vm, rss);
    // LOG_DEBUG << "Release label_values_ VM:" << (vm / 1024) << "MB RSS:" <<
    // (rss / 1024) << "MB";
#endif

  index::MemPostingsWithTrie* pl = posting_list.release();
  if (pl) delete pl;
  // leveldb::mem_usage(vm, rss);
  // LOG_DEBUG << "Release posting_list VM:" << (vm / 1024) << "MB RSS:" << (rss
  // / 1024) << "MB";

  MMapStripeSeries* ss = series_.release();
  if (ss) delete ss;
  // leveldb::mem_usage(vm, rss);
  // LOG_DEBUG << "Release series_ VM:" << (vm / 1024) << "MB RSS:" << (rss /
  // 1024) << "MB";

  MMapStripeGroups* sg = groups_.release();
  if (sg) delete sg;
  // leveldb::mem_usage(vm, rss);
  // LOG_DEBUG << "Release groups_ VM:" << (vm / 1024) << "MB RSS:" << (rss /
  // 1024) << "MB";
}

void MMapHeadWithTrie::_print_usage() {}

leveldb::Status MMapHeadWithTrie::recover_index_from_log() {
  // LOG_DEBUG << "Recover index from log";
  ::leveldb::Env* env = ::leveldb::Env::Default();
  ::leveldb::Status s;
  uint64_t fsize = 0;
  if (env->FileExists(dir_ + "/" + HEAD_LOG_NAME)) {
    ::leveldb::SequentialFile* sf;
    s = env->NewSequentialFile(dir_ + "/" + HEAD_LOG_NAME, &sf);
    if (!s.ok()) {
      delete sf;
      return s;
    }
    ::leveldb::log::Reader r(sf, nullptr, false, 0);
    ::leveldb::Slice record;
    std::string scratch;
    while (r.ReadRecord(&record, &scratch)) {
      if (record.data()[0] == leveldb::log::kSeries) {
        std::vector<::leveldb::log::RefSeries> rs;
        bool success = ::leveldb::log::series(record, &rs);
        if (!success)
          return ::leveldb::Status::Corruption("series recover_index_from_log");

        // Recover index.
        for (size_t j = 0; j < rs.size(); j++) {
#if USE_MMAP_LABELS
          get_or_create_with_id(rs[j].ref, label::lbs_hash(rs[j].lset),
                                rs[j].lset, false, -1);
#else
          get_or_create_with_id(rs[j].ref, label::lbs_hash(rs[j].lset),
                                rs[j].lset, false);
#endif
          if (rs[j].ref > last_series_id.get())
            last_series_id.getAndSet(rs[j].ref);
        }
      } else if (record.data()[0] == leveldb::log::kGroup) {
        ::leveldb::log::RefGroup rs;
        bool success = ::leveldb::log::group(record, &rs);
        if (!success)
          return ::leveldb::Status::Corruption("group recover_index_from_log");

#if USE_MMAP_LABELS
        std::pair<MMapMemGroup*, bool> p = get_or_create_group_with_id(
            rs.ref, label::lbs_hash(rs.group_lset), rs.group_lset, false, -1);
#else
        std::pair<MMapMemGroup*, bool> p = get_or_create_group_with_id(
            rs.ref, label::lbs_hash(rs.group_lset), rs.group_lset, false);
#endif
        if (rs.ref > last_series_id.get()) last_series_id.getAndSet(rs.ref);
        // Init individual TS in the group.
        p.first->get_indices_with_labels(rs.individual_lsets, nullptr, nullptr,
                                         false, -1);
      }
    }
    delete sf;

    env->GetFileSize(dir_ + "/" + HEAD_LOG_NAME, &fsize);
  }

  ::leveldb::WritableFile* f;
  s = env->NewAppendableFile(dir_ + "/" + HEAD_LOG_NAME, &f);
  if (!s.ok()) {
    delete f;
    return s;
  }
  log_file_.reset(f);
  log_writer_.reset(new ::leveldb::log::Writer(f, fsize));
  return ::leveldb::Status::OK();
}

#if USE_MMAP_LABELS
void MMapHeadWithTrie::recover_index_from_mmap() {
  MMapLabelsIterator it = mmap_labels_->iterator();
  uint64_t id, idx;
  label::Labels lset;
  while (it.next()) {
    lset.clear();
    it.at(&id, &idx, lset);
    if (id > last_series_id.get()) last_series_id.getAndSet(id);
    get_or_create_with_id(id, label::lbs_hash(lset), lset, false, idx);
  }
}

void MMapHeadWithTrie::recover_group_index_from_mmap() {
  MMapGroupLabelsIterator it = mmap_group_labels_->iterator();
  uint64_t id, idx;
  uint32_t slot;
  label::Labels lset;
  while (it.next()) {
    lset.clear();
    it.at(&id, &slot, &idx, lset);
    if ((id & 0x7fffffffffffffff) > last_series_id.get())
      last_series_id.getAndSet(id & 0x7fffffffffffffff);
    if (slot == std::numeric_limits<uint32_t>::max())
      get_or_create_group_with_id(id, label::lbs_hash(lset), lset, false, idx);
    else {
      MMapMemGroup* g = groups_->get_ptr_by_id(id);
      g->get_indices_with_labels({lset}, nullptr, nullptr, false, idx);
      // g->individual_labels_idx_[slot] = idx;
    }
  }
}
#endif

leveldb::Status MMapHeadWithTrie::recover_samples_from_log() {
  // LOG_DEBUG << "Recover samples from log";
  ::leveldb::Env* env = ::leveldb::Env::Default();
  ::leveldb::Status s;
  uint64_t fsize = 0;

  std::vector<std::string> existing_logs;
  std::vector<std::string> existing_flushes_logs;
  boost::filesystem::path p(dir_);
  boost::filesystem::directory_iterator end_itr;

  for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
    // If it's not a directory, list it. If you want to list directories too,
    // just remove this check.
    if (boost::filesystem::is_regular_file(itr->path())) {
      // assign current file name to current_file and echo it out to the
      // console.
      std::string current_file = itr->path().filename().string();
      if (current_file.size() > HEAD_SAMPLES_LOG_NAME.size() &&
          memcmp(current_file.c_str(), HEAD_SAMPLES_LOG_NAME.c_str(),
                 HEAD_SAMPLES_LOG_NAME.size()) == 0)
        existing_logs.push_back(current_file);
      else if (current_file.size() > HEAD_FLUSHES_LOG_NAME.size() &&
               memcmp(current_file.c_str(), HEAD_FLUSHES_LOG_NAME.c_str(),
                      HEAD_FLUSHES_LOG_NAME.size()) == 0)
        existing_flushes_logs.push_back(current_file);
    }
  }
  std::sort(existing_logs.begin(), existing_logs.end(),
            [&](const std::string& l, const std::string& r) {
              return std::stoi(l.substr(HEAD_SAMPLES_LOG_NAME.size())) <
                     std::stoi(r.substr(HEAD_SAMPLES_LOG_NAME.size()));
            });
  std::sort(existing_flushes_logs.begin(), existing_flushes_logs.end(),
            [&](const std::string& l, const std::string& r) {
              return std::stoi(l.substr(HEAD_FLUSHES_LOG_NAME.size())) <
                     std::stoi(r.substr(HEAD_FLUSHES_LOG_NAME.size()));
            });

  // First round to load the newest flushing txn.
  for (size_t i = 0; i < existing_flushes_logs.size(); i++) {
    ::leveldb::SequentialFile* sf;
    s = env->NewSequentialFile(
        dir_ + std::string("/") + existing_flushes_logs[i], &sf);
    if (!s.ok()) {
      delete sf;
      return s;
    }
    ::leveldb::log::Reader r(sf, nullptr, false, 0);
    ::leveldb::Slice record;
    std::string scratch;
    ::leveldb::log::RefFlush f;
    while (r.ReadRecord(&record, &scratch)) {
      if (record.data()[0] == leveldb::log::kFlush) {
        ::leveldb::log::flush(record, &f);
        // Update flushing txn.
        if ((f.ref >> 63) == 0) {
          MMapMemSeries* s = series_->get_ptr_by_id(f.ref);
          if (s) s->flushed_txn_ = f.txn;
        } else {
          MMapMemGroup* g = groups_->get_ptr_by_id(f.ref);
          if (g) g->flushed_txn_ = f.txn;
        }
      }
    }
    delete sf;
  }

  // Secpnd round to load the unflushed samples.
  for (size_t i = 0; i < existing_logs.size(); i++) {
    ::leveldb::SequentialFile* sf;
    s = env->NewSequentialFile(dir_ + std::string("/") + existing_logs[i], &sf);
    if (!s.ok()) {
      delete sf;
      return s;
    }
    ::leveldb::log::Reader r(sf, nullptr, false, 0);
    ::leveldb::Slice record;
    std::string scratch;
    std::vector<::leveldb::log::RefSample> rs;
    while (r.ReadRecord(&record, &scratch)) {
      if (record.data()[0] == leveldb::log::kSample) {
        rs.clear();
        ::leveldb::log::samples(record, &rs);

        for (size_t j = 0; j < rs.size(); j++) {
          MMapMemSeries* s = series_->get_ptr_by_id(rs[j].ref);
          if (s && rs[j].txn > s->flushed_txn_) {
            s->append(db_, rs[j].t, rs[j].v);
          }
        }
      } else if (record.data()[0] == leveldb::log::kGroupSample) {
        ::leveldb::log::RefGroupSample rs;
        ::leveldb::log::group_sample(record, &rs);
        MMapMemGroup* s = groups_->get_ptr_by_id(rs.ref);
        if (s && rs.txn > s->flushed_txn_) {
          if (rs.slots.empty())
            s->append(db_, rs.t, rs.v);
          else
            s->append(db_, rs.slots, rs.t, rs.v);
        }
      }
    }
    delete sf;
  }

  // Init samples log writer.
  if (existing_logs.empty())
    cur_samples_log_seq_ = 0;
  else
    cur_samples_log_seq_ =
        std::stoi(existing_logs.back().substr(HEAD_SAMPLES_LOG_NAME.size())) +
        1;
  ::leveldb::WritableFile* f;
  s = env->NewAppendableFile(dir_ + std::string("/") + HEAD_SAMPLES_LOG_NAME +
                                 std::to_string(cur_samples_log_seq_),
                             &f);
  if (!s.ok()) {
    delete f;
    return s;
  }
  samples_log_file_.reset(f);
  samples_log_writer_.reset(new ::leveldb::log::Writer(f));
  active_samples_logs_ = existing_logs.size();

  // Init flushes log writer.
  if (existing_flushes_logs.empty())
    cur_flushes_log_seq_ = 0;
  else
    cur_flushes_log_seq_ = std::stoi(existing_flushes_logs.back().substr(
                               HEAD_FLUSHES_LOG_NAME.size())) +
                           1;
  s = env->NewAppendableFile(dir_ + std::string("/") + HEAD_FLUSHES_LOG_NAME +
                                 std::to_string(cur_flushes_log_seq_),
                             &f);
  if (!s.ok()) {
    delete f;
    return s;
  }
  flushes_log_file_.reset(f);
  flushes_log_writer_.reset(new ::leveldb::log::Writer(f));

  return s;
}

leveldb::Status MMapHeadWithTrie::clean_samples_logs() {
  std::vector<std::vector<::leveldb::log::RefSample>> refsamples;
  std::vector<::leveldb::log::RefGroupSample> groupsamples;
  leveldb::Status s;
  ::leveldb::Env* env = ::leveldb::Env::Default();

  // ROUND1: find the max txn in the logs for each TS/Group.
  int end = cur_flushes_log_seq_ - 1;
  for (int i = 0; i <= end; i++) {
    std::string fname =
        dir_ + std::string("/") + HEAD_FLUSHES_LOG_NAME + std::to_string(i);
    if (!env->FileExists(fname)) continue;

    ::leveldb::SequentialFile* sf;
    s = env->NewSequentialFile(fname, &sf);
    if (!s.ok()) {
      std::cout << "NewSequentialFile " << s.ToString() << std::endl;
      return s;
    }
    ::leveldb::log::Reader r(sf, nullptr, false, 0);
    ::leveldb::Slice record;
    std::string scratch;
    while (r.ReadRecord(&record, &scratch)) {
      if (record.data()[0] == leveldb::log::kFlush) {
        ::leveldb::log::RefFlush flush;
        ::leveldb::log::flush(record, &flush);
        // Update flushing txn.
        if ((flush.ref >> 63) == 0) {
          MMapMemSeries* s = series_->get_ptr_by_id(flush.ref);
          if (s) s->log_clean_txn_ = flush.txn;
        } else {
          MMapMemGroup* g = groups_->get_ptr_by_id(flush.ref);
          if (g) g->log_clean_txn_ = flush.txn;
        }
      }
    }
    delete sf;

    s = env->DeleteFile(fname);
    if (!s.ok()) return s;
    LOG_DEBUG << "[REMOVE] " << fname;
  }

  // ROUND2: rewrite the sampels > clean log txn.
  end = cur_samples_log_seq_ - 1;
  for (int i = 0; i <= end; i++) {
    std::string fname =
        dir_ + std::string("/") + HEAD_SAMPLES_LOG_NAME + std::to_string(i);
    if (!env->FileExists(fname)) continue;

    ::leveldb::SequentialFile* sf;
    s = env->NewSequentialFile(fname, &sf);
    if (!s.ok()) {
      std::cout << "NewSequentialFile " << s.ToString() << std::endl;
      return s;
    }
    ::leveldb::log::Reader r(sf, nullptr, false, 0);
    ::leveldb::Slice record;
    std::string scratch;
    while (r.ReadRecord(&record, &scratch)) {
      if (record.data()[0] == leveldb::log::kSample) {
        std::vector<::leveldb::log::RefSample> rs;
        std::vector<::leveldb::log::RefSample> reserve;
        ::leveldb::log::samples(record, &rs);

        for (size_t j = 0; j < rs.size(); j++) {
          MMapMemSeries* s = series_->get_ptr_by_id(rs[j].ref);
          if (s && rs[j].txn > s->log_clean_txn_) reserve.push_back(rs[j]);
        }
        if (!reserve.empty()) refsamples.push_back(reserve);
      } else if (record.data()[0] == leveldb::log::kGroupSample) {
        ::leveldb::log::RefGroupSample rs;
        ::leveldb::log::group_sample(record, &rs);
        MMapMemGroup* s = groups_->get_ptr_by_id(rs.ref);
        if (s && rs.txn > s->log_clean_txn_) groupsamples.push_back(rs);
      }
    }
    delete sf;

    s = env->DeleteFile(fname);
    if (!s.ok()) return s;
    LOG_DEBUG << "[REMOVE] " << fname;

    if (!refsamples.empty() || !groupsamples.empty()) {
      ::leveldb::WritableFile* f;
      s = env->NewAppendableFile(fname, &f);
      if (!s.ok()) {
        std::cout << "NewAppendableFile " << s.ToString() << std::endl;
        return s;
      }
      leveldb::log::Writer* w = new ::leveldb::log::Writer(f);

      for (size_t j = 0; j < refsamples.size(); j++) {
        std::string rec = ::leveldb::log::samples(refsamples[j]);
        s = w->AddRecord(rec);
        if (!s.ok()) return s;
      }
      for (size_t j = 0; j < groupsamples.size(); j++) {
        std::string rec = ::leveldb::log::group_sample(groupsamples[j]);
        s = w->AddRecord(rec);
        if (!s.ok()) return s;
      }

      refsamples.clear();
      groupsamples.clear();
      delete f;
      delete w;
      LOG_DEBUG << "[ADD] " << fname;
    } else
      active_samples_logs_--;
  }
  return s;
}

void MMapHeadWithTrie::purge_time(int64_t timestamp) {
  int purge_ts = series_->purge_time(timestamp, posting_list.get());
  int purge_groups = groups_->purge_time(timestamp, posting_list.get());
  LOG_DEBUG << "# TS purged: " << purge_ts
            << " # Groups purged: " << purge_groups;
}

void MMapHeadWithTrie::update_min_max_time(int64_t mint, int64_t maxt) {
  while (true) {
    int64_t lt = min_time.get();
    if (mint >= lt || valid_time.get() >= mint) break;
    if (min_time.cas(lt, mint)) break;
  }
  while (true) {
    int64_t ht = max_time.get();
    if (maxt <= ht) break;
    if (max_time.cas(ht, maxt)) break;
  }
}

std::unique_ptr<db::AppenderInterface> MMapHeadWithTrie::head_appender(
    bool write_flush_mark) {
  return std::unique_ptr<db::AppenderInterface>(new MMapHeadWithTrieAppender(
      const_cast<MMapHeadWithTrie*>(this), db_, write_flush_mark
      // Set the minimum valid time to whichever is greater the head min valid
      // time or the compaciton window. This ensures that no samples will be
      // added within the compaction window to avoid races.
      ));
}

std::unique_ptr<db::AppenderInterface> MMapHeadWithTrie::appender(
    bool write_flush_mark) {
  return head_appender(write_flush_mark);
}

std::unique_ptr<MMapHeadWithTrieAppender> MMapHeadWithTrie::TEST_appender() {
  return std::unique_ptr<MMapHeadWithTrieAppender>(
      new MMapHeadWithTrieAppender(const_cast<MMapHeadWithTrie*>(this), db_));
}

leveldb::Status MMapHeadWithTrie::write_flush_marks(
    const std::vector<::leveldb::log::RefFlush>& marks) {
  leveldb::Status st;
  if (flushes_log_writer_) {
    for (const auto& mark : marks) {
      std::string rec = ::leveldb::log::flush(mark);
      st = flushes_log_writer_->AddRecord(rec);
      if (!st.ok()) return st;

      if ((mark.ref >> 63) == 0) {
        MMapMemSeries* s = series_->get_ptr_by_id(mark.ref);
        if (s) s->log_clean_txn_ = mark.txn;
      } else {
        MMapMemGroup* g = groups_->get_ptr_by_id(mark.ref);
        if (g) g->log_clean_txn_ = mark.txn;
      }
    }
  }
  return st;
}

leveldb::Status MMapHeadWithTrie::try_extend_samples_logs() {
  leveldb::Status s;
  if (samples_log_writer_ &&
      samples_log_writer_->write_off() > MAX_HEAD_SAMPLES_LOG_SIZE) {
    cur_samples_log_seq_++;
    ::leveldb::WritableFile* f;
    ::leveldb::Env* env = ::leveldb::Env::Default();
    s = env->NewAppendableFile(dir_ + "/" + HEAD_SAMPLES_LOG_NAME +
                                   std::to_string(cur_samples_log_seq_),
                               &f);
    if (!s.ok()) {
      delete f;
      return s;
    }
    samples_log_file_.reset(f);
    samples_log_writer_.reset(new ::leveldb::log::Writer(f));

    active_samples_logs_++;
  }
  return s;
}

leveldb::Status MMapHeadWithTrie::try_extend_flushes_logs() {
  leveldb::Status s;
  if (flushes_log_writer_ &&
      flushes_log_writer_->write_off() > MAX_HEAD_SAMPLES_LOG_SIZE) {
    cur_flushes_log_seq_++;
    ::leveldb::WritableFile* f;
    ::leveldb::Env* env = ::leveldb::Env::Default();
    s = env->NewAppendableFile(dir_ + "/" + HEAD_FLUSHES_LOG_NAME +
                                   std::to_string(cur_flushes_log_seq_),
                               &f);
    if (!s.ok()) {
      delete f;
      return s;
    }
    flushes_log_file_.reset(f);
    flushes_log_writer_.reset(new ::leveldb::log::Writer(f));
  }
  return s;
}

// tombstones returns a TombstoneReader over the block's deleted data.
std::pair<std::unique_ptr<tombstone::TombstoneReaderInterface>, bool>
MMapHeadWithTrie::tombstones() const {
  return {std::unique_ptr<tombstone::TombstoneReaderInterface>(
              new tombstone::MemTombstones()),
          true};
}

// init_time initializes a head with the first timestamp. This only needs to be
// called for a completely fresh head with an empty WAL. Returns true if the
// initialization took an effect.
bool MMapHeadWithTrie::init_time(int64_t t) {
  if (!min_time.cas(std::numeric_limits<int64_t>::max(), t)) return false;
  // Ensure that max time is initialized to at least the min time we just set.
  // Concurrent appenders may already have set it to a higher value.
  max_time.cas(std::numeric_limits<int64_t>::min(), t);
  return true;
}

// If there are 2 threads calling this function at the same time,
// it can be the situation that the 2 threads both generate an id.
// But only one will be finally push into StripeSeries, and the other id
// and its corresponding std::shared_ptr<MMapMemSeries> will be abandoned.
//
// In a word, this function is thread-safe.
std::pair<MMapMemSeries*, bool> MMapHeadWithTrie::get_or_create(
    uint64_t hash, const label::Labels& lset) {
  MMapMemSeries* s = series_->get_ptr_by_hash(hash, lset);
  if (s) return {s, false};
  // Optimistically assume that we are the first one to create the series.
  uint64_t id = last_series_id.incrementAndGet();
#if USE_MMAP_LABELS
  return get_or_create_with_id(id, hash, lset, true, -1);
#else
  return get_or_create_with_id(id, hash, lset, true);
#endif
}

std::pair<MMapMemGroup*, bool> MMapHeadWithTrie::get_or_create_group(
    uint64_t hash, const label::Labels& lset) {
  MMapMemGroup* s = groups_->get_ptr_by_hash(hash, lset);
  if (s) return {s, false};
  // Optimistically assume that we are the first one to create the series.
  uint64_t id = last_series_id.incrementAndGet() | ((uint64_t)(1) << 63);
#if USE_MMAP_LABELS
  return get_or_create_group_with_id(id, hash, lset, true, -1);
#else
  return get_or_create_group_with_id(id, hash, lset, true);
#endif
}

#if USE_MMAP_LABELS
std::pair<MMapMemSeries*, bool> MMapHeadWithTrie::get_or_create_with_id(
    uint64_t id, uint64_t hash, const label::Labels& lset, bool alloc_mmap_slot,
    int64_t mmap_labels_idx) {
  std::pair<MMapMemSeries*, bool> s2 = series_->get_or_set(
      this, hash, id, lset, alloc_mmap_slot, mmap_labels_idx);
#else
std::pair<MMapMemSeries*, bool> MMapHeadWithTrie::get_or_create_with_id(
    uint64_t id, uint64_t hash, const label::Labels& lset,
    bool alloc_mmap_slot) {
  std::pair<MMapMemSeries*, bool> s2 =
      series_->get_or_set(this, hash, id, lset, alloc_mmap_slot);
#endif

  if (!s2.second) return {s2.first, false};

  posting_list->add(s2.first->ref, lset);

  base::RWLockGuard lock(mutex_, 1);
  for (const label::Label& l : lset) {
    label_names_->update(l.label.c_str(), l.label.size(), 0);
    std::string tmp = l.label + label::HEAD_LABEL_SEP + l.value;
    label_values_->update(tmp.c_str(), tmp.size(), 0);

    symbols_->update(l.label.c_str(), l.label.size(), 0);
    symbols_->update(l.value.c_str(), l.value.size(), 0);
  }

  return {s2.first, true};
}

#if USE_MMAP_LABELS
std::pair<MMapMemGroup*, bool> MMapHeadWithTrie::get_or_create_group_with_id(
    uint64_t id, uint64_t hash, const label::Labels& group_lset,
    bool alloc_mmap_slot, int64_t mmap_labels_idx) {
  std::pair<MMapMemGroup*, bool> s2 = groups_->get_or_set(
      this, hash, id, group_lset, alloc_mmap_slot, mmap_labels_idx);
#else
std::pair<MMapMemGroup*, bool> MMapHeadWithTrie::get_or_create_group_with_id(
    uint64_t id, uint64_t hash, const label::Labels& group_lset,
    bool alloc_mmap_slot) {
  std::pair<MMapMemGroup*, bool> s2 =
      groups_->get_or_set(this, hash, id, group_lset, alloc_mmap_slot);
#endif

  if (!s2.second) return {s2.first, false};

  posting_list->add(s2.first->ref, group_lset);

  base::RWLockGuard lock(mutex_, 1);
  for (const label::Label& l : group_lset) {
    label_names_->update(l.label.c_str(), l.label.size(), 0);
    std::string tmp = l.label + label::HEAD_LABEL_SEP + l.value;
    label_values_->update(tmp.c_str(), tmp.size(), 0);

    symbols_->update(l.label.c_str(), l.label.size(), 0);
    symbols_->update(l.value.c_str(), l.value.size(), 0);
  }

  return {s2.first, true};
}

std::set<std::string> MMapHeadWithTrie::symbols() {
  int result;
  char* line = nullptr;
  std::set<std::string> r;
  size_t from = 0, len = 0, l;

  base::RWLockGuard lock(mutex_, 0);
  result = symbols_->begin(from, len);
  while (result != cedar::da<char>::CEDAR_NO_PATH) {
    if (line == nullptr) {
      l = len * 2;
      line = new char[l];
    } else if (len > l) {
      l = len * 2;
      delete line;
      line = new char[l];
    }
    symbols_->suffix(line, len, from);
    r.insert(std::string(line, len));
    result = symbols_->next(from, len);
  }
  delete line;
  return r;
}

// Return empty SerializedTuples when error
std::vector<std::string> MMapHeadWithTrie::label_values(
    const std::string& name) {
  std::vector<std::string> vec;

  base::RWLockGuard lock(mutex_, 0);
  size_t from = 0, pos = 0;
  if (label_values_->find(name.c_str(), from, pos) !=
      cedar::da<int>::CEDAR_NO_PATH) {
    size_t len = name.size();
    int result = label_values_->begin(from, len);
    size_t l = len * 2;
    char* line = new char[l];
    label_values_->suffix(line, len, from);
    if (memcmp(line + name.size(), label::HEAD_LABEL_SEP.c_str(),
               label::HEAD_LABEL_SEP.size()) == 0)
      vec.emplace_back(line + name.size() + label::HEAD_LABEL_SEP.size(),
                       len - name.size() - label::HEAD_LABEL_SEP.size());
    while (true) {
      result = label_values_->next(from, len);
      if (result == cedar::da<int>::CEDAR_NO_PATH) break;
      if (len > l) {
        delete line;
        l = len * 2;
        line = new char[l];
      }
      label_values_->suffix(line, len, from);
      if (memcmp(line, name.c_str(), name.size()) != 0) break;
      if (memcmp(line + name.size(), label::HEAD_LABEL_SEP.c_str(),
                 label::HEAD_LABEL_SEP.size()) == 0)
        vec.emplace_back(line + name.size() + label::HEAD_LABEL_SEP.size(),
                         len - name.size() - label::HEAD_LABEL_SEP.size());
    }
    delete line;
  }

  return vec;
}

// postings returns the postings list iterator for the label pair.
std::pair<std::unique_ptr<index::PostingsInterface>, bool>
MMapHeadWithTrie::postings(const std::string& name,
                           const std::string& value) const {
  auto p = posting_list->get(name, value);
  if (p)
    return {std::move(p), true};
  else
    return {nullptr, false};
}

bool MMapHeadWithTrie::series(uint64_t ref, label::Labels& lset,
                              std::string* chunk_contents) {
  MMapMemSeries* s = series_->get_ptr_by_id(ref);
  if (!s) {
    LOG_ERROR << "not existed, series id: " << ref;
    return false;
  }

  s->read_lock();
#if USE_MMAP_LABELS
  s->labels_->get_labels(s->labels_idx_, lset);
#else
  lset.insert(lset.end(), s->labels.begin(), s->labels.end());
#endif

  chunk_contents->append(reinterpret_cast<const char*>(s->bstream_->bytes()),
                         s->bstream_->size());
  s->read_unlock();

  return true;
}

bool MMapHeadWithTrie::group(
    uint64_t ref, const std::vector<::tsdb::label::MatcherInterface*>& l,
    std::vector<int>* slots, label::Labels* group_lset,
    std::vector<label::Labels>* individual_lsets, std::string* chunk_contents) {
  MMapMemGroup* s = groups_->get_ptr_by_id(ref);
  if (!s) {
    LOG_ERROR << "not existed, group id: " << ref;
    return false;
  }

  s->read_lock();
  s->query(l, slots, group_lset, individual_lsets, chunk_contents);
  s->read_unlock();
  return true;
}

std::vector<std::string> MMapHeadWithTrie::label_names() const {
  base::RWLockGuard lock(mutex_, 0);
  std::vector<std::string> names;
  size_t from = 0, len = 0;
  int result = label_names_->begin(from, len);
  if (result != cedar::da<int>::CEDAR_NO_PATH) {
    size_t l = len * 2;
    char* line = new char[l];
    label_names_->suffix(line, len, from);
    if (!(len >= label::ALL_POSTINGS_KEYS.label.size() &&
          memcmp(line, label::ALL_POSTINGS_KEYS.label.c_str(),
                 label::ALL_POSTINGS_KEYS.label.size()) == 0))
      names.emplace_back(line, len);
    while (true) {
      result = label_names_->next(from, len);
      if (result == cedar::da<int>::CEDAR_NO_PATH) break;
      if (len > l) {
        delete line;
        l = len * 2;
        line = new char[l];
      }
      label_names_->suffix(line, len, from);
      if (!(len >= label::ALL_POSTINGS_KEYS.label.size() &&
            memcmp(line, label::ALL_POSTINGS_KEYS.label.c_str(),
                   label::ALL_POSTINGS_KEYS.label.size()) == 0))
        names.emplace_back(line, len);
    }
    delete line;
  }

  return names;
}

std::unique_ptr<index::PostingsInterface> MMapHeadWithTrie::sorted_postings(
    std::unique_ptr<index::PostingsInterface>&& p) {
  // TODO(Alec), choice between vector and deque depends on later benchmark.
  // Current concern relies on the size of passed in Postings.

  std::vector<MMapMemSeries*> series;

  while (p->next()) {
    MMapMemSeries* s = series_->get_ptr_by_id(p->at());
    if (!s) {
      LOG_DEBUG << "msg=\"looked up series not found\"";
    } else {
      series.push_back(s);
    }
  }
  std::sort(series.begin(), series.end(),
            [](const MMapMemSeries* lhs, const MMapMemSeries* rhs) {
#if USE_MMAP_LABELS
              label::Labels lset1, lset2;
              lhs->labels_->get_labels(lhs->labels_idx_, lset1);
              rhs->labels_->get_labels(rhs->labels_idx_, lset2);
              return label::lbs_compare(lset1, lset2) < 0;
#else
              return label::lbs_compare(lhs->labels, rhs->labels) < 0;
#endif
            });

  // Avoid copying twice.
  index::VectorPostings* vp = new index::VectorPostings(series.size());
  for (auto const& s : series) vp->push_back(s->ref);
  // std::cerr << "vp size: " << vp->size() << std::endl;
  // std::unique_ptr<index::PostingsInterface> pp(vp);
  // while(vp->next())
  //     std::cerr << "vp: " << vp->at() << std::endl;
  return std::unique_ptr<index::PostingsInterface>(vp);
}

}  // namespace head
}  // namespace tsdb