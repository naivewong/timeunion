#ifndef HEADAPPENDER_H
#define HEADAPPENDER_H

#include <deque>
#include <iostream>

#include "base/TimeStamp.hpp"
#include "db/AppenderInterface.hpp"
#include "head/Head.hpp"
#include "head/HeadUtils.hpp"
#include "head/HeadWithTrie.hpp"
#include "head/MMapHeadWithTrie.hpp"
#include "leveldb/db.h"
#include "leveldb/db/log_writer.h"
#include "leveldb/db/partition_index.h"
#include "leveldb/write_batch.h"
#include "tsdbutil/RecordEncoder.hpp"
#include "tsdbutil/tsdbutils.hpp"

namespace tsdb {
namespace head {

// TODO(Alec), determine if lock is needed.
class HeadAppender : public db::AppenderInterface {
 private:
  Head* head;
  leveldb::DB* db;

  std::vector<::leveldb::log::RefSeries> series;
  std::deque<tsdbutil::RefSample> samples;

 public:
  HeadAppender(Head* head, leveldb::DB* db) : head(head), db(db) {}

  std::pair<uint64_t, leveldb::Status> add(const label::Labels& lset, int64_t t,
                                           double v) {
    // if (t < min_valid_time) return {0, ErrOutOfBounds};

    std::pair<std::shared_ptr<MemSeries>, bool> s =
        head->get_or_create(label::lbs_hash(lset), lset);
    if (s.second) series.emplace_back(s.first->ref, lset);

    return {s.first->ref, add_fast(s.first->ref, t, v)};
  }

  leveldb::Status add_fast(uint64_t ref, int64_t t, double v) {
    // if (t < min_valid_time) return ErrOutOfBounds;

    auto s = head->series_->get_ptr_by_id(ref);
    if (!s) return leveldb::Status::NotFound("unknown series");

    samples.emplace_back(ref, t, v, s);
    return leveldb::Status::OK();
  }

  leveldb::Status commit() {
    // auto start = base::TimeStamp::now();
    leveldb::Status err = log();
    if (!err.ok()) return leveldb::Status::IOError("write to WAL");
    // LOG_DEBUG << "log duration=" <<
    // base::timeDifference(base::TimeStamp::now(), start); LOG_DEBUG << "before
    // append"; auto start = base::TimeStamp::now();
    for (tsdbutil::RefSample& s : samples) {
      // base::MutexLockGuard lock(s.series_ptr->mutex_);
      s.series_ptr->write_lock();
      if (s.series_ptr->append(db, s.t, s.v)) {
        // if (s.t < min_time) min_time = s.t;
        // if (s.t > max_time) max_time = s.t;
      }
      // s.series_ptr->pending_commit = false;
      s.series_ptr->write_unlock();
    }
    // LOG_DEBUG << "append duration=" <<
    // base::timeDifference(base::TimeStamp::now(), start); LOG_DEBUG << "before
    // clean";
    series.clear();
    samples.clear();
    // head->update_min_max_time(min_time, max_time);
    return leveldb::Status::OK();
  }

  leveldb::Status rollback() {
    // for (tsdbutil::RefSample &s : samples) {
    // base::MutexLockGuard lock(s.series->mutex_);
    // s.series_ptr->write_lock();
    // s.series->pending_commit = false;
    // s.series_ptr->write_unlock();
    // }

    // Series are created in the head memory regardless of rollback. Thus we
    // have to log them to the WAL in any case.
    samples.clear();
    return log();
  }

  leveldb::Status log() {
    if (!head->log_writer_) return leveldb::Status::OK();

    if (!series.empty()) {
      std::string rec = ::leveldb::log::series(series);
      ::leveldb::Status s = head->log_writer_->AddRecord(rec);
      if (!s.ok()) return leveldb::Status::IOError("log series");
    }
    // NOTE(Alec), may not need to write samples.
    // if (!samples.empty()) {
    //   rec.clear();
    //   tsdbutil::RecordEncoder::samples(samples, rec);
    //   // LOG_DEBUG << "after RecordEncoder::samples()";
    //   leveldb::Status err = head->wal->log(rec);
    //   // LOG_DEBUG << "after wal->log()";
    //   if (err) return error::wrap(err, "log series");
    // }
    return leveldb::Status::OK();
  }
};

class HeadWithTrieAppender : public db::AppenderInterface {
 private:
  HeadWithTrie* head;
  leveldb::DB* db;

  std::vector<::leveldb::log::RefSeries> series;
  std::vector<tsdbutil::RefSample> samples;

 public:
  HeadWithTrieAppender(HeadWithTrie* head, leveldb::DB* db)
      : head(head), db(db) {}

  std::pair<uint64_t, leveldb::Status> add(const label::Labels& lset, int64_t t,
                                           double v) {
    std::pair<std::shared_ptr<MemSeries>, bool> s =
        head->get_or_create(label::lbs_hash(lset), lset);
    if (s.second) series.emplace_back(s.first->ref, lset);

    return {s.first->ref, add_fast(s.first->ref, t, v)};
  }

  leveldb::Status add_fast(uint64_t ref, int64_t t, double v) {
    auto s = head->series_->get_ptr_by_id(ref);
    if (!s) return leveldb::Status::NotFound("unknown series");

    samples.emplace_back(ref, t, v, s->flushed_txn_++, s);
    return leveldb::Status::OK();
  }

  leveldb::Status commit() {
    leveldb::Status err = log();
    if (!err.ok()) return leveldb::Status::IOError("write to WAL");
    ::leveldb::Status st;
    for (tsdbutil::RefSample& s : samples) {
      // base::MutexLockGuard lock(s.series_ptr->mutex_);
      s.series_ptr->write_lock();
      bool flushed = s.series_ptr->append(db, s.t, s.v);
      s.series_ptr->write_unlock();

      if (flushed && head->samples_log_writer_) {
        ::leveldb::log::RefFlush f(s.series_ptr->ref, s.txn);
        std::string rec = ::leveldb::log::flush(f);
        st = head->samples_log_writer_->AddRecord(rec);
        if (!st.ok()) return st;
      }
    }
    series.clear();
    samples.clear();
    return leveldb::Status::OK();
  }

  leveldb::Status TEST_commit() {
    leveldb::Status err = log();
    if (!err.ok()) return leveldb::Status::IOError("write to WAL");
    ::leveldb::Status st;
    for (tsdbutil::RefSample& s : samples) {
      // base::MutexLockGuard lock(s.series_ptr->mutex_);
      s.series_ptr->write_lock();
      bool flushed = s.series_ptr->TEST_append(s.t, s.v);
      s.series_ptr->write_unlock();

      if (flushed && head->samples_log_writer_) {
        ::leveldb::log::RefFlush f(s.series_ptr->ref, s.txn);
        std::string rec = ::leveldb::log::flush(f);
        st = head->samples_log_writer_->AddRecord(rec);
        if (!st.ok()) return st;
      }
    }
    series.clear();
    samples.clear();
    return leveldb::Status::OK();
  }

  leveldb::Status rollback() {
    samples.clear();
    return log();
  }

  leveldb::Status log() {
    if (!head->log_writer_) return leveldb::Status::OK();

    if (!series.empty()) {
      std::string rec = ::leveldb::log::series(series);
      ::leveldb::Status s = head->log_writer_->AddRecord(rec);
      if (!s.ok()) return s;
    }
    if (!samples.empty()) {
      std::string rec = ::leveldb::log::samples(samples);
      ::leveldb::Status s = head->samples_log_writer_->AddRecord(rec);
      if (!s.ok()) return s;

      if (head->samples_log_writer_->write_off() > MAX_HEAD_SAMPLES_LOG_SIZE) {
        head->cur_samples_log_seq_++;
        ::leveldb::WritableFile* f;
        ::leveldb::Env* env = ::leveldb::Env::Default();
        s = env->NewAppendableFile(
            head->dir_ + "/" + HEAD_SAMPLES_LOG_NAME +
                std::to_string(head->cur_samples_log_seq_),
            &f);
        if (!s.ok()) {
          delete f;
          return s;
        }
        head->samples_log_file_.reset(f);
        head->samples_log_writer_.reset(new ::leveldb::log::Writer(f));
      }
    }
    return leveldb::Status::OK();
  }
};

class MMapHeadWithTrieAppender : public db::AppenderInterface {
 private:
  MMapHeadWithTrie* head;
  leveldb::DB* db;

  std::vector<::leveldb::log::RefSeries> series;
  std::vector<::leveldb::log::RefGroup> groups;
  std::vector<tsdbutil::MMapRefSample> samples;
  std::vector<tsdbutil::MMapRefGroupSample> group_samples;

  bool write_flush_mark_;

  std::string rec_buf;

#if COMMIT_BATCH_WRITE
  std::vector<std::string> keys_batch;
  std::vector<std::string> values_batch;
#endif

 public:
  MMapHeadWithTrieAppender(MMapHeadWithTrie* head, leveldb::DB* db,
                           bool write_flush_mark = true)
      : head(head), db(db), write_flush_mark_(write_flush_mark) {
    samples.reserve(leveldb::MEM_TUPLE_SIZE);
  }

  std::pair<uint64_t, leveldb::Status> add(const label::Labels& lset, int64_t t,
                                           double v) {
    std::pair<MMapMemSeries*, bool> s =
        head->get_or_create(label::lbs_hash(lset), lset);
    if (s.second) series.emplace_back(s.first->ref, lset);

    return {s.first->ref, add_fast(s.first->ref, t, v)};
  }

  leveldb::Status add(const label::Labels& group_lset,
                      const std::vector<label::Labels>& individual_lsets,
                      int64_t timestamp, const std::vector<double>& values,
                      uint64_t* gid, std::vector<int>* slots) {
    std::pair<MMapMemGroup*, bool> s =
        head->get_or_create_group(label::lbs_hash(group_lset), group_lset);

    groups.emplace_back(s.first->ref, group_lset, individual_lsets);

    *gid = s.first->ref;
    std::vector<int> new_ts_idx;
    s.first->get_indices_with_labels(individual_lsets, slots, &new_ts_idx, true,
                                     -1);

    group_samples.emplace_back(s.first->ref, *slots, timestamp, values,
                               ++s.first->flushed_txn_, s.first);

    if (!new_ts_idx.empty()) {
      std::unordered_set<std::string> tags;
      base::RWLockGuard lock(head->mutex_, 1);
      for (int i : new_ts_idx) {
        for (const label::Label& l : individual_lsets[i]) {
          std::string tmp = l.label + label::HEAD_LABEL_SEP + l.value;
          if (tags.find(tmp) == tags.end()) {
            head->posting_list->add(s.first->ref, l);
            tags.insert(tmp);
          }
          head->label_names_->update(l.label.c_str(), l.label.size(), 0);
          head->label_values_->update(tmp.c_str(), tmp.size(), 0);

          head->symbols_->update(l.label.c_str(), l.label.size(), 0);
          head->symbols_->update(l.value.c_str(), l.value.size(), 0);
        }
      }
    }

    return leveldb::Status::OK();
  }

  leveldb::Status add(uint64_t ref, int64_t timestamp,
                      const std::vector<double>& values) {
    auto s = head->groups_->get_ptr_by_id(ref);
    if (!s) return leveldb::Status::NotFound("unknown group");

    assert(s->value_slots_.size() == values.size());
    group_samples.emplace_back(ref, timestamp, values, s->flushed_txn_++, s);
    return leveldb::Status::OK();
  }

  leveldb::Status add(uint64_t ref, const std::vector<int>& slots,
                      int64_t timestamp, const std::vector<double>& values) {
    auto s = head->groups_->get_ptr_by_id(ref);
    if (!s) return leveldb::Status::NotFound("unknown group");

    group_samples.emplace_back(ref, slots, timestamp, values, ++s->flushed_txn_,
                               s);
    return leveldb::Status::OK();
  }

  leveldb::Status add_fast(uint64_t ref, int64_t t, double v) {
    auto s = head->series_->get_ptr_by_id(ref);
    if (!s) return leveldb::Status::NotFound("unknown series");

    samples.emplace_back(ref, t, v, ++s->flushed_txn_, s);
    return leveldb::Status::OK();
  }

  leveldb::Status commit() {
    leveldb::Status st = log();
    if (!st.ok()) {
      LOG_DEBUG << "error write to WAL";
      return st;
    }
    for (tsdbutil::MMapRefSample& s : samples) {
      // base::MutexLockGuard lock(s.series_ptr->mutex_);
      s.series_ptr->write_lock();

#if COMMIT_BATCH_WRITE
      bool flushed =
          s.series_ptr->append(db, s.t, s.v, s.txn, &keys_batch, &values_batch);
#else
      bool flushed = s.series_ptr->append(db, s.t, s.v, s.txn);
#endif

      s.series_ptr->write_unlock();

      if (flushed && write_flush_mark_ && head->flushes_log_writer_) {
        ::leveldb::log::RefFlush f(s.series_ptr->ref, s.txn);
        rec_buf.clear();
        ::leveldb::log::flush(f, &rec_buf);
        st = head->flushes_log_writer_->AddRecord(rec_buf);
        if (!st.ok()) {
          LOG_DEBUG << "error log flush";
          return st;
        }

        st = head->try_extend_flushes_logs();
        if (!st.ok()) return st;
      }
    }
    for (tsdbutil::MMapRefGroupSample& s : group_samples) {
      bool flushed;
      s.group_ptr->write_lock();

#if COMMIT_BATCH_WRITE
      if (s.slots.empty())
        flushed = s.group_ptr->append(db, s.t, s.v, s.txn, &keys_batch,
                                      &values_batch);
      else
        flushed = s.group_ptr->append(db, s.slots, s.t, s.v, s.txn, &keys_batch,
                                      &values_batch);
#else
      if (s.slots.empty())
        flushed = s.group_ptr->append(db, s.t, s.v, s.txn);
      else
        flushed = s.group_ptr->append(db, s.slots, s.t, s.v, s.txn);
#endif

      s.group_ptr->write_unlock();

      if (flushed && write_flush_mark_ && head->flushes_log_writer_) {
        ::leveldb::log::RefFlush f(s.group_ptr->ref, s.txn);
        rec_buf.clear();
        ::leveldb::log::flush(f, &rec_buf);
        st = head->flushes_log_writer_->AddRecord(rec_buf);
        if (!st.ok()) {
          LOG_DEBUG << "error log flush";
          return st;
        }

        st = head->try_extend_flushes_logs();
        if (!st.ok()) return st;
      }
    }
    series.clear();
    groups.clear();
    samples.clear();
    group_samples.clear();

#if COMMIT_BATCH_WRITE
    if (!keys_batch.empty()) {
      leveldb::WriteBatch batch;
      for (size_t i = 0; i < keys_batch.size(); i++)
        batch.Put(keys_batch[i], values_batch[i]);
      st = db->Write(leveldb::WriteOptions(), &batch);
      if (!st.ok()) {
        LOG_DEBUG << "error write batch";
        return st;
      }
      keys_batch.clear();
      values_batch.clear();
    }
#endif

    return leveldb::Status::OK();
  }

  leveldb::Status TEST_commit() {
    leveldb::Status st = log();
    if (!st.ok()) return st;

    for (tsdbutil::MMapRefSample& s : samples) {
      // base::MutexLockGuard lock(s.series_ptr->mutex_);
      s.series_ptr->write_lock();
      bool flushed = s.series_ptr->TEST_append(s.t, s.v);
      s.series_ptr->write_unlock();

      if (flushed && head->flushes_log_writer_) {
        ::leveldb::log::RefFlush f(s.series_ptr->ref, s.txn);
        rec_buf.clear();
        ::leveldb::log::flush(f, &rec_buf);
        st = head->flushes_log_writer_->AddRecord(rec_buf);
        if (!st.ok()) return st;

        st = head->try_extend_flushes_logs();
        if (!st.ok()) return st;
      }
    }
    for (tsdbutil::MMapRefGroupSample& s : group_samples) {
      s.group_ptr->write_lock();
      bool flushed;
      if (s.slots.empty())
        flushed = s.group_ptr->TEST_append(s.t, s.v);
      else
        flushed = s.group_ptr->TEST_append(s.slots, s.t, s.v);
      s.group_ptr->write_unlock();

      if (flushed && head->flushes_log_writer_) {
        ::leveldb::log::RefFlush f(s.group_ptr->ref, s.txn);
        rec_buf.clear();
        ::leveldb::log::flush(f, &rec_buf);
        st = head->flushes_log_writer_->AddRecord(rec_buf);
        if (!st.ok()) return st;

        st = head->try_extend_flushes_logs();
        if (!st.ok()) return st;
      }
    }
    series.clear();
    groups.clear();
    samples.clear();
    group_samples.clear();
    return leveldb::Status::OK();
  }

  leveldb::Status rollback() {
    samples.clear();
    group_samples.clear();
    return log();
  }

  leveldb::Status log() {
    if (!head->log_writer_) return leveldb::Status::OK();

    if (!series.empty()) {
      rec_buf.clear();
      ::leveldb::log::series(series, &rec_buf);
      ::leveldb::Status s = head->log_writer_->AddRecord(rec_buf);
      if (!s.ok()) return s;
    }
    if (!groups.empty()) {
      for (const auto& g : groups) {
        rec_buf.clear();
        ::leveldb::log::group(g, &rec_buf);
        ::leveldb::Status s = head->log_writer_->AddRecord(rec_buf);
        if (!s.ok()) return s;
      }
    }
    if (!samples.empty() && head->samples_log_writer_) {
      rec_buf.clear();
      ::leveldb::log::samples(samples, &rec_buf);
      ::leveldb::Status s = head->samples_log_writer_->AddRecord(rec_buf);
      if (!s.ok()) return s;

      s = head->try_extend_samples_logs();
      if (!s.ok()) return s;
    }
    if (!group_samples.empty() && head->samples_log_writer_) {
      for (const auto& sample : group_samples) {
        rec_buf.clear();
        ::leveldb::log::group_sample(sample, &rec_buf);
        ::leveldb::Status s = head->samples_log_writer_->AddRecord(rec_buf);
        if (!s.ok()) return s;

        s = head->try_extend_samples_logs();
        if (!s.ok()) return s;
      }
    }
    return leveldb::Status::OK();
  }
};

}  // namespace head
}  // namespace tsdb

#endif