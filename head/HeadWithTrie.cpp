#include "head/HeadWithTrie.hpp"

#include <algorithm>
#include <boost/bind.hpp>
#include <iostream>
#include <limits>

#include "base/Logging.hpp"
#include "head/HeadAppender.hpp"
#include "head/HeadSeriesSet.hpp"
#include "head/HeadUtils.hpp"
#include "head/InitAppender.hpp"
#include "index/VectorPostings.hpp"
#include "leveldb/db/log_reader.h"
#include "leveldb/db/log_writer.h"
#include "leveldb/third_party/thread_pool.h"
#include "querier/QuerierUtils.hpp"
#include "tombstone/MemTombstones.hpp"
#include "tsdbutil/RecordDecoder.hpp"
#include "tsdbutil/RecordEncoder.hpp"

namespace tsdb {
namespace head {

HeadWithTrie::HeadWithTrie(uint64_t last_series_id, const std::string& dir,
                           const std::string& idx_dir, ::leveldb::DB* db)
    : dir_(dir), last_series_id(last_series_id), db_(db) {
  min_time.getAndSet(std::numeric_limits<int64_t>::max());
  max_time.getAndSet(std::numeric_limits<int64_t>::min());

#if USE_PERSISTENT_CEDAR
  symbols_.reset(new pcedar::da<char>(idx_dir, "sym_"));
  label_names_.reset(new pcedar::da<char>(idx_dir, "ln_"));
  label_values_.reset(new pcedar::da<char>(idx_dir, "lv_"));
#else
  symbols_.reset(new cedar::da<char>());
  label_names_.reset(new cedar::da<char>());
  label_values_.reset(new cedar::da<char>());
#endif

  series_ = std::unique_ptr<StripeSeries>(new StripeSeries());
  posting_list = std::unique_ptr<index::MemPostingsWithTrie>(
      new index::MemPostingsWithTrie());
  if (!dir_.empty()) {
    Timer t;
    t.start();
    recover_index_from_log();
    LOG_DEBUG << "[recover_index_from_log()] duration:" << t.since_start_nano()
              << " nano seconds";
    t.start();
    recover_samples_from_log();
    LOG_DEBUG << "[recover_samples_from_log()] duration:"
              << t.since_start_nano() << " nano seconds";
  }
}

leveldb::Status HeadWithTrie::recover_index_from_log() {
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
      std::vector<::leveldb::log::RefSeries> rs;
      bool success = ::leveldb::log::series(record, &rs);
      if (!success)
        return ::leveldb::Status::Corruption("recover_index_from_log");

      // Recover index.
      for (size_t j = 0; j < rs.size(); j++)
        get_or_create_with_id(rs[j].ref, label::lbs_hash(rs[j].lset),
                              rs[j].lset);
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

leveldb::Status HeadWithTrie::recover_samples_from_log() {
  // LOG_DEBUG << "Recover samples from log";
  ::leveldb::Env* env = ::leveldb::Env::Default();
  ::leveldb::Status s;
  uint64_t fsize = 0;

  std::vector<std::string> existing_logs;
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
                 HEAD_SAMPLES_LOG_NAME.size()) == 0) {
        existing_logs.push_back(current_file);
      }
    }
  }
  std::sort(existing_logs.begin(), existing_logs.end(),
            [&](const std::string& l, const std::string& r) {
              return std::stoi(l.substr(HEAD_SAMPLES_LOG_NAME.size())) <
                     std::stoi(r.substr(HEAD_SAMPLES_LOG_NAME.size()));
            });

  // First round to load the newest flushing txn.
  for (size_t i = 0; i < existing_logs.size(); i++) {
    ::leveldb::SequentialFile* sf;
    s = env->NewSequentialFile(dir_ + "/" + existing_logs[i], &sf);
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
        MemSeries* s = series_->get_ptr_by_id(f.ref);
        if (s) s->flushed_txn_ = f.txn;
      }
    }
    delete sf;
  }

  // Secpnd round to load the unflushed samples.
  for (size_t i = 0; i < existing_logs.size(); i++) {
    ::leveldb::SequentialFile* sf;
    s = env->NewSequentialFile(dir_ + "/" + existing_logs[i], &sf);
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
          MemSeries* s = series_->get_ptr_by_id(rs[j].ref);
          if (s && rs[j].txn > s->flushed_txn_) {
            s->append(db_, rs[j].t, rs[j].v);
          }
        }
      }
    }
    delete sf;
  }

  cur_samples_log_seq_ = existing_logs.size();

  ::leveldb::WritableFile* f;
  s = env->NewAppendableFile(
      dir_ + "/" + HEAD_SAMPLES_LOG_NAME + std::to_string(cur_samples_log_seq_),
      &f);
  if (!s.ok()) {
    delete f;
    return s;
  }
  samples_log_file_.reset(f);
  samples_log_writer_.reset(new ::leveldb::log::Writer(f));
  return ::leveldb::Status::OK();
}

void HeadWithTrie::update_min_max_time(int64_t mint, int64_t maxt) {
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

std::unique_ptr<db::AppenderInterface> HeadWithTrie::head_appender() {
  return std::unique_ptr<db::AppenderInterface>(new HeadWithTrieAppender(
      const_cast<HeadWithTrie*>(this), db_
      // Set the minimum valid time to whichever is greater the head min valid
      // time or the compaciton window. This ensures that no samples will be
      // added within the compaction window to avoid races.
      ));
}

std::unique_ptr<db::AppenderInterface> HeadWithTrie::appender() {
  // The head cache might not have a starting point yet. The init appender
  // picks up the first appended timestamp as the base.
  // LOG_DEBUG << MinTime() << " " << std::numeric_limits<int64_t>::max();
  // if (MinTime() == std::numeric_limits<int64_t>::max()) {
  //   return std::unique_ptr<db::AppenderInterface>(
  //       new InitAppender(const_cast<Head *>(this)));
  // }
  return head_appender();
}

std::unique_ptr<HeadWithTrieAppender> HeadWithTrie::TEST_appender() {
  return std::unique_ptr<HeadWithTrieAppender>(
      new HeadWithTrieAppender(const_cast<HeadWithTrie*>(this), db_));
}

// tombstones returns a TombstoneReader over the block's deleted data.
std::pair<std::unique_ptr<tombstone::TombstoneReaderInterface>, bool>
HeadWithTrie::tombstones() const {
  return {std::unique_ptr<tombstone::TombstoneReaderInterface>(
              new tombstone::MemTombstones()),
          true};
}

// init_time initializes a head with the first timestamp. This only needs to be
// called for a completely fresh head with an empty WAL. Returns true if the
// initialization took an effect.
bool HeadWithTrie::init_time(int64_t t) {
  if (!min_time.cas(std::numeric_limits<int64_t>::max(), t)) return false;
  // Ensure that max time is initialized to at least the min time we just set.
  // Concurrent appenders may already have set it to a higher value.
  max_time.cas(std::numeric_limits<int64_t>::min(), t);
  return true;
}

// If there are 2 threads calling this function at the same time,
// it can be the situation that the 2 threads both generate an id.
// But only one will be finally push into StripeSeries, and the other id
// and its corresponding std::shared_ptr<MemSeries> will be abandoned.
//
// In a word, this function is thread-safe.
std::pair<std::shared_ptr<MemSeries>, bool> HeadWithTrie::get_or_create(
    uint64_t hash, const label::Labels& lset) {
  std::shared_ptr<MemSeries> s = series_->get_by_hash(hash, lset);
  if (s) return {s, false};
  // Optimistically assume that we are the first one to create the series.
  uint64_t id = last_series_id.incrementAndGet();
  return get_or_create_with_id(id, hash, lset);
}

std::pair<std::shared_ptr<MemSeries>, bool> HeadWithTrie::get_or_create_with_id(
    uint64_t id, uint64_t hash, const label::Labels& lset) {
  std::shared_ptr<MemSeries> s1(new MemSeries(lset, id));

  std::pair<std::shared_ptr<MemSeries>, bool> s2 =
      series_->get_or_set(hash, s1);
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

std::set<std::string> HeadWithTrie::symbols() {
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
std::vector<std::string> HeadWithTrie::label_values(const std::string& name) {
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
HeadWithTrie::postings(const std::string& name,
                       const std::string& value) const {
  auto p = posting_list->get(name, value);
  if (p)
    return {std::move(p), true};
  else
    return {nullptr, false};
}

bool HeadWithTrie::series(uint64_t ref, label::Labels& lset,
                          std::string* chunk_contents) {
  std::shared_ptr<MemSeries> s = series_->get_by_id(ref);
  if (!s) {
    LOG_ERROR << "not existed, series id: " << ref;
    return false;
  }
  lset.insert(lset.end(), s->labels.begin(), s->labels.end());

  s->read_lock();
#if HEAD_USE_XORCHUNK
  chunk_contents->append(reinterpret_cast<const char*>(s->chunk.bytes()),
                         s->chunk.size());
#else
  chunk::XORChunk chunk;
  auto appender = chunk.appender();
  for (size_t i = 0; i < s->times_.size(); i++)
    appender->append(s->times_[i], s->values_[i]);
  chunk_contents->append(reinterpret_cast<const char*>(chunk.bytes()),
                         chunk.size());
#endif
  s->read_unlock();
  return true;
}

std::vector<std::string> HeadWithTrie::label_names() const {
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

std::unique_ptr<index::PostingsInterface> HeadWithTrie::sorted_postings(
    std::unique_ptr<index::PostingsInterface>&& p) {
  // TODO(Alec), choice between vector and deque depends on later benchmark.
  // Current concern relies on the size of passed in Postings.

  std::vector<std::shared_ptr<MemSeries>> series;

  while (p->next()) {
    std::shared_ptr<MemSeries> s = series_->get_by_id(p->at());
    if (!s) {
      LOG_DEBUG << "msg=\"looked up series not found\"";
    } else {
      series.push_back(s);
    }
  }
  std::sort(series.begin(), series.end(),
            [](const std::shared_ptr<MemSeries>& lhs,
               const std::shared_ptr<MemSeries>& rhs) {
              return label::lbs_compare(lhs->labels, rhs->labels) < 0;
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