#include "head/Head.hpp"

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
#include "querier/QuerierUtils.hpp"
#include "tombstone/MemTombstones.hpp"
#include "tsdbutil/RecordDecoder.hpp"
#include "tsdbutil/RecordEncoder.hpp"

namespace tsdb {
namespace head {

Head::Head(uint64_t last_series_id, const std::string &dir, ::leveldb::DB *db)
    : dir_(dir), last_series_id(last_series_id), db_(db) {
  min_time.getAndSet(std::numeric_limits<int64_t>::max());
  max_time.getAndSet(std::numeric_limits<int64_t>::min());
  series_ = std::unique_ptr<StripeSeries>(new StripeSeries());
  posting_list = std::unique_ptr<index::MemPostings>(new index::MemPostings());
  if (!dir_.empty()) recover_log();
}

::leveldb::Status Head::recover_log() {
  ::leveldb::Env *env = ::leveldb::Env::Default();
  ::leveldb::Status s;
  uint64_t fsize = 0;
  if (env->FileExists(dir_ + "/" + HEAD_LOG_NAME)) {
    ::leveldb::SequentialFile *sf;
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
      if (!success) return ::leveldb::Status::Corruption("recover_log");

      // Recover index.
      for (size_t j = 0; j < rs.size(); j++)
        get_or_create_with_id(rs[j].ref, label::lbs_hash(rs[j].lset),
                              rs[j].lset);
    }
    delete sf;

    env->GetFileSize(dir_ + "/" + HEAD_LOG_NAME, &fsize);
  }

  ::leveldb::WritableFile *f;
  s = env->NewAppendableFile(dir_ + "/" + HEAD_LOG_NAME, &f);
  if (!s.ok()) {
    delete f;
    return s;
  }
  log_file_.reset(f);
  log_writer_.reset(new ::leveldb::log::Writer(f, fsize));
  return ::leveldb::Status::OK();
}

void Head::update_min_max_time(int64_t mint, int64_t maxt) {
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

std::unique_ptr<db::AppenderInterface> Head::head_appender() {
  return std::unique_ptr<db::AppenderInterface>(new HeadAppender(
      const_cast<Head *>(this), db_
      // Set the minimum valid time to whichever is greater the head min valid
      // time or the compaciton window. This ensures that no samples will be
      // added within the compaction window to avoid races.
      ));
}

std::unique_ptr<db::AppenderInterface> Head::appender() {
  return head_appender();
}

// tombstones returns a TombstoneReader over the block's deleted data.
std::pair<std::unique_ptr<tombstone::TombstoneReaderInterface>, bool>
Head::tombstones() const {
  return {std::unique_ptr<tombstone::TombstoneReaderInterface>(
              new tombstone::MemTombstones()),
          true};
}

// init_time initializes a head with the first timestamp. This only needs to be
// called for a completely fresh head with an empty WAL. Returns true if the
// initialization took an effect.
bool Head::init_time(int64_t t) {
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
std::pair<std::shared_ptr<MemSeries>, bool> Head::get_or_create(
    uint64_t hash, const label::Labels &lset) {
  std::shared_ptr<MemSeries> s = series_->get_by_hash(hash, lset);
  if (s) return {s, false};
  // Optimistically assume that we are the first one to create the series.
  uint64_t id = last_series_id.incrementAndGet();
  return get_or_create_with_id(id, hash, lset);
}

std::pair<std::shared_ptr<MemSeries>, bool> Head::get_or_create_with_id(
    uint64_t id, uint64_t hash, const label::Labels &lset) {
  std::shared_ptr<MemSeries> s1(new MemSeries(lset, id));

  std::pair<std::shared_ptr<MemSeries>, bool> s2 =
      series_->get_or_set(hash, s1);
  if (!s2.second) return {s2.first, false};

  posting_list->add(s2.first->ref, lset);

  base::RWLockGuard lock(mutex_, 1);
  for (const label::Label &l : lset) {
    label_values_[l.label].insert(l.value);

    symbols_.insert(l.label);
    symbols_.insert(l.value);
  }

  return {s2.first, true};
}

void rebuild_symbols_lvals(const label::Label &l, const index::ListPostings &p,
                           Head *head) {
  if (l.label != label::ALL_POSTINGS_KEYS.label) {
    head->symbols_.insert(l.label);
    head->symbols_.insert(l.value);
    head->label_values_[l.label].insert(l.value);
  }
}

std::set<std::string> Head::symbols() {
  base::RWLockGuard lock(mutex_, 0);
  return std::set<std::string>(symbols_.begin(), symbols_.end());
}

// Return empty SerializedTuples when error
std::vector<std::string> Head::label_values(const std::string &name) {
  std::vector<std::string> l;
  {
    base::RWLockGuard lock(mutex_, 0);
    if (label_values_.find(name) == label_values_.end()) return l;

    for (auto const &s : label_values_[name]) l.push_back(s);
  }
  std::sort(l.begin(), l.end());

  return l;
}

// postings returns the postings list iterator for the label pair.
std::pair<std::unique_ptr<index::PostingsInterface>, bool> Head::postings(
    const std::string &name, const std::string &value) const {
  auto p = posting_list->get(name, value);
  if (p)
    return {std::move(p), true};
  else
    return {nullptr, false};
}

bool Head::series(uint64_t ref, label::Labels &lset,
                  std::string *chunk_contents) {
  std::shared_ptr<MemSeries> s = series_->get_by_id(ref);
  if (!s) {
    LOG_ERROR << "not existed, series id: " << ref;
    return false;
  }
  lset.insert(lset.end(), s->labels.begin(), s->labels.end());

  s->read_lock();
#if HEAD_USE_XORCHUNK
  chunk_contents->append(reinterpret_cast<const char *>(s->chunk.bytes()),
                         s->chunk.size());
#else
  chunk::XORChunk chunk;
  auto appender = chunk.appender();
  for (size_t i = 0; i < s->times_.size(); i++)
    appender->append(s->times_[i], s->values_[i]);
  chunk_contents->append(reinterpret_cast<const char *>(chunk.bytes()),
                         chunk.size());
#endif
  s->read_unlock();
  return true;
}

std::vector<std::string> Head::label_names() const {
  base::RWLockGuard lock(mutex_, 0);
  std::vector<std::string> names;
  for (auto const &p : label_values_) {
    if (p.first != label::ALL_POSTINGS_KEYS.label) names.push_back(p.first);
  }
  std::sort(names.begin(), names.end());

  return names;
}

std::unique_ptr<index::PostingsInterface> Head::sorted_postings(
    std::unique_ptr<index::PostingsInterface> &&p) {
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
            [](const std::shared_ptr<MemSeries> &lhs,
               const std::shared_ptr<MemSeries> &rhs) {
              return label::lbs_compare(lhs->labels, rhs->labels) < 0;
            });

  // Avoid copying twice.
  index::VectorPostings *vp = new index::VectorPostings(series.size());
  for (auto const &s : series) vp->push_back(s->ref);
  return std::unique_ptr<index::PostingsInterface>(vp);
}

}  // namespace head
}  // namespace tsdb