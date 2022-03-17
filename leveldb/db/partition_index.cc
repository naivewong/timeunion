#include "db/partition_index.h"

#include <boost/filesystem.hpp>
#include <map>

#include "chunk/XORAppender.hpp"
#include "cloud/S3File.hpp"
#include "head/HeadUtils.hpp"
#include "index/VectorPostings.hpp"

namespace leveldb {

int INDEX2_SHARDING_FACTOR = 1024;
int MEM_TUPLE_SIZE = 8;
int DISK_TUPLE_SIZE = 32;
int CLOUD_TUPLE_SIZE = 32;

/**********************************************
 *             PartitionMemIndex              *
 **********************************************/
void PartitionMemIndex::add(uint64_t id, const ::tsdb::label::Labels& lset,
                            int64_t time) {
  {
    MemSeriesMeta* s = new MemSeriesMeta(id, lset);
    s->add(time);
    uint64_t i = id & ::tsdb::head::STRIPE_MASK;
    ::tsdb::base::PadRWLockGuard lock_i(locks[i], 1);
    series_[i][id] = std::unique_ptr<MemSeriesMeta>(s);
  }
  posting_list->add(id, lset);

  ::tsdb::base::RWLockGuard lock(mutex_, 1);
  for (const ::tsdb::label::Label& l : lset) {
    label_values_[l.label].insert(l.value);

    symbols_.insert(l.label);
    symbols_.insert(l.value);
  }
}

void PartitionMemIndex::add(uint64_t id, int64_t time) {
  uint64_t i = id & ::tsdb::head::STRIPE_MASK;
  ::tsdb::base::PadRWLockGuard lock_i(locks[i], 1);
  series_[i][id]->add(time);
}

std::unique_ptr<::tsdb::tsdbutil::StringTuplesInterface>
PartitionMemIndex::label_values(const std::string& name) {
  ::tsdb::tsdbutil::ListStringTuples* l;
  {
    ::tsdb::base::RWLockGuard lock(mutex_, 0);
    if (label_values_.find(name) == label_values_.end()) return nullptr;
    l = new ::tsdb::tsdbutil::ListStringTuples(label_values_[name].size());
    for (auto const& s : label_values_[name]) l->push_back(s);
  }
  l->sort();

  return std::unique_ptr<::tsdb::tsdbutil::StringTuplesInterface>(l);
}

std::pair<std::unique_ptr<::tsdb::index::PostingsInterface>, bool>
PartitionMemIndex::postings(const std::string& name, const std::string& value) {
  auto p = posting_list->get(name, value);
  if (p)
    return {std::move(p), true};
  else
    return {nullptr, false};
}

bool PartitionMemIndex::series(uint64_t id, ::tsdb::label::Labels& lset,
                               std::vector<int64_t>& timestamps) {
  uint64_t i = id & ::tsdb::head::STRIPE_MASK;
  ::tsdb::base::PadRWLockGuard lock_i(locks[i], 0);
  if (series_[i].find(id) == series_[i].end()) return false;

  auto s = series_[i][id].get();
  lset.insert(lset.end(), s->labels.begin(), s->labels.end());
  timestamps.insert(timestamps.end(), s->time_boundaries.begin(),
                    s->time_boundaries.end());
  return true;
}

std::vector<std::string> PartitionMemIndex::label_names() {
  ::tsdb::base::RWLockGuard lock(mutex_, 0);
  std::vector<std::string> names;
  names.reserve(label_values_.size());
  for (auto& p : label_values_) names.push_back(p.first);
  return names;
}

std::unique_ptr<::tsdb::index::PostingsInterface>
PartitionMemIndex::sorted_postings(
    std::unique_ptr<::tsdb::index::PostingsInterface>&& p) {
  std::vector<MemSeriesMeta*> series;

  while (p->next()) {
    uint64_t i = p->at() & ::tsdb::head::STRIPE_MASK;
    ::tsdb::base::PadRWLockGuard lock_i(locks[i], 0);
    if (series_[i].find(p->at()) != series_[i].end())
      series.push_back(series_[i][p->at()].get());
  }
  std::sort(series.begin(), series.end(),
            [](const MemSeriesMeta* lhs, const MemSeriesMeta* rhs) {
              return ::tsdb::label::lbs_compare(lhs->labels, rhs->labels) < 0;
            });

  // Avoid copying twice.
  ::tsdb::index::VectorPostings* vp =
      new ::tsdb::index::VectorPostings(series.size());
  for (auto const& s : series) vp->push_back(s->ref);
  return std::unique_ptr<::tsdb::index::PostingsInterface>(vp);
}

void PartitionMemIndex::clear() {
  for (size_t i = 0; i < series_.size(); i++) {
    ::tsdb::base::PadRWLockGuard lock_i(locks[i], 1);
    series_[i].clear();
  }
  ::tsdb::base::RWLockGuard lock(mutex_, 1);
  symbols_.clear();
  label_values_.clear();
  posting_list->clear();
}

/**********************************************
 *                MMapIntArray                *
 **********************************************/
CloudTimeseriesFile::CloudTimeseriesFile(const std::string& dir, uint64_t tsid,
                                         int patch,
                                         ::tsdb::cloud::S3Wrapper* wrapper)
    : dir_(dir), tsid_(tsid), s3_wrapper_(wrapper) {
  Env* env = Env::Default();
  std::cout << dir << " " << tsid << " " << patch << std::endl;
  if (patch != -1)
    s_ = env->NewRandomAccessFile(
        dir + "/" + std::to_string(tsid) + "-patch" + std::to_string(patch),
        &b_);
  else
    s_ = env->NewRandomAccessFile(dir + "/" + std::to_string(tsid), &b_);
  if (!s_.ok()) {
    delete b_;
    b_ = nullptr;
    return;
  }
}

void CloudTimeseriesFile::init() {
  ::tsdb::base::MutexLockGuard lock(mutex_);
  if (!timestamps_.empty()) return;

  char tmp_b[4];
  Slice tmp_s;
  s_ = b_->Read(0, 4, &tmp_s, tmp_b);
  if (!s_.ok()) return;
  num_tuples_ = DecodeFixed32(tmp_s.data());
  if ((tsid_ >> 63) == 0) {
    timestamps_.reserve(num_tuples_);
    char* tmp_b2 = new char[num_tuples_ * 12];
    s_ = b_->Read(4, 12 * num_tuples_, &tmp_s, tmp_b);
    if (!s_.ok()) {
      std::cout << "CloudTimeseriesFile::init Read fails" << std::endl;
      delete tmp_b2;
      return;
    }
    for (uint32_t i = 0; i < num_tuples_; i++) {
      timestamps_.push_back(DecodeFixed64(tmp_s.data() + i * 12));
      offsets_.push_back(DecodeFixed32(tmp_s.data() + i * 12 + 8));
    }
    delete tmp_b2;
  } else {
    timestamps_.reserve(num_tuples_);
    char* tmp_b2 = new char[num_tuples_ * 12];
    s_ = b_->Read(4, 12 * num_tuples_, &tmp_s, tmp_b);
    if (!s_.ok()) {
      std::cout << "CloudTimeseriesFile::init Read fails" << std::endl;
      delete tmp_b2;
      return;
    }
    for (uint32_t i = 0; i < num_tuples_; i++) {
      timestamps_.push_back(DecodeFixed64(tmp_s.data() + i * 12));
      offsets_.push_back(DecodeFixed32(tmp_s.data() + i * 12 + 8));
    }
    delete tmp_b2;
  }
}

Status CloudTimeseriesFile::read(size_t off, size_t len, Slice* s, char* buf) {
  s_ = b_->Read(off, len, s, buf);
  return s_;
}

std::vector<int64_t> CloudTimeseriesFile::timestamps() {
  if (!timestamps_.empty()) return timestamps_;

  init();
  return timestamps_;
}

/**********************************************
 *        CloudTimeseriesFileIterator         *
 **********************************************/
CloudTimeseriesFileIterator::CloudTimeseriesFileIterator(CloudTimeseriesFile* f,
                                                         int64_t st, int64_t mt)
    : f_(f), starting_time_(st), max_time_(mt), buf_(nullptr), init_(false) {
  if (f_ && f_->timestamps_.empty()) f_->init();
}

bool CloudTimeseriesFileIterator::read_chunk() const {
  if (buf_) delete buf_;
  size_t s;
  if (idx_ == f_->timestamps_.size() - 1)
    s = f_->b_->FileSize() - f_->offsets_[idx_];
  else
    s = f_->offsets_[idx_ + 1] - f_->offsets_[idx_];
  buf_ = new char[s];
  f_->read(f_->offsets_[idx_], s, &s_, buf_);
  if (!f_->status().ok()) return false;
  iter_.reset();
  chunk_.reset(new ::tsdb::chunk::XORChunk(
      reinterpret_cast<const uint8_t*>(s_.data()), s_.size()));
  iter_ = std::move(chunk_->xor_iterator());
  return true;
}

bool CloudTimeseriesFileIterator::seek(int64_t t) const {
  if (!f_ || !f_->status().ok() || t > max_time_) return false;

  // Find upper bound for mint.
  // Note(Alec), elements[idx - 1] <= mint.
  int mid;
  int low = 0;
  int high = f_->timestamps_.size();
  while (low < high) {
    mid = low + (high - low) / 2;
    if (t >= f_->timestamps_[mid])
      low = mid + 1;
    else
      high = mid;
  }
  int left = low;
  if (left != 0) left = low - 1;

  idx_ = left;
  init_ = true;
  while (true) {
    if (!read_chunk()) return false;

    while (iter_->next()) {
      if (iter_->at().first >= t) return true;
    }
    idx_++;
    if (idx_ == f_->timestamps_.size()) return false;
  }
  return false;
}

bool CloudTimeseriesFileIterator::next() const {
  if (!f_ || !f_->status().ok()) return false;

  if (!init_) {
    int mid;
    int low = 0;
    int high = f_->timestamps_.size();
    while (low < high) {
      mid = low + (high - low) / 2;
      if (starting_time_ >= f_->timestamps_[mid])
        low = mid + 1;
      else
        high = mid;
    }
    if (low != 0) low--;

    idx_ = low;
    init_ = true;
    while (true) {
      if (!read_chunk()) return false;

      while (iter_->next()) {
        if (iter_->at().first > max_time_)
          return false;
        else if (iter_->at().first >= starting_time_)
          return true;
      }
      idx_++;
      if (idx_ == f_->timestamps_.size()) return false;
    }
    return false;
  }

  if (!iter_->next()) {
    if (idx_ == f_->timestamps_.size() - 1) return false;

    idx_++;
    if (!read_chunk()) return false;

    if (iter_->next()) {
      if (iter_->at().first > max_time_)
        return false;
      else
        return true;
    } else
      return false;
  } else {
    if (iter_->at().first > max_time_) return false;
  }
  return true;
}

/**********************************************
 *      CloudTimeseriesFileGroupIterator      *
 **********************************************/
CloudTimeseriesFileGroupIterator::CloudTimeseriesFileGroupIterator(
    CloudTimeseriesFile* f, int slot, int64_t st, int64_t mt)
    : f_(f),
      slot_(slot),
      starting_time_(st),
      max_time_(mt),
      buf_(nullptr),
      init_(false) {
  if (f_ && f_->timestamps_.empty()) f_->init();
}

bool CloudTimeseriesFileGroupIterator::read_chunk() const {
  if (buf_) delete buf_;

  size_t s;
  if (idx_ == f_->timestamps_.size() - 1)
    s = f_->b_->FileSize() - f_->offsets_[idx_];
  else
    s = f_->offsets_[idx_ + 1] - f_->offsets_[idx_];

  buf_ = new char[s];
  f_->read(f_->offsets_[idx_], s, &s_, buf_);
  if (!f_->status().ok()) return false;
  iter_.reset();
  chunk_.reset(new ::tsdb::chunk::NullSupportedXORGroupChunk(
      reinterpret_cast<const uint8_t*>(s_.data()), s_.size()));
  iter_ = std::move(chunk_->iterator(slot_));
  return true;
}

bool CloudTimeseriesFileGroupIterator::seek(int64_t t) const {
  if (!f_ || !f_->status().ok() || t > max_time_) return false;

  // Find upper bound for mint.
  // Note(Alec), elements[idx - 1] <= mint.
  int mid;
  int low = 0;
  int high = f_->timestamps_.size();
  while (low < high) {
    mid = low + (high - low) / 2;
    if (t >= f_->timestamps_[mid])
      low = mid + 1;
    else
      high = mid;
  }
  int left = low;
  if (left != 0) left = low - 1;

  idx_ = left;
  init_ = true;
  while (true) {
    if (!read_chunk()) return false;

    while (iter_->next()) {
      if (iter_->at().second == std::numeric_limits<double>::max())
        continue;
      else if (iter_->at().first >= t)
        return true;
    }
    idx_++;
    if (idx_ == f_->timestamps_.size()) return false;
  }
  return false;
}

bool CloudTimeseriesFileGroupIterator::next() const {
  if (!f_ || !f_->status().ok()) return false;

  if (!init_) {
    int mid;
    int low = 0;
    int high = f_->timestamps_.size();
    while (low < high) {
      mid = low + (high - low) / 2;
      if (starting_time_ >= f_->timestamps_[mid])
        low = mid + 1;
      else
        high = mid;
    }
    if (low != 0) low--;

    idx_ = low;
    init_ = true;
    while (true) {
      if (!read_chunk()) {
        // std::cout << f_->status().ToString() << std::endl;
        return false;
      }

      while (iter_->next()) {
        if (iter_->at().first > max_time_)
          return false;
        else if (iter_->at().second == std::numeric_limits<double>::max())
          continue;
        else if (iter_->at().first >= starting_time_)
          return true;
      }
      idx_++;
      if (idx_ == f_->timestamps_.size()) return false;
    }
    return false;
  }

  if (!iter_->next()) {
    if (idx_ == f_->timestamps_.size() - 1) return false;

    idx_++;
    if (!read_chunk()) return false;

    while (iter_->next()) {
      if (iter_->at().first > max_time_)
        return false;
      else if (iter_->at().second == std::numeric_limits<double>::max())
        continue;
      return true;
    }
    return false;
  } else {
    if (iter_->at().first > max_time_) return false;
  }
  return true;
}

/**********************************************
 *          PartitionCloudIndexReader         *
 **********************************************/
CloudTimeseriesFile* PartitionCloudIndexReader::get_timeseries_file(
    uint64_t tsid, int patch) {
  ::tsdb::base::MutexLockGuard lock(mutex_);
  if (m_.find(tsid) != m_.end() && m_[tsid].find(patch) != m_[tsid].end())
    return m_[tsid][patch];

  CloudTimeseriesFile* f;
  if (table_cache_) {
    Cache::Handle* handle = nullptr;
    s_ = table_cache_->FindCloudTimeseriesFile(dir_, tsid, patch, &handle);
    if (!s_.ok()) return nullptr;
    f = reinterpret_cast<CloudTimeseriesFile*>(table_cache_->Value(handle));
    m_handles_[tsid][patch] = handle;
  } else
    f = new CloudTimeseriesFile(dir_, tsid, patch, s3_wrapper_);
  m_[tsid][patch] = f;
  return f;
}

int PartitionCloudIndexReader::check_patches(uint64_t tsid) {
  ::tsdb::base::MutexLockGuard lock(mutex_);
  if (patches_num_.find(tsid) != patches_num_.end()) return patches_num_[tsid];

  std::string meta_name = dir_ + "/" + std::to_string(tsid) + "-meta";
  uint32_t n = 0;
  if (env_->FileExists(meta_name)) {
    RandomAccessFile* meta;
    Status s = env_->NewRandomAccessFile(meta_name, &meta);
    if (!s.ok()) {
      delete meta;
      return 0;
    }
    char num[4];
    Slice num_s;
    s = meta->Read(0, 4, &num_s, num);
    if (!s.ok()) {
      delete meta;
      return 0;
    }
    n = DecodeFixed32(num_s.data());
    delete meta;
  }
  patches_num_[tsid] = n;
  return n;
}

tsdb::querier::SeriesIteratorInterface* PartitionCloudIndexReader::iterator(
    uint64_t tsid, int64_t st, int64_t mt) {
  CloudTimeseriesFileIterator* it =
      new CloudTimeseriesFileIterator(get_timeseries_file(tsid, -1), st, mt);

  int num_patches = check_patches(tsid);
  if (num_patches == 0) return it;
  tsdb::querier::MergeSeriesIterator* mit =
      new tsdb::querier::MergeSeriesIterator();
  mit->push_back(it);
  for (int patch = 0; patch < num_patches; patch++)
    mit->push_back(new CloudTimeseriesFileIterator(
        get_timeseries_file(tsid, patch), st, mt));
  return mit;
}

tsdb::querier::SeriesIteratorInterface* PartitionCloudIndexReader::iterator(
    uint64_t gid, int slot, int64_t st, int64_t mt) {
  CloudTimeseriesFileGroupIterator* it = new CloudTimeseriesFileGroupIterator(
      get_timeseries_file(gid, -1), slot, st, mt);

  int num_patches = check_patches(gid);
  if (num_patches == 0) return it;
  tsdb::querier::MergeSeriesIterator* mit =
      new tsdb::querier::MergeSeriesIterator();
  mit->push_back(it);
  for (int patch = 0; patch < num_patches; patch++)
    mit->push_back(new CloudTimeseriesFileGroupIterator(
        get_timeseries_file(gid, patch), slot, st, mt));
  return mit;
}

}  // namespace leveldb