#include "head/MemSeries.hpp"

#include <limits>

#include "base/Logging.hpp"
#include "base/TSDBException.hpp"
#include "chunk/XORAppender.hpp"
#include "chunk/XORChunk.hpp"
#include "chunk/XORIterator.hpp"
#include "db/DBUtils.hpp"
#include "db/db_impl.h"
#include "db/partition_index.h"
#include "index/IntersectPostings.hpp"
#include "index/VectorPostings.hpp"
#include "leveldb/options.h"

namespace tsdb {
namespace head {

/**********************************************
 *                 MemIterator                *
 **********************************************/
MemIterator::MemIterator(const std::shared_ptr<MemChunk>& m,
                         const Sample* buf) {
  iterator = m->chunk->iterator();
  id = -1;
  total = m->chunk->num_samples();
  sample_buf[0] = buf[0];
  sample_buf[1] = buf[1];
  sample_buf[2] = buf[2];
  sample_buf[3] = buf[3];
}

std::pair<int64_t, double> MemIterator::at() const {
  if (total - id > 4) return iterator->at();
  return {sample_buf[4 - (total - id)].t, sample_buf[4 - (total - id)].v};
}

bool MemIterator::next() const {
  if (id + 1 >= total) return false;
  ++id;
  if (total - id > 4) return iterator->next();
  return true;
}

bool MemIterator::error() const { return iterator->error(); }

/**********************************************
 *                 MemSeries                  *
 **********************************************/
MemSeries::MemSeries(const label::Labels& labels, uint64_t id)
    : mutex_(),
      ref(id),
      labels(labels),
      min_time_(std::numeric_limits<int64_t>::max()),
      max_time_(std::numeric_limits<int64_t>::min()),
      num_samples_(0),
      time_boundary_(-1),
      flushed_txn_(0) {
#if HEAD_USE_XORCHUNK
  appender.reset();
  appender = std::move(chunk.appender());
#endif
}

int64_t MemSeries::min_time() {
  if (min_time_ == std::numeric_limits<int64_t>::max())
    return std::numeric_limits<int64_t>::min();
  return min_time_;
}

int64_t MemSeries::max_time() {
  if (max_time_ == std::numeric_limits<int64_t>::min())
    return std::numeric_limits<int64_t>::max();
  return max_time_;
}

bool MemSeries::append(leveldb::DB* db, int64_t timestamp, double value) {
  int64_t time_boundary =
      timestamp / leveldb::PARTITION_LENGTH * leveldb::PARTITION_LENGTH;
  leveldb::Status s;
  bool flushed = false;
  if (time_boundary != time_boundary_ && time_boundary_ != -1) {
    std::string key;
    leveldb::encodeKey(&key, "", ref, tuple_st_);
    // Attach end mark.
    if (num_samples_ != leveldb::MEM_TUPLE_SIZE)
#if HEAD_USE_XORCHUNK
      appender->append(std::numeric_limits<int64_t>::max(), 0);
#else
    {
      times_.push_back(std::numeric_limits<int64_t>::max());
      values_.push_back(0);
    }
    chunk::XORChunk chunk;
    auto appender = chunk.appender();
    for (size_t i = 0; i < times_.size(); i++)
      appender->append(times_[i], values_[i]);
    times_.clear();
    values_.clear();
#endif
    leveldb::Slice value(reinterpret_cast<const char*>(chunk.bytes() + 2),
                         chunk.size() - 2);
    s = db->Put(leveldb::WriteOptions(), key, value);
#if HEAD_USE_XORCHUNK
    chunk = chunk::XORChunk();
    appender.reset();
    appender = std::move(chunk.appender());
#endif
    flushed = true;
    num_samples_ = 0;
    min_time_ = std::numeric_limits<int64_t>::max();
    max_time_ = std::numeric_limits<int64_t>::min();
  }
#if HEAD_USE_XORCHUNK
  appender->append(timestamp, value);
#else
  times_.push_back(timestamp);
  values_.push_back(value);
#endif
  if (min_time_ == std::numeric_limits<int64_t>::max()) {
    min_time_ = timestamp;
  }

  max_time_ = timestamp;
  time_boundary_ = time_boundary;
  if (num_samples_ == 0) tuple_st_ = timestamp;

  ++num_samples_;
  if (num_samples_ == leveldb::MEM_TUPLE_SIZE) {
#if !HEAD_USE_XORCHUNK
    chunk::XORChunk chunk;
    auto appender = chunk.appender();
    for (size_t i = 0; i < times_.size(); i++)
      appender->append(times_[i], values_[i]);
    times_.clear();
    values_.clear();
#endif
    // Insert into LevelDB.
    std::string key;
    leveldb::encodeKey(&key, "", ref, tuple_st_);
    leveldb::Slice value(reinterpret_cast<const char*>(chunk.bytes() + 2),
                         chunk.size() - 2);
    s = db->Put(leveldb::WriteOptions(), key, value);
#if HEAD_USE_XORCHUNK
    chunk = chunk::XORChunk();
    appender.reset();
    appender = std::move(chunk.appender());
#endif
    flushed = true;
    num_samples_ = 0;
    min_time_ = std::numeric_limits<int64_t>::max();
    max_time_ = std::numeric_limits<int64_t>::min();
  }

  return flushed;
}

bool MemSeries::TEST_append(int64_t timestamp, double value) {
  int64_t time_boundary =
      timestamp / leveldb::PARTITION_LENGTH * leveldb::PARTITION_LENGTH;
  leveldb::Status s;
  bool flushed = false;
  if (time_boundary != time_boundary_ && time_boundary_ != -1) {
#if HEAD_USE_XORCHUNK
    chunk = chunk::XORChunk();
    appender.reset();
    appender = std::move(chunk.appender());
#else
    times_.clear();
    values_.clear();
#endif
    flushed = true;
    num_samples_ = 0;
    min_time_ = std::numeric_limits<int64_t>::max();
    max_time_ = std::numeric_limits<int64_t>::min();
  }
#if HEAD_USE_XORCHUNK
  appender->append(timestamp, value);
#else
  times_.push_back(timestamp);
  values_.push_back(value);
#endif
  if (min_time_ == std::numeric_limits<int64_t>::max()) {
    min_time_ = timestamp;
  }

  max_time_ = timestamp;
  time_boundary_ = time_boundary;
  if (num_samples_ == 0) tuple_st_ = timestamp;

  ++num_samples_;
  if (num_samples_ == leveldb::MEM_TUPLE_SIZE) {
#if HEAD_USE_XORCHUNK
    chunk = chunk::XORChunk();
    appender.reset();
    appender = std::move(chunk.appender());
#endif
    flushed = true;
    num_samples_ = 0;
    min_time_ = std::numeric_limits<int64_t>::max();
    max_time_ = std::numeric_limits<int64_t>::min();
  }

  return flushed;
}

/**********************************************
 *                MMapMemSeries               *
 **********************************************/
#if USE_MMAP_LABELS
MMapMemSeries::MMapMemSeries(const label::Labels& labels, uint64_t id,
                             MMapXORChunk* xor_array, bool alloc_mmap_slot,
                             MMapLabels* mlabels, int64_t lidx)
    : mutex_(),
      ref(id),
      labels_(mlabels),
      labels_idx_(lidx),
      min_time_(std::numeric_limits<int64_t>::max()),
      max_time_(std::numeric_limits<int64_t>::min()),
      global_max_time_(std::numeric_limits<int64_t>::min()),
      num_samples_(0),
      time_boundary_(-1),
      xor_array_(xor_array),
      slot_(std::numeric_limits<uint64_t>::max()),
      bstream_(nullptr),
      app_(nullptr),
      flushed_txn_(0),
      log_clean_txn_(-1) {
  if (alloc_mmap_slot) {
    // Allocate slots.
    auto p = xor_array_->alloc_slot(ref);
    slot_ = p.first;
    ptr_ = p.second;
    bstream_ = new chunk::BitStreamV2(ptr_, 2);
    app_ = new chunk::XORAppenderV2(*bstream_, 0, 0, 0, 0xff, 0);
  }
  if (labels_idx_ == -1) {
    // Allocate slot.
    labels_idx_ = labels_->add_labels(id, labels);
  }
}
#else
MMapMemSeries::MMapMemSeries(const label::Labels& labels, uint64_t id,
                             MMapXORChunk* xor_array, bool alloc_mmap_slot)
    : mutex_(),
      ref(id),
      labels(labels),
      min_time_(std::numeric_limits<int64_t>::max()),
      max_time_(std::numeric_limits<int64_t>::min()),
      global_max_time_(std::numeric_limits<int64_t>::min()),
      num_samples_(0),
      time_boundary_(-1),
      xor_array_(xor_array),
      slot_(std::numeric_limits<uint64_t>::max()),
      bstream_(nullptr),
      app_(nullptr),
      flushed_txn_(0),
      log_clean_txn_(-1) {
  if (alloc_mmap_slot) {
    // Allocate slots.
    auto p = xor_array_->alloc_slot(ref);
    slot_ = p.first;
    ptr_ = p.second;
    bstream_ = new chunk::BitStreamV2(ptr_, 2);
    app_ = new chunk::XORAppenderV2(*bstream_, 0, 0, 0, 0xff, 0);
  }
}
#endif

MMapMemSeries::~MMapMemSeries() {
  if (app_) delete app_;
  if (bstream_) delete bstream_;
}

int64_t MMapMemSeries::min_time() {
  if (min_time_ == std::numeric_limits<int64_t>::max())
    return std::numeric_limits<int64_t>::min();
  return min_time_;
}

int64_t MMapMemSeries::max_time() {
  if (max_time_ == std::numeric_limits<int64_t>::min())
    return std::numeric_limits<int64_t>::max();
  return max_time_;
}

leveldb::Status MMapMemSeries::_flush(leveldb::DB* db, int64_t txn,
                                      std::vector<std::string>* flush_keys,
                                      std::vector<std::string>* flush_values) {
  key_.clear();
  leveldb::encodeKey(&key_, "", ref, tuple_st_);

  val_.clear();
  leveldb::PutVarint64(&val_, txn);
  leveldb::PutVarint64(&val_, max_time_ - min_time_);
  leveldb::PutVarint32(&val_, bstream_->size());
  val_.append(reinterpret_cast<const char*>(bstream_->bytes()),
              bstream_->size());

  leveldb::Status s;
  if (flush_keys == nullptr)
    s = db->Put(leveldb::WriteOptions(), key_, val_);
  else {
    flush_keys->push_back(key_);
    flush_values->push_back(val_);
  }

  delete app_;
  delete bstream_;
  bstream_ = new chunk::BitStreamV2(ptr_, 2);
  app_ = new chunk::XORAppenderV2(*bstream_, 0, 0, 0, 0xff, 0);

  num_samples_ = 0;
  min_time_ = std::numeric_limits<int64_t>::max();
  max_time_ = std::numeric_limits<int64_t>::min();
  return s;
}

int MMapMemSeries::_lower_bound(const std::vector<int64_t>& times,
                                int64_t target) {
  int mid, low = 0, high = times.size();

  while (low < high) {
    mid = low + (high - low) / 2;
    if (target <= times[mid])
      high = mid;
    else
      low = mid + 1;
  }

  if (low < times.size() && times[low] < target) low++;

  return low;
}

bool MMapMemSeries::append(leveldb::DB* db, int64_t timestamp, double value,
                           int64_t txn, std::vector<std::string>* flush_keys,
                           std::vector<std::string>* flush_values) {
  if (timestamp > global_max_time_) global_max_time_ = timestamp;

  int64_t time_boundary;
  if (db)
    time_boundary = timestamp / db->PartitionLength() * db->PartitionLength();
  else
    time_boundary =
        timestamp / leveldb::PARTITION_LENGTH * leveldb::PARTITION_LENGTH;
  bool flushed = false;
  if (time_boundary != time_boundary_ && time_boundary_ != -1 &&
      num_samples_ != 0) {
    // This cover both the previous partition and the next partition.
    _flush(db, txn, flush_keys, flush_values);
    flushed = true;
  }

  if (slot_ == std::numeric_limits<uint64_t>::max()) {
    auto p = xor_array_->alloc_slot(ref);
    slot_ = p.first;
    ptr_ = p.second;
    bstream_ = new chunk::BitStreamV2(ptr_, 2);
    app_ = new chunk::XORAppenderV2(*bstream_, 0, 0, 0, 0xff, 0);
  }

  if (min_time_ == std::numeric_limits<int64_t>::max()) min_time_ = timestamp;
  if (max_time_ != std::numeric_limits<int64_t>::min() &&
      timestamp <= max_time_) {
    chunk::XORIteratorV2 it(ptr_, bstream_->size());
    std::vector<int64_t> times;
    std::vector<double> vals;
    while (it.next()) {
      times.push_back(it.at().first);
      vals.push_back(it.at().second);
    }

    int idx = _lower_bound(times, timestamp);
    if (times[idx] != timestamp) {
      // Insert new sample.
      times.insert(times.begin() + idx, timestamp);
      vals.insert(vals.begin() + idx, value);

      ++num_samples_;
    } else {
      // Replace the existing slot.
      times[idx] = timestamp;
      vals[idx] = value;
    }

    delete app_;
    delete bstream_;
    bstream_ = new chunk::BitStreamV2(ptr_, 2);
    app_ = new chunk::XORAppenderV2(*bstream_, 0, 0, 0, 0xff, 0);
    for (size_t i = 0; i < times.size(); i++) app_->append(times[i], vals[i]);
  } else {
    max_time_ = timestamp;
    app_->append(timestamp, value);

    ++num_samples_;
  }

  time_boundary_ = time_boundary;
  if (num_samples_ == 1) tuple_st_ = timestamp;

  if (num_samples_ == leveldb::MEM_TUPLE_SIZE) {
    _flush(db, txn, flush_keys, flush_values);
    flushed = true;
  }

  return flushed;
}

bool MMapMemSeries::TEST_append(int64_t timestamp, double value) {
  if (timestamp > global_max_time_) global_max_time_ = timestamp;

  int64_t time_boundary =
      timestamp / leveldb::PARTITION_LENGTH * leveldb::PARTITION_LENGTH;
  leveldb::Status s;
  bool flushed = false;
  if (time_boundary != time_boundary_ && time_boundary_ != -1 &&
      num_samples_ != 0) {
    delete app_;
    delete bstream_;
    bstream_ = new chunk::BitStreamV2(ptr_, 2);
    app_ = new chunk::XORAppenderV2(*bstream_, 0, 0, 0, 0xff, 0);

    flushed = true;

    num_samples_ = 0;
    min_time_ = std::numeric_limits<int64_t>::max();
    max_time_ = std::numeric_limits<int64_t>::min();
  }

  if (slot_ == std::numeric_limits<uint64_t>::max()) {
    auto p = xor_array_->alloc_slot(ref);
    slot_ = p.first;
    ptr_ = p.second;
    bstream_ = new chunk::BitStreamV2(ptr_, 2);
    app_ = new chunk::XORAppenderV2(*bstream_, 0, 0, 0, 0xff, 0);
  }

  if (min_time_ == std::numeric_limits<int64_t>::max()) min_time_ = timestamp;
  if (max_time_ != std::numeric_limits<int64_t>::min() &&
      timestamp <= max_time_) {
    chunk::XORIteratorV2 it(ptr_, bstream_->size());
    std::vector<int64_t> times;
    std::vector<double> vals;
    while (it.next()) {
      times.push_back(it.at().first);
      vals.push_back(it.at().second);
    }

    int idx = _lower_bound(times, timestamp);
    if (times[idx] != timestamp) {
      // Insert new sample.
      times.insert(times.begin() + idx, timestamp);
      vals.insert(vals.begin() + idx, value);

      ++num_samples_;
    } else {
      // Replace the existing slot.
      times[idx] = timestamp;
      vals[idx] = value;
    }

    delete app_;
    delete bstream_;
    bstream_ = new chunk::BitStreamV2(ptr_, 2);
    app_ = new chunk::XORAppenderV2(*bstream_, 0, 0, 0, 0xff, 0);
    for (size_t i = 0; i < times.size(); i++) app_->append(times[i], vals[i]);
  } else {
    max_time_ = timestamp;
    app_->append(timestamp, value);

    ++num_samples_;
  }

  time_boundary_ = time_boundary;
  if (num_samples_ == 1) tuple_st_ = timestamp;

  if (num_samples_ == leveldb::MEM_TUPLE_SIZE) {
    delete app_;
    delete bstream_;
    bstream_ = new chunk::BitStreamV2(ptr_, 2);
    app_ = new chunk::XORAppenderV2(*bstream_, 0, 0, 0, 0xff, 0);

    flushed = true;

    num_samples_ = 0;
    min_time_ = std::numeric_limits<int64_t>::max();
    max_time_ = std::numeric_limits<int64_t>::min();
  }

  return flushed;
}

/**********************************************
 *                 MMapMemGroup               *
 **********************************************/
#if USE_MMAP_LABELS
MMapMemGroup::MMapMemGroup(const label::Labels& group_lset,
                           const std::vector<label::Labels>& individual_lsets,
                           uint64_t id, MMapGroupXORChunk* xor_array,
                           bool alloc_mmap_slot, MMapGroupLabels* mlabels,
                           int64_t labels_idx)
    : mutex_(),
      ref(id),
      labels_(mlabels),
      min_time_(std::numeric_limits<int64_t>::max()),
      max_time_(std::numeric_limits<int64_t>::min()),
      global_max_time_(std::numeric_limits<int64_t>::min()),
      num_samples_(0),
      time_boundary_(-1),
      xor_array_(xor_array),
      t_bstream_(nullptr),
      app_(nullptr),
      flushed_txn_(0),
      log_clean_txn_(-1) {
  if (alloc_mmap_slot) {
    // Allocate slots.
    auto p = xor_array_->alloc_slot(id, std::numeric_limits<uint32_t>::max());
    time_slot_ = p.first;
    t_ptr_ = p.second;
    t_bstream_ = new chunk::BitStreamV2(t_ptr_, 2);
    app_ = new chunk::NullSupportedXORGroupTimeAppenderV2(t_bstream_);
    for (size_t i = 0; i < individual_lsets.size(); i++) {
      p = xor_array_->alloc_slot(id, i);
      value_slots_.push_back(p.first);
      v_ptrs_.push_back(p.second);
      v_bstreams_.push_back(new chunk::BitStreamV2(p.second));
      apps_.push_back(
          new chunk::NullSupportedXORGroupValueAppenderV2(v_bstreams_.back()));
    }
  } else {
    time_slot_ = std::numeric_limits<uint64_t>::max();
    for (size_t i = 0; i < individual_lsets.size(); i++)
      value_slots_.push_back(std::numeric_limits<uint64_t>::max());
  }
  if (labels_idx == -1) {
    // Allocate slot.
    group_labels_idx_ = labels_->add_labels(
        id, std::numeric_limits<uint32_t>::max(), group_lset);
    for (size_t i = 0; i < individual_lsets.size(); i++) {
      individual_labels_idx_.push_back(
          labels_->add_labels(id, i, individual_lsets[i]));
    }
  } else
    group_labels_idx_ = labels_idx;
}
MMapMemGroup::MMapMemGroup(const label::Labels& group_lset, uint64_t id,
                           MMapGroupXORChunk* xor_array, bool alloc_mmap_slot,
                           MMapGroupLabels* mlabels, int64_t labels_idx)
    : mutex_(),
      ref(id),
      labels_(mlabels),
      min_time_(std::numeric_limits<int64_t>::max()),
      max_time_(std::numeric_limits<int64_t>::min()),
      global_max_time_(std::numeric_limits<int64_t>::min()),
      num_samples_(0),
      time_boundary_(-1),
      xor_array_(xor_array),
      t_bstream_(nullptr),
      app_(nullptr),
      flushed_txn_(0),
      log_clean_txn_(-1) {
  if (alloc_mmap_slot) {
    // Allocate slots.
    auto p = xor_array_->alloc_slot(id, std::numeric_limits<uint32_t>::max());
    time_slot_ = p.first;
    t_ptr_ = p.second;
    t_bstream_ = new chunk::BitStreamV2(t_ptr_, 2);
    app_ = new chunk::NullSupportedXORGroupTimeAppenderV2(t_bstream_);
  } else
    time_slot_ = std::numeric_limits<uint64_t>::max();
  if (labels_idx == -1) {
    // Allocate slot.
    group_labels_idx_ = labels_->add_labels(
        id, std::numeric_limits<uint32_t>::max(), group_lset);
  } else
    group_labels_idx_ = labels_idx;
}
uint32_t MMapMemGroup::add_timeseries(const label::Labels& lset,
                                      bool alloc_value_slot,
                                      int64_t mmap_labels_slot) {
  if (alloc_value_slot) {
    auto p = xor_array_->alloc_slot(ref, value_slots_.size());
    value_slots_.push_back(p.first);
    v_ptrs_.push_back(p.second);
    v_bstreams_.push_back(new chunk::BitStreamV2(p.second));
    apps_.push_back(
        new chunk::NullSupportedXORGroupValueAppenderV2(v_bstreams_.back()));
  } else {
    value_slots_.push_back(std::numeric_limits<uint64_t>::max());
    v_ptrs_.push_back(nullptr);
    v_bstreams_.push_back(nullptr);
    apps_.push_back(nullptr);
  }

  int64_t labels_idx;
  if (mmap_labels_slot == -1)
    labels_idx = labels_->add_labels(ref, individual_labels_idx_.size(), lset);
  else
    labels_idx = mmap_labels_slot;

  // Update inverted index.
  for (const auto& l : lset)
    inverted_index_add(l.label, l.value, value_slots_.size() - 1);

  individual_labels_idx_.push_back(labels_idx);
  return value_slots_.size() - 1;
}
#else
MMapMemGroup::MMapMemGroup(const label::Labels& group_lset,
                           const std::vector<label::Labels>& individual_lsets,
                           uint64_t id, MMapGroupXORChunk* xor_array,
                           bool alloc_mmap_slot)
    : mutex_(),
      ref(id),
      group_labels_(group_lset),
      individual_labels_(individual_lsets),
      min_time_(std::numeric_limits<int64_t>::max()),
      max_time_(std::numeric_limits<int64_t>::min()),
      global_max_time_(std::numeric_limits<int64_t>::min()),
      num_samples_(0),
      time_boundary_(-1),
      xor_array_(xor_array),
      t_bstream_(nullptr),
      app_(nullptr),
      flushed_txn_(0),
      log_clean_txn_(-1) {
  if (alloc_mmap_slot) {
    // Allocate slots.
    auto p = xor_array_->alloc_slot(id, std::numeric_limits<uint32_t>::max());
    time_slot_ = p.first;
    t_ptr_ = p.second;
    t_bstream_ = new chunk::BitStreamV2(t_ptr_, 2);
    app_ = new chunk::NullSupportedXORGroupTimeAppenderV2(t_bstream_);
    for (size_t i = 0; i < individual_lsets.size(); i++) {
      p = xor_array_->alloc_slot(id, i);
      value_slots_.push_back(p.first);
      v_ptrs_.push_back(p.second);
      v_bstreams_.push_back(new chunk::BitStreamV2(p.second));
      apps_.push_back(
          new chunk::NullSupportedXORGroupValueAppenderV2(v_bstreams_.back()));
    }
  } else
    time_slot_ = std::numeric_limits<uint64_t>::max();
}
MMapMemGroup::MMapMemGroup(const label::Labels& group_lset, uint64_t id,
                           MMapGroupXORChunk* xor_array, bool alloc_mmap_slot)
    : mutex_(),
      ref(id),
      group_labels_(group_lset),
      min_time_(std::numeric_limits<int64_t>::max()),
      max_time_(std::numeric_limits<int64_t>::min()),
      global_max_time_(std::numeric_limits<int64_t>::min()),
      num_samples_(0),
      time_boundary_(-1),
      xor_array_(xor_array),
      t_bstream_(nullptr),
      app_(nullptr),
      flushed_txn_(0),
      log_clean_txn_(-1) {
  if (alloc_mmap_slot) {
    // Allocate slots.
    auto p = xor_array_->alloc_slot(id, std::numeric_limits<uint32_t>::max());
    time_slot_ = p.first;
    t_ptr_ = p.second;
    t_bstream_ = new chunk::BitStreamV2(t_ptr_, 2);
    app_ = new chunk::NullSupportedXORGroupTimeAppenderV2(t_bstream_);
  } else
    time_slot_ = std::numeric_limits<uint64_t>::max();
}
uint32_t MMapMemGroup::add_timeseries(const label::Labels& lset,
                                      bool alloc_value_slot) {
  uint64_t value_slot = std::numeric_limits<uint64_t>::max();
  if (alloc_value_slot) {
    auto p = xor_array_->alloc_slot(ref, value_slots_.size());
    value_slots_.push_back(p.first);
    v_ptrs_.push_back(p.second);
    v_bstreams_.push_back(new chunk::BitStreamV2(p.second));
    apps_.push_back(
        new chunk::NullSupportedXORGroupValueAppenderV2(v_bstreams_.back()));
  } else {
    value_slots_.push_back(std::numeric_limits<uint64_t>::max());
    v_ptrs_.push_back(nullptr);
    v_bstreams_.push_back(nullptr);
    apps_.push_back(nullptr);
  }

  // Update inverted index.
  for (const auto& l : lset)
    inverted_index_add(l.label, l.value, value_slots_.size() - 1);

  individual_labels_.push_back(lset);
  return value_slots_.size() - 1;
}
#endif

MMapMemGroup::~MMapMemGroup() {
  if (app_) delete app_;
  if (t_bstream_) delete t_bstream_;
  for (chunk::NullSupportedXORGroupValueAppenderV2* app : apps_) {
    if (app) delete app;
  }
  for (chunk::BitStreamV2* b : v_bstreams_) {
    if (b) delete b;
  }
}

inline void MMapMemGroup::_check_time_slot_init() {
  if (time_slot_ == std::numeric_limits<uint64_t>::max()) {
    auto p = xor_array_->alloc_slot(ref, std::numeric_limits<uint32_t>::max());
    time_slot_ = p.first;
    t_ptr_ = p.second;
    t_bstream_ = new chunk::BitStreamV2(t_ptr_, 2);
    app_ = new chunk::NullSupportedXORGroupTimeAppenderV2(t_bstream_);
  }
}

inline void MMapMemGroup::_check_value_slot_init(int idx) {
  if (value_slots_[idx] == std::numeric_limits<uint64_t>::max()) {
    auto p = xor_array_->alloc_slot(ref, idx);
    value_slots_[idx] = p.first;
    v_ptrs_[idx] = p.second;
    v_bstreams_[idx] = new chunk::BitStreamV2(p.second);
    apps_[idx] =
        new chunk::NullSupportedXORGroupValueAppenderV2(v_bstreams_[idx]);
  }
}

void MMapMemGroup::encode_group(std::string* s) {
  std::vector<int> slots;
  if (*current_idx_.begin() == -1) {
    slots.reserve(value_slots_.size());
    for (int i = 0; i < value_slots_.size(); i++) slots.push_back(i);
  } else {
    slots.reserve(current_idx_.size());
    slots.insert(slots.end(), current_idx_.begin(), current_idx_.end());
  }

  chunk::NullSupportedXORGroupChunkV2::encode(s, slots, t_bstream_,
                                              v_bstreams_);
}

void MMapMemGroup::encode_group(const std::vector<int>& slots, std::string* s) {
  _check_time_slot_init();
  std::vector<chunk::BitStreamV2*> vbs;
  vbs.reserve(slots.size());
  for (int slot : slots) vbs.push_back(v_bstreams_[slot]);

  chunk::NullSupportedXORGroupChunkV2::encode(s, slots, t_bstream_, vbs);
}

int MMapMemGroup::_inverted_index_lower_bound(const std::string& s) {
  int mid, low = 0, high = tags_.size();

  while (low < high) {
    mid = low + (high - low) / 2;
    if (s.compare(tags_[mid]) <= 0)
      high = mid;
    else
      low = mid + 1;
  }

  if (low < tags_.size() && tags_[low].compare(s) < 0) low++;

  return low;
}

void MMapMemGroup::inverted_index_add(const std::string& tag_name,
                                      const std::string& tag_value, int slot) {
  std::string s = tag_name + label::HEAD_LABEL_SEP + tag_value;
  int idx = _inverted_index_lower_bound(s);
  if (idx >= tags_.size()) {
    tags_.push_back(s);
    postings_.emplace_back();
    leveldb::PutVarint32(&postings_.back(), slot);
  } else if (tags_[idx] == s) {
    leveldb::PutVarint32(&postings_[idx], slot);
  } else {
    tags_.insert(tags_.begin() + idx, s);
    postings_.insert(postings_.begin() + idx, std::string());
    leveldb::PutVarint32(&postings_[idx], slot);
  }
}

void MMapMemGroup::inverted_index_get(const std::string& tag_name,
                                      const std::string& tag_value,
                                      std::vector<int>* l) {
  std::string s = tag_name + label::HEAD_LABEL_SEP + tag_value;
  int idx = _inverted_index_lower_bound(s);
  if (idx < tags_.size() && tags_[idx] == s) {
    leveldb::Slice tmp(postings_[idx]);
    uint32_t p;
    while (tmp.size() > 0) {
      leveldb::GetVarint32(&tmp, &p);
      l->push_back(p);
    }
  }
}

bool MMapMemGroup::_match_group_labels(
    const ::tsdb::label::MatcherInterface* m) {
#if USE_MMAP_LABELS
  label::Labels tmp_lset;
  uint32_t no_op_idx;
  labels_->get_labels(group_labels_idx_, tmp_lset, &no_op_idx);
  int mid, low = 0, high = tmp_lset.size();
  label::Label l(m->name(), m->value());

  while (low < high) {
    mid = low + (high - low) / 2;
    if (l <= tmp_lset[mid])
      high = mid;
    else
      low = mid + 1;
  }

  if (low < tmp_lset.size() && tmp_lset[low] < l) low++;

  if (low < tmp_lset.size() && tmp_lset[low] == l) return true;
  return false;
#else
  int mid, low = 0, high = group_labels_.size();
  label::Label l(m->name(), m->value());

  while (low < high) {
    mid = low + (high - low) / 2;
    if (l <= group_labels_[mid])
      high = mid;
    else
      low = mid + 1;
  }

  if (low < group_labels_.size() && group_labels_[low] < l) low++;

  if (low < group_labels_.size() && group_labels_[low] == l) return true;
  return false;
#endif
}

void MMapMemGroup::query(const std::vector<::tsdb::label::MatcherInterface*>& l,
                         std::vector<int>* slots, label::Labels* group_lset,
                         std::vector<label::Labels>* individual_lsets,
                         std::string* chunk_contents) {
#if USE_MMAP_LABELS
  uint32_t no_op_idx;
  labels_->get_labels(group_labels_idx_, *group_lset, &no_op_idx);
#else
  *group_lset = group_labels_;
#endif

  bool all = false;
  std::vector<std::unique_ptr<::tsdb::index::PostingsInterface>> ps;
  ps.reserve(l.size());
  std::vector<int> tmp_slots;
  for (auto m : l) {
    if (_match_group_labels(m))
      all = true;
    else {
      tmp_slots.clear();
      inverted_index_get(m->name(), m->value(), &tmp_slots);
      index::VectorPostings* tmp_p =
          new index::VectorPostings(tmp_slots.size());
      for (int slot : tmp_slots) tmp_p->push_back(slot);
      ps.emplace_back(tmp_p);
    }
  }

  if (all && ps.empty()) {
    for (int i = 0; i < value_slots_.size(); i++) {
      slots->push_back(i);
#if USE_MMAP_LABELS
      individual_lsets->emplace_back();
      labels_->get_labels(individual_labels_idx_[i], individual_lsets->back(),
                          &no_op_idx);
#else
      individual_lsets->push_back(individual_labels_[i]);
#endif
    }
  } else {
    index::IntersectPostings1 ip(std::move(ps));
    while (ip.next()) {
      slots->push_back(ip.at());
#if USE_MMAP_LABELS
      individual_lsets->emplace_back();
      labels_->get_labels(individual_labels_idx_[ip.at()],
                          individual_lsets->back(), &no_op_idx);
#else
      individual_lsets->push_back(individual_labels_[ip.at()]);
#endif
    }
  }
  encode_group(*slots, chunk_contents);
}

void MMapMemGroup::get_indices_with_labels(
    const std::vector<label::Labels>& lsets, std::vector<int>* indices) {
#if USE_MMAP_LABELS
  label::Labels tmp_lset;
  uint32_t no_op_idx;
  for (size_t i = 0; i < lsets.size(); i++) {
    bool found = false;
    for (size_t j = 0; j < individual_labels_idx_.size(); j++) {
      tmp_lset.clear();
      labels_->get_labels(individual_labels_idx_[j], tmp_lset, &no_op_idx);
      if (label::lbs_compare(tmp_lset, lsets[i]) == 0) {
        found = true;
        indices->push_back(j);
        break;
      }
    }
    if (!found) indices->push_back(add_timeseries(lsets[i], true, -1));
  }
#else
  for (size_t i = 0; i < lsets.size(); i++) {
    bool found = false;
    for (size_t j = 0; j < individual_labels_.size(); j++) {
      if (label::lbs_compare(individual_labels_[j], lsets[i]) == 0) {
        found = true;
        indices->push_back(j);
        break;
      }
    }
    if (!found) indices->push_back(add_timeseries(lsets[i], true));
  }
#endif
}

void MMapMemGroup::get_indices_with_labels(
    const std::vector<label::Labels>& lsets, std::vector<int>* indices,
    std::vector<int>* new_ts_idx, bool alloc_value_slot, int64_t labels_idx) {
#if USE_MMAP_LABELS
  label::Labels tmp_lset;
  uint32_t no_op_idx;
  for (size_t i = 0; i < lsets.size(); i++) {
    bool found = false;
    for (size_t j = 0; j < individual_labels_idx_.size(); j++) {
      tmp_lset.clear();
      labels_->get_labels(individual_labels_idx_[j], tmp_lset, &no_op_idx);
      if (label::lbs_compare(tmp_lset, lsets[i]) == 0) {
        found = true;
        if (indices) indices->push_back(j);
        break;
      }
    }
    if (!found) {
      if (new_ts_idx) new_ts_idx->push_back(i);
      uint32_t x = add_timeseries(lsets[i], alloc_value_slot, labels_idx);
      if (indices) indices->push_back(x);
    }
  }
#else
  for (size_t i = 0; i < lsets.size(); i++) {
    bool found = false;
    for (size_t j = 0; j < individual_labels_.size(); j++) {
      if (label::lbs_compare(individual_labels_[j], lsets[i]) == 0) {
        found = true;
        if (indices) indices->push_back(j);
        break;
      }
    }
    if (!found) {
      if (new_ts_idx) new_ts_idx->push_back(i);
      uint32_t x = add_timeseries(lsets[i], alloc_value_slot);
      if (indices) indices->push_back(x);
    }
  }
#endif
}

void MMapMemGroup::TEST_flush() {
  if (*current_idx_.begin() == -1) {
    // Means current tuple contains all timeseries.
    for (size_t i = 0; i < value_slots_.size(); i++) {
      delete apps_[i];
      delete v_bstreams_[i];
      v_bstreams_[i] = new chunk::BitStreamV2(v_ptrs_[i]);
      apps_[i] =
          new chunk::NullSupportedXORGroupValueAppenderV2(v_bstreams_[i]);
    }
  } else {
    for (int i : current_idx_) {
      delete apps_[i];
      delete v_bstreams_[i];
      v_bstreams_[i] = new chunk::BitStreamV2(v_ptrs_[i]);
      apps_[i] =
          new chunk::NullSupportedXORGroupValueAppenderV2(v_bstreams_[i]);
    }
  }
  delete app_;
  delete t_bstream_;
  t_bstream_ = new chunk::BitStreamV2(t_ptr_, 2);
  app_ = new chunk::NullSupportedXORGroupTimeAppenderV2(t_bstream_);
  current_idx_.clear();

  num_samples_ = 0;
  min_time_ = std::numeric_limits<int64_t>::max();
  max_time_ = std::numeric_limits<int64_t>::min();
}

leveldb::Status MMapMemGroup::flush(leveldb::DB* db, int64_t txn,
                                    std::vector<std::string>* flush_keys,
                                    std::vector<std::string>* flush_values) {
  std::vector<int> slots;
  std::vector<chunk::BitStreamV2*> vbs;
  slots.reserve((*current_idx_.begin() == -1 ? value_slots_.size()
                                             : current_idx_.size()));
  vbs.reserve((*current_idx_.begin() == -1 ? value_slots_.size()
                                           : current_idx_.size()));
  if (*current_idx_.begin() == -1) {
    // Means current tuple contains all timeseries.
    for (size_t i = 0; i < value_slots_.size(); i++) {
      vbs.push_back(v_bstreams_[i]);
      slots.push_back(i);
    }
  } else {
    for (int i : current_idx_) {
      vbs.push_back(v_bstreams_[i]);
      slots.push_back(i);
    }
  }
  current_idx_.clear();

  std::string value;
  leveldb::PutVarint64(&value, txn);
  leveldb::PutVarint64(&value, max_time_ - min_time_);
  // Encode with group xor chunk.
  std::string tmp;
  chunk::NullSupportedXORGroupChunkV2::encode(&tmp, slots, t_bstream_, vbs);
  leveldb::PutVarint32(&value, tmp.size());
  value.append(tmp.data(), tmp.size());

  delete app_;
  delete t_bstream_;
  t_bstream_ = new chunk::BitStreamV2(t_ptr_, 2);
  app_ = new chunk::NullSupportedXORGroupTimeAppenderV2(t_bstream_);
  for (int i : slots) {
    delete apps_[i];
    delete v_bstreams_[i];
    v_bstreams_[i] = new chunk::BitStreamV2(v_ptrs_[i]);
    apps_[i] = new chunk::NullSupportedXORGroupValueAppenderV2(v_bstreams_[i]);
  }

  num_samples_ = 0;
  min_time_ = std::numeric_limits<int64_t>::max();
  max_time_ = std::numeric_limits<int64_t>::min();

  std::string key;
  leveldb::encodeKey(&key, "", ref, tuple_st_);

  leveldb::Status s;
  if (flush_keys == nullptr)
    s = db->Put(leveldb::WriteOptions(), key, value);
  else {
    flush_keys->push_back(std::move(key));
    flush_values->push_back(std::move(value));
  }
  return s;
}

int MMapMemGroup::_lower_bound(const std::vector<int64_t>& times,
                               int64_t target) {
  int mid, low = 0, high = times.size();

  while (low < high) {
    mid = low + (high - low) / 2;
    if (target <= times[mid])
      high = mid;
    else
      low = mid + 1;
  }

  if (low < times.size() && times[low] < target) low++;

  return low;
}

bool MMapMemGroup::append(leveldb::DB* db, int64_t timestamp,
                          const std::vector<double>& values, int64_t txn,
                          std::vector<std::string>* flush_keys,
                          std::vector<std::string>* flush_values) {
  if (timestamp > global_max_time_) global_max_time_ = timestamp;

  int64_t time_boundary;
  if (db)
    time_boundary = timestamp / db->PartitionLength() * db->PartitionLength();
  else
    time_boundary =
        timestamp / leveldb::PARTITION_LENGTH * leveldb::PARTITION_LENGTH;
  leveldb::Status s;
  bool flushed = false;
  if (time_boundary != time_boundary_ && time_boundary_ != -1 &&
      num_samples_ != 0) {
    s = flush(db, txn, flush_keys, flush_values);
    flushed = true;
  }

  // Fill those missing timeseries.
  if (*current_idx_.begin() != -1) {
    for (size_t i = 0; i < value_slots_.size(); i++) {
      if (current_idx_.find(i) == current_idx_.end()) {
        _check_value_slot_init(i);
        for (int j = 0; j < num_samples_; j++)
          apps_[i]->append(std::numeric_limits<double>::max());
      }
    }
    current_idx_.clear();
    current_idx_.insert(-1);
  }

  if (min_time_ == std::numeric_limits<int64_t>::max()) min_time_ = timestamp;
  if (max_time_ != std::numeric_limits<int64_t>::min() &&
      timestamp <= max_time_) {
    std::vector<int64_t> times;
    chunk::NullSupportedXORGroupTimeIterator it1(t_ptr_, t_bstream_->size());
    while (it1.next()) times.push_back(it1.at());

    std::vector<double> vals;
    vals.reserve(times.size() + 1);

    int idx = _lower_bound(times, timestamp);
    if (times[idx] != timestamp) {
      // Insert new sample.
      delete app_;
      delete t_bstream_;
      t_bstream_ = new chunk::BitStreamV2(t_ptr_, 2);
      app_ = new chunk::NullSupportedXORGroupTimeAppenderV2(t_bstream_);
      times.insert(times.begin() + idx, timestamp);
      for (int64_t t : times) app_->append(t);

      for (size_t i = 0; i < value_slots_.size(); i++) {
        chunk::NullSupportedXORGroupValueIterator it2(
            v_ptrs_[i], v_bstreams_[i]->size(), times.size() - 1);
        vals.clear();
        while (it2.next()) vals.push_back(it2.at());
        vals.insert(vals.begin() + idx, values[i]);

        delete apps_[i];
        delete v_bstreams_[i];
        v_bstreams_[i] = new chunk::BitStreamV2(v_ptrs_[i]);
        apps_[i] =
            new chunk::NullSupportedXORGroupValueAppenderV2(v_bstreams_[i]);

        for (double v : vals) apps_[i]->append(v);
      }

      ++num_samples_;
    } else {
      // Replace the existing slot.
      for (size_t i = 0; i < value_slots_.size(); i++) {
        chunk::NullSupportedXORGroupValueIterator it2(
            v_ptrs_[i], v_bstreams_[i]->size(), times.size());
        vals.clear();
        while (it2.next()) vals.push_back(it2.at());
        vals[idx] = values[i];

        delete apps_[i];
        delete v_bstreams_[i];
        v_bstreams_[i] = new chunk::BitStreamV2(v_ptrs_[i]);
        apps_[i] =
            new chunk::NullSupportedXORGroupValueAppenderV2(v_bstreams_[i]);

        for (double v : vals) apps_[i]->append(v);
      }
    }
  } else {
    max_time_ = timestamp;
    _check_time_slot_init();
    app_->append(timestamp);
    for (size_t i = 0; i < value_slots_.size(); i++) {
      _check_value_slot_init(i);
      apps_[i]->append(values[i]);
    }

    ++num_samples_;
  }

  time_boundary_ = time_boundary;
  if (num_samples_ == 1) tuple_st_ = timestamp;

  if (num_samples_ == leveldb::MEM_TUPLE_SIZE) {
    s = flush(db, txn, flush_keys, flush_values);
    flushed = true;
  }

  return flushed;
}

void MMapMemGroup::_insert_null_into_series(int i, int idx, int samples) {
  chunk::NullSupportedXORGroupValueIterator it2(
      v_ptrs_[i], v_bstreams_[i]->size(), samples);
  std::vector<double> vals;
  while (it2.next()) vals.push_back(it2.at());
  vals.insert(vals.begin() + idx, std::numeric_limits<double>::max());

  delete apps_[i];
  delete v_bstreams_[i];
  v_bstreams_[i] = new chunk::BitStreamV2(v_ptrs_[i]);
  apps_[i] = new chunk::NullSupportedXORGroupValueAppenderV2(v_bstreams_[i]);

  for (double v : vals) apps_[i]->append(v);
}

bool MMapMemGroup::append(leveldb::DB* db, const std::vector<int>& slots,
                          int64_t timestamp, const std::vector<double>& values,
                          int64_t txn, std::vector<std::string>* flush_keys,
                          std::vector<std::string>* flush_values) {
  if (timestamp > global_max_time_) global_max_time_ = timestamp;

  int64_t time_boundary;
  if (db)
    time_boundary = timestamp / db->PartitionLength() * db->PartitionLength();
  else
    time_boundary =
        timestamp / leveldb::PARTITION_LENGTH * leveldb::PARTITION_LENGTH;
  leveldb::Status s;
  bool flushed = false;
  if (time_boundary != time_boundary_ && time_boundary_ != -1 &&
      num_samples_ != 0) {
    s = flush(db, txn, flush_keys, flush_values);
    flushed = true;
  }

  // Handle new timeseries.
  if (*current_idx_.begin() != -1) {
    for (int i : slots) {
      if (current_idx_.find(i) == current_idx_.end()) {
        _check_value_slot_init(i);
        for (int j = 0; j < num_samples_; j++)
          apps_[i]->append(std::numeric_limits<double>::max());
      }
    }
  }

  bool handle_missing_normal = true;
  bool handle_missing_shift = false;
  int idx;

  if (min_time_ == std::numeric_limits<int64_t>::max()) min_time_ = timestamp;
  if (max_time_ != std::numeric_limits<int64_t>::min() &&
      timestamp <= max_time_) {
    std::vector<int64_t> times;
    chunk::NullSupportedXORGroupTimeIterator it1(t_ptr_, t_bstream_->size());
    while (it1.next()) times.push_back(it1.at());

    std::vector<double> vals;
    vals.reserve(times.size() + 1);

    idx = _lower_bound(times, timestamp);
    if (times[idx] != timestamp) {
      // Insert new sample.
      delete app_;
      delete t_bstream_;
      t_bstream_ = new chunk::BitStreamV2(t_ptr_, 2);
      app_ = new chunk::NullSupportedXORGroupTimeAppenderV2(t_bstream_);
      times.insert(times.begin() + idx, timestamp);
      for (int64_t t : times) app_->append(t);

      for (size_t i = 0; i < slots.size(); i++) {
        chunk::NullSupportedXORGroupValueIterator it2(
            v_ptrs_[slots[i]], v_bstreams_[slots[i]]->size(), times.size() - 1);
        vals.clear();
        while (it2.next()) vals.push_back(it2.at());
        vals.insert(vals.begin() + idx, values[i]);

        delete apps_[slots[i]];
        delete v_bstreams_[slots[i]];
        v_bstreams_[slots[i]] = new chunk::BitStreamV2(v_ptrs_[slots[i]]);
        apps_[slots[i]] = new chunk::NullSupportedXORGroupValueAppenderV2(
            v_bstreams_[slots[i]]);

        for (double v : vals) apps_[slots[i]]->append(v);
      }

      ++num_samples_;
      handle_missing_shift = true;
    } else {
      // Replace the existing slot.
      for (size_t i = 0; i < slots.size(); i++) {
        chunk::NullSupportedXORGroupValueIterator it2(
            v_ptrs_[slots[i]], v_bstreams_[slots[i]]->size(), times.size());
        vals.clear();
        while (it2.next()) vals.push_back(it2.at());
        vals[idx] = values[i];

        delete apps_[slots[i]];
        delete v_bstreams_[slots[i]];
        v_bstreams_[slots[i]] = new chunk::BitStreamV2(v_ptrs_[slots[i]]);
        apps_[slots[i]] = new chunk::NullSupportedXORGroupValueAppenderV2(
            v_bstreams_[slots[i]]);

        for (double v : vals) apps_[slots[i]]->append(v);
      }
    }
    handle_missing_normal = false;
  } else {
    max_time_ = timestamp;
    _check_time_slot_init();
    app_->append(timestamp);
    for (size_t i = 0; i < slots.size(); i++) {
      _check_value_slot_init(slots[i]);
      apps_[slots[i]]->append(values[i]);
    }

    ++num_samples_;
  }

  if (*current_idx_.begin() == -1) {
    // Handle existing timeseries missing in this round.
    if (handle_missing_normal) {
      int j = 0;
      for (size_t i = 0; i < value_slots_.size(); i++) {
        if (j < slots.size()) {
          if (slots[j] > i)
            apps_[i]->append(std::numeric_limits<double>::max());
          else if (slots[j] < i) {
            j++;
            while (j < slots.size() && slots[j] < i) j++;
            if (!(j < slots.size() && slots[j] == i))
              apps_[i]->append(std::numeric_limits<double>::max());
          } else
            j++;
        } else
          apps_[i]->append(std::numeric_limits<double>::max());
      }
    } else if (handle_missing_shift) {
      int j = 0;
      for (size_t i = 0; i < value_slots_.size(); i++) {
        if (j < slots.size()) {
          if (slots[j] > i) {
            _insert_null_into_series(i, idx, num_samples_ - 1);
          } else if (slots[j] < i) {
            j++;
            while (j < slots.size() && slots[j] < i) j++;
            if (!(j < slots.size() && slots[j] == i)) {
              _insert_null_into_series(i, idx, num_samples_ - 1);
            }
          } else
            j++;
        } else {
          _insert_null_into_series(i, idx, num_samples_ - 1);
        }
      }
    }
  } else {
    std::vector<int> new_ts;
    // Handle existing timeseries missing in this round.
    if (handle_missing_normal) {
      int j = 0;
      for (int i : current_idx_) {
        _check_value_slot_init(i);
        if (j < slots.size()) {
          if (slots[j] > i)
            apps_[i]->append(std::numeric_limits<double>::max());
          else if (slots[j] < i) {
            j++;
            while (j < slots.size() && slots[j] < i) j++;
            if (!(j < slots.size() && slots[j] == i))
              apps_[i]->append(std::numeric_limits<double>::max());
          } else
            j++;
        } else
          apps_[i]->append(std::numeric_limits<double>::max());
      }
    } else if (handle_missing_shift) {
      int j = 0;
      for (int i : current_idx_) {
        _check_value_slot_init(i);
        if (j < slots.size()) {
          if (slots[j] > i) {
            _insert_null_into_series(i, idx, num_samples_ - 1);
          } else if (slots[j] < i) {
            j++;
            while (j < slots.size() && slots[j] < i) j++;
            if (!(j < slots.size() && slots[j] == i)) {
              _insert_null_into_series(i, idx, num_samples_ - 1);
            }
          } else
            j++;
        } else {
          _insert_null_into_series(i, idx, num_samples_ - 1);
        }
      }
    }

    // Handle new timeseries.
    for (int i : slots) {
      if (current_idx_.find(i) == current_idx_.end()) {
        new_ts.push_back(i);
      }
    }
    current_idx_.insert(new_ts.begin(), new_ts.end());
  }

  time_boundary_ = time_boundary;
  if (num_samples_ == 1) tuple_st_ = timestamp;

  if (num_samples_ == leveldb::MEM_TUPLE_SIZE) {
    s = flush(db, txn, flush_keys, flush_values);
    flushed = true;
  }

  return flushed;
}

bool MMapMemGroup::append(leveldb::DB* db,
                          const std::vector<label::Labels>& lsets,
                          int64_t timestamp, const std::vector<double>& values,
                          std::vector<int>* slots) {
  if (timestamp > global_max_time_) global_max_time_ = timestamp;

  get_indices_with_labels(lsets, slots);

  int64_t time_boundary;
  if (db)
    time_boundary = timestamp / db->PartitionLength() * db->PartitionLength();
  else
    time_boundary =
        timestamp / leveldb::PARTITION_LENGTH * leveldb::PARTITION_LENGTH;
  leveldb::Status s;
  bool flushed = false;
  if (time_boundary != time_boundary_ && time_boundary_ != -1 &&
      num_samples_ != 0) {
    s = flush(db);
    flushed = true;
  }

  bool handle_missing_normal = true;
  bool handle_missing_shift = false;
  int idx;
  std::set<int> ts_this_round(slots->begin(), slots->end());

  // Handle new timeseries.
  if (*current_idx_.begin() != -1) {
    for (int i : ts_this_round) {
      _check_value_slot_init(i);
      if (current_idx_.find(i) == current_idx_.end()) {
        for (int j = 0; j < num_samples_; j++)
          apps_[i]->append(std::numeric_limits<double>::max());
      }
    }
  }

  if (min_time_ == std::numeric_limits<int64_t>::max()) min_time_ = timestamp;
  if (max_time_ != std::numeric_limits<int64_t>::min() &&
      timestamp <= max_time_) {
    std::vector<int64_t> times;
    chunk::NullSupportedXORGroupTimeIterator it1(t_ptr_, t_bstream_->size());
    while (it1.next()) times.push_back(it1.at());

    std::vector<double> vals;
    vals.reserve(times.size() + 1);

    idx = _lower_bound(times, timestamp);
    if (times[idx] != timestamp) {
      // Insert new sample.
      delete app_;
      delete t_bstream_;
      t_bstream_ = new chunk::BitStreamV2(t_ptr_, 2);
      app_ = new chunk::NullSupportedXORGroupTimeAppenderV2(t_bstream_);
      times.insert(times.begin() + idx, timestamp);
      for (int64_t t : times) app_->append(t);

      int j = 0;
      for (int slot : ts_this_round) {
        chunk::NullSupportedXORGroupValueIterator it2(
            v_ptrs_[slot], v_bstreams_[slot]->size(), times.size() - 1);
        vals.clear();
        while (it2.next()) vals.push_back(it2.at());
        vals.insert(vals.begin() + idx, values[j++]);

        delete apps_[slot];
        delete v_bstreams_[slot];
        v_bstreams_[slot] = new chunk::BitStreamV2(v_ptrs_[slot]);
        apps_[slot] =
            new chunk::NullSupportedXORGroupValueAppenderV2(v_bstreams_[slot]);

        for (double v : vals) apps_[slot]->append(v);
      }

      handle_missing_shift = true;
      ++num_samples_;
    } else {
      // Replace the existing slot.
      int i = 0;
      for (int slot : ts_this_round) {
        chunk::NullSupportedXORGroupValueIterator it2(
            v_ptrs_[slot], v_bstreams_[slot]->size(), times.size());
        vals.clear();
        while (it2.next()) vals.push_back(it2.at());
        vals[idx] = values[i++];

        delete apps_[slot];
        delete v_bstreams_[slot];
        v_bstreams_[slot] = new chunk::BitStreamV2(v_ptrs_[slot]);
        apps_[slot] =
            new chunk::NullSupportedXORGroupValueAppenderV2(v_bstreams_[slot]);

        for (double v : vals) apps_[slot]->append(v);
      }
    }
    handle_missing_normal = false;
  } else {
    max_time_ = timestamp;
    _check_time_slot_init();
    int i = 0;
    app_->append(timestamp);
    for (int slot : ts_this_round) {
      _check_value_slot_init(slot);
      apps_[slot]->append(values[i++]);
    }

    ++num_samples_;
  }

  if (*current_idx_.begin() == -1) {
    // Handle existing timeseries missing in this round.
    if (handle_missing_normal) {
      for (size_t i = 0; i < value_slots_.size(); i++) {
        if (ts_this_round.find(i) == ts_this_round.end()) {
          _check_value_slot_init(i);
          apps_[i]->append(std::numeric_limits<double>::max());
        }
      }
    } else if (handle_missing_shift) {
      for (size_t i = 0; i < value_slots_.size(); i++) {
        if (ts_this_round.find(i) == ts_this_round.end()) {
          _check_value_slot_init(i);
          _insert_null_into_series(i, idx, num_samples_ - 1);
        }
      }
    }
  } else {
    std::vector<int> new_ts;
    // Handle existing timeseries missing in this round.
    if (handle_missing_normal) {
      for (int i : current_idx_) {
        if (ts_this_round.find(i) == ts_this_round.end())
          apps_[i]->append(std::numeric_limits<double>::max());
      }
    } else if (handle_missing_shift) {
      for (int i : current_idx_) {
        if (ts_this_round.find(i) == ts_this_round.end()) {
          _check_value_slot_init(i);
          _insert_null_into_series(i, idx, num_samples_ - 1);
        }
      }
    }

    // Handle new timeseries.
    for (int i : ts_this_round) {
      if (current_idx_.find(i) == current_idx_.end()) {
        new_ts.push_back(i);
      }
    }
    current_idx_.insert(new_ts.begin(), new_ts.end());
  }

  time_boundary_ = time_boundary;
  if (num_samples_ == 1) tuple_st_ = timestamp;

  if (num_samples_ == leveldb::MEM_TUPLE_SIZE) {
    s = flush(db);
    flushed = true;
  }

  return flushed;
}

bool MMapMemGroup::TEST_append(int64_t timestamp,
                               const std::vector<double>& values) {
  if (timestamp > global_max_time_) global_max_time_ = timestamp;

  int64_t time_boundary =
      timestamp / leveldb::PARTITION_LENGTH * leveldb::PARTITION_LENGTH;
  leveldb::Status s;
  bool flushed = false;
  if (time_boundary != time_boundary_ && time_boundary_ != -1 &&
      num_samples_ != 0) {
    TEST_flush();
    flushed = true;
  }

  // Fill those missing timeseries.
  if (*current_idx_.begin() != -1) {
    for (size_t i = 0; i < value_slots_.size(); i++) {
      if (current_idx_.find(i) == current_idx_.end()) {
        _check_value_slot_init(i);
        for (int j = 0; j < num_samples_; j++)
          apps_[i]->append(std::numeric_limits<double>::max());
      }
    }
    current_idx_.clear();
    current_idx_.insert(-1);
  }

  _check_time_slot_init();
  app_->append(timestamp);
  for (size_t i = 0; i < value_slots_.size(); i++) {
    _check_value_slot_init(i);
    apps_[i]->append(values[i]);
  }
  if (min_time_ == std::numeric_limits<int64_t>::max()) {
    min_time_ = timestamp;
  }

  max_time_ = timestamp;
  time_boundary_ = time_boundary;
  if (num_samples_ == 0) tuple_st_ = timestamp;

  ++num_samples_;
  if (num_samples_ == leveldb::MEM_TUPLE_SIZE) {
    TEST_flush();
    flushed = true;
  }

  return flushed;
}

bool MMapMemGroup::TEST_append(const std::vector<int>& slots, int64_t timestamp,
                               const std::vector<double>& values) {
  if (timestamp > global_max_time_) global_max_time_ = timestamp;

  int64_t time_boundary =
      timestamp / leveldb::PARTITION_LENGTH * leveldb::PARTITION_LENGTH;
  leveldb::Status s;
  bool flushed = false;
  if (time_boundary != time_boundary_ && time_boundary_ != -1 &&
      num_samples_ != 0) {
    TEST_flush();
    flushed = true;
  }

  if (*current_idx_.begin() == -1) {
    // Handle existing timeseries missing in this round.
    int j = 0;
    for (size_t i = 0; i < value_slots_.size(); i++) {
      _check_value_slot_init(i);
      if (j < slots.size()) {
        if (slots[j] > i)
          apps_[i]->append(std::numeric_limits<double>::max());
        else if (slots[j] < i) {
          j++;
          while (j < slots.size() && slots[j] < i) j++;
          if (!(j < slots.size() && slots[j] == i))
            apps_[i]->append(std::numeric_limits<double>::max());
        } else
          j++;
      } else
        apps_[i]->append(std::numeric_limits<double>::max());
    }
  } else {
    std::vector<int> new_ts;
    // Handle existing timeseries missing in this round.
    int j = 0;
    for (int i : current_idx_) {
      if (j < slots.size()) {
        _check_value_slot_init(i);
        if (slots[j] > i)
          apps_[i]->append(std::numeric_limits<double>::max());
        else if (slots[j] < i) {
          j++;
          while (j < slots.size() && slots[j] < i) j++;
          if (!(j < slots.size() && slots[j] == i))
            apps_[i]->append(std::numeric_limits<double>::max());
        } else
          j++;
      } else
        apps_[i]->append(std::numeric_limits<double>::max());
    }

    // Handle new timeseries.
    for (int i : slots) {
      if (current_idx_.find(i) == current_idx_.end()) {
        _check_value_slot_init(i);
        for (int j = 0; j < num_samples_; j++)
          apps_[i]->append(std::numeric_limits<double>::max());
        new_ts.push_back(i);
      }
    }
    current_idx_.insert(new_ts.begin(), new_ts.end());
  }

  _check_time_slot_init();
  app_->append(timestamp);
  for (size_t i = 0; i < slots.size(); i++) {
    _check_value_slot_init(i);
    apps_[i]->append(values[i]);
  }
  if (min_time_ == std::numeric_limits<int64_t>::max()) {
    min_time_ = timestamp;
  }

  max_time_ = timestamp;
  time_boundary_ = time_boundary;
  if (num_samples_ == 0) tuple_st_ = timestamp;

  ++num_samples_;
  if (num_samples_ == leveldb::MEM_TUPLE_SIZE) {
    TEST_flush();
    flushed = true;
  }

  return flushed;
}

}  // namespace head
}  // namespace tsdb