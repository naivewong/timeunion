#include "db/db_querier.h"

#include "db/db_impl.h"
#include "db/memtable.h"
#include "db/partition_index.h"
#include "db/version_set.h"

#include "table/merger.h"
#include "util/mutexlock.h"

#include "base/Logging.hpp"
#include "head/Head.hpp"
#include "head/HeadSeriesSet.hpp"
#include "head/MMapHeadWithTrie.hpp"
#include "index/EmptyPostings.hpp"
#include "index/IntersectPostings.hpp"
#include "querier/EmptySeriesSet.hpp"
#include "tsdbutil/StringTuplesInterface.hpp"

namespace leveldb {

static void DeleteTSSamples(const Slice& key, void* value) {
  CachedTSSamples* tf = reinterpret_cast<CachedTSSamples*>(value);
  delete tf->timestamps;
  delete tf->values;
  delete tf;
}

static void DeleteGroupTimestamps(const Slice& key, void* value) {
  std::vector<int64_t>* t = reinterpret_cast<std::vector<int64_t>*>(value);
  delete t;
}

static void DeleteGroupValues(const Slice& key, void* value) {
  std::vector<double>* t = reinterpret_cast<std::vector<double>*>(value);
  delete t;
}

/**********************************************
 *              MemSeriesIterator             *
 **********************************************/
MemSeriesIterator::MemSeriesIterator(MemTable* mem, int64_t min_time,
                                     int64_t max_time, uint64_t id,
                                     Cache* cache)
    : min_time_(min_time),
      max_time_(max_time),
      tsid_(id),
      slot_(-1),
      t_(nullptr),
      v_(nullptr),
      init_(false),
      cache_(cache),
      handle_(nullptr),
      handle2_(nullptr) {
  iter_.reset(mem->NewIterator());
  if (min_time_ < 0) min_time_ = 0;
}

MemSeriesIterator::MemSeriesIterator(MemTable* mem, int64_t min_time,
                                     int64_t max_time, uint64_t id, int slot,
                                     Cache* cache)
    : min_time_(min_time),
      max_time_(max_time),
      tsid_(id),
      slot_(slot),
      t_(nullptr),
      v_(nullptr),
      init_(false),
      cache_(cache),
      handle_(nullptr),
      handle2_(nullptr) {
  iter_.reset(mem->NewIterator());
  if (min_time_ < 0) min_time_ = 0;
}

MemSeriesIterator::~MemSeriesIterator() {
  if (handle_) cache_->Release(handle_);
  if (handle2_) cache_->Release(handle2_);
  if (!cache_) {
    if (t_) delete t_;
    if (v_) delete v_;
  }
}

inline void MemSeriesIterator::lookup_cached_ts(const Slice& key,
                                                CachedTSSamples** samples,
                                                bool* create_ts) const {
  handle_ = cache_->Lookup(key);
  if (handle_) {
    *samples = reinterpret_cast<CachedTSSamples*>(cache_->Value(handle_));
    t_ = (*samples)->timestamps;
    v_ = (*samples)->values;
    return;
  }
  *samples = new CachedTSSamples();
  (*samples)->timestamps = new std::vector<int64_t>();
  (*samples)->values = new std::vector<double>();
  t_ = (*samples)->timestamps;
  v_ = (*samples)->values;
  *create_ts = true;
}

inline void MemSeriesIterator::lookup_cached_group(const Slice& key,
                                                   bool* create_times,
                                                   bool* create_vals) const {
  handle_ = cache_->Lookup(key.ToString() + "t");
  if (handle_) {
    t_ = reinterpret_cast<std::vector<int64_t>*>(cache_->Value(handle_));
  } else {
    t_ = new std::vector<int64_t>();
    *create_times = true;
  }

  handle2_ = cache_->Lookup(key.ToString() + std::to_string(slot_));
  if (handle2_) {
    v_ = reinterpret_cast<std::vector<double>*>(cache_->Value(handle2_));
  } else {
    v_ = new std::vector<double>();
    *create_vals = true;
  }
}

void MemSeriesIterator::decode_value(const Slice& key, const Slice& s) const {
  CachedTSSamples* samples = nullptr;
  bool create_ts = false, create_times = false, create_vals = false;
  if (cache_) {
    if (handle_)  // We need to release the previous one.
      cache_->Release(handle_);
    if (slot_ == -1) {
      lookup_cached_ts(key, &samples, &create_ts);
      if (!create_ts) return;
    } else {
      if (handle2_)  // We need to release the previous one.
        cache_->Release(handle2_);
      lookup_cached_group(key, &create_times, &create_vals);
      if (!create_times && !create_vals) return;
    }
  } else if (!t_) {
    t_ = new std::vector<int64_t>();
    v_ = new std::vector<double>();
  } else {
    t_->clear();
    v_->clear();
  }

  // Decode the concatenating bit streams.
  uint32_t tmp_size;
  Slice tmp_value = s;
  uint64_t tmp;
  GetVarint64(&tmp_value, &tmp);  // txn.
  GetVarint64(&tmp_value, &tmp);  // tdiff.
  while (tmp_value.size() > 0) {
    GetVarint32(&tmp_value, &tmp_size);
    if (slot_ == -1)
      full_value_decode_helper(tmp_value, t_, v_);
    else {
      if (!create_times && cache_)
        group_value_decode_helper(tmp_value, slot_, nullptr, v_, true);
      else
        group_value_decode_helper(tmp_value, slot_, t_, v_, true);
    }
    tmp_value = Slice(tmp_value.data() + tmp_size, tmp_value.size() - tmp_size);
  }

  if (cache_) {
    if (create_ts)
      handle_ = cache_->Insert(key, samples, (samples->timestamps->size()) << 4,
                               &DeleteTSSamples);
    if (create_times)
      handle_ = cache_->Insert(key.ToString() + "t", t_, (t_->size()) << 3,
                               &DeleteGroupTimestamps);
    if (create_vals)
      handle2_ = cache_->Insert(key.ToString() + std::to_string(slot_), v_,
                                (v_->size()) << 3, &DeleteGroupValues);
  }
}

bool MemSeriesIterator::seek(int64_t t) const {
  if (err_ || t > max_time_) return false;

  init_ = true;
  std::string key;
  encodeKey(&key, "", tsid_, t);
  LookupKey lk(key, kMaxSequenceNumber);
  iter_->Seek(lk.internal_key());
  if (!iter_->Valid()) {
    err_.set("error seek seek1");
    return false;
  }
  iter_->Prev();
  if (!iter_->Valid()) {
    // It is the first element, seek again.
    iter_->Seek(lk.internal_key());
  }
  uint64_t tsid;
  decodeKey(iter_->key(), nullptr, &tsid, nullptr);
  if (tsid != tsid_) {
    iter_->Next();
    if (!iter_->Valid()) {
      err_.set("error seek next1");
      return false;
    }
    decodeKey(iter_->key(), nullptr, &tsid, nullptr);
    if (tsid != tsid_) return false;
  }

  sub_idx_ = 0;
  decode_value(iter_->key(), iter_->value());
  while (true) {
    while (sub_idx_ < t_->size()) {
      if (t_->at(sub_idx_) >= t) {
        return true;
      }
      sub_idx_++;
    }
    if (sub_idx_ >= t_->size()) {
      iter_->Next();
      if (!iter_->Valid()) {
        err_.set("error seek next2");
        return false;
      }
      decodeKey(iter_->key(), nullptr, &tsid, nullptr);
      if (tsid != tsid_) return false;

      sub_idx_ = 0;
      decode_value(iter_->key(), iter_->value());
    }
  }
  return false;
}

std::pair<int64_t, double> MemSeriesIterator::at() const {
  if (sub_idx_ < t_->size()) return {t_->at(sub_idx_), v_->at(sub_idx_)};
  return {0, 0};
}

bool MemSeriesIterator::next() const {
  if (err_) return false;

  if (!init_) {
    std::string key;
    encodeKey(&key, "", tsid_, min_time_);
    LookupKey lk(key, kMaxSequenceNumber);
    iter_->Seek(lk.internal_key());
    if (!iter_->Valid()) {
      err_.set("error next seek1");
      return false;
    }
    iter_->Prev();
    if (!iter_->Valid()) {
      // It is the first element, seek again.
      iter_->Seek(lk.internal_key());
    }

    uint64_t tsid;
    decodeKey(iter_->key(), nullptr, &tsid, nullptr);
    if (tsid != tsid_) {
      iter_->Next();
      if (!iter_->Valid()) {
        err_.set("error next next2");
        return false;
      }
      decodeKey(iter_->key(), nullptr, &tsid, nullptr);
      if (tsid != tsid_) return false;
    }

    sub_idx_ = 0;
    decode_value(iter_->key(), iter_->value());
    init_ = true;

    // Iterate until exceeding min_time.
    while (true) {
      while (sub_idx_ < t_->size()) {
        if (t_->at(sub_idx_) > max_time_) {
          err_.set("error next2");
          return false;
        } else if (t_->at(sub_idx_) >= min_time_)
          return true;
        sub_idx_++;
      }
      if (sub_idx_ >= t_->size()) {
        iter_->Next();
        if (!iter_->Valid()) {
          err_.set("error next next3");
          return false;
        }
        decodeKey(iter_->key(), nullptr, &tsid, nullptr);
        if (tsid != tsid_) return false;

        sub_idx_ = 0;
        decode_value(iter_->key(), iter_->value());
      }
    }
    return true;
  }

  sub_idx_++;
  if (sub_idx_ >= t_->size()) {
    while (true) {
      iter_->Next();
      if (!iter_->Valid()) {
        err_.set("error next next4");
        return false;
      }
      uint64_t tsid;
      decodeKey(iter_->key(), nullptr, &tsid, nullptr);
      if (tsid != tsid_) return false;

      sub_idx_ = 0;
      decode_value(iter_->key(), iter_->value());

      if (sub_idx_ >= t_->size())
        continue;
      else if (t_->at(sub_idx_) > max_time_) {
        err_.set("error next3");
        return false;
      }
      break;
    }
  } else if (t_->at(sub_idx_) > max_time_) {
    err_.set("error next4");
    return false;
  }
  return true;
}

/**********************************************
 *              L0SeriesIterator              *
 **********************************************/
L0SeriesIterator::L0SeriesIterator(const DBQuerier* q, int partition,
                                   uint64_t id, Cache* cache)
    : q_(q),
      partition_(partition),
      tsid_(id),
      slot_(-1),
      t_(nullptr),
      v_(nullptr),
      sub_idx_(0),
      init_(false),
      cache_(cache),
      handle_(nullptr),
      handle2_(nullptr) {
  std::vector<Iterator*> list;
  q_->current_->AddIterators(ReadOptions(), 0, q_->l0_indexes_[partition_],
                             &list, tsid_);

  iter_.reset(NewMergingIterator(q_->cmp_, &list[0], list.size()));
}

L0SeriesIterator::L0SeriesIterator(const DBQuerier* q, int partition,
                                   uint64_t id, int slot, Cache* cache)
    : q_(q),
      partition_(partition),
      tsid_(id),
      slot_(slot),
      t_(nullptr),
      v_(nullptr),
      sub_idx_(0),
      init_(false),
      cache_(cache),
      handle_(nullptr),
      handle2_(nullptr) {
  std::vector<Iterator*> list;
  q_->current_->AddIterators(ReadOptions(), 0, q_->l0_indexes_[partition_],
                             &list, tsid_);

  iter_.reset(NewMergingIterator(q_->cmp_, &list[0], list.size()));
}

L0SeriesIterator::L0SeriesIterator(const DBQuerier* q, uint64_t id,
                                   Iterator* it, Cache* cache)
    : q_(q),
      tsid_(id),
      slot_(-1),
      iter_(it),
      t_(nullptr),
      v_(nullptr),
      sub_idx_(0),
      init_(false),
      cache_(cache),
      handle_(nullptr),
      handle2_(nullptr) {}

L0SeriesIterator::L0SeriesIterator(const DBQuerier* q, uint64_t id, int slot,
                                   Iterator* it, Cache* cache)
    : q_(q),
      tsid_(id),
      slot_(slot),
      iter_(it),
      t_(nullptr),
      v_(nullptr),
      sub_idx_(0),
      init_(false),
      cache_(cache),
      handle_(nullptr),
      handle2_(nullptr) {}

L0SeriesIterator::~L0SeriesIterator() {
  if (handle_) cache_->Release(handle_);
  if (handle2_) cache_->Release(handle2_);
  if (!cache_) {
    if (t_) delete t_;
    if (v_) delete v_;
  }
}

inline void L0SeriesIterator::lookup_cached_ts(const Slice& key,
                                               CachedTSSamples** samples,
                                               bool* create_ts) const {
  handle_ = cache_->Lookup(key);
  if (handle_) {
    *samples = reinterpret_cast<CachedTSSamples*>(cache_->Value(handle_));
    t_ = (*samples)->timestamps;
    v_ = (*samples)->values;
    return;
  }
  *samples = new CachedTSSamples();
  (*samples)->timestamps = new std::vector<int64_t>();
  (*samples)->values = new std::vector<double>();
  t_ = (*samples)->timestamps;
  v_ = (*samples)->values;
  *create_ts = true;
}

inline void L0SeriesIterator::lookup_cached_group(const Slice& key,
                                                  bool* create_times,
                                                  bool* create_vals) const {
  handle_ = cache_->Lookup(key.ToString() + "t");
  if (handle_) {
    t_ = reinterpret_cast<std::vector<int64_t>*>(cache_->Value(handle_));
  } else {
    t_ = new std::vector<int64_t>();
    *create_times = true;
  }

  handle2_ = cache_->Lookup(key.ToString() + std::to_string(slot_));
  if (handle2_) {
    v_ = reinterpret_cast<std::vector<double>*>(cache_->Value(handle2_));
  } else {
    v_ = new std::vector<double>();
    *create_vals = true;
  }
}

void L0SeriesIterator::decode_value(const Slice& key, const Slice& s) const {
  CachedTSSamples* samples = nullptr;
  bool create_ts = false, create_times = false, create_vals = false;
  if (cache_) {
    if (handle_)  // We need to release the previous one.
      cache_->Release(handle_);
    if (slot_ == -1) {
      lookup_cached_ts(key, &samples, &create_ts);
      if (!create_ts) return;
    } else {
      if (handle2_)  // We need to release the previous one.
        cache_->Release(handle2_);
      lookup_cached_group(key, &create_times, &create_vals);
      if (!create_times && !create_vals) return;
    }
  } else if (!t_) {
    t_ = new std::vector<int64_t>();
    v_ = new std::vector<double>();
  } else {
    t_->clear();
    v_->clear();
  }

  // Decode the concatenating bit streams.
  uint32_t tmp_size;
  Slice tmp_value = s;
  uint64_t tdiff;
  GetVarint64(&tmp_value, &tdiff);
  while (tmp_value.size() > 0) {
    GetVarint32(&tmp_value, &tmp_size);
    if (slot_ == -1)
      full_value_decode_helper(tmp_value, t_, v_);
    else {
      if (!create_times && cache_)
        group_value_decode_helper(tmp_value, slot_, nullptr, v_, true);
      else
        group_value_decode_helper(tmp_value, slot_, t_, v_, true);
    }
    tmp_value = Slice(tmp_value.data() + tmp_size, tmp_value.size() - tmp_size);
  }

  if (cache_) {
    if (create_ts)
      handle_ = cache_->Insert(key, samples, (samples->timestamps->size()) << 4,
                               &DeleteTSSamples);
    if (create_times)
      handle_ = cache_->Insert(key.ToString() + "t", t_, (t_->size()) << 3,
                               &DeleteGroupTimestamps);
    if (create_vals)
      handle2_ = cache_->Insert(key.ToString() + std::to_string(slot_), v_,
                                (v_->size()) << 3, &DeleteGroupValues);
  }
}

bool L0SeriesIterator::seek(int64_t t) const {
  if (err_ || t > q_->max_time_) return false;

  init_ = true;
  std::string key;
  encodeKey(&key, "", tsid_, t);
  LookupKey lk(key, kMaxSequenceNumber);
  iter_->Seek(lk.internal_key());
  if (!iter_->Valid()) {
    err_.set("error seek seek1");
    return false;
  }
  iter_->Prev();
  if (!iter_->Valid()) {
    // It is the first element, seek again.
    iter_->Seek(lk.internal_key());
  }

  uint64_t tsid;
  decodeKey(iter_->key(), nullptr, &tsid, nullptr);
  if (tsid != tsid_) {
    iter_->Next();
    if (!iter_->Valid()) {
      err_.set("error seek next1");
      return false;
    }
    decodeKey(iter_->key(), nullptr, &tsid, nullptr);
    if (tsid != tsid_) return false;
  }

  sub_idx_ = 0;
  decode_value(iter_->key(), iter_->value());
  while (true) {
    while (sub_idx_ < t_->size()) {
      if (t_->at(sub_idx_) >= t) {
        return true;
      }
      sub_idx_++;
    }
    if (sub_idx_ >= t_->size()) {
      iter_->Next();
      if (!iter_->Valid()) {
        err_.set("error seek next2");
        return false;
      }
      decodeKey(iter_->key(), nullptr, &tsid, nullptr);
      if (tsid != tsid_) return false;

      sub_idx_ = 0;
      decode_value(iter_->key(), iter_->value());
    }
  }
  return false;
}

std::pair<int64_t, double> L0SeriesIterator::at() const {
  if (sub_idx_ < t_->size()) return {t_->at(sub_idx_), v_->at(sub_idx_)};
  return {0, 0};
}

bool L0SeriesIterator::next() const {
  if (err_) return false;

  if (!init_) {
    std::string key;
    encodeKey(&key, "", tsid_, q_->min_time_);
    LookupKey lk(key, kMaxSequenceNumber);
    iter_->Seek(lk.internal_key());
    if (!iter_->Valid()) {
      err_.set("error next next1");
      return false;
    }
    iter_->Prev();
    if (!iter_->Valid()) {
      // It is the first element, seek again.
      iter_->Seek(lk.internal_key());
    }

    uint64_t tsid;
    int64_t st;
    decodeKey(iter_->key(), nullptr, &tsid, nullptr);
    if (tsid != tsid_) {
      iter_->Next();
      if (!iter_->Valid()) {
        err_.set("error next next2");
        return false;
      }
      decodeKey(iter_->key(), nullptr, &tsid, nullptr);
      if (tsid != tsid_) return false;
    }

    sub_idx_ = 0;
    decode_value(iter_->key(), iter_->value());
    init_ = true;

    // Iterate until exceeding min_time.
    while (true) {
      while (sub_idx_ < t_->size()) {
        if (t_->at(sub_idx_) > q_->max_time_) {
          err_.set("error next2");
          return false;
        } else if (t_->at(sub_idx_) >= q_->min_time_)
          return true;
        sub_idx_++;
      }
      if (sub_idx_ >= t_->size()) {
        iter_->Next();
        if (!iter_->Valid()) {
          err_.set("error next next3");
          return false;
        }
        decodeKey(iter_->key(), nullptr, &tsid, nullptr);
        if (tsid != tsid_) return false;

        sub_idx_ = 0;
        decode_value(iter_->key(), iter_->value());
      }
    }
    return true;
  }

  sub_idx_++;
  if (sub_idx_ >= t_->size()) {
    while (true) {
      iter_->Next();
      if (!iter_->Valid()) {
        err_.set("error next next4");
        return false;
      }
      uint64_t tsid;
      decodeKey(iter_->key(), nullptr, &tsid, nullptr);
      if (tsid != tsid_) return false;

      sub_idx_ = 0;
      decode_value(iter_->key(), iter_->value());

      if (sub_idx_ >= t_->size())
        continue;
      else if (t_->at(sub_idx_) > q_->max_time_) {
        err_.set("error next3");
        return false;
      }
      break;
    }
  } else if (t_->at(sub_idx_) > q_->max_time_) {
    err_.set("error next4");
    return false;
  }
  return true;
}

/**********************************************
 *              L1SeriesIterator              *
 **********************************************/
L1SeriesIterator::L1SeriesIterator(const DBQuerier* q, int partition,
                                   uint64_t id, Cache* cache)
    : q_(q),
      partition_(partition),
      tsid_(id),
      slot_(-1),
      t_(nullptr),
      v_(nullptr),
      sub_idx_(0),
      init_(false),
      cache_(cache),
      handle_(nullptr),
      handle2_(nullptr) {
  std::vector<Iterator*> list;
  q_->current_->AddIterators(ReadOptions(), 1, q_->l1_indexes_[partition_],
                             &list, tsid_);

  iter_.reset(NewMergingIterator(q_->cmp_, &list[0], list.size()));
}

L1SeriesIterator::L1SeriesIterator(const DBQuerier* q, int partition,
                                   uint64_t id, int slot, Cache* cache)
    : q_(q),
      partition_(partition),
      tsid_(id),
      slot_(slot),
      t_(nullptr),
      v_(nullptr),
      sub_idx_(0),
      init_(false),
      cache_(cache),
      handle_(nullptr),
      handle2_(nullptr) {
  std::vector<Iterator*> list;
  q_->current_->AddIterators(ReadOptions(), 1, q_->l1_indexes_[partition_],
                             &list, tsid_);

  iter_.reset(NewMergingIterator(q_->cmp_, &list[0], list.size()));
}

L1SeriesIterator::~L1SeriesIterator() {
  if (handle_) cache_->Release(handle_);
  if (handle2_) cache_->Release(handle2_);
  if (!cache_) {
    if (t_) delete t_;
    if (v_) delete v_;
  }
}

inline void L1SeriesIterator::lookup_cached_ts(const Slice& key,
                                               CachedTSSamples** samples,
                                               bool* create_ts) const {
  handle_ = cache_->Lookup(key);
  if (handle_) {
    *samples = reinterpret_cast<CachedTSSamples*>(cache_->Value(handle_));
    t_ = (*samples)->timestamps;
    v_ = (*samples)->values;
    return;
  }
  *samples = new CachedTSSamples();
  (*samples)->timestamps = new std::vector<int64_t>();
  (*samples)->values = new std::vector<double>();
  t_ = (*samples)->timestamps;
  v_ = (*samples)->values;
  *create_ts = true;
}

inline void L1SeriesIterator::lookup_cached_group(const Slice& key,
                                                  bool* create_times,
                                                  bool* create_vals) const {
  handle_ = cache_->Lookup(key.ToString() + "t");
  if (handle_) {
    t_ = reinterpret_cast<std::vector<int64_t>*>(cache_->Value(handle_));
  } else {
    t_ = new std::vector<int64_t>();
    *create_times = true;
  }

  handle2_ = cache_->Lookup(key.ToString() + std::to_string(slot_));
  if (handle2_) {
    v_ = reinterpret_cast<std::vector<double>*>(cache_->Value(handle2_));
  } else {
    v_ = new std::vector<double>();
    *create_vals = true;
  }
}

void L1SeriesIterator::decode_value(const Slice& key, const Slice& s) const {
  CachedTSSamples* samples = nullptr;
  bool create_ts = false, create_times = false, create_vals = false;
  if (cache_) {
    if (handle_)  // We need to release the previous one.
      cache_->Release(handle_);
    if (slot_ == -1) {
      lookup_cached_ts(key, &samples, &create_ts);
      if (!create_ts) return;
    } else {
      if (handle2_)  // We need to release the previous one.
        cache_->Release(handle2_);
      lookup_cached_group(key, &create_times, &create_vals);
      if (!create_times && !create_vals) return;
    }
  } else if (!t_) {
    t_ = new std::vector<int64_t>();
    v_ = new std::vector<double>();
  } else {
    t_->clear();
    v_->clear();
  }

  // Decode the concatenating bit streams.
  uint32_t tmp_size;
  Slice tmp_value = s;
  uint64_t tdiff;
  GetVarint64(&tmp_value, &tdiff);
  while (tmp_value.size() > 0) {
    GetVarint32(&tmp_value, &tmp_size);
    if (slot_ == -1)
      full_value_decode_helper(tmp_value, t_, v_);
    else {
      if (!create_times && cache_)
        group_value_decode_helper(tmp_value, slot_, nullptr, v_, true);
      else
        group_value_decode_helper(tmp_value, slot_, t_, v_, true);
    }
    tmp_value = Slice(tmp_value.data() + tmp_size, tmp_value.size() - tmp_size);
  }

  if (cache_) {
    if (create_ts)
      handle_ = cache_->Insert(key, samples, (samples->timestamps->size()) << 4,
                               &DeleteTSSamples);
    if (create_times)
      handle_ = cache_->Insert(key.ToString() + "t", t_, (t_->size()) << 3,
                               &DeleteGroupTimestamps);
    if (create_vals)
      handle2_ = cache_->Insert(key.ToString() + std::to_string(slot_), v_,
                                (v_->size()) << 3, &DeleteGroupValues);
  }
}

bool L1SeriesIterator::seek(int64_t t) const {
  if (err_ || t > q_->max_time_) return false;

  init_ = true;
  std::string key;
  encodeKey(&key, "", tsid_, t);
  LookupKey lk(key, kMaxSequenceNumber);
  iter_->Seek(lk.internal_key());
  if (!iter_->Valid()) {
    err_.set("error seek seek1");
    return false;
  }
  iter_->Prev();
  if (!iter_->Valid()) {
    // It is the first element, seek again.
    iter_->Seek(lk.internal_key());
  }

  uint64_t tsid;
  decodeKey(iter_->key(), nullptr, &tsid, nullptr);
  if (tsid != tsid_) {
    iter_->Next();
    if (!iter_->Valid()) {
      err_.set("error seek next1");
      return false;
    }
    decodeKey(iter_->key(), nullptr, &tsid, nullptr);
    if (tsid != tsid_) return false;
  }

  sub_idx_ = 0;
  decode_value(iter_->key(), iter_->value());
  while (true) {
    while (sub_idx_ < t_->size()) {
      if (t_->at(sub_idx_) >= t) {
        return true;
      }
      sub_idx_++;
    }
    if (sub_idx_ >= t_->size()) {
      iter_->Next();
      if (!iter_->Valid()) {
        err_.set("error seek seek2");
        return false;
      }
      uint64_t tsid;
      decodeKey(iter_->key(), nullptr, &tsid, nullptr);
      if (tsid != tsid_) return false;

      sub_idx_ = 0;
      decode_value(iter_->key(), iter_->value());
    }
  }
  return false;
}

std::pair<int64_t, double> L1SeriesIterator::at() const {
  if (sub_idx_ < t_->size()) return {t_->at(sub_idx_), v_->at(sub_idx_)};
  return {0, 0};
}

bool L1SeriesIterator::next() const {
  if (err_) return false;

  if (!init_) {
    std::string key;
    encodeKey(&key, "", tsid_, q_->min_time_);
    LookupKey lk(key, kMaxSequenceNumber);
    iter_->Seek(lk.internal_key());
    if (!iter_->Valid()) {
      err_.set("error next seek1");
      return false;
    }
    iter_->Prev();
    if (!iter_->Valid()) {
      // It is the first element, seek again.
      iter_->Seek(lk.internal_key());
    }

    uint64_t tsid;
    decodeKey(iter_->key(), nullptr, &tsid, nullptr);
    if (tsid != tsid_) {
      iter_->Next();
      if (!iter_->Valid()) {
        err_.set("error next next2");
        return false;
      }
      decodeKey(iter_->key(), nullptr, &tsid, nullptr);
      if (tsid != tsid_) return false;
    }

    sub_idx_ = 0;
    decode_value(iter_->key(), iter_->value());
    init_ = true;

    // Iterate until exceeding min_time.
    while (true) {
      while (sub_idx_ < t_->size()) {
        if (t_->at(sub_idx_) > q_->max_time_) {
          err_.set("error next2");
          return false;
        } else if (t_->at(sub_idx_) >= q_->min_time_)
          return true;
        sub_idx_++;
      }
      if (sub_idx_ >= t_->size()) {
        iter_->Next();
        if (!iter_->Valid()) {
          err_.set("error next next3");
          return false;
        }
        decodeKey(iter_->key(), nullptr, &tsid, nullptr);
        if (tsid != tsid_) return false;

        sub_idx_ = 0;
        decode_value(iter_->key(), iter_->value());
      }
    }
    return true;
  }

  sub_idx_++;
  if (sub_idx_ >= t_->size()) {
    while (true) {
      iter_->Next();
      if (!iter_->Valid()) {
        err_.set("error next next4");
        return false;
      }
      uint64_t tsid;
      decodeKey(iter_->key(), nullptr, &tsid, nullptr);
      if (tsid != tsid_) return false;

      sub_idx_ = 0;
      decode_value(iter_->key(), iter_->value());

      if (sub_idx_ >= t_->size())
        continue;
      else if (t_->at(sub_idx_) > q_->max_time_) {
        err_.set("error next3");
        return false;
      }
      break;
    }
  } else if (t_->at(sub_idx_) > q_->max_time_) {
    err_.set("error next4");
    return false;
  }
  return true;
}

/**********************************************
 *              L2SeriesIterator              *
 **********************************************/
L2SeriesIterator::L2SeriesIterator(const DBQuerier* q, int partition,
                                   uint64_t id, Cache* cache)
    : q_(q),
      partition_(partition),
      tsid_(id),
      slot_(-1),
      t_(nullptr),
      v_(nullptr),
      sub_idx_(0),
      init_(false),
      cache_(cache),
      handle_(nullptr),
      handle2_(nullptr) {
  std::vector<Iterator*> list;
  q_->current_->AddIterators(ReadOptions(), 2, q_->l2_indexes_[partition_],
                             &list, tsid_, true);

  iter_.reset(NewMergingIterator(q_->cmp_, &list[0], list.size()));
}

L2SeriesIterator::L2SeriesIterator(const DBQuerier* q, int partition,
                                   uint64_t id, int slot, Cache* cache)
    : q_(q),
      partition_(partition),
      tsid_(id),
      slot_(slot),
      t_(nullptr),
      v_(nullptr),
      sub_idx_(0),
      init_(false),
      cache_(cache),
      handle_(nullptr),
      handle2_(nullptr) {
  std::vector<Iterator*> list;
  q_->current_->AddIterators(ReadOptions(), 2, q_->l2_indexes_[partition_],
                             &list, tsid_, true);

  iter_.reset(NewMergingIterator(q_->cmp_, &list[0], list.size()));
}

L2SeriesIterator::L2SeriesIterator(const DBQuerier* q, uint64_t id,
                                   Iterator* it, Cache* cache)
    : q_(q),
      tsid_(id),
      slot_(-1),
      iter_(it),
      t_(nullptr),
      v_(nullptr),
      sub_idx_(0),
      init_(false),
      cache_(cache),
      handle_(nullptr),
      handle2_(nullptr) {}

L2SeriesIterator::L2SeriesIterator(const DBQuerier* q, uint64_t id, int slot,
                                   Iterator* it, Cache* cache)
    : q_(q),
      tsid_(id),
      slot_(slot),
      iter_(it),
      t_(nullptr),
      v_(nullptr),
      sub_idx_(0),
      init_(false),
      cache_(cache),
      handle_(nullptr),
      handle2_(nullptr) {}

L2SeriesIterator::~L2SeriesIterator() {
  if (handle_) cache_->Release(handle_);
  if (handle2_) cache_->Release(handle2_);
  if (!cache_) {
    if (t_) delete t_;
    if (v_) delete v_;
  }
}

inline void L2SeriesIterator::lookup_cached_ts(const Slice& key,
                                               CachedTSSamples** samples,
                                               bool* create_ts) const {
  handle_ = cache_->Lookup(key);
  if (handle_) {
    *samples = reinterpret_cast<CachedTSSamples*>(cache_->Value(handle_));
    t_ = (*samples)->timestamps;
    v_ = (*samples)->values;
    return;
  }
  *samples = new CachedTSSamples();
  (*samples)->timestamps = new std::vector<int64_t>();
  (*samples)->values = new std::vector<double>();
  t_ = (*samples)->timestamps;
  v_ = (*samples)->values;
  *create_ts = true;
}

inline void L2SeriesIterator::lookup_cached_group(const Slice& key,
                                                  bool* create_times,
                                                  bool* create_vals) const {
  handle_ = cache_->Lookup(key.ToString() + "t");
  if (handle_) {
    t_ = reinterpret_cast<std::vector<int64_t>*>(cache_->Value(handle_));
  } else {
    t_ = new std::vector<int64_t>();
    *create_times = true;
  }

  handle2_ = cache_->Lookup(key.ToString() + std::to_string(slot_));
  if (handle2_) {
    v_ = reinterpret_cast<std::vector<double>*>(cache_->Value(handle2_));
  } else {
    v_ = new std::vector<double>();
    *create_vals = true;
  }
}

void L2SeriesIterator::decode_value(const Slice& key, const Slice& s) const {
  CachedTSSamples* samples = nullptr;
  bool create_ts = false, create_times = false, create_vals = false;
  if (cache_) {
    if (handle_)  // We need to release the previous one.
      cache_->Release(handle_);
    if (slot_ == -1) {
      lookup_cached_ts(key, &samples, &create_ts);
      if (!create_ts) return;
    } else {
      if (handle2_)  // We need to release the previous one.
        cache_->Release(handle2_);
      lookup_cached_group(key, &create_times, &create_vals);
      if (!create_times && !create_vals) return;
    }
  } else if (!t_) {
    t_ = new std::vector<int64_t>();
    v_ = new std::vector<double>();
  } else {
    t_->clear();
    v_->clear();
  }

  // Decode the concatenating bit streams.
  uint32_t tmp_size;
  Slice tmp_value = s;
  uint64_t tdiff;
  GetVarint64(&tmp_value, &tdiff);
  while (tmp_value.size() > 0) {
    GetVarint32(&tmp_value, &tmp_size);
    if (slot_ == -1)
      full_value_decode_helper(tmp_value, t_, v_);
    else {
      if (!create_times && cache_)
        group_value_decode_helper(tmp_value, slot_, nullptr, v_, true);
      else
        group_value_decode_helper(tmp_value, slot_, t_, v_, true);
    }
    tmp_value = Slice(tmp_value.data() + tmp_size, tmp_value.size() - tmp_size);
  }

  if (cache_) {
    if (create_ts)
      handle_ = cache_->Insert(key, samples, (samples->timestamps->size()) << 4,
                               &DeleteTSSamples);
    if (create_times)
      handle_ = cache_->Insert(key.ToString() + "t", t_, (t_->size()) << 3,
                               &DeleteGroupTimestamps);
    if (create_vals)
      handle2_ = cache_->Insert(key.ToString() + std::to_string(slot_), v_,
                                (v_->size()) << 3, &DeleteGroupValues);
  }
}

bool L2SeriesIterator::seek(int64_t t) const {
  if (err_ || t > q_->max_time_) return false;

  init_ = true;
  std::string key;
  encodeKey(&key, "", tsid_, t);
  LookupKey lk(key, kMaxSequenceNumber);
  iter_->Seek(lk.internal_key());
  if (!iter_->Valid()) {
    err_.set("error seek seek1");
    return false;
  }
  iter_->Prev();
  if (!iter_->Valid()) {
    // It is the first element, seek again.
    iter_->Seek(lk.internal_key());
  }

  uint64_t tsid;
  decodeKey(iter_->key(), nullptr, &tsid, nullptr);
  if (tsid != tsid_) {
    iter_->Next();
    if (!iter_->Valid()) {
      err_.set("error seek next1");
      return false;
    }
    decodeKey(iter_->key(), nullptr, &tsid, nullptr);
    if (tsid != tsid_) return false;
  }

  sub_idx_ = 0;
  decode_value(iter_->key(), iter_->value());
  while (true) {
    while (sub_idx_ < t_->size()) {
      if (t_->at(sub_idx_) >= t) {
        return true;
      }
      sub_idx_++;
    }
    if (sub_idx_ >= t_->size()) {
      iter_->Next();
      if (!iter_->Valid()) {
        err_.set("error seek seek2");
        return false;
      }
      uint64_t tsid;
      decodeKey(iter_->key(), nullptr, &tsid, nullptr);
      if (tsid != tsid_) return false;

      sub_idx_ = 0;
      decode_value(iter_->key(), iter_->value());
    }
  }
  return false;
}

std::pair<int64_t, double> L2SeriesIterator::at() const {
  if (sub_idx_ < t_->size()) return {t_->at(sub_idx_), v_->at(sub_idx_)};
  return {0, 0};
}

bool L2SeriesIterator::next() const {
  if (err_) return false;

  if (!init_) {
    std::string key;
    encodeKey(&key, "", tsid_, q_->min_time_);
    LookupKey lk(key, kMaxSequenceNumber);
    iter_->Seek(lk.internal_key());
    if (!iter_->Valid()) {
      err_.set("error next seek1");
      return false;
    }
    iter_->Prev();
    if (!iter_->Valid()) {
      // It is the first element, seek again.
      iter_->Seek(lk.internal_key());
    }

    uint64_t tsid;
    decodeKey(iter_->key(), nullptr, &tsid, nullptr);
    if (tsid != tsid_) {
      iter_->Next();
      if (!iter_->Valid()) {
        err_.set("error next next2");
        return false;
      }
      decodeKey(iter_->key(), nullptr, &tsid, nullptr);
      if (tsid != tsid_) return false;
    }

    sub_idx_ = 0;
    decode_value(iter_->key(), iter_->value());
    init_ = true;

    // Iterate until exceeding min_time.
    while (true) {
      while (sub_idx_ < t_->size()) {
        if (t_->at(sub_idx_) > q_->max_time_) {
          err_.set("error next2");
          return false;
        } else if (t_->at(sub_idx_) >= q_->min_time_)
          return true;
        sub_idx_++;
      }
      if (sub_idx_ >= t_->size()) {
        iter_->Next();
        if (!iter_->Valid()) {
          err_.set("error next next3");
          return false;
        }
        decodeKey(iter_->key(), nullptr, &tsid, nullptr);
        if (tsid != tsid_) return false;

        sub_idx_ = 0;
        decode_value(iter_->key(), iter_->value());
      }
    }
    return true;
  }

  sub_idx_++;
  if (sub_idx_ >= t_->size()) {
    while (true) {
      iter_->Next();
      if (!iter_->Valid()) {
        err_.set("error next next4");
        return false;
      }
      uint64_t tsid;
      decodeKey(iter_->key(), nullptr, &tsid, nullptr);
      if (tsid != tsid_) return false;

      sub_idx_ = 0;
      decode_value(iter_->key(), iter_->value());

      if (sub_idx_ >= t_->size())
        continue;
      else if (t_->at(sub_idx_) > q_->max_time_) {
        err_.set("error next3");
        return false;
      }
      break;
    }
  } else if (t_->at(sub_idx_) > q_->max_time_) {
    err_.set("error next4");
    return false;
  }
  return true;
}

/**********************************************
 *             MergeSeriesIterator            *
 **********************************************/
MergeSeriesIterator::~MergeSeriesIterator() {
  for (::tsdb::querier::SeriesIteratorInterface* it : iters_) delete it;
}

bool MergeSeriesIterator::seek(int64_t t) const {
  if (err_) return false;
  while (idx_ < iters_.size()) {
    if (!iters_[idx_]->seek(t)) {
      // if ((err_ = iters_[idx_]->error()))
      //   return false;
      idx_++;
      continue;
    }
    return true;
  }
  return false;
}

bool MergeSeriesIterator::next() const {
  if (err_ || idx_ >= iters_.size()) return false;
  if (iters_[idx_]->next()) return true;
  // if ((err_ = iters_[idx_]->error())) return false;

  while (++idx_ < iters_.size()) {
    // std::cout << "idx_ " << idx_ << std::endl;
    if (iters_[idx_]->next()) return true;
  }
  return false;
}

/**********************************************
 *                   DBSeries                 *
 **********************************************/
const ::tsdb::label::Labels& DBSeries::labels() const {
  if (init_) return lset_;
  q_->head_->series(tsid_, lset_, &head_chunk_contents_);
  init_ = true;
  return lset_;
}

std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> DBSeries::iterator() {
  if (!init_) {
    q_->head_->series(tsid_, lset_, &head_chunk_contents_);
    init_ = true;
  }
  std::vector<::tsdb::querier::SeriesIteratorInterface*> iters;
  iters.reserve(q_->l2_indexes_.size() + q_->l1_indexes_.size() +
                q_->l0_indexes_.size() + 3);
  // Add L2 iterators.
  for (int i = 0; i < q_->l2_indexes_.size(); i++) {
    ::tsdb::querier::MergeSeriesIterator* mit =
        new ::tsdb::querier::MergeSeriesIterator();
    mit->push_back(new L2SeriesIterator(q_, i, tsid_, q_->cache_));
    std::vector<Iterator*> list;
    q_->current_->AddIterators(ReadOptions(), 2, q_->l2_indexes_[i], &list,
                               tsid_, false);
    for (Iterator* it : list)
      mit->push_back(new L2SeriesIterator(q_, tsid_, it, q_->cache_));
    iters.push_back(mit);
  }

  // Add L1 iterators.
  for (int i = 0; i < q_->l1_indexes_.size(); i++)
    iters.push_back(new L1SeriesIterator(q_, i, tsid_, q_->cache_));

  // Add L0 iterators.
  // NOTE(Alec): SSTables in the same L0 partition may still be overlapping.
  for (int i = 0; i < q_->l0_indexes_.size(); i++) {
    ::tsdb::querier::MergeSeriesIterator* mit =
        new ::tsdb::querier::MergeSeriesIterator();
    std::vector<Iterator*> list;
    q_->current_->AddIterators(ReadOptions(), 0, q_->l0_indexes_[i], &list,
                               tsid_);
    for (Iterator* it : list)
      mit->push_back(new L0SeriesIterator(q_, tsid_, it, q_->cache_));
    iters.push_back(mit);
  }

  // Add mem iterators.
  for (int i = 0; i < q_->imms_.size(); i++)
    iters.push_back(new MemSeriesIterator(q_->imms_[i], q_->min_time_,
                                          q_->max_time_, tsid_, q_->cache_));
  if (q_->mem_)
    iters.push_back(new MemSeriesIterator(q_->mem_, q_->min_time_,
                                          q_->max_time_, tsid_, q_->cache_));

  // Add head iterator.
  if (!head_chunk_contents_.empty())
    iters.push_back(new ::tsdb::head::HeadIterator(
        &head_chunk_contents_, q_->min_time_, q_->max_time_));

  return std::unique_ptr<::tsdb::querier::SeriesIteratorInterface>(
      new ::tsdb::querier::MergeSeriesIterator(iters));
}

std::unique_ptr<::tsdb::querier::SeriesIteratorInterface>
DBSeries::chain_iterator() {
  if (!init_) {
    q_->head_->series(tsid_, lset_, &head_chunk_contents_);
    init_ = true;
  }
  std::vector<::tsdb::querier::SeriesIteratorInterface*> iters;
  iters.reserve(q_->l2_indexes_.size() + q_->l1_indexes_.size() +
                q_->l0_indexes_.size() + 3);

  // Add L2 iterators.
  for (int i = 0; i < q_->l2_indexes_.size(); i++) {
    iters.push_back(new L2SeriesIterator(q_, i, tsid_, q_->cache_));
    std::vector<Iterator*> list;
    q_->current_->AddIterators(ReadOptions(), 2, q_->l2_indexes_[i], &list,
                               tsid_, false);
    for (Iterator* it : list)
      iters.push_back(new L2SeriesIterator(q_, tsid_, it, q_->cache_));
  }

  // Add L1 iterators.
  for (int i = 0; i < q_->l1_indexes_.size(); i++)
    iters.push_back(new L1SeriesIterator(q_, i, tsid_, q_->cache_));

  // Add L0 iterators.
  // NOTE(Alec): SSTables in the same L0 partition may still be overlapping.
  for (int i = 0; i < q_->l0_indexes_.size(); i++) {
    std::vector<Iterator*> list;
    q_->current_->AddIterators(ReadOptions(), 0, q_->l0_indexes_[i], &list,
                               tsid_);
    for (Iterator* it : list)
      iters.push_back(new L0SeriesIterator(q_, tsid_, it, q_->cache_));
  }

  // Add mem iterators.
  for (int i = 0; i < q_->imms_.size(); i++)
    iters.push_back(new MemSeriesIterator(q_->imms_[i], q_->min_time_,
                                          q_->max_time_, tsid_, q_->cache_));
  if (q_->mem_)
    iters.push_back(new MemSeriesIterator(q_->mem_, q_->min_time_,
                                          q_->max_time_, tsid_, q_->cache_));

  // Add head iterator.
  if (!head_chunk_contents_.empty())
    iters.push_back(new ::tsdb::head::HeadIterator(
        &head_chunk_contents_, q_->min_time_, q_->max_time_));

  return std::unique_ptr<::tsdb::querier::SeriesIteratorInterface>(
      new MergeSeriesIterator(iters));
}

/**********************************************
 *                   DBGroup                  *
 **********************************************/
DBGroup::DBGroup(const DBQuerier* q, uint64_t id,
                 const std::vector<::tsdb::label::MatcherInterface*>& l)
    : q_(q), gid_(id), cur_idx_(-1) {
  err_ = !(q_->head_->group(gid_, l, &slots_, &group_lset_, &individual_lsets_,
                            &head_chunk_contents_));
  if (!err_)
    chunk_.reset(new ::tsdb::chunk::NullSupportedXORGroupChunk(
        reinterpret_cast<const uint8_t*>(head_chunk_contents_.c_str()),
        head_chunk_contents_.size()));
}

std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> DBGroup::iterator() {
  if (err_)
    return std::unique_ptr<::tsdb::querier::SeriesIteratorInterface>(
        new ::tsdb::querier::EmptySeriesIterator());

  std::vector<::tsdb::querier::SeriesIteratorInterface*> iters;
  iters.reserve(q_->l2_indexes_.size() + q_->l1_indexes_.size() +
                q_->l0_indexes_.size() + 3);
  // Add L2 iterators.
  for (int i = 0; i < q_->l2_indexes_.size(); i++) {
    ::tsdb::querier::MergeSeriesIterator* mit =
        new ::tsdb::querier::MergeSeriesIterator();
    mit->push_back(
        new L2SeriesIterator(q_, i, gid_, slots_[cur_idx_], q_->cache_));
    std::vector<Iterator*> list;
    q_->current_->AddIterators(ReadOptions(), 2, q_->l2_indexes_[i], &list,
                               gid_, false);
    for (Iterator* it : list)
      mit->push_back(
          new L2SeriesIterator(q_, gid_, slots_[cur_idx_], it, q_->cache_));
    iters.push_back(mit);
  }

  // Add L1 iterators.
  for (int i = 0; i < q_->l1_indexes_.size(); i++)
    iters.push_back(
        new L1SeriesIterator(q_, i, gid_, slots_[cur_idx_], q_->cache_));

  // Add L0 iterators.
  // NOTE(Alec): SSTables in the same L0 partition may still be overlapping.
  for (int i = 0; i < q_->l0_indexes_.size(); i++) {
    ::tsdb::querier::MergeSeriesIterator* mit =
        new ::tsdb::querier::MergeSeriesIterator();
    std::vector<Iterator*> list;
    q_->current_->AddIterators(ReadOptions(), 0, q_->l0_indexes_[i], &list,
                               gid_);
    for (Iterator* it : list)
      mit->push_back(
          new L0SeriesIterator(q_, gid_, slots_[cur_idx_], it, q_->cache_));
    iters.push_back(mit);
  }

  // Add mem iterators.
  for (int i = 0; i < q_->imms_.size(); i++)
    iters.push_back(new MemSeriesIterator(q_->imms_[i], q_->min_time_,
                                          q_->max_time_, gid_, slots_[cur_idx_],
                                          q_->cache_));
  if (q_->mem_)
    iters.push_back(new MemSeriesIterator(q_->mem_, q_->min_time_,
                                          q_->max_time_, gid_, slots_[cur_idx_],
                                          q_->cache_));

  // Add head iterator.
  if (!head_chunk_contents_.empty())
    iters.push_back(new ::tsdb::head::HeadGroupIterator(
        std::move(chunk_->iterator(slots_[cur_idx_])), q_->min_time_,
        q_->max_time_));

  return std::unique_ptr<::tsdb::querier::SeriesIteratorInterface>(
      new ::tsdb::querier::MergeSeriesIterator(iters));
}

std::unique_ptr<::tsdb::querier::SeriesIteratorInterface>
DBGroup::chain_iterator() {
  if (err_)
    return std::unique_ptr<::tsdb::querier::SeriesIteratorInterface>(
        new ::tsdb::querier::EmptySeriesIterator());

  std::vector<::tsdb::querier::SeriesIteratorInterface*> iters;
  iters.reserve(q_->l2_indexes_.size() + q_->l1_indexes_.size() +
                q_->l0_indexes_.size() + 3);

  // Add L2 iterators.
  for (int i = 0; i < q_->l2_indexes_.size(); i++) {
    iters.push_back(
        new L2SeriesIterator(q_, i, gid_, slots_[cur_idx_], q_->cache_));
    std::vector<Iterator*> list;
    q_->current_->AddIterators(ReadOptions(), 2, q_->l2_indexes_[i], &list,
                               gid_, false);
    for (Iterator* it : list)
      iters.push_back(
          new L2SeriesIterator(q_, gid_, slots_[cur_idx_], it, q_->cache_));
  }

  // Add L1 iterators.
  for (int i = 0; i < q_->l1_indexes_.size(); i++)
    iters.push_back(
        new L1SeriesIterator(q_, i, gid_, slots_[cur_idx_], q_->cache_));

  // Add L0 iterators.
  // NOTE(Alec): SSTables in the same L0 partition may still be overlapping.
  for (int i = 0; i < q_->l0_indexes_.size(); i++) {
    std::vector<Iterator*> list;
    q_->current_->AddIterators(ReadOptions(), 0, q_->l0_indexes_[i], &list,
                               gid_);
    for (Iterator* it : list)
      iters.push_back(
          new L0SeriesIterator(q_, gid_, slots_[cur_idx_], it, q_->cache_));
  }

  // Add mem iterators.
  for (int i = 0; i < q_->imms_.size(); i++)
    iters.push_back(new MemSeriesIterator(q_->imms_[i], q_->min_time_,
                                          q_->max_time_, gid_, slots_[cur_idx_],
                                          q_->cache_));
  if (q_->mem_)
    iters.push_back(new MemSeriesIterator(q_->mem_, q_->min_time_,
                                          q_->max_time_, gid_, slots_[cur_idx_],
                                          q_->cache_));

  // Add head iterator.
  if (!head_chunk_contents_.empty())
    iters.push_back(new ::tsdb::head::HeadGroupIterator(
        std::move(chunk_->iterator(slots_[cur_idx_])), q_->min_time_,
        q_->max_time_));

  return std::unique_ptr<::tsdb::querier::SeriesIteratorInterface>(
      new MergeSeriesIterator(iters));
}

/**********************************************
 *                DBSeriesSet                 *
 **********************************************/
DBSeriesSet::DBSeriesSet(const DBQuerier* q,
                         const std::vector<::tsdb::label::MatcherInterface*>& l)
    : q_(q), matchers_(l) {
  auto p = ::tsdb::head::postings_for_matchers(q->head_, l);
  p_ = std::move(p.first);
  if (!p.second) {
    err_.set("error DBSeriesSet postings_for_matchers");
    LOG_DEBUG << "error DBSeriesSet postings_for_matchers";
    p_.reset();
  }
}

/**********************************************
 *                  DBQuerier                 *
 **********************************************/
DBQuerier::DBQuerier(MemTable* mem, ::tsdb::head::HeadType* head,
                     int64_t min_time, int64_t max_time)
    : mem_(mem),
      head_(head),
      need_unref_current_(false),
      min_time_(min_time),
      max_time_(max_time) {
  if (min_time_ < 0) min_time_ = 0;
}

DBQuerier::DBQuerier(MemTable* mem1, MemTable* mem2,
                     ::tsdb::head::HeadType* head, int64_t min_time,
                     int64_t max_time)
    : mem_(mem1),
      head_(head),
      need_unref_current_(false),
      min_time_(min_time),
      max_time_(max_time) {
  imms_.push_back(mem2);
  if (min_time_ < 0) min_time_ = 0;
}

DBQuerier::DBQuerier(::tsdb::head::HeadType* head, int l, Version* v,
                     const std::vector<std::pair<int64_t, int64_t>>& partitions,
                     const std::vector<int>& indexes, const Comparator* cmp,
                     int64_t min_time, int64_t max_time)
    : mem_(nullptr),
      head_(head),
      current_(v),
      need_unref_current_(false),
      cmp_(cmp),
      min_time_(min_time),
      max_time_(max_time) {
  if (min_time_ < 0) min_time_ = 0;
  if (l == 0) {
    l0_partitions_ = partitions;
    l0_indexes_ = indexes;
  } else if (l == 1) {
    l1_partitions_ = partitions;
    l1_indexes_ = indexes;
  } else if (l == 2) {
    l2_partitions_ = partitions;
    l2_indexes_ = indexes;
  }
}

DBQuerier::DBQuerier(DBImpl* db, ::tsdb::head::HeadType* head, int64_t min_time,
                     int64_t max_time, Cache* cache)
    : db_(db),
      head_(head),
      need_unref_current_(true),
      min_time_(min_time),
      max_time_(max_time),
      cmp_(&db->internal_comparator_),
      cache_(cache) {
  if (min_time_ < 0) min_time_ = 0;
  MutexLock l(&db_->mutex_);
  register_mem_partitions();
  register_disk_partitions();
}

DBQuerier::~DBQuerier() {
  if (need_unref_current_) {
    MutexLock l(&db_->mutex_);
    current_->Unref();
  }
}

void DBQuerier::register_mem_partitions() {
  mem_ = db_->mem_;
  imms_ = db_->imms_;
}

void DBQuerier::register_disk_partitions() {
  current_ = db_->versions_->current();
  current_->Ref();
  current_->OverlappingPartitions(0, min_time_, max_time_, &l0_partitions_,
                                  &l0_indexes_);
  ::tsdb::cloud::S3Wrapper* wrapper = nullptr;
  if (db_->s3_wrapper_) wrapper = db_->s3_wrapper_.get();

  current_->OverlappingPartitions(1, min_time_, max_time_, &l1_partitions_,
                                  &l1_indexes_);

  current_->OverlappingPartitions(2, min_time_, max_time_, &l2_partitions_,
                                  &l2_indexes_);
}

std::unique_ptr<::tsdb::querier::SeriesSetInterface> DBQuerier::select(
    const std::vector<::tsdb::label::MatcherInterface*>& l) const {
  return std::unique_ptr<::tsdb::querier::SeriesSetInterface>(
      new DBSeriesSet(this, l));
}

std::vector<std::string> DBQuerier::label_values(const std::string& s) const {
  return head_->label_values(s);
}

std::vector<std::string> DBQuerier::label_names() const {
  return head_->label_names();
}

}  // namespace leveldb