#pragma once

#include "db/index_reader.h"
#include "db/table_cache.h"
#include <deque>
#include <initializer_list>
#include <memory>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "leveldb/env.h"
#include "leveldb/status.h"

#include "base/Endian.hpp"
#include "base/Mutex.hpp"
#include "chunk/BitStream.hpp"
#include "chunk/XORChunk.hpp"
#include "chunk/XORIterator.hpp"
#include "cloud/S3.hpp"
#include "index/DDPostings.hpp"
#include "index/IndexReader.hpp"
#include "index/IndexUtils.hpp"
#include "index/IndexWriter.hpp"
#include "index/MemPostings.hpp"
#include "index/PostingsInterface.hpp"
#include "label/Label.hpp"
#include "querier/SeriesIteratorInterface.hpp"
#include "tsdbutil/ByteSlice.hpp"
#include "tsdbutil/EncBuf.hpp"
#include "tsdbutil/ListStringTuples.hpp"
#include "tsdbutil/MMapSlice.hpp"
#include "tsdbutil/StringTuplesInterface.hpp"

namespace leveldb {

extern int INDEX2_SHARDING_FACTOR;
extern int MEM_TUPLE_SIZE;
extern int DISK_TUPLE_SIZE;
extern int CLOUD_TUPLE_SIZE;

class MemSeriesMeta {
 public:
  MemSeriesMeta(uint64_t id, const ::tsdb::label::Labels& lset)
      : ref(id), labels(lset) {}

  void add(int64_t time) { time_boundaries.push_back(time); }

  uint64_t ref;
  ::tsdb::label::Labels labels;
  std::vector<int64_t> time_boundaries;
};

// Note(Alec): this data structure is not thread-safe.
class PartitionMemIndex {
 public:
  PartitionMemIndex()
      : series_(1 << 14),
        locks(1 << 14),
        posting_list(new ::tsdb::index::MemPostings()),
        ref_count(0) {}
  PartitionMemIndex(int64_t t)
      : series_(1 << 14),
        locks(1 << 14),
        time_boundary(t),
        posting_list(new ::tsdb::index::MemPostings()),
        ref_count(0) {}

  int64_t get_time_boundary() { return time_boundary; }
  void set_time_boundary(int64_t time) { time_boundary = time; }

  void add(uint64_t id, const ::tsdb::label::Labels& lset, int64_t time);

  void add(uint64_t id, int64_t time);

  std::unordered_set<std::string> symbols() const { return symbols_; }

  std::pair<std::unique_ptr<::tsdb::index::PostingsInterface>, bool> postings(
      const std::string& name, const std::string& value);

  bool series(uint64_t id, ::tsdb::label::Labels& lset,
              std::vector<int64_t>& timestamps);

  std::unique_ptr<::tsdb::tsdbutil::StringTuplesInterface> label_values(
      const std::string& name);

  std::vector<std::string> label_names();

  std::unique_ptr<::tsdb::index::PostingsInterface> sorted_postings(
      std::unique_ptr<::tsdb::index::PostingsInterface>&& p);

  void clear();

  // Increase reference count.
  void Ref() { ++ref_count; }

  // Drop reference count.  Delete if no more references exist.
  void Unref() {
    --ref_count;
    assert(ref_count >= 0);
    if (ref_count <= 0) {
      delete this;
    }
  }

  mutable ::tsdb::base::RWMutexLock mutex_;
  mutable std::vector<::tsdb::base::PadRWMutexLock>
      locks;  // To align cache line (multiples of 64 bytes)
  std::unordered_set<std::string> symbols_;
  std::unordered_map<std::string, std::unordered_set<std::string>>
      label_values_;  // label name to possible values

  std::unique_ptr<::tsdb::index::MemPostings> posting_list;

  std::vector<std::unordered_map<uint64_t, std::unique_ptr<MemSeriesMeta>>>
      series_;  // Index by mod series ref.

  int64_t time_boundary;
  int ref_count;
};

class CloudTimeseriesFileIterator;
class CloudTimeseriesFile {
 public:
  CloudTimeseriesFile(const std::string& dir, uint64_t tsid, int patch,
                      ::tsdb::cloud::S3Wrapper* wrapper = nullptr);
  CloudTimeseriesFile(RandomAccessFile* f, uint64_t tsid)
      : b_(f), tsid_(tsid) {}
  ~CloudTimeseriesFile() {
    if (b_) delete b_;
  }

  Status status() { return s_; }
  void init();
  Status read(size_t off, size_t len, Slice* s, char* buf);
  std::vector<int64_t> timestamps();

 private:
  friend class CloudTimeseriesFileIterator;
  friend class CloudTimeseriesFileGroupIterator;

  RandomAccessFile* b_;
  std::string dir_;
  uint64_t tsid_;
  ::tsdb::cloud::S3Wrapper* s3_wrapper_;
  Status s_;
  ::tsdb::base::MutexLock mutex_;

  // TODO(Alec), whether to cache the header?
  std::vector<int64_t> timestamps_;
  std::vector<uint32_t> offsets_;
  // NOTE(Alec): represent #groups for group.
  uint32_t num_tuples_;
};

class CloudTimeseriesFileIterator
    : public ::tsdb::querier::SeriesIteratorInterface {
 public:
  CloudTimeseriesFileIterator(CloudTimeseriesFile* f, int64_t st, int64_t mt);
  ~CloudTimeseriesFileIterator() {
    if (buf_) delete buf_;
  }

  Status status() { return f_->status(); }
  bool read_chunk() const;
  bool seek(int64_t t) const;
  std::pair<int64_t, double> at() const { return iter_->at(); }
  bool next() const;

  bool error() const { return !f_->status().ok(); }

 private:
  mutable CloudTimeseriesFile* f_;
  int64_t starting_time_;
  int64_t max_time_;
  mutable char* buf_;
  mutable Slice s_;
  mutable std::unique_ptr<::tsdb::chunk::XORChunk> chunk_;
  mutable std::unique_ptr<::tsdb::chunk::XORIterator> iter_;
  mutable bool init_;
  mutable int idx_;
};

class CloudTimeseriesFileGroupIterator
    : public ::tsdb::querier::SeriesIteratorInterface {
 public:
  CloudTimeseriesFileGroupIterator(CloudTimeseriesFile* f, int slot, int64_t st,
                                   int64_t mt);
  ~CloudTimeseriesFileGroupIterator() {
    if (buf_) delete buf_;
  }

  Status status() { return f_->status(); }
  bool read_chunk() const;
  bool seek(int64_t t) const;
  std::pair<int64_t, double> at() const { return iter_->at(); }
  bool next() const;
  bool error() const { return !f_->status().ok(); }

 private:
  mutable CloudTimeseriesFile* f_;
  int slot_;
  int64_t starting_time_;
  int64_t max_time_;
  mutable char* buf_;
  mutable Slice s_;
  mutable std::unique_ptr<::tsdb::chunk::NullSupportedXORGroupChunk> chunk_;
  mutable std::unique_ptr<::tsdb::chunk::ChunkIteratorInterface> iter_;
  mutable bool init_;
  mutable int idx_;
};

class PartitionCloudIndexReader {
 public:
  PartitionCloudIndexReader() = default;
  PartitionCloudIndexReader(const std::string& dir, int64_t time, Env* env,
                            ::tsdb::cloud::S3Wrapper* wrapper = nullptr)
      : dir_(dir),
        time_(time),
        s3_wrapper_(wrapper),
        table_cache_(nullptr),
        env_(env) {}

  PartitionCloudIndexReader(const std::string& dir, int64_t time,
                            TableCache* tc, Env* env,
                            ::tsdb::cloud::S3Wrapper* wrapper = nullptr)
      : dir_(dir),
        time_(time),
        s3_wrapper_(wrapper),
        table_cache_(tc),
        env_(env) {}

  ~PartitionCloudIndexReader() {
    if (!table_cache_) {
      // delete reader_;
      for (auto& p1 : m_)
        for (auto& p2 : p1.second) delete p2.second;
    } else {
      for (auto& p1 : m_handles_)
        for (auto& p2 : p1.second) table_cache_->ReleaseHandle(p2.second);
    }
  }

  Status status() { return s_; }

  CloudTimeseriesFile* get_timeseries_file(uint64_t tsid, int patch);

  int check_patches(uint64_t tsid);

  std::vector<int64_t> timestamps(uint64_t tsid) {
    return get_timeseries_file(tsid, -1)->timestamps();
  }

  tsdb::querier::SeriesIteratorInterface* iterator(uint64_t tsid, int64_t st,
                                                   int64_t mt);

  tsdb::querier::SeriesIteratorInterface* iterator(uint64_t gid, int slot,
                                                   int64_t st, int64_t mt);

 private:
  std::string dir_;
  int64_t time_;
  ::tsdb::cloud::S3Wrapper* s3_wrapper_;
  TableCache* table_cache_;
  Env* env_;

  // NOTE(Alec), Hold the files accessed by iterators.
  std::unordered_map<uint64_t, std::unordered_map<int, CloudTimeseriesFile*>>
      m_;
  std::unordered_map<uint64_t, std::unordered_map<int, Cache::Handle*>>
      m_handles_;

  std::unordered_map<uint64_t, int> patches_num_;

  ::tsdb::base::MutexLock mutex_;
  Status s_;
};

}  // namespace leveldb.