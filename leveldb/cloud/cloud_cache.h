#pragma once
#include <atomic>
#include <deque>
#include <list>
#include <memory>
#include <set>
#include <stdint.h>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "leveldb/cache.h"
#include "leveldb/env.h"

#include "port/port.h"
// #include "third-party/thread_pool.h"

namespace leveldb {

extern uint32_t PAGE_SIZE;
extern uint32_t FIRST_LEVEL_GRANULARITY;
extern uint32_t SECOND_LEVEL_GRANULARITY;
extern uint32_t THIRD_LEVEL_GRANULARITY;
extern uint8_t MAX_SAMPLING_COUNT;
extern uint32_t FIXED_GRANULARITY;
extern int SHARDED_FRAGMENT_MAP_NUM;
extern int SHARDED_PERSISTENT_MAP_NUM;
extern int MUTABLE_PERSISTENT_BLOCK_NUM;

// Map the page range to a unique id for each cloud file.
class FragmentMap {
 public:
  FragmentMap(uint64_t file_size);
  ~FragmentMap() = default;

  uint8_t lookup(uint32_t offset);

  void insert(uint32_t offset, uint32_t size, uint8_t typ);

  void evict(uint32_t offset);

  uint64_t file_size() { return file_size_; }
  uint64_t cached_size() { return cached_size_; }

  void print();

  void clean() {
    cached_size_ = 0;
    fragments_.clear();
  }

 private:
  uint64_t file_size_;
  uint64_t cached_size_;
  std::deque<std::pair<uint64_t, uint8_t>> fragments_;
};

class CloudCache;
class PersistentCacheManager {
 public:
  struct VarBlock {
    char* buf_;
    uint32_t size_;
    uint64_t key_;
    VarBlock() : buf_(nullptr) {}
    // ~VarBlock() {
    //  if (buf_)
    //    delete buf_;
    // }
  };
  PersistentCacheManager(Env* env, uint64_t cache_size,
                         uint64_t block_size = 1 * 1024 * 1024,
                         CloudCache* ccache = nullptr);

  Status insert_page(uint64_t key, char* data, uint32_t size);
  Status get_page(uint64_t key, char* data, uint32_t size);

  uint64_t convert(int block_id, int idx, bool on_disk);
  uint64_t convert(uint32_t file_id, uint32_t offset);
  void decode(uint64_t value, int* block_id, int* idx, bool* on_disk);
  uint64_t cache_size() { return cache_size_; }
  uint64_t disk_size() { return disk_size_; }
  void get_block_range(int* start, int* end);

 private:
  Status create_block(int block_id);
  Status evict_block();

  Env* env_;
  EnvOptions opts_;
  uint64_t cache_size_;
  uint64_t block_size_;
  uint32_t unflushed_size_;
  uint64_t disk_size_;
  uint32_t block_id_;
  uint32_t first_block_;

  std::vector<VarBlock> unflushed_blocks_;
  std::deque<uint32_t> disk_block_sizes_;
  std::deque<std::unique_ptr<RandomRWFile>> blocks_;
  std::unordered_map<uint64_t, uint64_t> indices_;
  std::unordered_map<uint64_t, uint64_t> inverted_indices_;

  port::Mutex lock_;
  CloudCache* ccache_;
};

// Call functions when file exists.
class CloudCache {
 public:
  CloudCache(const std::shared_ptr<Cache>& cache,
             uint64_t cache_size = 1 * 1024 * 1024 * 1024,
             uint64_t block_size = 1 * 1024 * 1024, Env* env = nullptr);
  ~CloudCache() {
    if (pc_) pc_.reset();
    page_cache_.reset();
  }

  void clean();

  void insert(const std::string& file, uint32_t off, uint32_t size, void* value,
              size_t charge, void (*deleter)(const Slice& key, void* value),
              bool low_pri = true);
  void insert(const std::string& file, uint32_t off, uint32_t size, void* value,
              size_t charge, bool low_pri = true);
  void insert(int file_id, uint32_t off, uint32_t size, void* value,
              size_t charge, void (*deleter)(const Slice& key, void* value),
              bool low_pri = true);

  Cache::Handle* lookup(const std::string& file, uint32_t off, uint32_t size);
  Cache::Handle* lookup(int file_id, uint32_t off, uint32_t size);

  void evict_page(const std::string& file, int off);
  void evict_page(int file_id, int off);

  int file_id(const std::string& file);
  uint64_t file_size(const std::string& file);
  uint64_t cached_size(const std::string& file);

  // Dangerous to use.
  FragmentMap* get_map(const std::string& file);
  FragmentMap* get_map(uint32_t file_id);

  bool release(Cache::Handle* handle, bool force_erase = false);

  void add_file(const std::string& file_path, uint64_t file_size);
  void remove_file(const std::string& file_path);

  void add_compaction_file(const std::string& file);
  void remove_compaction_file(const std::string& file);
  bool in_compaction(const std::string& file);

  void* value(Cache::Handle* handle) { return page_cache_->Value(handle); }

  void print_summary(bool detail = false);

  // void TEST_persist_info(const std::string& file, int page_id1, int*
  // block_id, int* page_id2, bool* on_disk);

  void add_cache_stat(uint64_t size, bool is_key);
  void clear_cache_stat();
  // void adjust_cache_high_pri_ratio();

  // private:
  std::string convert(int file_id, int page_id);

  std::shared_ptr<Cache> page_cache_;

  // Map file name to hash id (indexed through hash id).
  std::vector<std::unordered_map<std::string, int>> sharded_ids_;

  // Map file_id_ to map of FragmentMap.
  std::vector<std::unordered_map<int, std::unique_ptr<FragmentMap>>>
      sharded_fm_;
  std::vector<port::RWMutex> fm_locks_;
  // std::vector<std::atomic<bool>> lock_held_;

  std::atomic<int> file_id_;

  std::unique_ptr<PersistentCacheManager> pc_;

  std::unordered_set<std::string> compaction_files_;
  port::Mutex compaction_files_lock_;

  uint64_t acc_key_size_;
  uint64_t acc_value_size_;

  // ThreadPool pool_;
  uint64_t persistent_cache_size_;
  uint64_t persistent_block_size_;
  Env* env_;
};

}  // namespace leveldb.