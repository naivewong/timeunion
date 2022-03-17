#include "cloud/cloud_cache.h"

#include <algorithm>
#include <functional>
#include <iostream>
#include <limits>
#include <sstream>
#include <thread>

#include "util/coding.h"
#include "util/mutexlock.h"

#include "cloud/aws/aws_env.h"

namespace leveldb {

uint32_t PAGE_SIZE = 2048;
uint32_t FIRST_LEVEL_GRANULARITY = 8 * 1024 * 1024;
uint32_t SECOND_LEVEL_GRANULARITY = 1 * 1024 * 1024;
uint32_t THIRD_LEVEL_GRANULARITY = 128 * 1024;
uint8_t MAX_SAMPLING_COUNT = 3;
uint32_t FIXED_GRANULARITY = 1 * 1024 * 1024;
int SHARDED_FRAGMENT_MAP_NUM = 64;
int SHARDED_PERSISTENT_MAP_NUM = 256;
int MUTABLE_PERSISTENT_BLOCK_NUM = 4;

struct fm_compare {
  bool operator()(const std::pair<uint64_t, uint8_t>& value,
                  const uint32_t& key) {
    return ((value.first >> 32) < key);
  }
  bool operator()(const uint32_t& key,
                  const std::pair<uint64_t, uint8_t>& value) {
    return (key < (value.first >> 32));
  }
};

FragmentMap::FragmentMap(uint64_t file_size)
    : file_size_(file_size), cached_size_(0) {}

uint8_t FragmentMap::lookup(uint32_t offset) {
  auto it = std::lower_bound(fragments_.begin(), fragments_.end(), offset,
                             fm_compare());
  if (!(it == fragments_.end() || ((it->first) >> 32) != offset)) {
    return it->second;
  } else {
    return 0;
  }
}

void FragmentMap::insert(uint32_t offset, uint32_t size, uint8_t typ) {
  auto it = std::lower_bound(fragments_.begin(), fragments_.end(), offset,
                             fm_compare());
  if (it == fragments_.end() || ((it->first) >> 32) != offset) {
    cached_size_ += size;
    fragments_.insert(it, {((uint64_t)(offset) << 32) | (uint64_t)size, typ});
  } else {
    (*it).second = typ;
  }
}

void FragmentMap::evict(uint32_t offset) {
  auto it = std::lower_bound(fragments_.begin(), fragments_.end(), offset,
                             fm_compare());
  if (!(it == fragments_.end() || ((it->first) >> 32) != offset)) {
    cached_size_ -= it->first & 0xffffffff;
    fragments_.erase(it);
  }
}

void FragmentMap::print() {
  for (const auto& p : fragments_)
    std::cout << "<" << (p.first >> 32) << "," << (p.first & 0x00000000ffffffff)
              << "," << p.second << ">, " << std::endl;
}

PersistentCacheManager::PersistentCacheManager(Env* env, uint64_t cache_size,
                                               uint64_t block_size,
                                               CloudCache* ccache)
    : env_(env),
      cache_size_(cache_size),
      block_size_(block_size),
      unflushed_size_(0),
      disk_size_(0),
      block_id_(0),
      first_block_(0),
      ccache_(ccache) {
  // Init the first block.
  std::vector<std::string> files;
  if (env_->GetChildren("cloud_pcache", &files).ok()) {
    for (const std::string& file : files)
      env_->DeleteFile("cloud_pcache/" + file);
  }
  env_->DeleteDir("cloud_pcache");
  env_->CreateDir("cloud_pcache");
}

inline uint64_t PersistentCacheManager::convert(int block_id, int idx,
                                                bool on_disk) {
  return (((uint64_t)(block_id) << 32) | (uint64_t)(idx)) |
         ((uint64_t)(!on_disk) << 63);
}

inline uint64_t PersistentCacheManager::convert(uint32_t file_id,
                                                uint32_t offset) {
  return (((uint64_t)(file_id) << 32) | (uint64_t)(offset));
}

inline void PersistentCacheManager::decode(uint64_t value, int* block_id,
                                           int* idx, bool* on_disk) {
  *block_id = (value >> 32) & 0x7fffffff;
  *idx = value & 0xffffffff;
  *on_disk = (value & 0x8000000000000000) == 0;
}

Status PersistentCacheManager::create_block(int block_id) {
  char name[20];
  sprintf(name, "cloud_pcache/%06d", block_id);
  WritableFile* w;
  Status st = env_->NewWritableFile(name, &w, opts_);
  if (!st.ok()) {
    return st;
  }
  delete w;
  blocks_.push_back(std::unique_ptr<RandomRWFile>());
  st = env_->NewRandomRWFile(name, &(blocks_.back()), opts_);

  return st;
}

// TODO(Alec), block selection.
Status PersistentCacheManager::evict_block() {
  Status st;
  char name[20];
  sprintf(name, "cloud_pcache/%06d", first_block_);

  // Clear the indices of this block.
  Slice s;
  char* data = new char[4];
  st = blocks_[0]->Read(0, 4, &s, data);
  if (!st.ok()) return st;
  uint32_t num;
  GetFixed32Ptr(data, &num);
  delete data;
  for (size_t i = 0; i < num; i++) {
    uint64_t key = convert(first_block_, i, true);
    uint64_t value = inverted_indices_[key];
    indices_.erase(value);
    inverted_indices_.erase(key);

    // Evict those on-disk pages(some may be already read into mem).
    if (ccache_) ccache_->evict_page(value >> 32, value & 0xffffffff);
  }

  disk_size_ -= disk_block_sizes_[0];
  disk_block_sizes_.pop_front();
  blocks_.pop_front();
  env_->DeleteFile(name);

  first_block_++;
  return st;
}

Status PersistentCacheManager::insert_page(uint64_t key, char* data,
                                           uint32_t size) {
  Status st;
  MutexLock lock(&lock_);

  unflushed_blocks_.emplace_back();
  unflushed_blocks_.back().buf_ = new char[size];
  memcpy(unflushed_blocks_.back().buf_, data, size);
  unflushed_blocks_.back().key_ = key;
  unflushed_blocks_.back().size_ = size;
  uint64_t value = convert(unflushed_blocks_.size() - 1, 0, false);
  indices_.insert({key, value});
  unflushed_size_ += size;

  if (unflushed_size_ >= block_size_) {
    // Create a new disk block and flush.
    uint32_t off1 = unflushed_blocks_.size() * 8 + 4;
    uint32_t off2 = 0;
    std::string tmp_buf;
    PutFixed32(&tmp_buf, unflushed_blocks_.size());
    for (size_t i = 0; i < unflushed_blocks_.size(); i++) {
      PutFixed32(&tmp_buf, off1 + off2);
      PutFixed32(&tmp_buf, unflushed_blocks_[i].size_);
      off2 += unflushed_blocks_[i].size_;

      // Update indices_/inverted_indices_
      value = convert(block_id_, i, true);
      indices_[unflushed_blocks_[i].key_] = value;
      inverted_indices_[value] = unflushed_blocks_[i].key_;
    }
    for (size_t i = 0; i < unflushed_blocks_.size(); i++) {
      tmp_buf.append(unflushed_blocks_[i].buf_, unflushed_blocks_[i].size_);
    }

    st = create_block(block_id_);
    if (!st.ok()) {
      std::cout << st.ToString() << std::endl;
      return st;
    }

    // Write block.
    int relative_block_id = block_id_ - first_block_;
    st = blocks_[relative_block_id]->Write(0, Slice(tmp_buf));
    if (!st.ok()) return st;
    disk_block_sizes_.push_back(unflushed_size_);
    disk_size_ += unflushed_size_;

    // Evict block if necessary.
    if (disk_size_ > cache_size_) {
      st = evict_block();
      if (!st.ok()) return st;
    }

    block_id_++;

    // Manually clean the buf_.
    for (size_t i = 0; i < unflushed_blocks_.size(); i++) {
      if (unflushed_blocks_[i].buf_) delete unflushed_blocks_[i].buf_;
    }
    unflushed_blocks_.clear();
    unflushed_size_ = 0;
  }
  return st;
}

Status PersistentCacheManager::get_page(uint64_t key, char* data,
                                        uint32_t size) {
  Status st;
  MutexLock lock(&lock_);
  auto it = indices_.find(key);
  if (it == indices_.end()) return Status::NotFound();
  int block_id;
  int idx;
  bool on_disk;
  decode(it->second, &block_id, &idx, &on_disk);
  if (on_disk) {
    int relative_block_id = block_id - first_block_;
    if (relative_block_id < 0) return Status::NotFound();
    Slice s;
    char* tmp_buf = new char[8];
    st = blocks_[relative_block_id]->Read(4 + 8 * idx, 8, &s, tmp_buf);
    if (!st.ok()) {
      delete tmp_buf;
      return st;
    }
    uint32_t off;
    GetFixed32Ptr(tmp_buf, &off);
    // GetFixed32Ptr(tmp_buf + 4, &size);
    delete tmp_buf;
    st = blocks_[relative_block_id]->Read(off, size, &s, data);
    if (!st.ok()) return st;
  } else {
    memcpy(data, unflushed_blocks_[block_id].buf_, size);
  }

  return st;
}

void PersistentCacheManager::get_block_range(int* start, int* end) {
  MutexLock lock(&lock_);
  *start = first_block_;
  *end = block_id_;
}

CloudCache::CloudCache(const std::shared_ptr<Cache>& cache, uint64_t cache_size,
                       uint64_t block_size, Env* env)
    : page_cache_(cache),
      sharded_ids_(SHARDED_FRAGMENT_MAP_NUM),
      sharded_fm_(SHARDED_FRAGMENT_MAP_NUM),
      fm_locks_(SHARDED_FRAGMENT_MAP_NUM),
      // lock_held_(SHARDED_FRAGMENT_MAP_NUM, false),
      file_id_(0),
      pc_(nullptr),
      persistent_cache_size_(cache_size),
      persistent_block_size_(block_size),
      env_(nullptr) {
  if (env) {
    env_ = env;
    pc_ = std::unique_ptr<PersistentCacheManager>(
        new PersistentCacheManager(env, cache_size, block_size, this));
  }
}

void CloudCache::clean() {
  pc_.reset();
  page_cache_->Prune();
  if (env_)
    pc_.reset(new PersistentCacheManager(env_, persistent_cache_size_,
                                         persistent_block_size_, this));
  for (auto& shard : sharded_fm_) {
    for (auto& m : shard) m.second->clean();
  }
}

std::string CloudCache::convert(int file_id, int cache_id) {
  std::string r(8, '\0');
  r[0] = (file_id >> 24) & 0xff;
  r[1] = (file_id >> 16) & 0xff;
  r[2] = (file_id >> 8) & 0xff;
  r[3] = file_id & 0xff;
  r[4] = (cache_id >> 24) & 0xff;
  r[5] = (cache_id >> 16) & 0xff;
  r[6] = (cache_id >> 8) & 0xff;
  r[7] = cache_id & 0xff;
  return r;
}

void cloud_cache_callback(const Slice& key_, void* value_, CloudCache* c,
                          int file_id_tmp, uint32_t off_tmp, uint32_t size_tmp,
                          void (*deleter_)(const Slice& key, void* value)) {
  int idx2 = file_id_tmp % SHARDED_FRAGMENT_MAP_NUM;
  bool need_release_lock = false;

  // NOTE(Alec), lock if current thread did not hold the lock.
  if (c->fm_locks_[idx2].HeldPid() != port::CurrentThread::pid()) {
    c->fm_locks_[idx2].WriteLock();
    need_release_lock = true;
  }
  std::unordered_map<int, std::unique_ptr<FragmentMap>>::iterator it2 =
      c->sharded_fm_[idx2].find(file_id_tmp);
  if (it2 != c->sharded_fm_[idx2].end()) {
    if (c->pc_) {
      // Evict the page from FragmentMap.
      // TODO(Alec), sanity check.
      it2->second->insert(off_tmp, size_tmp, 2);
      c->pc_->insert_page(c->pc_->convert(file_id_tmp, off_tmp),
                          (char*)(value_), size_tmp);
    } else {
      // Evict the page from FragmentMap.
      // TODO(Alec), sanity check.
      it2->second->evict(off_tmp);
    }
  }
  if (need_release_lock) c->fm_locks_[idx2].WriteUnlock();

  if (deleter_) deleter_(key_, value_);
}

void CloudCache::insert(const std::string& file, uint32_t off, uint32_t size,
                        void* value, size_t charge,
                        void (*deleter)(const Slice& key, void* value),
                        bool low_pri) {
  int idx1 = std::hash<std::string>{}(file) % SHARDED_FRAGMENT_MAP_NUM;
  std::unordered_map<std::string, int>::iterator it1;
  int file_id;
  {
    ReadLock lock(&fm_locks_[idx1]);

    it1 = sharded_ids_[idx1].find(file);
    if (it1 == sharded_ids_[idx1].end()) return;
    file_id = it1->second;
  }
  int idx2 = file_id % SHARDED_FRAGMENT_MAP_NUM;
  {
    // Insert page to fragment map.
    WriteLock lock(&fm_locks_[idx2]);
    // TODO(Alec), sanity check.
    std::unordered_map<int, std::unique_ptr<FragmentMap>>::iterator it2 =
        sharded_fm_[idx2].find(file_id);
    it2->second->insert(off, size, 1);

    auto h = page_cache_->InsertFunc(
        convert(file_id, off), value, charge,
        std::bind(cloud_cache_callback, std::placeholders::_1,
                  std::placeholders::_2, this, file_id, off, size, deleter));
    page_cache_->Release(h);
  }
}

void CloudCache::insert(const std::string& file, uint32_t off, uint32_t size,
                        void* value, size_t charge, bool low_pri) {
  insert(file, off, size, value, charge, nullptr, low_pri);
}

void CloudCache::insert(int file_id, uint32_t off, uint32_t size, void* value,
                        size_t charge,
                        void (*deleter)(const Slice& key, void* value),
                        bool low_pri) {
  int idx2 = file_id % SHARDED_FRAGMENT_MAP_NUM;
  {
    // Insert page to fragment map.
    WriteLock l(&fm_locks_[idx2]);
    // TODO(Alec), sanity check.
    std::unordered_map<int, std::unique_ptr<FragmentMap>>::iterator it2 =
        sharded_fm_[idx2].find(file_id);
    it2->second->insert(off, size, 1);

    auto h = page_cache_->InsertFunc(
        convert(file_id, off), value, charge,
        std::bind(cloud_cache_callback, std::placeholders::_1,
                  std::placeholders::_2, this, file_id, off, size, deleter));
    page_cache_->Release(h);
  }
}

Cache::Handle* CloudCache::lookup(const std::string& file, uint32_t off,
                                  uint32_t size) {
  int idx1 = std::hash<std::string>{}(file) % SHARDED_FRAGMENT_MAP_NUM;
  std::unordered_map<std::string, int>::iterator it1;
  int file_id;
  {
    ReadLock lock(&fm_locks_[idx1]);
    it1 = sharded_ids_[idx1].find(file);
    if (it1 == sharded_ids_[idx1].end()) return nullptr;
    file_id = it1->second;
  }

  int idx2 = file_id % SHARDED_FRAGMENT_MAP_NUM;
  LRUHandle* lh = nullptr;
  {
    WriteLock lock(&fm_locks_[idx2]);
    // TODO(Alec), sanity check.
    std::unordered_map<int, std::unique_ptr<FragmentMap>>::iterator it2 =
        sharded_fm_[idx2].find(file_id);
    uint8_t typ = it2->second->lookup(off);

    if (typ == 2) {
      lh = new LRUHandle();
      lh->refs = 0;
      lh->charge = size;
      lh->value = (void*)(new char[size]);
      Status s =
          pc_->get_page(pc_->convert(file_id, off), (char*)(lh->value), size);
      if (s.IsNotFound()) {
        delete (char*)(lh->value);
        delete lh;
        // TODO(Alec).
        return nullptr;
      }

      return reinterpret_cast<Cache::Handle*>(lh);
    }
  }

  return page_cache_->Lookup(convert(file_id, off));
}

Cache::Handle* CloudCache::lookup(int file_id, uint32_t off, uint32_t size) {
  int idx2 = file_id % SHARDED_FRAGMENT_MAP_NUM;
  LRUHandle* lh = nullptr;
  {
    WriteLock lock(&fm_locks_[idx2]);
    // TODO(Alec), sanity check.
    std::unordered_map<int, std::unique_ptr<FragmentMap>>::iterator it2 =
        sharded_fm_[idx2].find(file_id);
    uint8_t typ = it2->second->lookup(off);
    if (typ == 2) {
      lh = new LRUHandle();
      lh->refs = 0;
      lh->charge = size;
      lh->value = (void*)(new char[size]);
      Status s =
          pc_->get_page(pc_->convert(file_id, off), (char*)(lh->value), size);
      if (s.IsNotFound()) {
        delete (char*)(lh->value);
        delete lh;
        return nullptr;
      }

      return reinterpret_cast<Cache::Handle*>(lh);
    }
  }
  return page_cache_->Lookup(convert(file_id, off));
}

void CloudCache::evict_page(const std::string& file, int off) {
  int idx1 = std::hash<std::string>{}(file) % SHARDED_FRAGMENT_MAP_NUM;
  std::unordered_map<std::string, int>::iterator it1;
  int file_id;
  {
    ReadLock lock(&fm_locks_[idx1]);
    it1 = sharded_ids_[idx1].find(file);
    if (it1 == sharded_ids_[idx1].end()) return;
    file_id = it1->second;
  }

  int idx2 = file_id % SHARDED_FRAGMENT_MAP_NUM;
  bool need_release_lock = false;

  // NOTE(Alec), lock if current thread did not hold the lock.
  if (fm_locks_[idx2].HeldPid() != port::CurrentThread::pid()) {
    fm_locks_[idx2].WriteLock();
    need_release_lock = true;
  }
  if (sharded_fm_[idx2].find(file_id)->second->lookup(off) == 2) {
    // Only evict those on-disk pages.
    sharded_fm_[idx2].find(file_id)->second->evict(off);
  }
  if (need_release_lock) fm_locks_[idx2].WriteUnlock();
}

void CloudCache::evict_page(int file_id, int off) {
  int idx2 = file_id % SHARDED_FRAGMENT_MAP_NUM;
  bool need_release_lock = false;

  // NOTE(Alec), lock if current thread did not hold the lock.
  if (fm_locks_[idx2].HeldPid() != port::CurrentThread::pid()) {
    fm_locks_[idx2].WriteLock();
    need_release_lock = true;
  }
  if (sharded_fm_[idx2].find(file_id)->second->lookup(off) == 2) {
    // Only evict those on-disk pages.
    sharded_fm_[idx2].find(file_id)->second->evict(off);
  }
  if (need_release_lock) fm_locks_[idx2].WriteUnlock();
}

bool CloudCache::release(Cache::Handle* handle, bool force_erase) {
  LRUHandle* e = reinterpret_cast<LRUHandle*>(handle);
  if (e->refs == 0) {
    // NOTE(Alec), do not delete e->value because it's still in cache.
    delete (char*)(e->value);
    delete e;
    return false;
  }
  page_cache_->Release(handle);
  return true;
}

int CloudCache::file_id(const std::string& file) {
  int idx1 = std::hash<std::string>{}(file) % SHARDED_FRAGMENT_MAP_NUM;
  ReadLock lock(&fm_locks_[idx1]);
  return sharded_ids_[idx1].find(file)->second;
}

uint64_t CloudCache::file_size(const std::string& file) {
  int idx1 = std::hash<std::string>{}(file) % SHARDED_FRAGMENT_MAP_NUM;
  std::unordered_map<std::string, int>::iterator it1;
  int fid;
  {
    ReadLock lock(&fm_locks_[idx1]);
    it1 = sharded_ids_[idx1].find(file);
    if (it1 == sharded_ids_[idx1].end()) return 0;
    fid = it1->second;
  }
  int idx2 = fid % SHARDED_FRAGMENT_MAP_NUM;
  ReadLock lock(&fm_locks_[idx2]);
  return sharded_fm_[idx2][fid]->file_size();
}

uint64_t CloudCache::cached_size(const std::string& file) {
  int idx1 = std::hash<std::string>{}(file) % SHARDED_FRAGMENT_MAP_NUM;
  std::unordered_map<std::string, int>::iterator it1;
  int fid;
  {
    ReadLock lock(&fm_locks_[idx1]);
    it1 = sharded_ids_[idx1].find(file);
    if (it1 == sharded_ids_[idx1].end()) return 0;
    fid = it1->second;
  }
  int idx2 = fid % SHARDED_FRAGMENT_MAP_NUM;
  ReadLock lock(&fm_locks_[idx2]);
  return sharded_fm_[idx2][fid]->cached_size();
}

FragmentMap* CloudCache::get_map(const std::string& file) {
  int idx1 = std::hash<std::string>{}(file) % SHARDED_FRAGMENT_MAP_NUM;
  std::unordered_map<std::string, int>::iterator it1;
  int fid;
  {
    ReadLock lock(&fm_locks_[idx1]);
    it1 = sharded_ids_[idx1].find(file);
    if (it1 == sharded_ids_[idx1].end()) return nullptr;
    fid = it1->second;
  }
  int idx2 = fid % SHARDED_FRAGMENT_MAP_NUM;
  ReadLock lock(&fm_locks_[idx2]);
  return sharded_fm_[idx2][fid].get();
}

FragmentMap* CloudCache::get_map(uint32_t file_id) {
  int idx2 = file_id % SHARDED_FRAGMENT_MAP_NUM;
  ReadLock lock(&fm_locks_[idx2]);
  return sharded_fm_[idx2][file_id].get();
}

void CloudCache::add_file(const std::string& file_path, uint64_t file_size) {
  int idx1 = std::hash<std::string>{}(file_path) % SHARDED_FRAGMENT_MAP_NUM;
  int fid = -1;
  {
    WriteLock lock(&fm_locks_[idx1]);
    auto n = sharded_ids_[idx1].find(file_path);
    if (n == sharded_ids_[idx1].end()) {
      fid = file_id_.fetch_add(1);
      sharded_ids_[idx1][file_path] = fid;
    }
  }
  if (fid != -1) {
    int idx2 = fid % SHARDED_FRAGMENT_MAP_NUM;
    WriteLock lock(&fm_locks_[idx2]);
    sharded_fm_[idx2][fid] =
        std::unique_ptr<FragmentMap>(new FragmentMap(file_size));
  }
}

void CloudCache::remove_file(const std::string& file_path) {
  int idx1 = std::hash<std::string>{}(file_path) % SHARDED_FRAGMENT_MAP_NUM;
  int fid = -1;
  {
    WriteLock lock(&fm_locks_[idx1]);
    auto n = sharded_ids_[idx1].find(file_path);
    if (n == sharded_ids_[idx1].end()) {
      return;
    }
    fid = n->second;
    sharded_ids_[idx1].erase(file_path);
  }
  if (fid != -1) {
    int idx2 = fid % SHARDED_FRAGMENT_MAP_NUM;
    WriteLock lock(&fm_locks_[idx2]);
    sharded_fm_[idx2].erase(fid);
  }
}

// void CloudCache::TEST_persist_info(const std::string& file, int page_id1,
// int* block_id, int* page_id2, bool* on_disk) {
//  if (pc_) {
//    int idx1 = std::hash<std::string>{}(file) % SHARDED_FRAGMENT_MAP_NUM;
//    std::unordered_map<std::string, int>::iterator it1;
//    int file_id;
//    {
//      ReadLock lock(&fm_locks_[idx1]);
//      it1 = sharded_ids_[idx1].find(file);
//      if (it1 == sharded_ids_[idx1].end())
//        return;
//      file_id = it1->second;
//    }
//    pc_->TEST_get_info(pc_->convert(file_id, page_id1), block_id, page_id2,
//    on_disk);
//  }
// }

void CloudCache::print_summary(bool detail) {
  printf("----------------------------------\n");
  printf("CloudCache memory:%lu/%lu=%.2f%% disk:%lu/%lu=%.2f%%\n",
         page_cache_->GetUsage(), page_cache_->GetCapacity(),
         static_cast<double>(page_cache_->GetUsage()) /
             static_cast<double>(page_cache_->GetCapacity()) * 100.0,
         pc_ == nullptr ? 0lu : pc_->disk_size(),
         pc_ == nullptr ? 0lu : pc_->cache_size(),
         pc_ == nullptr || pc_->cache_size() == 0
             ? 0
             : static_cast<double>(pc_->disk_size() /
                                   static_cast<double>(pc_->cache_size()) *
                                   100.0));
  if (detail) {
    for (size_t i = 0; i < sharded_ids_.size(); i++) {
      for (auto& p : sharded_ids_[i]) {
        if (sharded_fm_[p.second % SHARDED_FRAGMENT_MAP_NUM][p.second]
                ->cached_size() == 0)
          continue;
        printf(
            "%s: %.2f%%\n", p.first.c_str(),
            (double)(sharded_fm_[p.second % SHARDED_FRAGMENT_MAP_NUM][p.second]
                         ->cached_size()) /
                (double)(sharded_fm_[p.second % SHARDED_FRAGMENT_MAP_NUM]
                                    [p.second]
                                        ->file_size()) *
                100.0);
      }
    }
  }
  if (pc_ && detail) {
    int start, end;
    pc_->get_block_range(&start, &end);
    printf("PersistentCacheManager start_block:%d end_block:%d\n", start, end);
  }
  printf("----------------------------------\n");
}

void CloudCache::add_compaction_file(const std::string& file) {
  MutexLock mlock(&compaction_files_lock_);
  compaction_files_.insert(file);
}

void CloudCache::remove_compaction_file(const std::string& file) {
  MutexLock mlock(&compaction_files_lock_);
  compaction_files_.erase(file);
}

bool CloudCache::in_compaction(const std::string& file) {
  MutexLock mlock(&compaction_files_lock_);
  int c = compaction_files_.count(file);
  return c == 1;
}

void CloudCache::add_cache_stat(uint64_t size, bool is_key) {
  if (is_key) {
    uint64_t new_key_size = acc_key_size_ + size;
    if (new_key_size < acc_key_size_) {
      // Handle overflow.
      acc_key_size_ /= 10;
      acc_key_size_ += size;
    } else {
      acc_key_size_ = new_key_size;
    }
  } else {
    uint64_t new_value_size = acc_value_size_ + size;
    if (new_value_size < acc_value_size_) {
      // Handle overflow.
      acc_value_size_ /= 10;
      acc_value_size_ += size;
    } else {
      acc_value_size_ = new_value_size;
    }
  }
}

void CloudCache::clear_cache_stat() {
  acc_key_size_ = 0;
  acc_value_size_ = 0;
}

// void CloudCache::adjust_cache_high_pri_ratio() {
//   if (acc_key_size_ + acc_value_size_ < (uint64_t)(0.1 *
//   (double)(page_cache_->GetCapacity())))
//     return;
//   double ratio = (double)(acc_key_size_) / (double)(acc_value_size_);
//   if (ratio < 0.1 || ratio > 0.7)
//     return;
//   page_cache_->SetHighPriorityPoolRatio(ratio);
// }

}  // namespace leveldb