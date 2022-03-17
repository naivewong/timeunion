// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#include "db/db_impl.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include <algorithm>
#include <cstdio>

#include "leveldb/env.h"
#include "leveldb/table_builder.h"

#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"

#include "base/Logging.hpp"

namespace leveldb {

int L0_MERGE_FACTOR = 1;
int L1_MERGE_FACTOR = 4;

int L2_MERGE_COMPACTION_THRESHOLD = 3;

bool DYNAMIC_PARTITION_LENGTH = false;
int DYNAMIC_CONTROL_LOCK = 0;
int L2_PARTITION_LENGTH = 7200000;
int FAST_STORAGE_SIZE = 250 * 1024 * 1024;

static size_t TargetFileSize(const Options* options) {
  return options->max_file_size;
}

// Maximum bytes of overlaps in grandparent (i.e., level+2) before we
// stop building a single file in a level->level+1 compaction.
static int64_t MaxGrandParentOverlapBytes(const Options* options) {
  return 10 * TargetFileSize(options);
}

// Maximum number of bytes in all compacted files.  We avoid expanding
// the lower level file set of a compaction if it would make the
// total compaction cover more than this many bytes.
static int64_t ExpandedCompactionByteSizeLimit(const Options* options) {
  return 25 * TargetFileSize(options);
}

static double MaxBytesForLevel(const Options* options, int level) {
  // Note: the result for level zero is not really used since we set
  // the level-0 compaction threshold based on number of files.

  // Result for both level-0 and level-1
  double result = 10. * 1048576.0;
  while (level > 1) {
    result *= 10;
    level--;
  }
  return result;
}

static uint64_t MaxFileSizeForLevel(const Options* options, int level) {
  // We could vary per level to reduce number of files?
  return TargetFileSize(options);
}

static int64_t TotalFileSize(const std::vector<FileMetaData*>& files) {
  int64_t sum = 0;
  for (size_t i = 0; i < files.size(); i++) {
    sum += files[i]->file_size;
  }
  return sum;
}

Version::~Version() {
  assert(refs_ == 0);

  // Remove from linked list
  prev_->next_ = next_;
  next_->prev_ = prev_;

  // Drop references to files
  for (int level = 0; level < config::kNumLevels; level++) {
    for (size_t i = 0; i < files_[level].size(); i++) {
      FileMetaData* f = files_[level][i];
      assert(f->refs > 0);
      f->refs--;
      if (f->refs <= 0) {
        delete f;
      }
    }
  }
}

int FindFile(const InternalKeyComparator& icmp,
             const std::vector<FileMetaData*>& files, const Slice& key) {
  uint32_t left = 0;
  uint32_t right = files.size();
  while (left < right) {
    uint32_t mid = (left + right) / 2;
    const FileMetaData* f = files[mid];
    if (icmp.InternalKeyComparator::Compare(f->largest.Encode(), key) < 0) {
      // Key at "mid.largest" is < "target".  Therefore all
      // files at or before "mid" are uninteresting.
      left = mid + 1;
    } else {
      // Key at "mid.largest" is >= "target".  Therefore all files
      // after "mid" are uninteresting.
      right = mid;
    }
  }
  return right;
}

static bool AfterFile(const Comparator* ucmp, const Slice* user_key,
                      const FileMetaData* f) {
  // null user_key occurs before all keys and is therefore never after *f
  return (user_key != nullptr &&
          ucmp->Compare(*user_key, f->largest.user_key()) > 0);
}

static bool BeforeFile(const Comparator* ucmp, const Slice* user_key,
                       const FileMetaData* f) {
  // null user_key occurs after all keys and is therefore never before *f
  return (user_key != nullptr &&
          ucmp->Compare(*user_key, f->smallest.user_key()) < 0);
}

bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                           bool disjoint_sorted_files,
                           const std::vector<FileMetaData*>& files,
                           const Slice* smallest_user_key,
                           const Slice* largest_user_key) {
  const Comparator* ucmp = icmp.user_comparator();
  if (!disjoint_sorted_files) {
    // Need to check against all files
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      if (AfterFile(ucmp, smallest_user_key, f) ||
          BeforeFile(ucmp, largest_user_key, f)) {
        // No overlap
      } else {
        return true;  // Overlap
      }
    }
    return false;
  }

  // Binary search over file list
  uint32_t index = 0;
  if (smallest_user_key != nullptr) {
    // Find the earliest possible internal key for smallest_user_key
    InternalKey small_key(*smallest_user_key, kMaxSequenceNumber,
                          kValueTypeForSeek);
    index = FindFile(icmp, files, small_key.Encode());
  }

  if (index >= files.size()) {
    // beginning of range is after all files, so no overlap.
    return false;
  }

  return !BeforeFile(ucmp, largest_user_key, files[index]);
}

// An internal iterator.  For a given version/level pair, yields
// information about the files in the level.  For a given entry, key()
// is the largest key that occurs in the file, and value() is an
// 16-byte value containing the file number and file size, both
// encoded using EncodeFixed64.
class Version::LevelFileNumIterator : public Iterator {
 public:
  LevelFileNumIterator(const InternalKeyComparator& icmp,
                       const std::vector<FileMetaData*>* flist)
      : icmp_(icmp), flist_(flist), index_(flist->size()) {  // Marks as invalid
  }
  bool Valid() const override { return index_ < flist_->size(); }
  void Seek(const Slice& target) override {
    index_ = FindFile(icmp_, *flist_, target);
  }
  void SeekToFirst() override { index_ = 0; }
  void SeekToLast() override {
    index_ = flist_->empty() ? 0 : flist_->size() - 1;
  }
  void Next() override {
    assert(Valid());
    index_++;
  }
  void Prev() override {
    assert(Valid());
    if (index_ == 0) {
      index_ = flist_->size();  // Marks as invalid
    } else {
      index_--;
    }
  }
  Slice key() const override {
    assert(Valid());
    return (*flist_)[index_]->largest.Encode();
  }
  Slice value() const override {
    assert(Valid());
    EncodeFixed64(value_buf_, (*flist_)[index_]->number);
    EncodeFixed64(value_buf_ + 8, (*flist_)[index_]->file_size);
    return Slice(value_buf_, sizeof(value_buf_));
  }
  Status status() const override { return Status::OK(); }

 private:
  const InternalKeyComparator icmp_;
  const std::vector<FileMetaData*>* const flist_;
  uint32_t index_;

  // Backing store for value().  Holds the file number and size.
  mutable char value_buf_[16];
};

static Iterator* GetFileIterator(void* arg, const ReadOptions& options,
                                 const Slice& file_value) {
  TableCache* cache = reinterpret_cast<TableCache*>(arg);
  if (file_value.size() != 16) {
    return NewErrorIterator(
        Status::Corruption("FileReader invoked with unexpected value"));
  } else {
    return cache->NewIterator(options, DecodeFixed64(file_value.data()),
                              DecodeFixed64(file_value.data() + 8));
  }
}

Iterator* Version::NewConcatenatingIterator(const ReadOptions& options,
                                            int level) const {
  return NewTwoLevelIterator(
      new LevelFileNumIterator(vset_->icmp_, &files_[level]), &GetFileIterator,
      vset_->table_cache_, options);
}

void Version::AddIterators(const ReadOptions& options,
                           std::vector<Iterator*>* iters) {
  // Merge all level zero files together since they may overlap
  for (size_t i = 0; i < files_[0].size(); i++) {
    iters->push_back(vset_->table_cache_->NewIterator(
        options, files_[0][i]->number, files_[0][i]->file_size));
  }

  // For levels > 0, we can use a concatenating iterator that sequentially
  // walks through the non-overlapping files in the level, opening them
  // lazily.
  for (int level = 1; level < config::kNumLevels; level++) {
    if (!files_[level].empty()) {
      iters->push_back(NewConcatenatingIterator(options, level));
    }
  }
}

void Version::AddIterators(const ReadOptions& options, int level, int start_idx,
                           std::vector<Iterator*>* iters, uint64_t tsid,
                           bool base_files) {
  int64_t tb = files_[level][start_idx]->time_boundary,
          ti = files_[level][start_idx]->time_interval;
  uint64_t min_tsid, max_tsid;

  if (level == 2) {
    for (size_t i = start_idx; i < files_[level].size(); i++) {
      if (files_[level][i]->time_boundary != tb ||
          files_[level][i]->time_interval != ti)
        return;
      if (files_[level][i]->start_id <= tsid &&
          tsid <= files_[level][i]->end_id) {
        if ((base_files && files_[level][i]->num_patches == 0) ||
            (!base_files && files_[level][i]->num_patches != 0))
          iters->push_back(vset_->table_cache_->NewIterator(
              options, files_[level][i]->number, files_[level][i]->file_size));
      }
    }
    return;
  }

  // NOTE(Alec): need to examine all because there can be
  // out-of-order SST in L1.
  for (size_t i = start_idx; i < files_[level].size(); i++) {
    if (files_[level][i]->time_boundary != tb ||
        files_[level][i]->time_interval != ti)
      return;
    decodeKey(files_[level][i]->smallest.user_key(), nullptr, &min_tsid,
              nullptr);
    decodeKey(files_[level][i]->largest.user_key(), nullptr, &max_tsid,
              nullptr);
    if (min_tsid <= tsid && tsid <= max_tsid)
      iters->push_back(vset_->table_cache_->NewIterator(
          options, files_[level][i]->number, files_[level][i]->file_size));
  }
}

Iterator* Version::TEST_merge_iterator() {
  std::vector<Iterator*> iters;
  for (size_t i = 0; i < files_[0].size(); i++)
    iters.push_back(vset_->table_cache_->NewIterator(
        ReadOptions(), files_[0][i]->number, files_[0][i]->file_size));
  return NewMergingIterator(&vset_->asce_icmp_, &iters[0], iters.size());
}

Iterator* Version::GetIterator(const ReadOptions& options, int level,
                               int start_idx, uint32_t sst) {
  for (size_t i = start_idx; i < files_[level].size(); i++) {
    if (files_[level][i]->number == sst)
      return vset_->table_cache_->NewIterator(options, files_[level][i]->number,
                                              files_[level][i]->file_size);
  }
  return nullptr;
}

// Callback from TableCache::Get()
namespace {
enum SaverState {
  kNotFound,
  kFound,
  kDeleted,
  kCorrupt,
};
struct Saver {
  SaverState state;
  const Comparator* ucmp;
  Slice user_key;
  std::string* value;
};
}  // namespace
static void SaveValue(void* arg, const Slice& ikey, const Slice& v) {
  Saver* s = reinterpret_cast<Saver*>(arg);
  ParsedInternalKey parsed_key;
  if (!ParseInternalKey(ikey, &parsed_key)) {
    s->state = kCorrupt;
  } else {
    if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0) {
      s->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
      if (s->state == kFound) {
        s->value->assign(v.data(), v.size());
      }
    }
  }
}

static bool NewestFirst(FileMetaData* a, FileMetaData* b) {
  return a->number > b->number;
}

void Version::ForEachOverlapping(Slice user_key, Slice internal_key, void* arg,
                                 bool (*func)(void*, int, FileMetaData*)) {
  const Comparator* ucmp = vset_->icmp_.user_comparator();

  // Search level-0 in order from newest to oldest.
  std::vector<FileMetaData*> tmp;
  tmp.reserve(files_[0].size());
  for (uint32_t i = 0; i < files_[0].size(); i++) {
    FileMetaData* f = files_[0][i];
    if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
        ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
      tmp.push_back(f);
    }
  }
  if (!tmp.empty()) {
    std::sort(tmp.begin(), tmp.end(), NewestFirst);
    for (uint32_t i = 0; i < tmp.size(); i++) {
      if (!(*func)(arg, 0, tmp[i])) {
        return;
      }
    }
  }

  // Search other levels.
  for (int level = 1; level < config::kNumLevels; level++) {
    size_t num_files = files_[level].size();
    if (num_files == 0) continue;

    // Binary search to find earliest index whose largest key >= internal_key.
    uint32_t index = FindFile(vset_->icmp_, files_[level], internal_key);
    if (index < num_files) {
      FileMetaData* f = files_[level][index];
      if (ucmp->Compare(user_key, f->smallest.user_key()) < 0) {
        // All of "f" is past any data for user_key
      } else {
        if (!(*func)(arg, level, f)) {
          return;
        }
      }
    }
  }
}

Status Version::Get(const ReadOptions& options, const LookupKey& k,
                    std::string* value, GetStats* stats) {
  stats->seek_file = nullptr;
  stats->seek_file_level = -1;

  struct State {
    Saver saver;
    GetStats* stats;
    const ReadOptions* options;
    Slice ikey;
    FileMetaData* last_file_read;
    int last_file_read_level;

    VersionSet* vset;
    Status s;
    bool found;

    static bool Match(void* arg, int level, FileMetaData* f) {
      State* state = reinterpret_cast<State*>(arg);

      if (state->stats->seek_file == nullptr &&
          state->last_file_read != nullptr) {
        // We have had more than one seek for this read.  Charge the 1st file.
        state->stats->seek_file = state->last_file_read;
        state->stats->seek_file_level = state->last_file_read_level;
      }

      state->last_file_read = f;
      state->last_file_read_level = level;

      state->s = state->vset->table_cache_->Get(*state->options, f->number,
                                                f->file_size, state->ikey,
                                                &state->saver, SaveValue);
      if (!state->s.ok()) {
        state->found = true;
        return false;
      }
      switch (state->saver.state) {
        case kNotFound:
          return true;  // Keep searching in other files
        case kFound:
          state->found = true;
          return false;
        case kDeleted:
          return false;
        case kCorrupt:
          state->s =
              Status::Corruption("corrupted key for ", state->saver.user_key);
          state->found = true;
          return false;
      }

      // Not reached. Added to avoid false compilation warnings of
      // "control reaches end of non-void function".
      return false;
    }
  };

  State state;
  state.found = false;
  state.stats = stats;
  state.last_file_read = nullptr;
  state.last_file_read_level = -1;

  state.options = &options;
  state.ikey = k.internal_key();
  state.vset = vset_;

  state.saver.state = kNotFound;
  state.saver.ucmp = vset_->icmp_.user_comparator();
  state.saver.user_key = k.user_key();
  state.saver.value = value;

  ForEachOverlapping(state.saver.user_key, state.ikey, &state, &State::Match);

  return state.found ? state.s : Status::NotFound(Slice());
}

bool Version::UpdateStats(const GetStats& stats) {
  FileMetaData* f = stats.seek_file;
  if (f != nullptr) {
    f->allowed_seeks--;
    if (f->allowed_seeks <= 0 && file_to_compact_ == nullptr) {
      file_to_compact_ = f;
      file_to_compact_level_ = stats.seek_file_level;
      return true;
    }
  }
  return false;
}

bool Version::RecordReadSample(Slice internal_key) {
  ParsedInternalKey ikey;
  if (!ParseInternalKey(internal_key, &ikey)) {
    return false;
  }

  struct State {
    GetStats stats;  // Holds first matching file
    int matches;

    static bool Match(void* arg, int level, FileMetaData* f) {
      State* state = reinterpret_cast<State*>(arg);
      state->matches++;
      if (state->matches == 1) {
        // Remember first match.
        state->stats.seek_file = f;
        state->stats.seek_file_level = level;
      }
      // We can stop iterating once we have a second match.
      return state->matches < 2;
    }
  };

  State state;
  state.matches = 0;
  ForEachOverlapping(ikey.user_key, internal_key, &state, &State::Match);

  // Must have at least two matches since we want to merge across
  // files. But what if we have a single file that contains many
  // overwrites and deletions?  Should we have another mechanism for
  // finding such files?
  if (state.matches >= 2) {
    // 1MB cost is about 1 seek (see comment in Builder::Apply).
    return UpdateStats(state.stats);
  }
  return false;
}

void Version::Ref() { ++refs_; }

void Version::Unref() {
  assert(this != &vset_->dummy_versions_);
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    delete this;
  }
}

bool Version::OverlapInLevel(int level, const Slice* smallest_user_key,
                             const Slice* largest_user_key) {
  return SomeFileOverlapsRange(vset_->icmp_, (level > 0), files_[level],
                               smallest_user_key, largest_user_key);
}

int Version::PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                        const Slice& largest_user_key) {
  int level = 0;
  if (!OverlapInLevel(0, &smallest_user_key, &largest_user_key)) {
    // Push to next level if there is no overlap in next level,
    // and the #bytes overlapping in the level after that are limited.
    InternalKey start(smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey limit(largest_user_key, 0, static_cast<ValueType>(0));
    std::vector<FileMetaData*> overlaps;
    while (level < config::kMaxMemCompactLevel) {
      if (OverlapInLevel(level + 1, &smallest_user_key, &largest_user_key)) {
        break;
      }
      if (level + 2 < config::kNumLevels) {
        // Check that file does not overlap too many grandparent bytes.
        GetOverlappingInputs(level + 2, &start, &limit, &overlaps);
        const int64_t sum = TotalFileSize(overlaps);
        if (sum > MaxGrandParentOverlapBytes(vset_->options_)) {
          break;
        }
      }
      level++;
    }
  }
  return level;
}

// Store in "*inputs" all files in "level" that overlap [begin,end]
void Version::GetOverlappingInputs(int level, const InternalKey* begin,
                                   const InternalKey* end,
                                   std::vector<FileMetaData*>* inputs) {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  inputs->clear();
  Slice user_begin, user_end;
  if (begin != nullptr) {
    user_begin = begin->user_key();
  }
  if (end != nullptr) {
    user_end = end->user_key();
  }
  const Comparator* user_cmp = vset_->icmp_.user_comparator();
  for (size_t i = 0; i < files_[level].size();) {
    FileMetaData* f = files_[level][i++];
    const Slice file_start = f->smallest.user_key();
    const Slice file_limit = f->largest.user_key();
    if (begin != nullptr && user_cmp->Compare(file_limit, user_begin) < 0) {
      // "f" is completely before specified range; skip it
    } else if (end != nullptr && user_cmp->Compare(file_start, user_end) > 0) {
      // "f" is completely after specified range; skip it
    } else {
      inputs->push_back(f);
      if (level == 0) {
        // Level-0 files may overlap each other.  So check if the newly
        // added file has expanded the range.  If so, restart search.
        if (begin != nullptr && user_cmp->Compare(file_start, user_begin) < 0) {
          user_begin = file_start;
          inputs->clear();
          i = 0;
        } else if (end != nullptr &&
                   user_cmp->Compare(file_limit, user_end) > 0) {
          user_end = file_limit;
          inputs->clear();
          i = 0;
        }
      }
    }
  }
}

std::string Version::DebugString() const {
  std::string r;
  for (int level = 0; level < config::kNumLevels; level++) {
    // E.g.,
    //   --- level 1 ---
    //   17:123['a' .. 'd']
    //   20:43['e' .. 'g']
    uint64_t level_size = 0;
    r.append("--- level ");
    AppendNumberTo(&r, level);
    r.append(" ---\n");
    const std::vector<FileMetaData*>& files = files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      r.push_back(' ');
      AppendNumberTo(&r, files[i]->number);
      r.push_back(':');
      AppendNumberTo(&r, files[i]->file_size);
      level_size += files[i]->file_size;
      r.append("[");
      r.append(files[i]->smallest.DebugString());
      r.append(" .. ");
      r.append(files[i]->largest.DebugString());
      r.append("][");
      AppendNumberTo(&r, files[i]->time_boundary);
      r.append(", ");
      AppendNumberTo(&r, files[i]->time_interval);
      if (level == 2) {
        r.append("](");
        AppendNumberTo(&r, files[i]->num_patches);
        r.append(")[");
        AppendNumberTo(&r, files[i]->start_id);
        r.append(", ");
        AppendNumberTo(&r, files[i]->end_id);
      }
      r.append("]\n");
    }
    r.append("level size:");
    AppendNumberTo(&r, level_size);
    r.append("\n");
  }
  r.append("\n");
  for (int level = 0; level < config::kNumLevels; level++) {
    r.append("--- level ");
    AppendNumberTo(&r, level);
    r.append(" ---\n");
    if (!time_partitions_[level].empty()) {
      for (size_t i = 0; i < time_partitions_[level].size(); i++) {
        AppendNumberTo(&r, time_partitions_[level][i]);
        r.append(" ");
      }
      r.append("\n");
      for (size_t i = 0; i < time_partition_indexes_[level].size(); i++) {
        AppendNumberTo(&r, time_partition_indexes_[level][i]);
        r.append(" ");
      }
      r.append("\n");
    }
  }
  return r;
}

void Version::OverlappingPartitions(int level, int64_t mint, int64_t maxt,
                                    std::vector<std::pair<int64_t, int64_t>>* v,
                                    std::vector<int>* indexes) {
  if (files_[level].empty()) return;
  // Find lower bound (right boundaries) for mint.
  // Note(Alec), the first element >= mint.
  int mid;
  int low = 0;
  int high = time_partitions_[level].size();
  while (low < high) {
    mid = low + (high - low) / 2;
    if (mint <=
        time_partitions_[level][mid] +
            files_[level][time_partition_indexes_[level][mid]]->time_interval -
            1)
      high = mid;
    else
      low = mid + 1;
  }
  int left = low;
  if (low >= time_partitions_[level].size()) return;

  // Find upper bound (left boundaries) for maxt.
  // Note(Alec), lower bound - 1 is the first element <= maxt.
  low = 0;
  high = time_partitions_[level].size();
  while (low < high) {
    mid = low + (high - low) / 2;
    if (maxt >= time_partitions_[level][mid])
      low = mid + 1;
    else
      high = mid;
  }
  int right = low - 1;
  if (right == -1) return;

  if (right >= time_partition_indexes_[level].size())
    right = time_partition_indexes_[level].size() - 1;
  for (int i = left; i <= right; i++) {
    indexes->push_back(time_partition_indexes_[level][i]);
    v->emplace_back(
        time_partitions_[level][i],
        files_[level][time_partition_indexes_[level][i]]->time_interval);
  }
}

void Version::OverlappingPartitions(int level, int64_t mint, int64_t maxt,
                                    std::vector<int>* indexes) {
  if (files_[level].empty()) return;
  // Find lower bound (right boundaries) for mint.
  // Note(Alec), the first element >= mint.
  int mid;
  int low = 0;
  int high = time_partitions_[level].size();
  while (low < high) {
    mid = low + (high - low) / 2;
    if (mint <=
        time_partitions_[level][mid] +
            files_[level][time_partition_indexes_[level][mid]]->time_interval -
            1)
      high = mid;
    else
      low = mid + 1;
  }
  int left = low;
  if (low >= time_partitions_[level].size()) return;

  // Find upper bound (left boundaries) for maxt.
  // Note(Alec), lower bound - 1 is the first element <= maxt.
  low = 0;
  high = time_partitions_[level].size();
  while (low < high) {
    mid = low + (high - low) / 2;
    if (maxt >= time_partitions_[level][mid])
      low = mid + 1;
    else
      high = mid;
  }
  int right = low - 1;
  if (right == -1) return;

  if (right >= time_partition_indexes_[level].size() - 1) {
    for (int i = time_partition_indexes_[level][left]; i < files_[level].size();
         i++)
      indexes->push_back(i);
  } else {
    for (int i = time_partition_indexes_[level][left];
         i < time_partition_indexes_[level][right + 1]; i++)
      indexes->push_back(i);
  }
}

int Version::OverlappingPartitions(int level, int64_t mint, int64_t maxt) {
  if (files_[level].empty()) return 0;
  // Find lower bound (right boundaries) for mint.
  // Note(Alec), the first element >= mint.
  int mid;
  int low = 0;
  int high = time_partitions_[level].size();
  while (low < high) {
    mid = low + (high - low) / 2;
    if (mint <=
        time_partitions_[level][mid] +
            files_[level][time_partition_indexes_[level][mid]]->time_interval -
            1)
      high = mid;
    else
      low = mid + 1;
  }
  int left = low;
  if (low >= time_partitions_[level].size()) return 0;

  // Find upper bound (left boundaries) for maxt.
  // Note(Alec), lower bound - 1 is the first element <= maxt.
  low = 0;
  high = time_partitions_[level].size();
  while (low < high) {
    mid = low + (high - low) / 2;
    if (maxt >= time_partitions_[level][mid])
      low = mid + 1;
    else
      high = mid;
  }
  int right = low - 1;
  if (right == -1) return 0;

  int count = 0;
  if (right >= time_partition_indexes_[level].size() - 1) {
    for (int i = time_partition_indexes_[level][left]; i < files_[level].size();
         i++)
      ++count;
  } else {
    for (int i = time_partition_indexes_[level][left];
         i < time_partition_indexes_[level][right + 1]; i++)
      ++count;
  }
  return count;
}

void Compaction::FindTSIDOverlap(int64_t t, uint64_t tsid, int* idx1,
                                 int* idx2) {
  if (inputs_[1].empty()) {
    *idx1 = -1;
    *idx2 = -1;
    return;
  }
  for (size_t i = 0; i < base_file_indexes_.size(); i++) {
    if (t < inputs_[1][base_file_indexes_[i][0]]->time_boundary ||
        t >= inputs_[1][base_file_indexes_[i][0]]->time_boundary +
                 inputs_[1][base_file_indexes_[i][0]]->time_interval)
      continue;
    int mid;
    int low = 0;
    int high = base_file_indexes_[i].size();
    while (low < high) {
      mid = low + (high - low) / 2;
      if (tsid <= inputs_[1][base_file_indexes_[i][mid]]->start_id)
        high = mid;
      else
        low = mid + 1;
    }

    if (low < base_file_indexes_[i].size() &&
        inputs_[1][base_file_indexes_[i][low]]->start_id <= tsid &&
        inputs_[1][base_file_indexes_[i][low]]->end_id >= tsid) {
      *idx1 = i;
      *idx2 = low;
      return;
    }

    if (low > 0 &&
        inputs_[1][base_file_indexes_[i][low - 1]]->start_id <= tsid &&
        inputs_[1][base_file_indexes_[i][low - 1]]->end_id >= tsid) {
      *idx1 = i;
      *idx2 = low - 1;
      return;
    }
  }

  *idx1 = -1;
  *idx2 = -1;
  return;
}

void Compaction::InitNumPatches() {
  if (!num_patches_.empty()) return;
  size_t i = 0;
  while (i < inputs_[1].size()) {
    base_file_indexes_.emplace_back();
    num_patches_.emplace_back();
    size_t j = i + 1;
    while (j < inputs_[1].size() &&
           inputs_[1][j]->time_boundary == inputs_[1][i]->time_boundary)
      j++;
    size_t k = i + 1;
    while (i < j) {
      while (k < j && inputs_[1][k]->start_id == inputs_[1][i]->start_id &&
             inputs_[1][k]->end_id == inputs_[1][i]->end_id)
        k++;
      base_file_indexes_.back().push_back(i);
      num_patches_.back().push_back(inputs_[1][k - 1]->num_patches);
      i = k;
    }
  }
  // for (size_t i = 0; i < base_file_indexes_.size(); i++) {
  //   LOG_DEBUG << "time_boundary:" <<
  //   inputs_[1][base_file_indexes_[i][0]]->time_boundary << " time_interval:"
  //   << inputs_[1][base_file_indexes_[i][0]]->time_interval; for (size_t j =
  //   0; j < base_file_indexes_[i].size(); j++) {
  //     LOG_DEBUG << "idx:" << base_file_indexes_[i][j] << " start_id:" <<
  //     inputs_[1][base_file_indexes_[i][j]]->start_id << " end_id:" <<
  //     inputs_[1][base_file_indexes_[i][j]]->end_id << " num_patches:" <<
  //     num_patches_[i][j];
  //   }
  // }
}

size_t Version::level_size(int level) {
  size_t s = 0;
  for (const auto& f : files_[level]) s += f->file_size;
  return s;
}

size_t Version::level_size(int level, int num_partitions) {
  size_t s = 0;
  if (num_partitions >= time_partitions_[level].size()) {
    for (const auto& f : files_[level]) s += f->file_size;
  } else {
    for (int i = time_partition_indexes_[level]
                                        [time_partition_indexes_[level].size() -
                                         num_partitions];
         i < files_[level].size(); i++)
      s += files_[level][i]->file_size;
  }
  return s;
}

int Version::count_partition(int level, int64_t pl) {
  int count = 0;
  for (size_t i = 0; i < files_[level].size(); i++) {
    if (files_[level][i]->time_interval == pl) ++count;
  }
  return count;
}

// A helper class so we can efficiently apply a whole sequence
// of edits to a particular state without creating intermediate
// Versions that contain full copies of the intermediate state.
class VersionSet::Builder {
 private:
  // Helper to sort by v->files_[file_number].smallest
  struct BySmallestKey {
    const InternalKeyComparator* internal_comparator;

    bool operator()(FileMetaData* f1, FileMetaData* f2) const {
      int r = internal_comparator->Compare(f1->smallest, f2->smallest);
      if (r != 0) {
        return (r < 0);
      } else {
        // Break ties by file number
        return (f1->number < f2->number);
      }
    }
  };

  // NOTE(Alec): first sort time, then sort key.
  struct BySmallestTime {
    const InternalKeyComparator* internal_comparator;

    bool operator()(FileMetaData* f1, FileMetaData* f2) const {
      if (f1->time_boundary != f2->time_boundary)
        return f1->time_boundary < f2->time_boundary;

      // int r = internal_comparator->Compare(f1->smallest, f2->smallest);
      // if (r != 0) {
      //   return (r < 0);
      // } else {
      //   // Break ties by file number
      //   return (f1->number < f2->number);
      // }

      // The file with larger number should have newer data.
      if (f1->start_id != f2->start_id) return f1->start_id < f2->start_id;
      if (f1->num_patches != f2->num_patches)
        return f1->num_patches < f2->num_patches;
      return (f1->number < f2->number);
    }
  };

  typedef std::set<FileMetaData*, BySmallestTime> FileSet;
  struct LevelState {
    std::set<uint64_t> deleted_files;
    FileSet* added_files;
    int64_t current_boundary_;
    int current_idx_;
  };

  VersionSet* vset_;
  Version* base_;
  LevelState levels_[config::kNumLevels];
  std::set<int64_t> time_partitions_;

 public:
  // Initialize a builder with the files from *base and other info from *vset
  Builder(VersionSet* vset, Version* base) : vset_(vset), base_(base) {
    base_->Ref();
    BySmallestTime cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < config::kNumLevels; level++) {
      levels_[level].added_files = new FileSet(cmp);
      levels_[level].current_boundary_ = -1;
      levels_[level].current_idx_ = -1;
    }
  }

  ~Builder() {
    for (int level = 0; level < config::kNumLevels; level++) {
      const FileSet* added = levels_[level].added_files;
      std::vector<FileMetaData*> to_unref;
      to_unref.reserve(added->size());
      for (FileSet::const_iterator it = added->begin(); it != added->end();
           ++it) {
        to_unref.push_back(*it);
      }
      delete added;
      for (uint32_t i = 0; i < to_unref.size(); i++) {
        FileMetaData* f = to_unref[i];
        f->refs--;
        if (f->refs <= 0) {
          delete f;
        }
      }
    }
    base_->Unref();
  }

  // Apply all of the edits in *edit to the current state.
  void Apply(VersionEdit* edit) {
    // Update compaction pointers
    for (size_t i = 0; i < edit->compact_pointers_.size(); i++) {
      const int level = edit->compact_pointers_[i].first;
      vset_->compact_pointer_[level] =
          edit->compact_pointers_[i].second.Encode().ToString();
    }

    // Delete files
    for (const auto& deleted_file_set_kvp : edit->deleted_files_) {
      const int level = deleted_file_set_kvp.first;
      const uint64_t number = deleted_file_set_kvp.second;
      levels_[level].deleted_files.insert(number);
    }

    // Add new files
    for (size_t i = 0; i < edit->new_files_.size(); i++) {
      const int level = edit->new_files_[i].first;
      FileMetaData* f = new FileMetaData(edit->new_files_[i].second);
      f->refs = 1;

      // We arrange to automatically compact this file after
      // a certain number of seeks.  Let's assume:
      //   (1) One seek costs 10ms
      //   (2) Writing or reading 1MB costs 10ms (100MB/s)
      //   (3) A compaction of 1MB does 25MB of IO:
      //         1MB read from this level
      //         10-12MB read from next level (boundaries may be misaligned)
      //         10-12MB written to next level
      // This implies that 25 seeks cost the same as the compaction
      // of 1MB of data.  I.e., one seek costs approximately the
      // same as the compaction of 40KB of data.  We are a little
      // conservative and allow approximately one seek for every 16KB
      // of data before triggering a compaction.
      f->allowed_seeks = static_cast<int>((f->file_size / 16384U));
      if (f->allowed_seeks < 100) f->allowed_seeks = 100;

      levels_[level].deleted_files.erase(f->number);
      levels_[level].added_files->insert(f);
    }

    // Time partitions.
    for (auto t : edit->time_partitions_) time_partitions_.insert(t);
  }

  // Save the current state in *v.
  void SaveTo(Version* v) {
    BySmallestTime cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < config::kNumLevels; level++) {
      // Merge the set of added files with the set of pre-existing files.
      // Drop any deleted files.  Store the result in *v.
      const std::vector<FileMetaData*>& base_files = base_->files_[level];
      std::vector<FileMetaData*>::const_iterator base_iter = base_files.begin();
      std::vector<FileMetaData*>::const_iterator base_end = base_files.end();

      const FileSet* added_files = levels_[level].added_files;
      v->files_[level].reserve(base_files.size() + added_files->size());
      for (const auto& added_file : *added_files) {
        // bool find_patch = false;
        // Add all smaller files listed in base_
        for (std::vector<FileMetaData*>::const_iterator bpos =
                 std::upper_bound(base_iter, base_end, added_file, cmp);
             base_iter != bpos; ++base_iter) {
          // if (level == 2) {
          //   if ((*base_iter)->time_boundary == added_file->time_boundary &&
          //       (*base_iter)->time_interval == added_file->time_interval &&
          //       (*base_iter)->start_id == added_file->start_id &&
          //       (*base_iter)->end_id == added_file->end_id) {
          //     // if (vset_->icmp_.Compare((*base_iter)->smallest,
          //     added_file->smallest) < 0)
          //     //   added_file->smallest = (*base_iter)->smallest;
          //     // if (vset_->icmp_.Compare((*base_iter)->largest,
          //     added_file->largest) > 0)
          //     //   added_file->largest = (*base_iter)->largest;
          //     if (added_file->num_patches_ > (*base_iter)->num_patches_)
          //       (*base_iter)->num_patches_ = added_file->num_patches_;
          //     find_patch = true;
          //   }
          // }
          MaybeAddFile(v, level, *base_iter);
        }

        // if (find_patch)
        MaybeAddFile(v, level, added_file);
      }

      // Add remaining base files
      for (; base_iter != base_end; ++base_iter) {
        MaybeAddFile(v, level, *base_iter);
      }

      // #ifndef NDEBUG
      //       // Make sure there is no overlap in levels > 0
      //       if (level > 0) {
      //         for (uint32_t i = 1; i < v->files_[level].size(); i++) {
      //           const InternalKey& prev_end = v->files_[level][i -
      //           1]->largest; const InternalKey& this_begin =
      //           v->files_[level][i]->smallest; if
      //           (vset_->icmp_.Compare(prev_end, this_begin) >= 0) {
      //             std::fprintf(stderr, "overlapping ranges in same level %s
      //             vs. %s\n",
      //                          prev_end.DebugString().c_str(),
      //                          this_begin.DebugString().c_str());
      //             std::abort();
      //           }
      //         }
      //       }
      // #endif
      // v->time_partitions_.clear();
      // v->time_partitions_.insert(time_partitions_.begin(),
      // time_partitions_.end());
    }
  }

  void MaybeAddFile(Version* v, int level, FileMetaData* f) {
    if (levels_[level].deleted_files.count(f->number) > 0) {
      // File is deleted: do nothing
    } else {
      std::vector<FileMetaData*>* files = &v->files_[level];
      if (level > 0 && !files->empty()) {
        // Must not overlap
        if (level < 2)
          assert((*files)[files->size() - 1]->time_boundary <
                     f->time_boundary ||
                 vset_->icmp_.Compare((*files)[files->size() - 1]->largest,
                                      f->smallest) < 0);
      }
      if (levels_[level].current_boundary_ == -1) {
        levels_[level].current_boundary_ = f->time_boundary;
        levels_[level].current_idx_ = 0;
        v->time_partitions_[level].push_back(f->time_boundary);
        v->time_partition_indexes_[level].push_back(0);
      } else if (levels_[level].current_boundary_ != f->time_boundary) {
        levels_[level].current_boundary_ = f->time_boundary;
        levels_[level].current_idx_ = files->size();
        ;
        v->time_partitions_[level].push_back(f->time_boundary);
        v->time_partition_indexes_[level].push_back(files->size());
      }
      f->refs++;
      files->push_back(f);
    }
  }
};

VersionSet::VersionSet(const std::string& dbname, const Options* options,
                       TableCache* table_cache,
                       const InternalKeyComparator* cmp,
                       const SeqAsceInternalKeyComparator* asce_cmp)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      table_cache_(table_cache),
      icmp_(*cmp),
      asce_icmp_(*asce_cmp),
      next_file_number_(2),
      manifest_file_number_(0),  // Filled by Recover()
      last_sequence_(0),
      log_number_(0),
      prev_log_number_(0),
      descriptor_file_(nullptr),
      descriptor_log_(nullptr),
      dummy_versions_(this),
      current_(nullptr) {
  AppendVersion(new Version(this));
}

VersionSet::~VersionSet() {
  current_->Unref();
  assert(dummy_versions_.next_ == &dummy_versions_);  // List must be empty
  delete descriptor_log_;
  delete descriptor_file_;
}

void VersionSet::AppendVersion(Version* v) {
  // Make "v" current
  assert(v->refs_ == 0);
  assert(v != current_);
  if (current_ != nullptr) {
    current_->Unref();
  }
  current_ = v;
  v->Ref();

  // Append to linked list
  v->prev_ = dummy_versions_.prev_;
  v->next_ = &dummy_versions_;
  v->prev_->next_ = v;
  v->next_->prev_ = v;
}

Status VersionSet::LogAndApply(VersionEdit* edit, port::Mutex* mu) {
  if (edit->has_log_number_) {
    assert(edit->log_number_ >= log_number_);
    assert(edit->log_number_ < next_file_number_);
  } else {
    edit->SetLogNumber(log_number_);
  }

  if (!edit->has_prev_log_number_) {
    edit->SetPrevLogNumber(prev_log_number_);
  }

  edit->SetNextFile(next_file_number_);
  edit->SetLastSequence(last_sequence_);

  Version* v = new Version(this);
  {
    Builder builder(this, current_);
    builder.Apply(edit);
    builder.SaveTo(v);
  }
  Finalize(v);

  // Initialize new descriptor log file if necessary by creating
  // a temporary file that contains a snapshot of the current version.
  std::string new_manifest_file;
  Status s;
  if (descriptor_log_ == nullptr) {
    // No reason to unlock *mu here since we only hit this path in the
    // first call to LogAndApply (when opening the database).
    assert(descriptor_file_ == nullptr);
    new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_);
    edit->SetNextFile(next_file_number_);
    LOG_DEBUG << "new_manifest_file: " << new_manifest_file;
    s = env_->NewWritableFile(new_manifest_file, &descriptor_file_);
    if (s.ok()) {
      descriptor_log_ = new log::Writer(descriptor_file_);
      s = WriteSnapshot(descriptor_log_);
    }
  }

  // Unlock during expensive MANIFEST log write
  {
    mu->Unlock();

    // Write new record to MANIFEST log
    if (s.ok()) {
      std::string record;
      edit->EncodeTo(&record);
      s = descriptor_log_->AddRecord(record);
      if (s.ok()) {
        s = descriptor_file_->Sync();
      }
      if (!s.ok()) {
        Log(options_->info_log, "MANIFEST write: %s\n", s.ToString().c_str());
      }
    }

    // If we just created a new descriptor file, install it by writing a
    // new CURRENT file that points to it.
    if (s.ok() && !new_manifest_file.empty()) {
      s = SetCurrentFile(env_, dbname_, manifest_file_number_);
    }

    mu->Lock();
  }

  // Install the new version
  if (s.ok()) {
    AppendVersion(v);
    log_number_ = edit->log_number_;
    prev_log_number_ = edit->prev_log_number_;
  } else {
    delete v;
    if (!new_manifest_file.empty()) {
      delete descriptor_log_;
      delete descriptor_file_;
      descriptor_log_ = nullptr;
      descriptor_file_ = nullptr;
      env_->RemoveFile(new_manifest_file);
    }
  }

  return s;
}

Status VersionSet::Recover(bool* save_manifest) {
  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    void Corruption(size_t bytes, const Status& s) override {
      if (this->status->ok()) *this->status = s;
    }
  };

  // Read "CURRENT" file, which contains a pointer to the current manifest file
  std::string current;
  Status s = ReadFileToString(env_, CurrentFileName(dbname_), &current);
  if (!s.ok()) {
    return s;
  }
  if (current.empty() || current[current.size() - 1] != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  current.resize(current.size() - 1);

  std::string dscname = dbname_ + "/" + current;
  SequentialFile* file;
  s = env_->NewSequentialFile(dscname, &file);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      return Status::Corruption("CURRENT points to a non-existent file",
                                s.ToString());
    }
    return s;
  }

  bool have_log_number = false;
  bool have_prev_log_number = false;
  bool have_next_file = false;
  bool have_last_sequence = false;
  uint64_t next_file = 0;
  uint64_t last_sequence = 0;
  uint64_t log_number = 0;
  uint64_t prev_log_number = 0;
  Builder builder(this, current_);
  int read_records = 0;

  {
    LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(file, &reporter, true /*checksum*/,
                       0 /*initial_offset*/);
    Slice record;
    std::string scratch;
    while (reader.ReadRecord(&record, &scratch) && s.ok()) {
      ++read_records;
      VersionEdit edit;
      s = edit.DecodeFrom(record);
      if (s.ok()) {
        if (edit.has_comparator_ &&
            edit.comparator_ != icmp_.user_comparator()->Name()) {
          s = Status::InvalidArgument(
              edit.comparator_ + " does not match existing comparator ",
              icmp_.user_comparator()->Name());
        }
      }

      if (s.ok()) {
        builder.Apply(&edit);
      }

      if (edit.has_log_number_) {
        log_number = edit.log_number_;
        have_log_number = true;
      }

      if (edit.has_prev_log_number_) {
        prev_log_number = edit.prev_log_number_;
        have_prev_log_number = true;
      }

      if (edit.has_next_file_number_) {
        next_file = edit.next_file_number_;
        have_next_file = true;
      }

      if (edit.has_last_sequence_) {
        last_sequence = edit.last_sequence_;
        have_last_sequence = true;
      }
    }
  }
  delete file;
  file = nullptr;

  if (s.ok()) {
    if (!have_next_file) {
      s = Status::Corruption("no meta-nextfile entry in descriptor");
    } else if (!have_log_number) {
      s = Status::Corruption("no meta-lognumber entry in descriptor");
    } else if (!have_last_sequence) {
      s = Status::Corruption("no last-sequence-number entry in descriptor");
    }

    if (!have_prev_log_number) {
      prev_log_number = 0;
    }

    MarkFileNumberUsed(prev_log_number);
    MarkFileNumberUsed(log_number);
  }

  if (s.ok()) {
    Version* v = new Version(this);
    builder.SaveTo(v);
    // Install recovered version
    Finalize(v);
    AppendVersion(v);
    manifest_file_number_ = next_file;
    next_file_number_ = next_file + 1;
    last_sequence_ = last_sequence;
    log_number_ = log_number;
    prev_log_number_ = prev_log_number;

    // See if we can reuse the existing MANIFEST file.
    if (ReuseManifest(dscname, current)) {
      // No need to save new manifest
    } else {
      *save_manifest = true;
    }
  } else {
    std::string error = s.ToString();
    Log(options_->info_log, "Error recovering version set with %d records: %s",
        read_records, error.c_str());
  }

  return s;
}

bool VersionSet::ReuseManifest(const std::string& dscname,
                               const std::string& dscbase) {
  if (!options_->reuse_logs) {
    return false;
  }
  FileType manifest_type;
  uint64_t manifest_number;
  uint64_t manifest_size;
  if (!ParseFileName(dscbase, &manifest_number, &manifest_type) ||
      manifest_type != kDescriptorFile ||
      !env_->GetFileSize(dscname, &manifest_size).ok() ||
      // Make new compacted MANIFEST if old one is too big
      manifest_size >= TargetFileSize(options_)) {
    return false;
  }

  assert(descriptor_file_ == nullptr);
  assert(descriptor_log_ == nullptr);
  Status r = env_->NewAppendableFile(dscname, &descriptor_file_);
  if (!r.ok()) {
    Log(options_->info_log, "Reuse MANIFEST: %s\n", r.ToString().c_str());
    assert(descriptor_file_ == nullptr);
    return false;
  }

  Log(options_->info_log, "Reusing MANIFEST %s\n", dscname.c_str());
  descriptor_log_ = new log::Writer(descriptor_file_, manifest_size);
  manifest_file_number_ = manifest_number;
  return true;
}

void VersionSet::MarkFileNumberUsed(uint64_t number) {
  if (next_file_number_ <= number) {
    next_file_number_ = number + 1;
  }
}

void VersionSet::Finalize(Version* v) {
  v->compaction_level_ = 0;
  v->compaction_score_ = 0;
  if (v->time_partition_indexes_[0].size() > L0_MERGE_FACTOR + 1)
    v->compaction_score_ = 1;
  else if (!v->files_[1].empty() &&
           (v->files_[1].back()->time_boundary +
                    v->files_[1].back()->time_interval -
                    v->files_[1].front()->time_boundary >
                L2_PARTITION_LENGTH ||
            v->OverlappingPartitions(2, v->files_[1].front()->time_boundary,
                                     v->files_[1].back()->time_boundary +
                                         v->files_[1].back()->time_interval -
                                         1) > 0)) {
    v->compaction_level_ = 1;
    v->compaction_score_ = 1;
  } else {
    for (size_t i = 0; i < v->files_[2].size(); i++) {
      if (v->files_[2][i]->num_patches >= L2_MERGE_COMPACTION_THRESHOLD) {
        v->compaction_level_ = 2;
        v->compaction_score_ = 1;
        break;
      }
    }
  }

  // LOG_DEBUG << "disk size:" << (v->level_size(0) + v->level_size(1)) << "
  // FAST_STORAGE_SIZE:" << FAST_STORAGE_SIZE << " Time(ms):" <<
  // std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count();

  // Dynamic controlling if no compaction triggers
  // and most partitions are stable with current PL.
  if (DYNAMIC_PARTITION_LENGTH && v->compaction_score_ < 1 &&
      !v->files_[1].empty() &&
      (double)(v->count_partition(0, PARTITION_LENGTH) +
               v->count_partition(1, PARTITION_LENGTH)) >=
          0.6 * (double)(v->files_[0].size() + v->files_[1].size()) &&
      __sync_bool_compare_and_swap(&DYNAMIC_CONTROL_LOCK, 0, 1)) {
    uint64_t l0_size = v->level_size(0);
    uint64_t l1_size = v->level_size(1);
    uint64_t total_size = l0_size + l1_size;
    // LOG_DEBUG << "total_size:" << total_size << " FAST_STORAGE_SIZE:" <<
    // FAST_STORAGE_SIZE;
    if (total_size > FAST_STORAGE_SIZE) {
      int64_t threshold =
          (int64_t)((double)(PARTITION_LENGTH) * (double)(FAST_STORAGE_SIZE) /
                    (double)(total_size));
      uint64_t original_partition_length = PARTITION_LENGTH;
      while (PARTITION_LENGTH > threshold && PARTITION_LENGTH > 450000) {
        PARTITION_LENGTH /= 2;
        L2_PARTITION_LENGTH /= 2;
      }

      LOG_INFO
          << "Time(ms):"
          << std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::high_resolution_clock::now().time_since_epoch())
                 .count()
          << " PARTITION_LENGTH:" << PARTITION_LENGTH
          << " L2_PARTITION_LENGTH:" << L2_PARTITION_LENGTH;
    } else if ((double)(v->files_[1].back()->time_boundary +
                        v->files_[1].back()->time_interval -
                        v->files_[1].front()->time_boundary) >=
                   0.75 * (double)(L2_PARTITION_LENGTH) ||
               v->time_partitions_[1].size() > 3) {
      int64_t threshold =
          (int64_t)((double)(PARTITION_LENGTH) * (double)(FAST_STORAGE_SIZE) /
                    (double)(total_size));
      uint64_t original_partition_length = PARTITION_LENGTH;
      while (PARTITION_LENGTH * 2 < threshold) {
        L2_PARTITION_LENGTH *= 2;
        PARTITION_LENGTH *= 2;
      }

      LOG_INFO
          << "Time(ms):"
          << std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::high_resolution_clock::now().time_since_epoch())
                 .count()
          << " PARTITION_LENGTH:" << PARTITION_LENGTH
          << " L2_PARTITION_LENGTH:" << L2_PARTITION_LENGTH;
    }

    __sync_lock_test_and_set(&DYNAMIC_CONTROL_LOCK, 0);
  }
}

Status VersionSet::WriteSnapshot(log::Writer* log) {
  // TODO: Break up into multiple records to reduce memory usage on recovery?

  // Save metadata
  VersionEdit edit;
  edit.SetComparatorName(icmp_.user_comparator()->Name());

  // Save compaction pointers
  for (int level = 0; level < config::kNumLevels; level++) {
    if (!compact_pointer_[level].empty()) {
      InternalKey key;
      key.DecodeFrom(compact_pointer_[level]);
      edit.SetCompactPointer(level, key);
    }
  }

  // Save files
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = current_->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      edit.AddFile(level, f->number, f->file_size, f->smallest, f->largest,
                   f->time_boundary, f->time_interval);
    }
  }

  std::string record;
  edit.EncodeTo(&record);
  return log->AddRecord(record);
}

int VersionSet::NumLevelFiles(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return current_->files_[level].size();
}

const char* VersionSet::LevelSummary(LevelSummaryStorage* scratch) const {
  // Update code if kNumLevels changes
  static_assert(config::kNumLevels == 7, "");
  std::snprintf(
      scratch->buffer, sizeof(scratch->buffer), "files[ %d %d %d %d %d %d %d ]",
      int(current_->files_[0].size()), int(current_->files_[1].size()),
      int(current_->files_[2].size()), int(current_->files_[3].size()),
      int(current_->files_[4].size()), int(current_->files_[5].size()),
      int(current_->files_[6].size()));
  return scratch->buffer;
}

uint64_t VersionSet::ApproximateOffsetOf(Version* v, const InternalKey& ikey) {
  uint64_t result = 0;
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = v->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      if (icmp_.Compare(files[i]->largest, ikey) <= 0) {
        // Entire file is before "ikey", so just add the file size
        result += files[i]->file_size;
      } else if (icmp_.Compare(files[i]->smallest, ikey) > 0) {
        // Entire file is after "ikey", so ignore
        if (level > 0) {
          // Files other than level 0 are sorted by meta->smallest, so
          // no further files in this level will contain data for
          // "ikey".
          break;
        }
      } else {
        // "ikey" falls in the range for this table.  Add the
        // approximate offset of "ikey" within the table.
        Table* tableptr;
        Iterator* iter = table_cache_->NewIterator(
            ReadOptions(), files[i]->number, files[i]->file_size, &tableptr);
        if (tableptr != nullptr) {
          result += tableptr->ApproximateOffsetOf(ikey.Encode());
        }
        delete iter;
      }
    }
  }
  return result;
}

void VersionSet::AddLiveFiles(std::set<uint64_t>* live) {
  for (Version* v = dummy_versions_.next_; v != &dummy_versions_;
       v = v->next_) {
    for (int level = 0; level < config::kNumLevels; level++) {
      const std::vector<FileMetaData*>& files = v->files_[level];
      for (size_t i = 0; i < files.size(); i++) {
        live->insert(files[i]->number);
      }
    }
  }
}

int64_t VersionSet::NumLevelBytes(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return TotalFileSize(current_->files_[level]);
}

int64_t VersionSet::MaxNextLevelOverlappingBytes() {
  int64_t result = 0;
  std::vector<FileMetaData*> overlaps;
  for (int level = 1; level < config::kNumLevels - 1; level++) {
    for (size_t i = 0; i < current_->files_[level].size(); i++) {
      const FileMetaData* f = current_->files_[level][i];
      current_->GetOverlappingInputs(level + 1, &f->smallest, &f->largest,
                                     &overlaps);
      const int64_t sum = TotalFileSize(overlaps);
      if (sum > result) {
        result = sum;
      }
    }
  }
  return result;
}

// Stores the minimal range that covers all entries in inputs in
// *smallest, *largest.
// REQUIRES: inputs is not empty
void VersionSet::GetRange(const std::vector<FileMetaData*>& inputs,
                          InternalKey* smallest, InternalKey* largest) {
  assert(!inputs.empty());
  smallest->Clear();
  largest->Clear();
  for (size_t i = 0; i < inputs.size(); i++) {
    FileMetaData* f = inputs[i];
    if (i == 0) {
      *smallest = f->smallest;
      *largest = f->largest;
    } else {
      if (icmp_.Compare(f->smallest, *smallest) < 0) {
        *smallest = f->smallest;
      }
      if (icmp_.Compare(f->largest, *largest) > 0) {
        *largest = f->largest;
      }
    }
  }
}

// Stores the minimal range that covers all entries in inputs1 and inputs2
// in *smallest, *largest.
// REQUIRES: inputs is not empty
void VersionSet::GetRange2(const std::vector<FileMetaData*>& inputs1,
                           const std::vector<FileMetaData*>& inputs2,
                           InternalKey* smallest, InternalKey* largest) {
  std::vector<FileMetaData*> all = inputs1;
  all.insert(all.end(), inputs2.begin(), inputs2.end());
  GetRange(all, smallest, largest);
}

Iterator* VersionSet::MakeInputIterator(Compaction* c, bool all) {
  ReadOptions options;
  options.verify_checksums = options_->paranoid_checks;
  options.fill_cache = false;

  int space = c->inputs_[0].size() + 1;
  if (all) space += c->inputs_[1].size();
  Iterator** list = new Iterator*[space];

  // Just use merge iterators.
  int num = 0;
  const std::vector<FileMetaData*>& files = c->inputs_[0];
  for (size_t i = 0; i < files.size(); i++) {
    list[num++] = table_cache_->NewIterator(options, files[i]->number,
                                            files[i]->file_size);
  }
  if (all) {
    for (size_t i = 0; i < c->inputs_[1].size(); i++)
      list[num++] = table_cache_->NewIterator(options, c->inputs_[1][i]->number,
                                              c->inputs_[1][i]->file_size);
  }
  assert(num <= space);

  // Note(Alec), here use ascending seq comparator,
  // because we want newer samples come out later to overwrite the older ones.
  Iterator* result = NewMergingIterator(&asce_icmp_, list, num);
  delete[] list;
  return result;
}

Compaction* VersionSet::PickCompaction() {
  Compaction* c;

  while (DYNAMIC_PARTITION_LENGTH &&
         !__sync_bool_compare_and_swap(&DYNAMIC_CONTROL_LOCK, 0, 1)) {
  }

  int64_t smallest_interval;

  // std::cout << "PickCompaction " <<
  // current_->time_partition_indexes_[0].size() << " " <<
  // current_->time_partition_indexes_[1].size() << std::endl;
  if (current_->time_partition_indexes_[0].size() > L0_MERGE_FACTOR + 1) {
    // First, search level 0 for stale partitions (#partitions > 1).
    c = new Compaction(options_, 0, L0_MERGE_FACTOR);
    std::vector<FileMetaData*>& files = current_->files_[0];
    c->input_time_boundaries_.push_back(files[0]->time_boundary);
    int64_t right_bound = files[0]->time_boundary + files[0]->time_interval;
    smallest_interval = files[0]->time_interval;
    for (size_t i = 0;
         i < current_->time_partition_indexes_[0][L0_MERGE_FACTOR]; i++) {
      c->inputs_[0].push_back(files[i]);
      if (files[i]->time_boundary != c->input_time_boundaries_.back())
        c->input_time_boundaries_.push_back(files[i]->time_boundary);

      if (files[i]->time_boundary + files[i]->time_interval > right_bound)
        right_bound = files[i]->time_boundary + files[i]->time_interval;

      if (files[i]->time_interval < smallest_interval)
        smallest_interval = files[i]->time_interval;
    }

    // If L1 has overlapping partitions, they should not be overlapping.
    std::vector<int> l1_overlaps;
    current_->OverlappingPartitions(1, c->input_time_boundaries_.front(),
                                    right_bound - 1, &l1_overlaps);
    for (int idx : l1_overlaps) {
      c->inputs_[1].push_back(current_->files_[1][idx]);

      if (current_->files_[1][idx]->time_interval < smallest_interval)
        smallest_interval = current_->files_[1][idx]->time_interval;
    }

    // Generate output time partitions based on the smallest interval.
    std::set<int64_t> out_time_boundaries;
    for (size_t i = 0; i < c->inputs_[0].size(); i++) {
      FileMetaData* f = c->inputs_[0][i];
      for (int64_t b = f->time_boundary / smallest_interval * smallest_interval;
           b < f->time_boundary + f->time_interval; b += smallest_interval) {
        if (f->time_boundary < b + smallest_interval)
          out_time_boundaries.insert(b);
      }
    }
    for (size_t i = 0; i < c->inputs_[1].size(); i++) {
      FileMetaData* f = c->inputs_[1][i];
      for (int64_t b = f->time_boundary / smallest_interval * smallest_interval;
           b < f->time_boundary + f->time_interval; b += smallest_interval) {
        if (f->time_boundary < b + smallest_interval)
          out_time_boundaries.insert(b);
      }
    }
    for (int64_t b : out_time_boundaries) {
      c->output_time_boundaries_.push_back(b);
      c->output_time_intervals_.push_back(smallest_interval);
    }
    // } else if (current_->time_partition_indexes_[1].size() > L1_MERGE_FACTOR
    // + 1) { NOTE(Alec): we discard l1 merge factor and directly compare time
    // span.
  } else if (!current_->files_[1].empty() &&
             (current_->files_[1].back()->time_boundary +
                      current_->files_[1].back()->time_interval -
                      current_->files_[1].front()->time_boundary >
                  L2_PARTITION_LENGTH ||
              current_->OverlappingPartitions(
                  2, current_->files_[1].front()->time_boundary,
                  current_->files_[1].back()->time_boundary +
                      current_->files_[1].back()->time_interval - 1) > 0)) {
    // Second, search level 1 for partitions merge.
    std::vector<FileMetaData*>& files = current_->files_[1];
    int64_t start =
        files[0]->time_boundary / L2_PARTITION_LENGTH * L2_PARTITION_LENGTH;
    smallest_interval = L2_PARTITION_LENGTH;

    if (current_->time_partition_indexes_[1].size() > 1 &&
        files[0]->time_boundary + files[0]->time_interval <
            files[current_->time_partition_indexes_[1][1]]->time_boundary) {
      // Out-of-order data.
      c = new Compaction(options_, 1, 1);
      c->input_time_boundaries_.push_back(files[0]->time_boundary);
      for (size_t i = 0; i < current_->time_partition_indexes_[1][1]; i++) {
        c->inputs_[0].push_back(files[i]);
        if (files[i]->time_boundary != c->input_time_boundaries_.back())
          c->input_time_boundaries_.push_back(files[i]->time_boundary);
      }

      std::vector<int> l2_overlaps;
      current_->OverlappingPartitions(
          2, files[0]->time_boundary,
          files[0]->time_boundary + files[0]->time_interval - 1, &l2_overlaps);
      for (int idx : l2_overlaps) {
        c->inputs_[1].push_back(current_->files_[2][idx]);
      }
    } else {
      c = new Compaction(options_, 1, L1_MERGE_FACTOR);

      int64_t right_bound = files[0]->time_boundary + files[0]->time_interval;

      c->input_time_boundaries_.push_back(files[0]->time_boundary);
      for (size_t i = 0; i < current_->time_partition_indexes_[1].back(); i++) {
        if (files[i]->time_boundary != c->input_time_boundaries_.back()) {
          if (start + L2_PARTITION_LENGTH <
                  files[i]->time_boundary + files[i]->time_interval ||
              current_->OverlappingPartitions(
                  2, files[0]->time_boundary,
                  files[i]->time_boundary + files[i]->time_interval - 1) > 1) {
            // NOTE(Alec): make the compaction possibly only affect one
            // partition in L2.
            c->set_merge_factor(c->input_time_boundaries_.size());
            break;
          }
          c->input_time_boundaries_.push_back(files[i]->time_boundary);
        }

        if (files[i]->time_boundary + files[i]->time_interval > right_bound)
          right_bound = files[i]->time_boundary + files[i]->time_interval;
        c->inputs_[0].push_back(files[i]);
      }

      std::vector<int> l2_overlaps;
      current_->OverlappingPartitions(2, files[0]->time_boundary,
                                      right_bound - 1, &l2_overlaps);
      for (int idx : l2_overlaps)
        c->inputs_[1].push_back(current_->files_[2][idx]);
    }

    // Generate output time partitions.
    std::set<int64_t> out_time_boundaries;
    if (c->inputs_[1].empty()) {
      // Uniform interval if no overlaps.
      for (size_t i = 0; i < c->inputs_[0].size(); i++) {
        FileMetaData* f = c->inputs_[0][i];
        for (int64_t b =
                 f->time_boundary / smallest_interval * smallest_interval;
             b < f->time_boundary + f->time_interval; b += smallest_interval) {
          if (f->time_boundary < b + smallest_interval)
            out_time_boundaries.insert(b);
        }
      }
      for (int64_t b : out_time_boundaries) {
        c->output_time_boundaries_.push_back(b);
        c->output_time_intervals_.push_back(smallest_interval);
      }
    } else {
      // Get L2 time segments.
      std::vector<std::pair<int64_t, int64_t>> l2_ranges;
      for (size_t i = 0; i < c->inputs_[1].size(); i++) {
        size_t j = 0;
        for (; j < l2_ranges.size(); j++) {
          if (l2_ranges[j].first >= c->inputs_[1][i]->time_boundary) break;
        }
        if (j >= l2_ranges.size())
          l2_ranges.push_back(std::make_pair(c->inputs_[1][i]->time_boundary,
                                             c->inputs_[1][i]->time_interval));
        else if (l2_ranges[j].first != c->inputs_[1][i]->time_boundary)
          l2_ranges.insert(l2_ranges.begin() + j,
                           std::make_pair(c->inputs_[1][i]->time_boundary,
                                          c->inputs_[1][i]->time_interval));

        if (c->inputs_[1][i]->time_interval < smallest_interval)
          smallest_interval = c->inputs_[1][i]->time_interval;
      }

      // Generate output time partitions that do not overlap with L2 partitions.
      for (size_t i = 0; i < c->inputs_[0].size(); i++) {
        for (size_t j = 0; j < l2_ranges.size(); j++) {
          // Find the overlapping range.
          if (l2_ranges[j].first < c->inputs_[0][i]->time_boundary +
                                       c->inputs_[0][i]->time_interval &&
              c->inputs_[0][i]->time_boundary <
                  l2_ranges[j].first + l2_ranges[j].second) {
            // Find the left empty space.
            if (c->inputs_[0][i]->time_boundary < l2_ranges[j].first) {
              if (j == 0) {
                int64_t b = l2_ranges[j].first - smallest_interval;
                while (b + smallest_interval >
                       c->inputs_[0][i]->time_boundary) {
                  out_time_boundaries.insert(b);
                  b -= smallest_interval;
                }
              } else {
                for (int64_t b =
                         l2_ranges[j - 1].first + l2_ranges[j - 1].second;
                     b < l2_ranges[j].first; b += smallest_interval) {
                  if (b + smallest_interval > c->inputs_[0][i]->time_boundary)
                    out_time_boundaries.insert(b);
                }
              }
            }
            // Find the right empty space.
            if (c->inputs_[0][i]->time_boundary +
                    c->inputs_[0][i]->time_interval >
                l2_ranges[j].first + l2_ranges[j].second) {
              if (j == l2_ranges.size() - 1) {
                int64_t b = l2_ranges.back().first + l2_ranges.back().second;
                while (b < c->inputs_[0][i]->time_boundary +
                               c->inputs_[0][i]->time_interval) {
                  out_time_boundaries.insert(b);
                  b += smallest_interval;
                }
              } else {
                for (int64_t b = l2_ranges[j].first + l2_ranges[j].second;
                     b < l2_ranges[j + 1].first; b += smallest_interval) {
                  if (b < c->inputs_[0][i]->time_boundary +
                              c->inputs_[0][i]->time_interval)
                    out_time_boundaries.insert(b);
                }
              }
            }
          }
        }
      }

      for (int64_t b : out_time_boundaries) {
        c->output_time_boundaries_.push_back(b);
        c->output_time_intervals_.push_back(smallest_interval);
      }
      for (size_t i = 0; i < l2_ranges.size(); i++) {
        size_t j = 0;
        for (; j < c->output_time_boundaries_.size(); j++) {
          if (c->output_time_boundaries_[j] >= l2_ranges[i].first) break;
        }
        if (j >= c->output_time_boundaries_.size()) {
          c->output_time_boundaries_.push_back(l2_ranges[i].first);
          c->output_time_intervals_.push_back(l2_ranges[i].second);
        } else if (l2_ranges[i].first != c->output_time_boundaries_[j]) {
          c->output_time_boundaries_.insert(
              c->output_time_boundaries_.begin() + j, l2_ranges[i].first);
          c->output_time_intervals_.insert(
              c->output_time_intervals_.begin() + j, l2_ranges[i].second);
        }
      }
    }
  } else {
    // Search for L2 for patch merging.
    bool found = false;
    int64_t tb;
    uint64_t start_id, end_id;
    std::vector<FileMetaData*>& files = current_->files_[2];
    for (size_t i = 0; i < files.size(); i++) {
      if (files[i]->num_patches >= L2_MERGE_COMPACTION_THRESHOLD) {
        found = true;
        tb = files[i]->time_boundary;
        start_id = files[i]->start_id;
        end_id = files[i]->end_id;
        break;
      }
    }

    if (found) {
      c = new Compaction(options_, 2, L1_MERGE_FACTOR);
      c->input_time_boundaries_.push_back(tb);
      for (size_t i = 0; i < files.size(); i++) {
        if (files[i]->time_boundary < tb)
          continue;
        else if (files[i]->time_boundary == tb &&
                 files[i]->start_id == start_id && files[i]->end_id == end_id) {
          c->inputs_[0].push_back(files[i]);
        } else if (files[i]->time_boundary > tb)
          break;
      }
    } else {
      if (DYNAMIC_PARTITION_LENGTH)
        __sync_lock_test_and_set(&DYNAMIC_CONTROL_LOCK, 0);

      return nullptr;
    }
  }

  LOG_DEBUG << c->output_time_ranges()
            << " PARTITION_LENGTH:" << PARTITION_LENGTH
            << " L2_PARTITION_LENGTH:" << L2_PARTITION_LENGTH;

  c->input_version_ = current_;
  c->input_version_->Ref();

  return c;
}

Compaction* VersionSet::NewPurgeCompaction(int64_t timestamp, int level) {
  Compaction* c = new Compaction(timestamp, level);
  c->input_version_ = current_;
  c->input_version_->Ref();

  for (int i = 0; i <= 2; i++) {
    std::vector<FileMetaData*>& files = current_->files_[i];
    for (size_t j = 0; j < files.size(); j++) {
      if (files[j]->time_boundary + files[j]->time_interval <= timestamp)
        c->purge_inputs_[i].push_back(files[j]);
    }
  }

  return c;
}

// Finds the largest key in a vector of files. Returns true if files it not
// empty.
bool FindLargestKey(const InternalKeyComparator& icmp,
                    const std::vector<FileMetaData*>& files,
                    InternalKey* largest_key) {
  if (files.empty()) {
    return false;
  }
  *largest_key = files[0]->largest;
  for (size_t i = 1; i < files.size(); ++i) {
    FileMetaData* f = files[i];
    if (icmp.Compare(f->largest, *largest_key) > 0) {
      *largest_key = f->largest;
    }
  }
  return true;
}

// Finds minimum file b2=(l2, u2) in level file for which l2 > u1 and
// user_key(l2) = user_key(u1)
FileMetaData* FindSmallestBoundaryFile(
    const InternalKeyComparator& icmp,
    const std::vector<FileMetaData*>& level_files,
    const InternalKey& largest_key) {
  const Comparator* user_cmp = icmp.user_comparator();
  FileMetaData* smallest_boundary_file = nullptr;
  for (size_t i = 0; i < level_files.size(); ++i) {
    FileMetaData* f = level_files[i];
    if (icmp.Compare(f->smallest, largest_key) > 0 &&
        user_cmp->Compare(f->smallest.user_key(), largest_key.user_key()) ==
            0) {
      if (smallest_boundary_file == nullptr ||
          icmp.Compare(f->smallest, smallest_boundary_file->smallest) < 0) {
        smallest_boundary_file = f;
      }
    }
  }
  return smallest_boundary_file;
}

// Extracts the largest file b1 from |compaction_files| and then searches for a
// b2 in |level_files| for which user_key(u1) = user_key(l2). If it finds such a
// file b2 (known as a boundary file) it adds it to |compaction_files| and then
// searches again using this new upper bound.
//
// If there are two blocks, b1=(l1, u1) and b2=(l2, u2) and
// user_key(u1) = user_key(l2), and if we compact b1 but not b2 then a
// subsequent get operation will yield an incorrect result because it will
// return the record from b2 in level i rather than from b1 because it searches
// level by level for records matching the supplied user key.
//
// parameters:
//   in     level_files:      List of files to search for boundary files.
//   in/out compaction_files: List of files to extend by adding boundary files.
void AddBoundaryInputs(const InternalKeyComparator& icmp,
                       const std::vector<FileMetaData*>& level_files,
                       std::vector<FileMetaData*>* compaction_files) {
  InternalKey largest_key;

  // Quick return if compaction_files is empty.
  if (!FindLargestKey(icmp, *compaction_files, &largest_key)) {
    return;
  }

  bool continue_searching = true;
  while (continue_searching) {
    FileMetaData* smallest_boundary_file =
        FindSmallestBoundaryFile(icmp, level_files, largest_key);

    // If a boundary file was found advance largest_key, otherwise we're done.
    if (smallest_boundary_file != NULL) {
      compaction_files->push_back(smallest_boundary_file);
      largest_key = smallest_boundary_file->largest;
    } else {
      continue_searching = false;
    }
  }
}

void VersionSet::SetupOtherInputs(Compaction* c) {
  const int level = c->level();
  InternalKey smallest, largest;

  AddBoundaryInputs(icmp_, current_->files_[level], &c->inputs_[0]);
  GetRange(c->inputs_[0], &smallest, &largest);

  current_->GetOverlappingInputs(level + 1, &smallest, &largest,
                                 &c->inputs_[1]);

  // Get entire range covered by compaction
  InternalKey all_start, all_limit;
  GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);

  // See if we can grow the number of inputs in "level" without
  // changing the number of "level+1" files we pick up.
  if (!c->inputs_[1].empty()) {
    std::vector<FileMetaData*> expanded0;
    current_->GetOverlappingInputs(level, &all_start, &all_limit, &expanded0);
    AddBoundaryInputs(icmp_, current_->files_[level], &expanded0);
    const int64_t inputs0_size = TotalFileSize(c->inputs_[0]);
    const int64_t inputs1_size = TotalFileSize(c->inputs_[1]);
    const int64_t expanded0_size = TotalFileSize(expanded0);
    if (expanded0.size() > c->inputs_[0].size() &&
        inputs1_size + expanded0_size <
            ExpandedCompactionByteSizeLimit(options_)) {
      InternalKey new_start, new_limit;
      GetRange(expanded0, &new_start, &new_limit);
      std::vector<FileMetaData*> expanded1;
      current_->GetOverlappingInputs(level + 1, &new_start, &new_limit,
                                     &expanded1);
      if (expanded1.size() == c->inputs_[1].size()) {
        Log(options_->info_log,
            "Expanding@%d %d+%d (%ld+%ld bytes) to %d+%d (%ld+%ld bytes)\n",
            level, int(c->inputs_[0].size()), int(c->inputs_[1].size()),
            long(inputs0_size), long(inputs1_size), int(expanded0.size()),
            int(expanded1.size()), long(expanded0_size), long(inputs1_size));
        smallest = new_start;
        largest = new_limit;
        c->inputs_[0] = expanded0;
        c->inputs_[1] = expanded1;
        GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);
      }
    }
  }

  // Compute the set of grandparent files that overlap this compaction
  // (parent == level+1; grandparent == level+2)
  if (level + 2 < config::kNumLevels) {
    current_->GetOverlappingInputs(level + 2, &all_start, &all_limit,
                                   &c->grandparents_);
  }

  // Update the place where we will do the next compaction for this level.
  // We update this immediately instead of waiting for the VersionEdit
  // to be applied so that if the compaction fails, we will try a different
  // key range next time.
  compact_pointer_[level] = largest.Encode().ToString();
  c->edit_.SetCompactPointer(level, largest);
}

Compaction* VersionSet::CompactRange(int level, const InternalKey* begin,
                                     const InternalKey* end) {
  std::vector<FileMetaData*> inputs;
  current_->GetOverlappingInputs(level, begin, end, &inputs);
  if (inputs.empty()) {
    return nullptr;
  }

  // Avoid compacting too much in one shot in case the range is large.
  // But we cannot do this for level-0 since level-0 files can overlap
  // and we must not pick one file and drop another older file if the
  // two files overlap.
  if (level > 0) {
    const uint64_t limit = MaxFileSizeForLevel(options_, level);
    uint64_t total = 0;
    for (size_t i = 0; i < inputs.size(); i++) {
      uint64_t s = inputs[i]->file_size;
      total += s;
      if (total >= limit) {
        inputs.resize(i + 1);
        break;
      }
    }
  }

  Compaction* c = new Compaction(options_, level);
  c->input_version_ = current_;
  c->input_version_->Ref();
  c->inputs_[0] = inputs;
  SetupOtherInputs(c);
  return c;
}

Compaction::Compaction(const Options* options, int level)
    : level_(level),
      max_output_file_size_(MaxFileSizeForLevel(options, level)),
      input_version_(nullptr),
      grandparent_index_(0),
      seen_key_(false),
      overlapped_bytes_(0),
      merge_factor_(1),
      purge_time_(-1) {
  for (int i = 0; i < config::kNumLevels; i++) {
    level_ptrs_[i] = 0;
  }
}

Compaction::Compaction(const Options* options, int level, int merge_factor,
                       int merge_number)
    : level_(level),
      max_output_file_size_(MaxFileSizeForLevel(options, level)),
      input_version_(nullptr),
      grandparent_index_(0),
      seen_key_(false),
      overlapped_bytes_(0),
      merge_factor_(merge_factor),
      merge_number_(merge_number),
      purge_time_(-1) {
  for (int i = 0; i < config::kNumLevels; i++) {
    level_ptrs_[i] = 0;
  }
}

Compaction::Compaction(int64_t purge_time, int level)
    : level_(level),
      purge_time_(purge_time),
      purge_inputs_(config::kNumLevels) {}

Compaction::~Compaction() {
  if (input_version_ != nullptr) {
    input_version_->Unref();
  }
}

bool Compaction::IsTrivialMove() const {
  const VersionSet* vset = input_version_->vset_;
  // Avoid a move if there is lots of overlapping grandparent data.
  // Otherwise, the move could create a parent file that will require
  // a very expensive merge later on.
  return (num_input_files(0) == 1 && num_input_files(1) == 0 &&
          TotalFileSize(grandparents_) <=
              MaxGrandParentOverlapBytes(vset->options_));
}

void Compaction::AddInputDeletions(VersionEdit* edit) {
  for (int which = 0; which < 2; which++) {
    if (level_ == 1 && which == 1) break;
    for (size_t i = 0; i < inputs_[which].size(); i++) {
      edit->RemoveFile(level_ + which, inputs_[which][i]->number);
    }
  }
}

void Compaction::AddPurgeDeletions(VersionEdit* edit) {
  for (int which = 0; which < purge_inputs_.size(); which++) {
    for (size_t i = 0; i < purge_inputs_[which].size(); i++) {
      edit->RemoveFile(which, purge_inputs_[which][i]->number);
    }
  }
}

bool Compaction::IsBaseLevelForKey(const Slice& user_key) {
  // Maybe use binary search to find right entry instead of linear search?
  const Comparator* user_cmp = input_version_->vset_->icmp_.user_comparator();
  for (int lvl = level_ + 2; lvl < config::kNumLevels; lvl++) {
    const std::vector<FileMetaData*>& files = input_version_->files_[lvl];
    while (level_ptrs_[lvl] < files.size()) {
      FileMetaData* f = files[level_ptrs_[lvl]];
      if (user_cmp->Compare(user_key, f->largest.user_key()) <= 0) {
        // We've advanced far enough
        if (user_cmp->Compare(user_key, f->smallest.user_key()) >= 0) {
          // Key falls in this file's range, so definitely not base level
          return false;
        }
        break;
      }
      level_ptrs_[lvl]++;
    }
  }
  return true;
}

bool Compaction::ShouldStopBefore(const Slice& internal_key) {
  const VersionSet* vset = input_version_->vset_;
  // Scan to find earliest grandparent file that contains key.
  const InternalKeyComparator* icmp = &vset->icmp_;
  while (grandparent_index_ < grandparents_.size() &&
         icmp->Compare(internal_key,
                       grandparents_[grandparent_index_]->largest.Encode()) >
             0) {
    if (seen_key_) {
      overlapped_bytes_ += grandparents_[grandparent_index_]->file_size;
    }
    grandparent_index_++;
  }
  seen_key_ = true;

  if (overlapped_bytes_ > MaxGrandParentOverlapBytes(vset->options_)) {
    // Too much overlap for current output; start new output
    overlapped_bytes_ = 0;
    return true;
  } else {
    return false;
  }
}

void Compaction::ReleaseInputs() {
  if (input_version_ != nullptr) {
    input_version_->Unref();
    input_version_ = nullptr;
  }
}

void Compaction::output_time_range(int64_t time, int64_t* boundary,
                                   int64_t* interval) {
  for (size_t i = 0; i < output_time_boundaries_.size(); i++) {
    if (time >= output_time_boundaries_[i] &&
        time < output_time_boundaries_[i] + output_time_intervals_[i]) {
      *boundary = output_time_boundaries_[i];
      *interval = output_time_intervals_[i];
      return;
    }
  }
  *boundary = -1;
  *interval = -1;
}

std::string Compaction::output_time_ranges() {
  std::string r;
  for (size_t i = 0; i < output_time_boundaries_.size(); i++) {
    r.append("[");
    AppendNumberTo(&r, output_time_boundaries_[i]);
    r.append(",");
    AppendNumberTo(&r, output_time_intervals_[i]);
    r.append("] ");
  }
  return r;
}

}  // namespace leveldb
