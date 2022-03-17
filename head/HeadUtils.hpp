#ifndef HEADUTILS_H
#define HEADUTILS_H

#include <algorithm>
#include <boost/filesystem.hpp>
#include <limits>

#include "base/Endian.hpp"
#include "base/Error.hpp"
#include "label/Label.hpp"
#include "leveldb/util/coding.h"
#include "tsdbutil/MMapLinuxManager.hpp"

#define USE_PERSISTENT_CEDAR 1
#define HEAD_USE_XORCHUNK 1
#define USE_MMAP_LABELS 1
#define MMAP_ARRAY_SLOTS 1048576
// #define MMAP_ARRAY_SLOTS_MASK 1048575
// #define MMAP_ARRAY_SLOTS_BITS 20

#define COMMIT_BATCH_WRITE 0

namespace tsdb {

namespace head {

// ErrNotFound is returned if a looked up resource was not found.
extern const error::Error ErrNotFound;

// ErrOutOfOrderSample is returned if an appended sample has a
// timestamp larger than the most recent sample.
extern const error::Error ErrOutOfOrderSample;

// ErrAmendSample is returned if an appended sample has the same timestamp
// as the most recent sample but a different value.
extern const error::Error ErrAmendSample;

// ErrOutOfBounds is returned if an appended sample is out of the
// writable time range.
extern const error::Error ErrOutOfBounds;

extern const int SAMPLES_PER_CHUNK;

extern const int STRIPE_SIZE;
extern const uint64_t STRIPE_MASK;

extern std::string HEAD_LOG_NAME;
extern std::string HEAD_SAMPLES_LOG_NAME;
extern std::string HEAD_FLUSHES_LOG_NAME;
extern size_t MAX_HEAD_SAMPLES_LOG_SIZE;

extern size_t MAX_HEAD_LABELS_FILE_SIZE;
extern size_t AVG_HEAD_LABELS_SIZE;

extern int HEAD_SAMPLES_LOG_CLEANING_THRES;

class Sample {
 public:
  int64_t t;
  double v;

  Sample() {
    t = std::numeric_limits<int64_t>::min();
    v = 0;
  }

  Sample(int64_t t, double v) {
    this->t = t;
    this->v = v;
  }

  void reset() {
    t = std::numeric_limits<int64_t>::min();
    v = 0;
  }

  void reset(int64_t t, double v) {
    this->t = t;
    this->v = v;
  }

  Sample& operator=(const Sample& s) {
    t = s.t;
    v = s.v;
    return (*this);
  }
};

// compute_chunk_end_time estimates the end timestamp based the beginning of a
// chunk, its current timestamp and the upper bound up to which we insert data.
// It assumes that the time range is 1/4 full.
int64_t compute_chunk_end_time(int64_t min_time, int64_t max_time,
                               int64_t next_at);

// packChunkID packs a seriesID and a chunkID within it into a global 8 byte ID.
// It panics if the seriesID exceeds 5 bytes or the chunk ID 3 bytes.
uint64_t pack_chunk_id(uint64_t series_id, uint64_t chunk_id);
std::pair<uint64_t, uint64_t> unpack_chunk_id(uint64_t id);

class MMapXORChunk {
 private:
  std::string dir_;
  std::string prefix_;
  int slot_size_;
  int slots_per_file_;
  int slots_per_file_mask_;
  int slots_per_file_bits_;
  int cur_file_idx_;
  int header_size_;
  std::vector<std::unique_ptr<tsdbutil::MMapLinuxManager>> files_;

  std::vector<uint64_t> holes_;

 public:
  MMapXORChunk(const std::string& dir, int slot_size, int num_slots,
               const std::string& prefix = "");

  bool empty();
  size_t num_files();

  void get_ids(int idx, std::vector<uint64_t>* ids,
               std::vector<uint64_t>* slots, std::vector<uint8_t*>* ptrs);

  void set_bitmap(int idx1, int idx2);

  uint8_t* encode_slot_header(uint64_t idx1, uint64_t idx2, uint64_t id);

  std::pair<uint64_t, uint8_t*> alloc_slot(uint64_t id);

  uint8_t* get_ptr(uint64_t slot);

  void sync(uint64_t slot);

  void delete_slot(uint64_t slot) { holes_.push_back(slot); }

  uint64_t current_size() {
    uint32_t num = leveldb::DecodeFixed32(files_.back()->data());
    return header_size_ +
           (2 * slot_size_ + 1) * 8 * slots_per_file_ * (files_.size() - 1) +
           header_size_ + (2 * slot_size_ + 1) * 8 * num;
  }

  static void remove_all(int slot_size, const std::string& dir,
                         const std::string& prefix = "");
};

class MMapGroupXORChunk {
 private:
  std::string dir_;
  std::string prefix_;
  int slot_size_;
  int slots_per_file_;
  int slots_per_file_mask_;
  int slots_per_file_bits_;
  int cur_file_idx_;
  int header_size_;
  std::vector<std::unique_ptr<tsdbutil::MMapLinuxManager>> files_;

  std::vector<uint64_t> holes_;

 public:
  MMapGroupXORChunk(const std::string& dir, int slot_size, int num_slots,
                    const std::string& prefix = "");

  bool empty();
  size_t num_files();

  void get_ids(int idx, std::vector<uint64_t>* ids,
               std::vector<uint32_t>* indices, std::vector<uint64_t>* slots,
               std::vector<uint8_t*>* ptrs);

  void set_bitmap(int idx1, int idx2);

  uint8_t* encode_slot_header(uint64_t idx1, uint64_t idx2, uint64_t id,
                              uint32_t slot);

  std::pair<uint64_t, uint8_t*> alloc_slot(uint64_t id, uint32_t slot);

  uint8_t* get_ptr(uint64_t slot);

  void sync(uint64_t slot);

  void delete_slot(uint64_t slot) { holes_.push_back(slot); }

  uint64_t current_size() {}

  static void remove_all(int slot_size, const std::string& dir,
                         const std::string& prefix = "");
};

class MMapIntArray {
 private:
  std::string dir_;
  std::string prefix_;
  int slot_size_;
  int slots_per_file_;
  int slots_per_file_mask_;
  int slots_per_file_bits_;
  int cur_file_idx_;
  int header_size_;
  std::vector<std::unique_ptr<tsdbutil::MMapLinuxManager>> files_;

  std::vector<uint64_t> holes_;

 public:
  MMapIntArray(const std::string& dir, int slot_size, int num_slots,
               const std::string& prefix = "");

  bool empty();
  size_t num_files();

  void get_ids(int idx, std::vector<uint64_t>* ids,
               std::vector<uint64_t>* slots);

  void set_bitmap(int idx1, int idx2);

  void encode_slot_header(uint64_t idx1, uint64_t idx2, int num, uint64_t id);

  uint64_t alloc_slot(uint64_t id);

  // Return the slot sample number after insertion.
  int write(uint64_t slot, int64_t value);
  void write(uint64_t slot, int idx, int64_t value);

  int64_t read(uint64_t slot, int idx);

  std::vector<int64_t> read(uint64_t slot);

  void clean(uint64_t slot);

  void sync(uint64_t slot);

  void delete_slot(uint64_t slot) { holes_.push_back(slot); }

  static void remove_all(int slot_size, const std::string& dir,
                         const std::string& prefix = "");
};

class MMapFloatArray {
 private:
  std::string dir_;
  int slot_size_;
  int slots_per_file_;
  int slots_per_file_mask_;
  int slots_per_file_bits_;
  int cur_file_idx_;
  int header_size_;
  std::vector<std::unique_ptr<tsdbutil::MMapLinuxManager>> files_;

  std::vector<uint64_t> holes_;

 public:
  MMapFloatArray(const std::string& dir, int slot_size, int num_slots);

  bool empty();
  size_t num_files();

  void get_ids(int idx, std::vector<uint64_t>* ids,
               std::vector<uint64_t>* slots);

  void set_bitmap(int idx1, int idx2);

  void encode_slot_header(uint64_t idx1, uint64_t idx2, int num, uint64_t id);

  uint64_t alloc_slot(uint64_t id);

  // Return the slot sample number after insertion.
  int write(uint64_t slot, double value);
  void write(uint64_t slot, int idx, double value);

  double read(uint64_t slot, int idx);

  std::vector<double> read(uint64_t slot);

  void clean(uint64_t slot);

  void sync(uint64_t slot);

  void delete_slot(uint64_t slot) { holes_.push_back(slot); }

  static void remove_all(int slot_size, const std::string& dir);
};

class MMapGroupFloatArray {
 private:
  std::string dir_;
  int slot_size_;
  int slots_per_file_;
  int slots_per_file_mask_;
  int slots_per_file_bits_;
  int cur_file_idx_;
  int header_size_;
  std::vector<std::unique_ptr<tsdbutil::MMapLinuxManager>> files_;

  std::vector<uint64_t> holes_;

 public:
  MMapGroupFloatArray(const std::string& dir, int slot_size, int num_slots);

  bool empty();
  size_t num_files();

  void get_ids(int idx, std::vector<uint64_t>* gids,
               std::vector<uint32_t>* indexes, std::vector<uint64_t>* slots);

  void set_bitmap(int idx1, int idx2);

  void encode_slot_header(uint64_t idx1, uint64_t idx2, int num, uint64_t id,
                          uint32_t idx);

  uint64_t alloc_slot(uint64_t gid, uint32_t idx);

  // Return the slot sample number after insertion.
  int write(uint64_t slot, double value);
  void write(uint64_t slot, int idx, double value);

  double read(uint64_t slot, int idx);

  std::vector<double> read(uint64_t slot);

  void clean(uint64_t slot);

  void sync(uint64_t slot);

  void delete_slot(uint64_t slot) { holes_.push_back(slot); }

  static void remove_all(int slot_size, const std::string& dir);
};

/* ------------------------------------------------------------------
 * Single Timeseries
 * ------------------------------------------------------------------
 * | size <8b> | #slots <8b> | slot1 off <8b> | slot1 size <2b> | ...
 * | TSID <var> | # labels <1b> | label1 | value1 | ...
 * ------------------------------------------------------------------
 */
class MMapLabelsIterator;
class MMapLabels {
 private:
  friend class MMapLabelsIterator;
  std::string dir_;
  int slots_per_file_;
  int cur_file_idx_;
  int header_size_;
  uint64_t cur_slot_;
  std::vector<std::unique_ptr<tsdbutil::MMapLinuxManager>> files_;
  std::vector<int> file_slots;

  // TODO.
  std::vector<uint64_t> holes_;

  uint64_t get_labels(uint64_t idx1, uint64_t idx2, label::Labels& lset);

 public:
  MMapLabels(const std::string& dir);

  static void remove_all(const std::string& dir);

  bool empty();
  size_t num_files();

  uint64_t add_labels(uint64_t id, const label::Labels& lset);

  void get_labels(uint64_t idx, label::Labels& lset);

  MMapLabelsIterator iterator();
};

class MMapLabelsIterator {
 private:
  int cur_file_;
  int idx_inside_file_;
  int64_t total_idx_;
  MMapLabels* mmap_labels_;

 public:
  MMapLabelsIterator(MMapLabels* m);

  bool next();
  void at(uint64_t* id, uint64_t* idx);
  void at(uint64_t* id, uint64_t* idx, label::Labels& lset);
};

/* -----------------------------------------------------------------------------------------------
 * Group
 * -----------------------------------------------------------------------------------------------
 * | size <8b> | #slots <8b> | slot1 off <8b> | slot1 size <2b> | ...
 * | GID(the highest bit is reset to 0) <var> | idx <var> | # labels <1b> |
 * label1 | value1 | ...
 * -----------------------------------------------------------------------------------------------
 */
class MMapGroupLabelsIterator;
class MMapGroupLabels {
 private:
  friend class MMapGroupLabelsIterator;
  std::string dir_;
  int slots_per_file_;
  int cur_file_idx_;
  int header_size_;
  uint64_t cur_slot_;
  std::vector<std::unique_ptr<tsdbutil::MMapLinuxManager>> files_;
  std::vector<int> file_slots;

  // TODO.
  std::vector<uint64_t> holes_;

  uint64_t get_labels(uint64_t idx1, uint64_t idx2, label::Labels& lset,
                      uint32_t* idx);

 public:
  MMapGroupLabels(const std::string& dir);

  static void remove_all(const std::string& dir);

  bool empty();
  size_t num_files();

  // max 32-bit int represents group tags.
  uint64_t add_labels(uint64_t id, uint32_t idx, const label::Labels& lset);

  void get_labels(uint64_t idx, label::Labels& lset, uint32_t* index);

  MMapGroupLabelsIterator iterator();
};

class MMapGroupLabelsIterator {
 private:
  int cur_file_;
  int idx_inside_file_;
  int64_t total_idx_;
  MMapGroupLabels* mmap_labels_;

 public:
  MMapGroupLabelsIterator(MMapGroupLabels* m);

  bool next();
  void at(uint64_t* id, uint32_t* idx_inside_group, uint64_t* idx);
  void at(uint64_t* id, uint32_t* idx_inside_group, uint64_t* idx,
          label::Labels& lset);
};

}  // namespace head
}  // namespace tsdb

#endif