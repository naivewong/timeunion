#include "head/HeadUtils.hpp"

#include <iostream>

#include "base/Logging.hpp"
#include "label/Label.hpp"

namespace tsdb {
namespace head {

const error::Error ErrNotFound = error::Error("not found");
const error::Error ErrOutOfOrderSample = error::Error("out of order sample");
const error::Error ErrAmendSample = error::Error("amending sample");
const error::Error ErrOutOfBounds = error::Error("out of bounds");

const int SAMPLES_PER_CHUNK = 120;

const int STRIPE_SIZE = 1 << 14;
const uint64_t STRIPE_MASK = STRIPE_SIZE - 1;

std::string HEAD_LOG_NAME = "head.log";
std::string HEAD_SAMPLES_LOG_NAME = "head_samples_log";
std::string HEAD_FLUSHES_LOG_NAME = "head_flushes_log";
size_t MAX_HEAD_SAMPLES_LOG_SIZE = 512 * 1024 * 1024;

size_t MAX_HEAD_LABELS_FILE_SIZE = 512 * 1024 * 1024;
size_t AVG_HEAD_LABELS_SIZE = 30;

int HEAD_SAMPLES_LOG_CLEANING_THRES = 8;

// compute_chunk_end_time estimates the end timestamp based the beginning of a
// chunk, its current timestamp and the upper bound up to which we insert data.
// It assumes that the time range is 1/4 full.
int64_t compute_chunk_end_time(int64_t min_time, int64_t max_time,
                               int64_t next_at) {
  int a = (next_at - min_time) /
          ((max_time - min_time + 1) * 4);  // Avoid dividing by 0
  if (a == 0) return next_at;
  return (min_time + (next_at - min_time) / a);
}

// packChunkID packs a seriesID and a chunkID within it into a global 8 byte ID.
// It panics if the seriesID exceeds 5 bytes or the chunk ID 3 bytes.
uint64_t pack_chunk_id(uint64_t series_id, uint64_t chunk_id) {
  // std::cerr << series_id << " " << (static_cast<uint64_t>(1) << 40) -
  // static_cast<uint64_t>(1) << std::endl;
  if (series_id > (static_cast<uint64_t>(1) << 40) - static_cast<uint64_t>(1))
    LOG_FATAL << "head series id exceeds 5 bytes";
  if (chunk_id > (static_cast<uint64_t>(1) << 24) - static_cast<uint64_t>(1))
    LOG_FATAL << "head chunk id exceeds 3 bytes";
  return (series_id << 24) | chunk_id;
}
std::pair<uint64_t, uint64_t> unpack_chunk_id(uint64_t id) {
  return {id >> 24, (id << 40) >> 40};
}

/**********************************************
 *                MMapXORChunk                *
 **********************************************/
MMapXORChunk::MMapXORChunk(const std::string& dir, int slot_size, int num_slots,
                           const std::string& prefix)
    : dir_(dir),
      prefix_(prefix),
      slot_size_(slot_size),
      slots_per_file_(num_slots),
      slots_per_file_mask_(slots_per_file_ - 1),
      header_size_(4 + (slots_per_file_ + 7) / 8) {
  files_.reserve(500);

  slots_per_file_bits_ = 0;
  while (num_slots != 1) {
    num_slots >>= 1;
    ++slots_per_file_bits_;
  }

  std::vector<std::string> existing_files;
  boost::filesystem::path p(dir);
  boost::filesystem::directory_iterator end_itr;
  std::string tmp = std::to_string(slot_size) + prefix_ + "xorarray";
  // cycle through the directory
  for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
    // If it's not a directory, list it. If you want to list directories too,
    // just remove this check.
    if (boost::filesystem::is_regular_file(itr->path())) {
      // assign current file name to current_file and echo it out to the
      // console.
      std::string current_file = itr->path().filename().string();
      if (current_file.size() > tmp.size() &&
          memcmp(current_file.c_str(), tmp.c_str(), tmp.size()) == 0) {
        existing_files.push_back(current_file);
      }
    }
  }
  std::sort(existing_files.begin(), existing_files.end(),
            [&](const std::string& l, const std::string& r) {
              return std::stoi(l.substr(tmp.size())) <
                     std::stoi(r.substr(tmp.size()));
            });
  cur_file_idx_ = existing_files.size();

  for (const std::string& s : existing_files) {
    files_.emplace_back(new tsdb::tsdbutil::MMapLinuxManager(dir + "/" + s));
  }
}

void MMapXORChunk::remove_all(int slot_size, const std::string& dir,
                              const std::string& prefix) {
  std::vector<std::string> existing_files;
  boost::filesystem::path p(dir);
  boost::filesystem::directory_iterator end_itr;
  std::string tmp = std::to_string(slot_size) + prefix + "xorarray";
  // cycle through the directory
  for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
    // If it's not a directory, list it. If you want to list directories too,
    // just remove this check.
    if (boost::filesystem::is_regular_file(itr->path())) {
      // assign current file name to current_file and echo it out to the
      // console.
      std::string current_file = itr->path().filename().string();
      if (current_file.size() > tmp.size() &&
          memcmp(current_file.c_str(), tmp.c_str(), tmp.size()) == 0) {
        existing_files.push_back(current_file);
      }
    }
  }

  for (const std::string& s : existing_files)
    boost::filesystem::remove(dir + "/" + s);
}

bool MMapXORChunk::empty() { return files_.empty(); }
size_t MMapXORChunk::num_files() { return files_.size(); }

void MMapXORChunk::get_ids(int idx, std::vector<uint64_t>* ids,
                           std::vector<uint64_t>* slots,
                           std::vector<uint8_t*>* ptrs) {
  uint32_t num = leveldb::DecodeFixed32(files_[idx]->data());
  for (uint32_t i = 0; i < num; i++) {
    uint8_t* start = reinterpret_cast<uint8_t*>(
        files_[idx]->data() + header_size_ + (2 * slot_size_ + 1) * 8 * i);
    ids->push_back((static_cast<uint64_t>(start[0]) << 56) |
                   (static_cast<uint64_t>(start[1]) << 48) |
                   (static_cast<uint64_t>(start[2]) << 40) |
                   (static_cast<uint64_t>(start[3]) << 32) |
                   (static_cast<uint64_t>(start[4]) << 24) |
                   (static_cast<uint64_t>(start[5]) << 16) |
                   (static_cast<uint64_t>(start[6]) << 8) |
                   (static_cast<uint64_t>(start[7])));
    slots->push_back(i);
    ptrs->push_back(start + 8);
  }
}

void MMapXORChunk::set_bitmap(int idx1, int idx2) {
  int i1 = idx2 >> 3, i2 = idx2 & 0x7;
  files_[idx1]->data()[4 + i1] |= ((char)(1) << (7 - i2));
}

uint8_t* MMapXORChunk::encode_slot_header(uint64_t idx1, uint64_t idx2,
                                          uint64_t id) {
  char* start =
      files_[idx1]->data() + header_size_ + (2 * slot_size_ + 1) * 8 * idx2;
  leveldb::EncodeFixed64BE(start, id);
  return reinterpret_cast<uint8_t*>(start + 8);
}

std::pair<uint64_t, uint8_t*> MMapXORChunk::alloc_slot(uint64_t id) {
  if (!holes_.empty()) {
    size_t idx1 = holes_.back() >> slots_per_file_bits_,
           idx2 = holes_.back() & slots_per_file_mask_;
    holes_.pop_back();
    set_bitmap(idx1, idx2);
    uint8_t* ptr = encode_slot_header((uint64_t)(idx1), (uint64_t)(idx2), id);
    return {idx1 * slots_per_file_ + idx2, ptr};
  }
  uint32_t num;
  for (size_t i = 0; i < files_.size(); i++) {
    num = leveldb::DecodeFixed32(files_[i]->data());
    if (num < slots_per_file_) {
      set_bitmap(i, num);
      uint8_t* ptr = encode_slot_header((uint64_t)(i), (uint64_t)(num), id);
      num++;
      leveldb::EncodeFixed32(files_[i]->data(), num);
      return {i * slots_per_file_ + num - 1, ptr};
    }
  }
  files_.emplace_back(new tsdb::tsdbutil::MMapLinuxManager(
      dir_ + "/" + std::to_string(slot_size_) + prefix_ + "xorarray" +
          std::to_string(cur_file_idx_++),
      header_size_ + (2 * slot_size_ + 1) * 8 * slots_per_file_));
  leveldb::EncodeFixed32(files_.back()->data(), 1);
  set_bitmap(files_.size() - 1, 0);
  uint8_t* ptr =
      encode_slot_header((uint64_t)(files_.size() - 1), (uint64_t)(0), id);
  return {(files_.size() - 1) * slots_per_file_, ptr};
}

uint8_t* MMapXORChunk::get_ptr(uint64_t slot) {
  return reinterpret_cast<uint8_t*>(
      files_[slot >> slots_per_file_bits_]->data() + header_size_ +
      (2 * slot_size_ + 1) * 8 * (slot & slots_per_file_mask_));
}

void MMapXORChunk::sync(uint64_t slot) {
  size_t idx1 = slot >> slots_per_file_bits_,
         idx2 = slot & slots_per_file_mask_;
  files_[idx1]->sync(4 + idx2 / 8, 1);
  files_[idx1]->sync(header_size_ + 8 * (2 * slot_size_ + 1) * idx2,
                     8 * (2 * slot_size_ + 1));
}

/**********************************************
 *             MMapGroupXORChunk              *
 **********************************************/
MMapGroupXORChunk::MMapGroupXORChunk(const std::string& dir, int slot_size,
                                     int num_slots, const std::string& prefix)
    : dir_(dir),
      prefix_(prefix),
      slot_size_(slot_size),
      slots_per_file_(num_slots),
      slots_per_file_mask_(slots_per_file_ - 1),
      header_size_(4 + (slots_per_file_ + 7) / 8) {
  files_.reserve(500);

  slots_per_file_bits_ = 0;
  while (num_slots != 1) {
    num_slots >>= 1;
    ++slots_per_file_bits_;
  }

  std::vector<std::string> existing_files;
  boost::filesystem::path p(dir);
  boost::filesystem::directory_iterator end_itr;
  std::string tmp = std::to_string(slot_size) + prefix_ + "xorarray";
  // cycle through the directory
  for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
    // If it's not a directory, list it. If you want to list directories too,
    // just remove this check.
    if (boost::filesystem::is_regular_file(itr->path())) {
      // assign current file name to current_file and echo it out to the
      // console.
      std::string current_file = itr->path().filename().string();
      if (current_file.size() > tmp.size() &&
          memcmp(current_file.c_str(), tmp.c_str(), tmp.size()) == 0) {
        existing_files.push_back(current_file);
      }
    }
  }
  std::sort(existing_files.begin(), existing_files.end(),
            [&](const std::string& l, const std::string& r) {
              return std::stoi(l.substr(tmp.size())) <
                     std::stoi(r.substr(tmp.size()));
            });
  cur_file_idx_ = existing_files.size();

  for (const std::string& s : existing_files) {
    files_.emplace_back(new tsdb::tsdbutil::MMapLinuxManager(dir + "/" + s));
  }
}

void MMapGroupXORChunk::remove_all(int slot_size, const std::string& dir,
                                   const std::string& prefix) {
  std::vector<std::string> existing_files;
  boost::filesystem::path p(dir);
  boost::filesystem::directory_iterator end_itr;
  std::string tmp = std::to_string(slot_size) + prefix + "xorarray";
  // cycle through the directory
  for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
    // If it's not a directory, list it. If you want to list directories too,
    // just remove this check.
    if (boost::filesystem::is_regular_file(itr->path())) {
      // assign current file name to current_file and echo it out to the
      // console.
      std::string current_file = itr->path().filename().string();
      if (current_file.size() > tmp.size() &&
          memcmp(current_file.c_str(), tmp.c_str(), tmp.size()) == 0) {
        existing_files.push_back(current_file);
      }
    }
  }

  for (const std::string& s : existing_files)
    boost::filesystem::remove(dir + "/" + s);
}

bool MMapGroupXORChunk::empty() { return files_.empty(); }
size_t MMapGroupXORChunk::num_files() { return files_.size(); }

void MMapGroupXORChunk::get_ids(int idx, std::vector<uint64_t>* ids,
                                std::vector<uint32_t>* indices,
                                std::vector<uint64_t>* slots,
                                std::vector<uint8_t*>* ptrs) {
  uint32_t num = leveldb::DecodeFixed32(files_[idx]->data());
  for (uint32_t i = 0; i < num; i++) {
    uint8_t* start = reinterpret_cast<uint8_t*>(
        files_[idx]->data() + header_size_ + (slot_size_ + 1) * 8 * i + 4 * i);
    ids->push_back((static_cast<uint64_t>(start[0]) << 56) |
                   (static_cast<uint64_t>(start[1]) << 48) |
                   (static_cast<uint64_t>(start[2]) << 40) |
                   (static_cast<uint64_t>(start[3]) << 32) |
                   (static_cast<uint64_t>(start[4]) << 24) |
                   (static_cast<uint64_t>(start[5]) << 16) |
                   (static_cast<uint64_t>(start[6]) << 8) |
                   (static_cast<uint64_t>(start[7])));
    indices->push_back((static_cast<uint64_t>(start[8]) << 24) |
                       (static_cast<uint64_t>(start[9]) << 16) |
                       (static_cast<uint64_t>(start[10]) << 8) |
                       (static_cast<uint64_t>(start[11])));
    slots->push_back(i);
    ptrs->push_back(start + 12);
  }
}

void MMapGroupXORChunk::set_bitmap(int idx1, int idx2) {
  int i1 = idx2 >> 3, i2 = idx2 & 0x7;
  files_[idx1]->data()[4 + i1] |= ((char)(1) << (7 - i2));
}

uint8_t* MMapGroupXORChunk::encode_slot_header(uint64_t idx1, uint64_t idx2,
                                               uint64_t id, uint32_t slot) {
  char* start = files_[idx1]->data() + header_size_ +
                (slot_size_ + 1) * 8 * idx2 + 4 * idx2;
  leveldb::EncodeFixed64BE(start, id);
  leveldb::EncodeFixed32BE(start + 8, slot);
  return reinterpret_cast<uint8_t*>(start + 12);
}

std::pair<uint64_t, uint8_t*> MMapGroupXORChunk::alloc_slot(uint64_t id,
                                                            uint32_t slot) {
  if (!holes_.empty()) {
    size_t idx1 = holes_.back() >> slots_per_file_bits_,
           idx2 = holes_.back() & slots_per_file_mask_;
    holes_.pop_back();
    set_bitmap(idx1, idx2);
    uint8_t* ptr =
        encode_slot_header((uint64_t)(idx1), (uint64_t)(idx2), id, slot);
    return {idx1 * slots_per_file_ + idx2, ptr};
  }
  uint32_t num;
  for (size_t i = 0; i < files_.size(); i++) {
    num = leveldb::DecodeFixed32(files_[i]->data());
    if (num < slots_per_file_) {
      set_bitmap(i, num);
      uint8_t* ptr =
          encode_slot_header((uint64_t)(i), (uint64_t)(num), id, slot);
      num++;
      leveldb::EncodeFixed32(files_[i]->data(), num);
      return {i * slots_per_file_ + num - 1, ptr};
    }
  }
  files_.emplace_back(new tsdb::tsdbutil::MMapLinuxManager(
      dir_ + "/" + std::to_string(slot_size_) + prefix_ + "xorarray" +
          std::to_string(cur_file_idx_++),
      header_size_ + (slot_size_ + 1) * 8 * slots_per_file_ +
          4 * slots_per_file_));
  leveldb::EncodeFixed32(files_.back()->data(), 1);
  set_bitmap(files_.size() - 1, 0);
  uint8_t* ptr = encode_slot_header((uint64_t)(files_.size() - 1),
                                    (uint64_t)(0), id, slot);
  return {(files_.size() - 1) * slots_per_file_, ptr};
}

uint8_t* MMapGroupXORChunk::get_ptr(uint64_t slot) {
  return reinterpret_cast<uint8_t*>(
      files_[slot >> slots_per_file_bits_]->data() + header_size_ +
      ((slot_size_ + 1) * 8 + 4) * (slot & slots_per_file_mask_));
}

void MMapGroupXORChunk::sync(uint64_t slot) {
  size_t idx1 = slot >> slots_per_file_bits_,
         idx2 = slot & slots_per_file_mask_;
  files_[idx1]->sync(4 + idx2 / 8, 1);
  files_[idx1]->sync(header_size_ + 8 * (slot_size_ + 1) * idx2 + 4 * idx2,
                     8 * (slot_size_ + 1) + 4);
}

/**********************************************
 *                MMapIntArray                *
 **********************************************/
MMapIntArray::MMapIntArray(const std::string& dir, int slot_size, int num_slots,
                           const std::string& prefix)
    : dir_(dir),
      prefix_(prefix),
      slot_size_(slot_size),
      slots_per_file_(num_slots),
      slots_per_file_mask_(slots_per_file_ - 1),
      header_size_(4 + (slots_per_file_ + 7) / 8) {
  files_.reserve(500);

  slots_per_file_bits_ = 0;
  while (num_slots != 1) {
    num_slots >>= 1;
    ++slots_per_file_bits_;
  }

  std::vector<std::string> existing_files;
  boost::filesystem::path p(dir);
  boost::filesystem::directory_iterator end_itr;
  std::string tmp = std::to_string(slot_size) + prefix_ + "intarray";
  // cycle through the directory
  for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
    // If it's not a directory, list it. If you want to list directories too,
    // just remove this check.
    if (boost::filesystem::is_regular_file(itr->path())) {
      // assign current file name to current_file and echo it out to the
      // console.
      std::string current_file = itr->path().filename().string();
      if (current_file.size() > tmp.size() &&
          memcmp(current_file.c_str(), tmp.c_str(), tmp.size()) == 0) {
        existing_files.push_back(current_file);
      }
    }
  }
  std::sort(existing_files.begin(), existing_files.end(),
            [&](const std::string& l, const std::string& r) {
              return std::stoi(l.substr(tmp.size())) <
                     std::stoi(r.substr(tmp.size()));
            });
  cur_file_idx_ = existing_files.size();

  for (const std::string& s : existing_files) {
    files_.emplace_back(new tsdb::tsdbutil::MMapLinuxManager(dir + "/" + s));
  }
}

void MMapIntArray::remove_all(int slot_size, const std::string& dir,
                              const std::string& prefix) {
  std::vector<std::string> existing_files;
  boost::filesystem::path p(dir);
  boost::filesystem::directory_iterator end_itr;
  std::string tmp = std::to_string(slot_size) + prefix + "intarray";
  // cycle through the directory
  for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
    // If it's not a directory, list it. If you want to list directories too,
    // just remove this check.
    if (boost::filesystem::is_regular_file(itr->path())) {
      // assign current file name to current_file and echo it out to the
      // console.
      std::string current_file = itr->path().filename().string();
      if (current_file.size() > tmp.size() &&
          memcmp(current_file.c_str(), tmp.c_str(), tmp.size()) == 0) {
        existing_files.push_back(current_file);
      }
    }
  }

  for (const std::string& s : existing_files)
    boost::filesystem::remove(dir + "/" + s);
}

bool MMapIntArray::empty() { return files_.empty(); }
size_t MMapIntArray::num_files() { return files_.size(); }

void MMapIntArray::get_ids(int idx, std::vector<uint64_t>* ids,
                           std::vector<uint64_t>* slots) {
  uint32_t num = leveldb::DecodeFixed32(files_[idx]->data());
  for (uint32_t i = 0; i < num; i++) {
    slots->push_back(i);
    uint8_t* start = reinterpret_cast<uint8_t*>(
        files_[idx]->data() + header_size_ + (slot_size_ + 1) * 8 * i);
    ids->push_back(
        ((static_cast<uint64_t>(start[0]) << 56) & 0x07ffffffffffffff) |
        (static_cast<uint64_t>(start[1]) << 48) |
        (static_cast<uint64_t>(start[2]) << 40) |
        (static_cast<uint64_t>(start[3]) << 32) |
        (static_cast<uint64_t>(start[4]) << 24) |
        (static_cast<uint64_t>(start[5]) << 16) |
        (static_cast<uint64_t>(start[6]) << 8) |
        (static_cast<uint64_t>(start[7])));
  }
}

void MMapIntArray::set_bitmap(int idx1, int idx2) {
  int i1 = idx2 >> 3, i2 = idx2 & 0x7;
  files_[idx1]->data()[4 + i1] |= ((char)(1) << (7 - i2));
}

void MMapIntArray::encode_slot_header(uint64_t idx1, uint64_t idx2, int num,
                                      uint64_t id) {
  char* start =
      files_[idx1]->data() + header_size_ + (slot_size_ + 1) * 8 * idx2;
  leveldb::EncodeFixed64BE(start, id);
  *start |= (num << 3);
}

uint64_t MMapIntArray::alloc_slot(uint64_t id) {
  if (!holes_.empty()) {
    size_t idx1 = holes_.back() >> slots_per_file_bits_,
           idx2 = holes_.back() & slots_per_file_mask_;
    holes_.pop_back();
    set_bitmap(idx1, idx2);
    encode_slot_header((uint64_t)(idx1), (uint64_t)(idx2), 0, id);
    return idx1 * slots_per_file_ + idx2;
  }
  uint32_t num;
  for (size_t i = 0; i < files_.size(); i++) {
    num = leveldb::DecodeFixed32(files_[i]->data());
    if (num < slots_per_file_) {
      set_bitmap(i, num);
      encode_slot_header((uint64_t)(i), (uint64_t)(num), 0, id);
      num++;
      leveldb::EncodeFixed32(files_[i]->data(), num);
      return i * slots_per_file_ + num - 1;
    }
  }
  files_.emplace_back(new tsdb::tsdbutil::MMapLinuxManager(
      dir_ + "/" + std::to_string(slot_size_) + prefix_ + "intarray" +
          std::to_string(cur_file_idx_++),
      header_size_ + (slot_size_ + 1) * 8 * slots_per_file_));
  leveldb::EncodeFixed32(files_.back()->data(), 1);
  set_bitmap(files_.size() - 1, 0);
  encode_slot_header((uint64_t)(files_.size() - 1), (uint64_t)(0), 0, id);
  return (files_.size() - 1) * slots_per_file_;
}

// Return the slot sample number after insertion.
int MMapIntArray::write(uint64_t slot, int64_t value) {
  size_t idx1 = slot >> slots_per_file_bits_,
         idx2 = slot & slots_per_file_mask_;
  uint8_t* start = reinterpret_cast<uint8_t*>(
      files_[idx1]->data() + header_size_ + (slot_size_ + 1) * 8 * idx2);
  int num = (int)((*start) >> 3);
  leveldb::EncodeFixed64(reinterpret_cast<char*>(start + 8 * (1 + num)), value);
  num++;
  *start &= 0x07;
  *start |= (num << 3);
  return num;
}

void MMapIntArray::write(uint64_t slot, int idx, int64_t value) {
  size_t idx1 = slot >> slots_per_file_bits_,
         idx2 = slot & slots_per_file_mask_;
  uint8_t* start = reinterpret_cast<uint8_t*>(
      files_[idx1]->data() + header_size_ + (slot_size_ + 1) * 8 * idx2);
  leveldb::EncodeFixed64(reinterpret_cast<char*>(start + 8 * (1 + idx)), value);
}

int64_t MMapIntArray::read(uint64_t slot, int idx) {
  size_t idx1 = slot >> slots_per_file_bits_,
         idx2 = slot & slots_per_file_mask_;
  return leveldb::DecodeFixed64(files_[idx1]->data() + header_size_ +
                                (slot_size_ + 1) * 8 * idx2 + 8 * (1 + idx));
}

std::vector<int64_t> MMapIntArray::read(uint64_t slot) {
  size_t idx1 = slot >> slots_per_file_bits_,
         idx2 = slot & slots_per_file_mask_;
  uint8_t* start = reinterpret_cast<uint8_t*>(
      files_[idx1]->data() + header_size_ + (slot_size_ + 1) * 8 * idx2);
  int num = (int)((*start) >> 3);

  std::vector<int64_t> r;
  for (int i = 0; i < num; i++)
    r.push_back(
        leveldb::DecodeFixed64(reinterpret_cast<char*>(start + 8 * (1 + i))));
  return r;
}

void MMapIntArray::clean(uint64_t slot) {
  size_t idx1 = slot >> slots_per_file_bits_,
         idx2 = slot & slots_per_file_mask_;
  char* start =
      files_[idx1]->data() + header_size_ + (slot_size_ + 1) * 8 * idx2;
  *start &= 0x07;
}

void MMapIntArray::sync(uint64_t slot) {
  size_t idx1 = slot >> slots_per_file_bits_,
         idx2 = slot & slots_per_file_mask_;
  files_[idx1]->sync(4 + idx2 / 8, 1);
  files_[idx1]->sync(header_size_ + 8 * (slot_size_ + 1) * idx2,
                     8 * (slot_size_ + 1));
}

/**********************************************
 *                MMapFloatArray              *
 **********************************************/
MMapFloatArray::MMapFloatArray(const std::string& dir, int slot_size,
                               int num_slots)
    : dir_(dir),
      slot_size_(slot_size),
      slots_per_file_(num_slots),
      slots_per_file_mask_(num_slots - 1),
      header_size_(4 + (slots_per_file_ + 7) / 8) {
  files_.reserve(500);

  slots_per_file_bits_ = 0;
  while (num_slots != 1) {
    num_slots >>= 1;
    ++slots_per_file_bits_;
  }

  std::vector<std::string> existing_files;
  boost::filesystem::path p(dir);
  boost::filesystem::directory_iterator end_itr;
  std::string tmp = std::to_string(slot_size) + "floatarray";
  // cycle through the directory
  for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
    // If it's not a directory, list it. If you want to list directories too,
    // just remove this check.
    if (boost::filesystem::is_regular_file(itr->path())) {
      // assign current file name to current_file and echo it out to the
      // console.
      std::string current_file = itr->path().filename().string();
      if (current_file.size() > tmp.size() &&
          memcmp(current_file.c_str(), tmp.c_str(), tmp.size()) == 0) {
        existing_files.push_back(current_file);
      }
    }
  }
  std::sort(existing_files.begin(), existing_files.end(),
            [&](const std::string& l, const std::string& r) {
              return std::stoi(l.substr(tmp.size())) <
                     std::stoi(r.substr(tmp.size()));
            });
  cur_file_idx_ = existing_files.size();

  for (const std::string& s : existing_files) {
    files_.emplace_back(new tsdb::tsdbutil::MMapLinuxManager(dir + "/" + s));
  }
}

void MMapFloatArray::remove_all(int slot_size, const std::string& dir) {
  std::vector<std::string> existing_files;
  boost::filesystem::path p(dir);
  boost::filesystem::directory_iterator end_itr;
  std::string tmp = std::to_string(slot_size) + "floatarray";
  // cycle through the directory
  for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
    // If it's not a directory, list it. If you want to list directories too,
    // just remove this check.
    if (boost::filesystem::is_regular_file(itr->path())) {
      // assign current file name to current_file and echo it out to the
      // console.
      std::string current_file = itr->path().filename().string();
      if (current_file.size() > tmp.size() &&
          memcmp(current_file.c_str(), tmp.c_str(), tmp.size()) == 0) {
        existing_files.push_back(current_file);
      }
    }
  }

  for (const std::string& s : existing_files)
    boost::filesystem::remove(dir + "/" + s);
}

bool MMapFloatArray::empty() { return files_.empty(); }
size_t MMapFloatArray::num_files() { return files_.size(); }

void MMapFloatArray::get_ids(int idx, std::vector<uint64_t>* ids,
                             std::vector<uint64_t>* slots) {
  uint32_t num = leveldb::DecodeFixed32(files_[idx]->data());
  uint8_t* start =
      reinterpret_cast<uint8_t*>(files_[idx]->data() + header_size_);
  for (uint32_t i = 0; i < num; i++) {
    slots->push_back(i);
    ids->push_back(
        ((static_cast<uint64_t>(start[0]) << 56) & 0x07ffffffffffffff) |
        (static_cast<uint64_t>(start[1]) << 48) |
        (static_cast<uint64_t>(start[2]) << 40) |
        (static_cast<uint64_t>(start[3]) << 32) |
        (static_cast<uint64_t>(start[4]) << 24) |
        (static_cast<uint64_t>(start[5]) << 16) |
        (static_cast<uint64_t>(start[6]) << 8) |
        (static_cast<uint64_t>(start[7])));
    start += (slot_size_ + 1) * 8;
  }
}

void MMapFloatArray::set_bitmap(int idx1, int idx2) {
  int i1 = idx2 >> 3, i2 = idx2 & 0x7;
  files_[idx1]->data()[4 + i1] |= ((char)(1) << (7 - i2));
}

void MMapFloatArray::encode_slot_header(uint64_t idx1, uint64_t idx2, int num,
                                        uint64_t id) {
  char* start =
      files_[idx1]->data() + header_size_ + (slot_size_ + 1) * 8 * idx2;
  leveldb::EncodeFixed64BE(start, id);
  *start |= (num << 3);
}

uint64_t MMapFloatArray::alloc_slot(uint64_t id) {
  if (!holes_.empty()) {
    size_t idx1 = holes_.back() >> slots_per_file_bits_,
           idx2 = holes_.back() & slots_per_file_mask_;
    holes_.pop_back();
    set_bitmap(idx1, idx2);
    encode_slot_header((uint64_t)(idx1), (uint64_t)(idx2), 0, id);
    return idx1 * slots_per_file_ + idx2;
  }
  uint32_t num;
  for (size_t i = 0; i < files_.size(); i++) {
    num = leveldb::DecodeFixed32(files_[i]->data());
    if (num < slots_per_file_) {
      set_bitmap(i, num);
      encode_slot_header((uint64_t)(i), (uint64_t)(num), 0, id);
      num++;
      leveldb::EncodeFixed32(files_[i]->data(), num);
      return i * slots_per_file_ + num - 1;
    }
  }
  files_.emplace_back(new tsdb::tsdbutil::MMapLinuxManager(
      dir_ + "/" + std::to_string(slot_size_) + "floatarray" +
          std::to_string(cur_file_idx_++),
      header_size_ + (slot_size_ + 1) * 8 * slots_per_file_));
  leveldb::EncodeFixed32(files_.back()->data(), 1);
  set_bitmap(files_.size() - 1, 0);
  encode_slot_header((uint64_t)(files_.size() - 1), (uint64_t)(0), 0, id);
  return (files_.size() - 1) * slots_per_file_;
}

// Return the slot sample number after insertion.
int MMapFloatArray::write(uint64_t slot, double value) {
  size_t idx1 = slot >> slots_per_file_bits_,
         idx2 = slot & slots_per_file_mask_;
  uint8_t* start = reinterpret_cast<uint8_t*>(
      files_[idx1]->data() + header_size_ + (slot_size_ + 1) * 8 * idx2);
  int num = (int)((*start) >> 3);
  leveldb::EncodeFixed64(reinterpret_cast<char*>(start + 8 * (1 + num)),
                         base::encode_double(value));
  num++;
  *start &= 0x07;
  *start |= (num << 3);
  return num;
}

void MMapFloatArray::write(uint64_t slot, int idx, double value) {
  size_t idx1 = slot >> slots_per_file_bits_,
         idx2 = slot & slots_per_file_mask_;
  uint8_t* start = reinterpret_cast<uint8_t*>(
      files_[idx1]->data() + header_size_ + (slot_size_ + 1) * 8 * idx2);
  leveldb::EncodeFixed64(reinterpret_cast<char*>(start + 8 * (1 + idx)),
                         base::encode_double(value));
}

double MMapFloatArray::read(uint64_t slot, int idx) {
  size_t idx1 = slot >> slots_per_file_bits_,
         idx2 = slot & slots_per_file_mask_;
  return base::decode_double(
      leveldb::DecodeFixed64(files_[idx1]->data() + header_size_ +
                             (slot_size_ + 1) * 8 * idx2 + 8 * (1 + idx)));
}

std::vector<double> MMapFloatArray::read(uint64_t slot) {
  size_t idx1 = slot >> slots_per_file_bits_,
         idx2 = slot & slots_per_file_mask_;
  uint8_t* start = reinterpret_cast<uint8_t*>(
      files_[idx1]->data() + header_size_ + (slot_size_ + 1) * 8 * idx2);
  int num = (int)((*start) >> 3);

  std::vector<double> r;
  for (int i = 0; i < num; i++)
    r.push_back(base::decode_double(
        leveldb::DecodeFixed64(reinterpret_cast<char*>(start + 8 * (1 + i)))));
  return r;
}

void MMapFloatArray::clean(uint64_t slot) {
  size_t idx1 = slot >> slots_per_file_bits_,
         idx2 = slot & slots_per_file_mask_;
  char* start =
      files_[idx1]->data() + header_size_ + (slot_size_ + 1) * 8 * idx2;
  *start &= 0x07;
}

void MMapFloatArray::sync(uint64_t slot) {
  size_t idx1 = slot >> slots_per_file_bits_,
         idx2 = slot & slots_per_file_mask_;
  files_[idx1]->sync(4 + idx2 / 8, 1);
  files_[idx1]->sync(header_size_ + 8 * (slot_size_ + 1) * idx2,
                     8 * (slot_size_ + 1));
}

/**********************************************
 *             MMapGroupFloatArray            *
 **********************************************/
MMapGroupFloatArray::MMapGroupFloatArray(const std::string& dir, int slot_size,
                                         int num_slots)
    : dir_(dir),
      slot_size_(slot_size),
      slots_per_file_(num_slots),
      slots_per_file_mask_(num_slots - 1),
      header_size_(4 + (slots_per_file_ + 7) / 8) {
  files_.reserve(500);

  slots_per_file_bits_ = 0;
  while (num_slots != 1) {
    num_slots >>= 1;
    ++slots_per_file_bits_;
  }

  std::vector<std::string> existing_files;
  boost::filesystem::path p(dir);
  boost::filesystem::directory_iterator end_itr;
  std::string tmp = std::to_string(slot_size) + "gfloatarray";
  // cycle through the directory
  for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
    // If it's not a directory, list it. If you want to list directories too,
    // just remove this check.
    if (boost::filesystem::is_regular_file(itr->path())) {
      // assign current file name to current_file and echo it out to the
      // console.
      std::string current_file = itr->path().filename().string();
      if (current_file.size() > tmp.size() &&
          memcmp(current_file.c_str(), tmp.c_str(), tmp.size()) == 0) {
        existing_files.push_back(current_file);
      }
    }
  }
  std::sort(existing_files.begin(), existing_files.end(),
            [&](const std::string& l, const std::string& r) {
              return std::stoi(l.substr(tmp.size())) <
                     std::stoi(r.substr(tmp.size()));
            });
  cur_file_idx_ = existing_files.size();

  for (const std::string& s : existing_files) {
    files_.emplace_back(new tsdb::tsdbutil::MMapLinuxManager(dir + "/" + s));
  }
}

void MMapGroupFloatArray::remove_all(int slot_size, const std::string& dir) {
  std::vector<std::string> existing_files;
  boost::filesystem::path p(dir);
  boost::filesystem::directory_iterator end_itr;
  std::string tmp = std::to_string(slot_size) + "gfloatarray";
  // cycle through the directory
  for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
    // If it's not a directory, list it. If you want to list directories too,
    // just remove this check.
    if (boost::filesystem::is_regular_file(itr->path())) {
      // assign current file name to current_file and echo it out to the
      // console.
      std::string current_file = itr->path().filename().string();
      if (current_file.size() > tmp.size() &&
          memcmp(current_file.c_str(), tmp.c_str(), tmp.size()) == 0) {
        existing_files.push_back(current_file);
      }
    }
  }

  for (const std::string& s : existing_files)
    boost::filesystem::remove(dir + "/" + s);
}

bool MMapGroupFloatArray::empty() { return files_.empty(); }
size_t MMapGroupFloatArray::num_files() { return files_.size(); }

void MMapGroupFloatArray::get_ids(int idx, std::vector<uint64_t>* ids,
                                  std::vector<uint32_t>* indexes,
                                  std::vector<uint64_t>* slots) {
  uint32_t num = leveldb::DecodeFixed32(files_[idx]->data());
  uint8_t* start =
      reinterpret_cast<uint8_t*>(files_[idx]->data() + header_size_);
  for (uint32_t i = 0; i < num; i++) {
    slots->push_back(i);
    ids->push_back((static_cast<uint64_t>(start[0]) << 56) |
                   (static_cast<uint64_t>(start[1]) << 48) |
                   (static_cast<uint64_t>(start[2]) << 40) |
                   (static_cast<uint64_t>(start[3]) << 32) |
                   (static_cast<uint64_t>(start[4]) << 24) |
                   (static_cast<uint64_t>(start[5]) << 16) |
                   (static_cast<uint64_t>(start[6]) << 8) |
                   (static_cast<uint64_t>(start[7])));
    indexes->push_back(((static_cast<uint64_t>(start[8]) << 24) & 0x07ffffff) |
                       (static_cast<uint64_t>(start[9]) << 16) |
                       (static_cast<uint64_t>(start[10]) << 8) |
                       (static_cast<uint64_t>(start[11])));
    start += slot_size_ * 8 + 12;
  }
}

void MMapGroupFloatArray::set_bitmap(int idx1, int idx2) {
  int i1 = idx2 >> 3, i2 = idx2 & 0x7;
  files_[idx1]->data()[4 + i1] |= ((char)(1) << (7 - i2));
}

void MMapGroupFloatArray::encode_slot_header(uint64_t idx1, uint64_t idx2,
                                             int num, uint64_t id,
                                             uint32_t idx) {
  char* start =
      files_[idx1]->data() + header_size_ + ((slot_size_ + 1) * 8 + 4) * idx2;
  leveldb::EncodeFixed64BE(start, id);
  start += 8;
  leveldb::EncodeFixed32BE(start, idx);
  *start |= (num << 3);
}

uint64_t MMapGroupFloatArray::alloc_slot(uint64_t gid, uint32_t idx) {
  if (!holes_.empty()) {
    size_t idx1 = holes_.back() >> slots_per_file_bits_,
           idx2 = holes_.back() & slots_per_file_mask_;
    holes_.pop_back();
    set_bitmap(idx1, idx2);
    encode_slot_header((uint64_t)(idx1), (uint64_t)(idx2), 0, gid, idx);
    return idx1 * slots_per_file_ + idx2;
  }
  uint32_t num;
  for (size_t i = 0; i < files_.size(); i++) {
    num = leveldb::DecodeFixed32(files_[i]->data());
    if (num < slots_per_file_) {
      set_bitmap(i, num);
      encode_slot_header((uint64_t)(i), (uint64_t)(num), 0, gid, idx);
      num++;
      leveldb::EncodeFixed32(files_[i]->data(), num);
      return i * slots_per_file_ + num - 1;
    }
  }
  files_.emplace_back(new tsdb::tsdbutil::MMapLinuxManager(
      dir_ + "/" + std::to_string(slot_size_) + "gfloatarray" +
          std::to_string(cur_file_idx_++),
      header_size_ + (slot_size_ + 1) * 8 * slots_per_file_ +
          4 * slots_per_file_));
  leveldb::EncodeFixed32(files_.back()->data(), 1);
  set_bitmap(files_.size() - 1, 0);
  encode_slot_header((uint64_t)(files_.size() - 1), (uint64_t)(0), 0, gid, idx);
  return (files_.size() - 1) * slots_per_file_;
}

// Return the slot sample number after insertion.
int MMapGroupFloatArray::write(uint64_t slot, double value) {
  size_t idx1 = slot >> slots_per_file_bits_,
         idx2 = slot & slots_per_file_mask_;
  uint8_t* start = reinterpret_cast<uint8_t*>(
      files_[idx1]->data() + header_size_ + ((slot_size_ + 1) * 8 + 4) * idx2);
  int num = (int)((*(start + 8)) >> 3);
  leveldb::EncodeFixed64(reinterpret_cast<char*>(start + 8 * (1 + num) + 4),
                         base::encode_double(value));
  num++;
  start += 8;
  *start &= 0x07;
  *start |= (num << 3);
  return num;
}

void MMapGroupFloatArray::write(uint64_t slot, int idx, double value) {
  size_t idx1 = slot >> slots_per_file_bits_,
         idx2 = slot & slots_per_file_mask_;
  uint8_t* start = reinterpret_cast<uint8_t*>(
      files_[idx1]->data() + header_size_ + ((slot_size_ + 1) * 8 + 4) * idx2);
  leveldb::EncodeFixed64(reinterpret_cast<char*>(start + 8 * (1 + idx) + 4),
                         base::encode_double(value));
}

double MMapGroupFloatArray::read(uint64_t slot, int idx) {
  size_t idx1 = slot >> slots_per_file_bits_,
         idx2 = slot & slots_per_file_mask_;
  return base::decode_double(leveldb::DecodeFixed64(
      files_[idx1]->data() + header_size_ + ((slot_size_ + 1) * 8 + 4) * idx2 +
      8 * (1 + idx) + 4));
}

std::vector<double> MMapGroupFloatArray::read(uint64_t slot) {
  size_t idx1 = slot >> slots_per_file_bits_,
         idx2 = slot & slots_per_file_mask_;
  uint8_t* start = reinterpret_cast<uint8_t*>(
      files_[idx1]->data() + header_size_ + ((slot_size_ + 1) * 8 + 4) * idx2);
  start += 8;
  int num = (int)((*start) >> 3);
  start += 4;

  std::vector<double> r;
  for (int i = 0; i < num; i++)
    r.push_back(base::decode_double(
        leveldb::DecodeFixed64(reinterpret_cast<char*>(start + 8 * i))));
  return r;
}

void MMapGroupFloatArray::clean(uint64_t slot) {
  size_t idx1 = slot >> slots_per_file_bits_,
         idx2 = slot & slots_per_file_mask_;
  char* start = files_[idx1]->data() + header_size_ +
                ((slot_size_ + 1) * 8 + 4) * idx2 + 8;
  *start &= 0x07;
}

void MMapGroupFloatArray::sync(uint64_t slot) {
  size_t idx1 = slot >> slots_per_file_bits_,
         idx2 = slot & slots_per_file_mask_;
  files_[idx1]->sync(4 + idx2 / 8, 1);
  files_[idx1]->sync(header_size_ + (8 * (slot_size_ + 1) + 4) * idx2,
                     8 * (slot_size_ + 1) + 4);
}

/**********************************************
 *                  MMapLabels                *
 **********************************************/
MMapLabels::MMapLabels(const std::string& dir)
    : dir_(dir),
      slots_per_file_(MAX_HEAD_LABELS_FILE_SIZE / AVG_HEAD_LABELS_SIZE),
      header_size_(16 + slots_per_file_ * 10),
      cur_slot_(0) {
  files_.reserve(500);
  std::vector<std::string> existing_files;
  boost::filesystem::path p(dir);
  boost::filesystem::directory_iterator end_itr;
  // cycle through the directory
  for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
    // If it's not a directory, list it. If you want to list directories too,
    // just remove this check.
    if (boost::filesystem::is_regular_file(itr->path())) {
      // assign current file name to current_file and echo it out to the
      // console.
      std::string current_file = itr->path().filename().string();
      if (current_file.size() > 6 &&
          memcmp(current_file.c_str(), "labels", 6) == 0) {
        existing_files.push_back(current_file);
      }
    }
  }
  std::sort(existing_files.begin(), existing_files.end(),
            [&](const std::string& l, const std::string& r) {
              return std::stoi(l.substr(6)) < std::stoi(r.substr(6));
            });
  cur_file_idx_ = existing_files.size();

  for (const std::string& s : existing_files) {
    files_.emplace_back(new tsdb::tsdbutil::MMapLinuxManager(dir + "/" + s));
    file_slots.push_back(leveldb::DecodeFixed32(files_.back()->data() + 8));
    cur_slot_ += file_slots.back();
  }
}

void MMapLabels::remove_all(const std::string& dir) {
  std::vector<std::string> existing_files;
  boost::filesystem::path p(dir);
  boost::filesystem::directory_iterator end_itr;
  // cycle through the directory
  for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
    // If it's not a directory, list it. If you want to list directories too,
    // just remove this check.
    if (boost::filesystem::is_regular_file(itr->path())) {
      // assign current file name to current_file and echo it out to the
      // console.
      std::string current_file = itr->path().filename().string();
      if (current_file.size() > 6 &&
          memcmp(current_file.c_str(), "labels", 6) == 0) {
        existing_files.push_back(current_file);
      }
    }
  }

  for (const std::string& s : existing_files)
    boost::filesystem::remove(dir + "/" + s);
}

bool MMapLabels::empty() { return files_.empty(); }
size_t MMapLabels::num_files() { return files_.size(); }

uint64_t MMapLabels::add_labels(uint64_t id, const label::Labels& lset) {
  // std::cout << id << " " << label::lbs_string(lset) << std::endl;
  uint64_t offset = 0, cur_size = header_size_, cur_idx = 0;
  if (files_.empty()) {
    files_.emplace_back(new tsdb::tsdbutil::MMapLinuxManager(
        dir_ + "/labels" + std::to_string(cur_file_idx_++),
        MAX_HEAD_LABELS_FILE_SIZE));
    file_slots.push_back(0);
    leveldb::EncodeFixed64(files_.back()->data(),
                           header_size_);                   // current size
    leveldb::EncodeFixed64(files_.back()->data() + 8, 1);   // # slot
    leveldb::EncodeFixed64(files_.back()->data() + 16, 0);  // offset
  } else {
    size_t estimated_lset_size = 15;
    for (const label::Label& l : lset)
      estimated_lset_size += 10 + l.label.size() + l.value.size();
    cur_size = leveldb::DecodeFixed64(files_.back()->data());
    cur_idx = leveldb::DecodeFixed64(files_.back()->data() + 8);
    if (MAX_HEAD_LABELS_FILE_SIZE - cur_size < estimated_lset_size ||
        cur_idx >= slots_per_file_) {
      // New file.
      files_.emplace_back(new tsdb::tsdbutil::MMapLinuxManager(
          dir_ + "/labels" + std::to_string(cur_file_idx_++),
          MAX_HEAD_LABELS_FILE_SIZE));
      file_slots.push_back(0);
      leveldb::EncodeFixed64(files_.back()->data(),
                             header_size_);                   // current size
      leveldb::EncodeFixed64(files_.back()->data() + 8, 1);   // # slot
      leveldb::EncodeFixed64(files_.back()->data() + 16, 0);  // offset
      cur_size = header_size_;
      cur_idx = 0;
    } else {
      offset = leveldb::DecodeFixed64(files_.back()->data() + 16 +
                                      10 * (cur_idx - 1)) +
               leveldb::DecodeFixed16(files_.back()->data() + 16 +
                                      10 * (cur_idx - 1) + 8);
    }
  }

  // Write labels offset.
  leveldb::EncodeFixed64(files_.back()->data() + 16 + 10 * cur_idx, offset);

  char* ptr = files_.back()->data() + header_size_ + offset;
  char* start = ptr;
  ptr = leveldb::EncodeVarint64(ptr, id);
  ptr = leveldb::EncodeVarint32(ptr, lset.size());
  for (size_t i = 0; i < lset.size(); i++) {
    ptr = leveldb::EncodeVarint32(ptr, lset[i].label.size());
    for (size_t j = 0; j < lset[i].label.size(); j++)
      *(ptr++) = lset[i].label[j];
    ptr = leveldb::EncodeVarint32(ptr, lset[i].value.size());
    for (size_t j = 0; j < lset[i].value.size(); j++)
      *(ptr++) = lset[i].value[j];
  }

  // Write labels size.
  uint16_t lset_size = (uint16_t)((uintptr_t)(ptr) - (uintptr_t)(start));
  leveldb::EncodeFixed16(files_.back()->data() + 16 + 10 * cur_idx + 8,
                         lset_size);

  // Update meta.
  leveldb::EncodeFixed64(files_.back()->data(), cur_size + lset_size);
  leveldb::EncodeFixed64(files_.back()->data() + 8, cur_idx + 1);

  file_slots.back()++;
  return cur_slot_++;
}

uint64_t MMapLabels::get_labels(uint64_t idx1, uint64_t idx2,
                                label::Labels& lset) {
  uint64_t offset =
      leveldb::DecodeFixed64(files_[idx1]->data() + 16 + 10 * idx2);

  uint64_t tsid;
  const char* ptr = files_[idx1]->data() + header_size_ + offset;
  ptr = leveldb::GetVarint64Ptr(ptr, ptr + 10, &tsid);
  uint32_t num_labels;
  uint32_t label_size;
  ptr = leveldb::GetVarint32Ptr(ptr, ptr + 5, &num_labels);
  for (int i = 0; i < num_labels; i++) {
    lset.emplace_back();
    ptr = leveldb::GetVarint32Ptr(ptr, ptr + 5, &label_size);
    for (int j = 0; j < label_size; j++) lset.back().label.push_back(*(ptr++));

    ptr = leveldb::GetVarint32Ptr(ptr, ptr + 5, &label_size);
    for (int j = 0; j < label_size; j++) lset.back().value.push_back(*(ptr++));
  }
  return tsid;
}

void MMapLabels::get_labels(uint64_t idx, label::Labels& lset) {
  if (idx >= cur_slot_) return;

  // TODO(Alec), can speed up this search.
  int i = 0, slots = 0;
  while (i < file_slots.size() && idx >= slots + file_slots[i]) {
    slots += file_slots[i];
    i++;
  }

  get_labels(i, idx - slots, lset);
}

MMapLabelsIterator MMapLabels::iterator() { return MMapLabelsIterator(this); };

MMapLabelsIterator::MMapLabelsIterator(MMapLabels* m)
    : cur_file_(0), idx_inside_file_(-1), total_idx_(-1), mmap_labels_(m) {}

bool MMapLabelsIterator::next() {
  if (mmap_labels_->files_.empty() ||
      (cur_file_ == mmap_labels_->files_.size() - 1 &&
       idx_inside_file_ == mmap_labels_->file_slots.back() - 1))
    return false;

  idx_inside_file_++;
  total_idx_++;
  if (idx_inside_file_ == mmap_labels_->file_slots[cur_file_]) {
    idx_inside_file_ = 0;
    cur_file_++;
  }
  return true;
}

void MMapLabelsIterator::at(uint64_t* id, uint64_t* idx) {
  uint64_t offset = leveldb::DecodeFixed64(
      mmap_labels_->files_[cur_file_]->data() + 16 + 10 * idx_inside_file_);
  const char* ptr = mmap_labels_->files_[cur_file_]->data() +
                    mmap_labels_->header_size_ + offset;
  leveldb::GetVarint64Ptr(ptr, ptr + 10, id);
  *idx = total_idx_;
}

void MMapLabelsIterator::at(uint64_t* id, uint64_t* idx, label::Labels& lset) {
  *id = mmap_labels_->get_labels(cur_file_, idx_inside_file_, lset);
  *idx = total_idx_;
}

/**********************************************
 *                MMapGroupLabels             *
 **********************************************/
MMapGroupLabels::MMapGroupLabels(const std::string& dir)
    : dir_(dir),
      slots_per_file_(MAX_HEAD_LABELS_FILE_SIZE / AVG_HEAD_LABELS_SIZE),
      header_size_(16 + slots_per_file_ * 10),
      cur_slot_(0) {
  files_.reserve(500);
  std::vector<std::string> existing_files;
  boost::filesystem::path p(dir);
  boost::filesystem::directory_iterator end_itr;
  // cycle through the directory
  for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
    // If it's not a directory, list it. If you want to list directories too,
    // just remove this check.
    if (boost::filesystem::is_regular_file(itr->path())) {
      // assign current file name to current_file and echo it out to the
      // console.
      std::string current_file = itr->path().filename().string();
      if (current_file.size() > 7 &&
          memcmp(current_file.c_str(), "glabels", 7) == 0) {
        existing_files.push_back(current_file);
      }
    }
  }
  std::sort(existing_files.begin(), existing_files.end(),
            [&](const std::string& l, const std::string& r) {
              return std::stoi(l.substr(7)) < std::stoi(r.substr(7));
            });
  cur_file_idx_ = existing_files.size();

  for (const std::string& s : existing_files) {
    files_.emplace_back(new tsdb::tsdbutil::MMapLinuxManager(dir + "/" + s));
    file_slots.push_back(leveldb::DecodeFixed32(files_.back()->data() + 8));
    cur_slot_ += file_slots.back();
  }
}

void MMapGroupLabels::remove_all(const std::string& dir) {
  std::vector<std::string> existing_files;
  boost::filesystem::path p(dir);
  boost::filesystem::directory_iterator end_itr;
  // cycle through the directory
  for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
    // If it's not a directory, list it. If you want to list directories too,
    // just remove this check.
    if (boost::filesystem::is_regular_file(itr->path())) {
      // assign current file name to current_file and echo it out to the
      // console.
      std::string current_file = itr->path().filename().string();
      if (current_file.size() > 7 &&
          memcmp(current_file.c_str(), "glabels", 7) == 0) {
        existing_files.push_back(current_file);
      }
    }
  }

  for (const std::string& s : existing_files)
    boost::filesystem::remove(dir + "/" + s);
}

bool MMapGroupLabels::empty() { return files_.empty(); }
size_t MMapGroupLabels::num_files() { return files_.size(); }

uint64_t MMapGroupLabels::add_labels(uint64_t id, uint32_t idx,
                                     const label::Labels& lset) {
  // Reset the left most bit to save space.
  id &= 0x7fffffffffffffff;

  uint64_t offset = 0, cur_size = header_size_, cur_idx = 0;
  if (files_.empty()) {
    files_.emplace_back(new tsdb::tsdbutil::MMapLinuxManager(
        dir_ + "/glabels" + std::to_string(cur_file_idx_++),
        MAX_HEAD_LABELS_FILE_SIZE));
    file_slots.push_back(0);
    leveldb::EncodeFixed64(files_.back()->data(),
                           header_size_);                   // current size
    leveldb::EncodeFixed64(files_.back()->data() + 8, 1);   // # slot
    leveldb::EncodeFixed64(files_.back()->data() + 16, 0);  // offset
  } else {
    size_t estimated_lset_size = 20;
    for (const label::Label& l : lset)
      estimated_lset_size += 10 + l.label.size() + l.value.size();
    cur_size = leveldb::DecodeFixed64(files_.back()->data());
    cur_idx = leveldb::DecodeFixed64(files_.back()->data() + 8);
    if (MAX_HEAD_LABELS_FILE_SIZE - cur_size < estimated_lset_size ||
        cur_idx >= slots_per_file_) {
      // New file.
      files_.emplace_back(new tsdb::tsdbutil::MMapLinuxManager(
          dir_ + "/glabels" + std::to_string(cur_file_idx_++),
          MAX_HEAD_LABELS_FILE_SIZE));
      file_slots.push_back(0);
      leveldb::EncodeFixed64(files_.back()->data(),
                             header_size_);                   // current size
      leveldb::EncodeFixed64(files_.back()->data() + 8, 1);   // # slot
      leveldb::EncodeFixed64(files_.back()->data() + 16, 0);  // offset
      cur_size = header_size_;
      cur_idx = 0;
    } else {
      offset = leveldb::DecodeFixed64(files_.back()->data() + 16 +
                                      10 * (cur_idx - 1)) +
               leveldb::DecodeFixed16(files_.back()->data() + 16 +
                                      10 * (cur_idx - 1) + 8);
    }
  }

  // Write labels offset.
  leveldb::EncodeFixed64(files_.back()->data() + 16 + 10 * cur_idx, offset);

  char* ptr = files_.back()->data() + header_size_ + offset;
  ptr = leveldb::EncodeVarint64(ptr, id);
  ptr = leveldb::EncodeVarint32(ptr, idx);
  ptr = leveldb::EncodeVarint32(ptr, lset.size());
  for (size_t i = 0; i < lset.size(); i++) {
    ptr = leveldb::EncodeVarint32(ptr, lset[i].label.size());
    for (size_t j = 0; j < lset[i].label.size(); j++)
      *(ptr++) = lset[i].label[j];
    ptr = leveldb::EncodeVarint32(ptr, lset[i].value.size());
    for (size_t j = 0; j < lset[i].value.size(); j++)
      *(ptr++) = lset[i].value[j];
  }

  // Write labels size.
  uint16_t lset_size =
      (uint16_t)(ptr - files_.back()->data() - header_size_ - offset);
  leveldb::EncodeFixed16(files_.back()->data() + 16 + 10 * cur_idx + 8,
                         lset_size);

  // Update meta.
  leveldb::EncodeFixed64(files_.back()->data(), cur_size + lset_size);
  leveldb::EncodeFixed64(files_.back()->data() + 8, cur_idx + 1);

  file_slots.back()++;
  return cur_slot_++;
}

uint64_t MMapGroupLabels::get_labels(uint64_t idx1, uint64_t idx2,
                                     label::Labels& lset, uint32_t* idx) {
  uint64_t offset =
      leveldb::DecodeFixed64(files_[idx1]->data() + 16 + 10 * idx2);

  uint64_t tsid;
  const char* ptr = files_[idx1]->data() + header_size_ + offset;
  ptr = leveldb::GetVarint64Ptr(ptr, ptr + 10, &tsid);
  ptr = leveldb::GetVarint32Ptr(ptr, ptr + 5, idx);
  uint32_t num_labels;
  uint32_t label_size;
  ptr = leveldb::GetVarint32Ptr(ptr, ptr + 5, &num_labels);
  for (int i = 0; i < num_labels; i++) {
    lset.emplace_back();
    ptr = leveldb::GetVarint32Ptr(ptr, ptr + 5, &label_size);
    for (int j = 0; j < label_size; j++) lset.back().label.push_back(*(ptr++));

    ptr = leveldb::GetVarint32Ptr(ptr, ptr + 5, &label_size);
    for (int j = 0; j < label_size; j++) lset.back().value.push_back(*(ptr++));
  }
  return tsid | 0x8000000000000000;
}

void MMapGroupLabels::get_labels(uint64_t idx, label::Labels& lset,
                                 uint32_t* index) {
  if (idx >= cur_slot_) return;

  // TODO(Alec), can speed up this search.
  int i = 0, slots = 0;
  while (i < file_slots.size() && idx >= slots + file_slots[i]) {
    slots += file_slots[i];
    i++;
  }

  get_labels(i, idx - slots, lset, index);
}

MMapGroupLabelsIterator MMapGroupLabels::iterator() {
  return MMapGroupLabelsIterator(this);
};

MMapGroupLabelsIterator::MMapGroupLabelsIterator(MMapGroupLabels* m)
    : cur_file_(0), idx_inside_file_(-1), total_idx_(-1), mmap_labels_(m) {}

bool MMapGroupLabelsIterator::next() {
  if (mmap_labels_->files_.empty() ||
      (cur_file_ == mmap_labels_->files_.size() - 1 &&
       idx_inside_file_ == mmap_labels_->file_slots.back() - 1))
    return false;

  idx_inside_file_++;
  total_idx_++;
  if (idx_inside_file_ == mmap_labels_->file_slots[cur_file_]) {
    idx_inside_file_ = 0;
    cur_file_++;
  }
  return true;
}

void MMapGroupLabelsIterator::at(uint64_t* id, uint32_t* idx_inside_group,
                                 uint64_t* idx) {
  uint64_t offset = leveldb::DecodeFixed64(
      mmap_labels_->files_[cur_file_]->data() + 16 + 10 * idx_inside_file_);
  const char* ptr = mmap_labels_->files_[cur_file_]->data() +
                    mmap_labels_->header_size_ + offset;
  ptr = leveldb::GetVarint64Ptr(ptr, ptr + 10, id);
  leveldb::GetVarint32Ptr(ptr, ptr + 5, idx_inside_group);
  *idx = total_idx_;
}

void MMapGroupLabelsIterator::at(uint64_t* id, uint32_t* idx_inside_group,
                                 uint64_t* idx, label::Labels& lset) {
  *id = mmap_labels_->get_labels(cur_file_, idx_inside_file_, lset,
                                 idx_inside_group);
  *idx = total_idx_;
}

}  // namespace head
}  // namespace tsdb