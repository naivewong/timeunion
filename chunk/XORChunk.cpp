#include "chunk/XORChunk.hpp"

#include <iostream>

#include "base/Endian.hpp"
#include "base/TSDBException.hpp"
#include "chunk/EmptyAppender.hpp"
#include "chunk/EmptyIterator.hpp"
#include "chunk/XORAppender.hpp"
#include "chunk/XORIterator.hpp"
#include "leveldb/util/coding.h"

namespace tsdb {
namespace chunk {

/**********************************************
 *                XORChunk                    *
 **********************************************/
// The first two bytes store the num of samples using big endian
XORChunk::XORChunk() : bstream(2), read_mode(false) {}

XORChunk::XORChunk(const uint8_t* stream_ptr, uint64_t size)
    : bstream(stream_ptr, size), read_mode(true), size_(size) {}

const uint8_t* XORChunk::bytes() {
  if (read_mode)
    return bstream.stream_ptr;
  else {
    // std::cout << bstream.stream.size() << std::endl;
    return bstream.bytes_ptr();
  }
}

uint8_t XORChunk::encoding() { return static_cast<uint8_t>(EncXOR); }

std::unique_ptr<ChunkAppenderInterface> XORChunk::appender() {
  if (read_mode)
    return std::unique_ptr<ChunkAppenderInterface>(new EmptyAppender());
  std::unique_ptr<XORIterator> it = xor_iterator();
  while (it->next()) {
  }
  if (it->error()) {
    // LOG_ERROR << "Broken BitStream in XORChunk";
    throw base::TSDBException("Broken BitStream in XORChunk");
  }

  uint8_t lz = base::get_uint16_big_endian(*bstream.bytes()) == 0
                   ? 0xff
                   : it->leading_zero;
  return std::unique_ptr<ChunkAppenderInterface>(
      new XORAppender(bstream, it->timestamp, it->value, it->delta_timestamp,
                      lz, it->trailing_zero));
}

std::unique_ptr<ChunkIteratorInterface> XORChunk::iterator() {
  return std::unique_ptr<ChunkIteratorInterface>(
      new XORIterator(bstream, !read_mode));
}

std::unique_ptr<XORIterator> XORChunk::xor_iterator() {
  // No need to use safe mode here because there can be only one appender at the
  // same time.
  return std::unique_ptr<XORIterator>(new XORIterator(bstream, false));
}

std::unique_ptr<XORIterator> XORChunk::xor_iterator(int num_samples) {
  return std::unique_ptr<XORIterator>(
      new XORIterator(bstream, false, num_samples));
}

int XORChunk::num_samples() { return base::get_uint16_big_endian(bytes()); }

uint64_t XORChunk::size() {
  if (read_mode)
    return size_;
  else
    return bstream.bytes()->size();
}

/**********************************************
 *                XORChunkV2                  *
 **********************************************/
// The first two bytes store the num of samples using big endian
XORChunkV2::XORChunkV2(uint8_t* stream_ptr) : bstream(stream_ptr, 2) {}

const uint8_t* XORChunkV2::bytes() { return bstream.bytes_ptr(); }

uint8_t XORChunkV2::encoding() { return static_cast<uint8_t>(EncXOR); }

std::unique_ptr<ChunkAppenderInterface> XORChunkV2::appender() {
  return std::unique_ptr<ChunkAppenderInterface>(
      new XORAppenderV2(bstream, 0, 0, 0, 0xff, 0));
}

std::unique_ptr<ChunkIteratorInterface> XORChunkV2::iterator() {
  return std::unique_ptr<ChunkIteratorInterface>(new XORIteratorV2(bstream));
}

std::unique_ptr<XORIteratorV2> XORChunkV2::xor_iterator() {
  // No need to use safe mode here because there can be only one appender at the
  // same time.
  return std::unique_ptr<XORIteratorV2>(new XORIteratorV2(bstream));
}

std::unique_ptr<XORIteratorV2> XORChunkV2::xor_iterator(int num_samples) {
  return std::unique_ptr<XORIteratorV2>(
      new XORIteratorV2(bstream, num_samples));
}

int XORChunkV2::num_samples() { return base::get_uint16_big_endian(bytes()); }

uint64_t XORChunkV2::size() { return bstream.size(); }

/**********************************************
 *           NullSupportedXORChunk            *
 **********************************************/
// The first two bytes store the num of samples using big endian
NullSupportedXORChunk::NullSupportedXORChunk() : bstream(2), read_mode(false) {}

NullSupportedXORChunk::NullSupportedXORChunk(const uint8_t* stream_ptr,
                                             uint64_t size)
    : bstream(stream_ptr, size), read_mode(true), size_(size) {}

const uint8_t* NullSupportedXORChunk::bytes() {
  if (read_mode)
    return bstream.stream_ptr;
  else {
    // std::cout << bstream.stream.size() << std::endl;
    return bstream.bytes_ptr();
  }
}

uint8_t NullSupportedXORChunk::encoding() {
  return static_cast<uint8_t>(EncXOR);
}

std::unique_ptr<ChunkAppenderInterface> NullSupportedXORChunk::appender() {
  if (read_mode)
    return std::unique_ptr<ChunkAppenderInterface>(new EmptyAppender());
  std::unique_ptr<NullSupportedXORIterator> it = xor_iterator();
  while (it->next()) {
  }
  if (it->error()) {
    // LOG_ERROR << "Broken BitStream in NullSupportedXORChunk";
    throw base::TSDBException("Broken BitStream in NullSupportedXORChunk");
  }

  uint8_t lz = base::get_uint16_big_endian(*bstream.bytes()) == 0
                   ? 0xff
                   : it->leading_zero;
  return std::unique_ptr<ChunkAppenderInterface>(
      new NullSupportedXORAppender(bstream, it->timestamp, it->value,
                                   it->delta_timestamp, lz, it->trailing_zero));
}

std::unique_ptr<ChunkIteratorInterface> NullSupportedXORChunk::iterator() {
  return std::unique_ptr<ChunkIteratorInterface>(
      new NullSupportedXORIterator(bstream, !read_mode));
}

std::unique_ptr<NullSupportedXORIterator>
NullSupportedXORChunk::xor_iterator() {
  // No need to use safe mode here because there can be only one appender at the
  // same time.
  return std::unique_ptr<NullSupportedXORIterator>(
      new NullSupportedXORIterator(bstream, false));
}

std::unique_ptr<NullSupportedXORIterator> NullSupportedXORChunk::xor_iterator(
    int num_samples) {
  return std::unique_ptr<NullSupportedXORIterator>(
      new NullSupportedXORIterator(bstream, false, num_samples));
}

int NullSupportedXORChunk::num_samples() {
  return base::get_uint16_big_endian(bytes());
}

uint64_t NullSupportedXORChunk::size() {
  if (read_mode)
    return size_;
  else
    return bstream.bytes()->size();
}

/**********************************************
 *        NullSupportedXORGroupChunk          *
 **********************************************/
NullSupportedXORGroupChunk::NullSupportedXORGroupChunk(
    const std::vector<int>& slots)
    : t_bstream_(2),
      v_bstreams_(slots.size()),
      read_mode_(false),
      slots_(slots) {}

NullSupportedXORGroupChunk::NullSupportedXORGroupChunk(
    const uint8_t* stream_ptr, uint64_t size)
    : read_mode_(true), size_(size), ptr_(stream_ptr) {
  uint32_t num_slots =
      leveldb::DecodeFixed32(reinterpret_cast<const char*>(ptr_));
  ptr_ += 4;
  for (uint32_t i = 0; i < num_slots; i++) {
    slots_.push_back(
        leveldb::DecodeFixed32(reinterpret_cast<const char*>(ptr_)));
    ptr_ += 4;
    offs_.push_back(
        leveldb::DecodeFixed32(reinterpret_cast<const char*>(ptr_)));
    ptr_ += 4;
  }
}

inline int NullSupportedXORGroupChunk::find_index_of_slot(int slot) {
  int l = 0, r = slots_.size() - 1;
  while (l <= r) {
    int m = l + (r - l) / 2;
    if (slots_[m] == slot) return m;
    if (slots_[m] < slot)
      l = m + 1;
    else
      r = m - 1;
  }
  return -1;
}

std::unique_ptr<NullSupportedXORGroupAppender>
NullSupportedXORGroupChunk::appender() {
  if (read_mode_) return nullptr;

  return std::unique_ptr<NullSupportedXORGroupAppender>(
      new NullSupportedXORGroupAppender(&t_bstream_, &v_bstreams_));
}

std::unique_ptr<ChunkIteratorInterface> NullSupportedXORGroupChunk::iterator(
    int slot) {
  int idx = find_index_of_slot(slot);
  if (idx == -1)
    return std::unique_ptr<ChunkIteratorInterface>(new EmptyIterator());

  if (read_mode_)
    return std::unique_ptr<ChunkIteratorInterface>(
        new NullSupportedXORGroupIterator(ptr_, offs_[idx], size_));
  return std::unique_ptr<ChunkIteratorInterface>(
      new NullSupportedXORGroupIterator(t_bstream_, v_bstreams_[idx]));
}

void NullSupportedXORGroupChunk::encode(std::string* s) {
  size_t start_off = s->size();
  leveldb::PutFixed32(s, slots_.size());
  for (size_t i = 0; i < slots_.size(); i++) {
    leveldb::PutFixed32(s, slots_[i]);
    s->append("\0\0\0\0", 4);
  }
  s->append(reinterpret_cast<const char*>(t_bstream_.bytes_ptr()),
            t_bstream_.size());
  uint32_t off = t_bstream_.size();
  for (size_t i = 0; i < v_bstreams_.size(); i++) {
    leveldb::EncodeFixed32(&((*s)[start_off + 8 + i * 8]), off);
    s->append(reinterpret_cast<const char*>(v_bstreams_[i].bytes_ptr()),
              v_bstreams_[i].size());
    off += v_bstreams_[i].size();
  }
}

void NullSupportedXORGroupChunk::encode(
    std::string* s, const std::vector<int>& slots,
    const std::vector<int64_t>& timestamps,
    const std::vector<std::vector<double>>& values) {
  size_t start_off = s->size();
  leveldb::PutFixed32(s, slots.size());
  for (size_t i = 0; i < slots.size(); i++) {
    leveldb::PutFixed32(s, slots[i]);
    s->append("\0\0\0\0", 4);
  }

  BitStream t_bstream(2);
  NullSupportedXORGroupTimeAppender ta(&t_bstream);
  for (int64_t t : timestamps) ta.append(t);
  s->append(reinterpret_cast<const char*>(t_bstream.bytes_ptr()),
            t_bstream.size());

  uint32_t off = t_bstream.size();
  for (size_t i = 0; i < values.size(); i++) {
    BitStream v_bstream;
    NullSupportedXORGroupValueAppender va(&v_bstream);
    for (double v : values[i]) va.append(v);
    leveldb::EncodeFixed32(&((*s)[start_off + 8 + i * 8]), off);
    s->append(reinterpret_cast<const char*>(v_bstream.bytes_ptr()),
              v_bstream.size());
    off += v_bstream.size();
  }
}

void NullSupportedXORGroupChunk::decode(
    std::vector<int>* slots, std::vector<int64_t>* timestamps,
    std::vector<std::vector<double>>* values) {
  slots->insert(slots->end(), slots_.begin(), slots_.end());
  NullSupportedXORGroupTimeIterator tit(ptr_, size_);
  while (tit.next()) timestamps->push_back(tit.at());

  for (size_t i = 0; i < slots->size(); i++) {
    NullSupportedXORGroupValueIterator vit(ptr_ + offs_[i], size_,
                                           base::get_uint16_big_endian(ptr_));
    values->emplace_back();
    for (size_t j = 0; j < timestamps->size(); j++) {
      vit.next();
      values->back().push_back(vit.at());
    }
  }
}

void NullSupportedXORGroupChunk::decode(int slot,
                                        std::vector<int64_t>* timestamps,
                                        std::vector<double>* values,
                                        bool filter) {
  int idx = find_index_of_slot(slot);
  if (idx == -1) return;
  NullSupportedXORGroupTimeIterator tit(ptr_, size_);
  NullSupportedXORGroupValueIterator vit(ptr_ + offs_[idx], size_,
                                         base::get_uint16_big_endian(ptr_));

  while (tit.next()) {
    vit.next();
    if (!filter || tit.at() != std::numeric_limits<double>::max()) {
      if (timestamps) timestamps->push_back(tit.at());
      values->push_back(vit.at());
    }
  }
}

int NullSupportedXORGroupChunk::num_samples() {
  return base::get_uint16_big_endian(t_bstream_.bytes_ptr());
}

uint64_t NullSupportedXORGroupChunk::size() { return size_; }

/**********************************************
 *        NullSupportedXORGroupChunkV2        *
 **********************************************/
NullSupportedXORGroupChunkV2::NullSupportedXORGroupChunkV2(
    const std::vector<int>& slots, uint8_t* time_ptr,
    const std::vector<uint8_t*>& val_ptrs)
    : t_bstream_(time_ptr, 2), slots_(slots) {
  v_bstreams_.reserve(slots.size());
  for (size_t i = 0; i < slots.size(); i++)
    v_bstreams_.emplace_back(val_ptrs[i]);
}

inline int NullSupportedXORGroupChunkV2::find_index_of_slot(int slot) {
  int l = 0, r = slots_.size() - 1;
  while (l <= r) {
    int m = l + (r - l) / 2;
    if (slots_[m] == slot) return m;
    if (slots_[m] < slot)
      l = m + 1;
    else
      r = m - 1;
  }
  return -1;
}

std::unique_ptr<NullSupportedXORGroupAppenderV2>
NullSupportedXORGroupChunkV2::appender() {
  return std::unique_ptr<NullSupportedXORGroupAppenderV2>(
      new NullSupportedXORGroupAppenderV2(&t_bstream_, &v_bstreams_));
}

std::unique_ptr<ChunkIteratorInterface> NullSupportedXORGroupChunkV2::iterator(
    int slot) {
  int idx = find_index_of_slot(slot);
  if (idx == -1)
    return std::unique_ptr<ChunkIteratorInterface>(new EmptyIterator());

  return std::unique_ptr<ChunkIteratorInterface>(
      new NullSupportedXORGroupIteratorV2(t_bstream_, v_bstreams_[idx]));
}

void NullSupportedXORGroupChunkV2::encode(std::string* s) {
  size_t start_off = s->size();
  leveldb::PutFixed32(s, slots_.size());
  for (size_t i = 0; i < slots_.size(); i++) {
    leveldb::PutFixed32(s, slots_[i]);
    s->append("\0\0\0\0", 4);
  }
  s->append(reinterpret_cast<const char*>(t_bstream_.bytes_ptr()),
            t_bstream_.size());
  uint32_t off = t_bstream_.size();
  for (size_t i = 0; i < v_bstreams_.size(); i++) {
    leveldb::EncodeFixed32(&((*s)[start_off + 8 + i * 8]), off);
    s->append(reinterpret_cast<const char*>(v_bstreams_[i].bytes_ptr()),
              v_bstreams_[i].size());
    off += v_bstreams_[i].size();
  }
}

void NullSupportedXORGroupChunkV2::encode(
    std::string* s, const std::vector<int>& slots, BitStreamV2* tb,
    const std::vector<BitStreamV2*>& vbs) {
  size_t start_off = s->size();
  leveldb::PutFixed32(s, slots.size());
  for (size_t i = 0; i < slots.size(); i++) {
    leveldb::PutFixed32(s, slots[i]);
    s->append("\0\0\0\0", 4);
  }
  s->append(reinterpret_cast<const char*>(tb->bytes_ptr()), tb->size());
  uint32_t off = tb->size();
  for (size_t i = 0; i < vbs.size(); i++) {
    leveldb::EncodeFixed32(&((*s)[start_off + 8 + i * 8]), off);
    s->append(reinterpret_cast<const char*>(vbs[i]->bytes_ptr()),
              vbs[i]->size());
    off += vbs[i]->size();
  }
}

void NullSupportedXORGroupChunkV2::encode(
    std::string* s, const std::vector<int>& slots,
    const std::vector<int64_t>& timestamps,
    const std::vector<std::vector<double>>& values) {
  size_t start_off = s->size();
  leveldb::PutFixed32(s, slots.size());
  for (size_t i = 0; i < slots.size(); i++) {
    leveldb::PutFixed32(s, slots[i]);
    s->append("\0\0\0\0", 4);
  }

  BitStream t_bstream(2);
  NullSupportedXORGroupTimeAppender ta(&t_bstream);
  for (int64_t t : timestamps) ta.append(t);
  s->append(reinterpret_cast<const char*>(t_bstream.bytes_ptr()),
            t_bstream.size());

  uint32_t off = t_bstream.size();
  for (size_t i = 0; i < values.size(); i++) {
    BitStream v_bstream;
    NullSupportedXORGroupValueAppender va(&v_bstream);
    for (double v : values[i]) va.append(v);
    leveldb::EncodeFixed32(&((*s)[start_off + 8 + i * 8]), off);
    s->append(reinterpret_cast<const char*>(v_bstream.bytes_ptr()),
              v_bstream.size());
    off += v_bstream.size();
  }
}

int NullSupportedXORGroupChunkV2::num_samples() {
  return base::get_uint16_big_endian(t_bstream_.bytes_ptr());
}

uint64_t NullSupportedXORGroupChunkV2::size() {
  size_t s = t_bstream_.size();
  for (size_t i = 0; i < v_bstreams_.size(); i++) s += v_bstreams_[i].size();
  return s;
}

}  // namespace chunk
}  // namespace tsdb