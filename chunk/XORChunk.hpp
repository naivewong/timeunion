#ifndef XORCHUNK_H
#define XORCHUNK_H

#include "chunk/BitStream.hpp"
#include "chunk/ChunkAppenderInterface.hpp"
#include "chunk/ChunkInterface.hpp"
#include "chunk/ChunkIteratorInterface.hpp"

namespace tsdb {
namespace chunk {

class XORIterator;
class XORIteratorV2;
class NullSupportedXORGroupAppender;
class NullSupportedXORGroupAppenderV2;
class NullSupportedXORIterator;
class NullSupportedXORIteratorV2;

class XORChunk : public ChunkInterface {
 private:
  BitStream bstream;
  bool read_mode;
  uint64_t size_;

 public:
  // The first two bytes store the num of samples using big endian
  XORChunk();

  XORChunk(const uint8_t* stream_ptr, uint64_t size);

  const uint8_t* bytes();

  const std::vector<uint8_t>* bytes_vector() { return &bstream.stream; }

  uint8_t encoding();

  std::unique_ptr<ChunkAppenderInterface> appender();

  std::unique_ptr<ChunkIteratorInterface> iterator();

  std::unique_ptr<XORIterator> xor_iterator();
  // Headless chunk.
  std::unique_ptr<XORIterator> xor_iterator(int num_samples);

  int num_samples();

  uint64_t size();
};

class XORChunkV2 : public ChunkInterface {
 private:
  BitStreamV2 bstream;

 public:
  // The first two bytes store the num of samples using big endian
  XORChunkV2(uint8_t* stream_ptr);

  const uint8_t* bytes();

  uint8_t encoding();

  std::unique_ptr<ChunkAppenderInterface> appender();

  std::unique_ptr<ChunkIteratorInterface> iterator();

  std::unique_ptr<XORIteratorV2> xor_iterator();
  // Headless chunk.
  std::unique_ptr<XORIteratorV2> xor_iterator(int num_samples);

  int num_samples();

  uint64_t size();
};

class NullSupportedXORChunk : public ChunkInterface {
 private:
  BitStream bstream;
  bool read_mode;
  uint64_t size_;

 public:
  // The first two bytes store the num of samples using big endian
  NullSupportedXORChunk();

  NullSupportedXORChunk(const uint8_t* stream_ptr, uint64_t size);

  const uint8_t* bytes();

  const std::vector<uint8_t>* bytes_vector() { return &bstream.stream; }

  uint8_t encoding();

  std::unique_ptr<ChunkAppenderInterface> appender();

  std::unique_ptr<ChunkIteratorInterface> iterator();

  std::unique_ptr<NullSupportedXORIterator> xor_iterator();
  // Headless chunk.
  std::unique_ptr<NullSupportedXORIterator> xor_iterator(int num_samples);

  int num_samples();

  uint64_t size();
};

class NullSupportedXORGroupChunk {
 private:
  BitStream t_bstream_;                // contain 2-byte header for #samples.
  std::vector<BitStream> v_bstreams_;  // no 2-byte headers.
  bool read_mode_;
  uint64_t size_;
  std::vector<int> slots_;
  std::vector<int> offs_;
  const uint8_t* ptr_;

 public:
  // The first two bytes store the num of samples using big endian
  NullSupportedXORGroupChunk(const std::vector<int>& slots);

  NullSupportedXORGroupChunk(const uint8_t* stream_ptr, uint64_t size);

  int find_index_of_slot(int slot);

  // NOTE(Alec): can only be called once.
  std::unique_ptr<NullSupportedXORGroupAppender> appender();
  // NOTE(Alec): this is not the index.
  std::unique_ptr<ChunkIteratorInterface> iterator(int slot);

  void encode(std::string* s);
  static void encode(std::string* s, const std::vector<int>& slots,
                     const std::vector<int64_t>& timestamps,
                     const std::vector<std::vector<double>>& values);

  // Currently only supports read chunk.
  void decode(std::vector<int>* slots, std::vector<int64_t>* timestamps,
              std::vector<std::vector<double>>* values);
  void decode(int slot, std::vector<int64_t>* timestamps,
              std::vector<double>* values, bool filter);

  int num_samples();

  uint64_t size();
};

class NullSupportedXORGroupChunkV2 {
 private:
  BitStreamV2 t_bstream_;                // contain 2-byte header for #samples.
  std::vector<BitStreamV2> v_bstreams_;  // no 2-byte headers.
  std::vector<int> slots_;
  std::vector<int> offs_;

 public:
  NullSupportedXORGroupChunkV2(const std::vector<int>& slots, uint8_t* time_ptr,
                               const std::vector<uint8_t*>& val_ptrs);

  int find_index_of_slot(int slot);

  // NOTE(Alec): can only be called once.
  std::unique_ptr<NullSupportedXORGroupAppenderV2> appender();
  // NOTE(Alec): this is not the index.
  std::unique_ptr<ChunkIteratorInterface> iterator(int slot);

  void encode(std::string* s);
  static void encode(std::string* s, const std::vector<int>& slots,
                     BitStreamV2* tb, const std::vector<BitStreamV2*>& vbs);
  static void encode(std::string* s, const std::vector<int>& slots,
                     const std::vector<int64_t>& timestamps,
                     const std::vector<std::vector<double>>& values);

  int num_samples();

  uint64_t size();
};

}  // namespace chunk
}  // namespace tsdb

#endif