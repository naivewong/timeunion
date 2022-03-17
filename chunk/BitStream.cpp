#include "chunk/BitStream.hpp"

#include "base/Endian.hpp"

namespace tsdb {
namespace chunk {

const bool ZERO = false;
const bool ONE = true;

BitStream::BitStream(BitStream &bstream, bool iterator_mode)
    : vector_mode(bstream.vector_mode), end(bstream.end) {
  if (vector_mode) {
    stream = bstream.stream;
    if (iterator_mode) {
      head_count = (stream.size() > 0 ? 8 : 0);
      tail_count = 0;
      index = 0;
    } else {
      head_count = bstream.head_count;
      tail_count = bstream.tail_count;
      index = bstream.index;
    }
  } else {
    stream_ptr = bstream.stream_ptr;
    index = 0;
    if (end - index > 0)
      head_count = 8;
    else
      head_count = 0;
    tail_count = 0;
  }
}

// Read mode & vector_mode
BitStream::BitStream(const std::vector<uint8_t> &stream)
    : stream(stream),
      head_count(stream.size() >= 1 ? 8 : 0),
      tail_count(0),
      vector_mode(true),
      index(0),
      end(stream.size()) {}

// Read mode & pointer_mode
BitStream::BitStream(const uint8_t *stream_ptr, int size)
    : stream_ptr(stream_ptr),
      head_count(size >= 1 ? 8 : 0),
      tail_count(0),
      vector_mode(false),
      index(0),
      end(size) {}

// Write mode, tail_count
BitStream::BitStream() {
  stream = std::vector<uint8_t>(0);
  head_count = 0;
  tail_count = 0;
  vector_mode = true;
  index = 0;
  end = 0;
}

// Write mode, tail_count
BitStream::BitStream(int size) {
  stream = std::vector<uint8_t>(size, 0);
  stream.reserve(128);
  head_count = 0;
  tail_count = 0;
  vector_mode = true;
  index = 0;
  end = size;
}

void BitStream::write_bit(bool bit) {
  if (tail_count == 0) {
    stream.push_back(0);
    // tail_count = 8;
    ++end;
  }
  // -- tail_count;
  ++tail_count;
  if (bit) stream.back() |= (bit << (8 - tail_count));
  // stream.back() |= (bit << tail_count);
  tail_count &= 0x07;
}

void BitStream::write_byte(uint8_t byte) {
  if (tail_count == 0) {
    stream.push_back(0);
    ++end;
  }
  stream.back() |= (byte >> tail_count);
  if (tail_count != 0) {
    stream.push_back((byte << (8 - tail_count)) & 0xff);
    ++end;
  }
}

void BitStream::write_bytes_be(int64_t bytes, int num) {
  for (int i = num - 1; i >= 0; i--) {
    write_byte((bytes >> (8 * i)) & 0xff);
  }
}

void BitStream::write_bits(uint64_t bits, int num) {
  bits <<= (64 - num);
  while (num >= 8) {
    write_byte(static_cast<uint8_t>(0xff & (bits >> 56)));
    num -= 8;
    bits <<= 8;
  }
  while (num > 0) {
    write_bit((bits >> 63) == 1);
    bits <<= 1;
    --num;
  }
}

void BitStream::write_stream(const BitStream &s, int num) {
  auto ptr = s.bytes_ptr();
  int i = 0;
  while (num >= 8) {
    write_byte(ptr[i]);
    num -= 8;
    ++i;
  }
  // NOTE.
  if (num > 0) {
    uint8_t j = ptr[i];
    while (num > 0) {
      write_bit((j >> 7) == 1);
      j <<= 1;
      --num;
    }
  }
}
void BitStream::write_padding(int num) {
  while (num >= 8) {
    write_byte(static_cast<uint8_t>(0));
    num -= 8;
  }
  while (num > 0) {
    write_bit(0);
    --num;
  }
}

bool BitStream::read_bit() {
  if (index >= end) {
    throw base::TSDBException("bitstream EOF");
  }
  if (head_count == 0) {
    // all bits in first byte are invalid
    // stream.pop_front();
    ++index;
    if (index >= end) {
      throw base::TSDBException("bitstream EOF");
    }
    head_count = 8;
  }

  // May not use reinterpret_cast
  // In case of invalidation of address when vector expanding
  if (vector_mode) {
    return ((stream[index] << (8 - (head_count--))) & 0x80) == 0x80;
  } else {
    return ((stream_ptr[index] << (8 - (head_count--))) & 0x80) == 0x80;
  }
}

uint8_t BitStream::read_byte() {
  if (index >= end) {
    throw base::TSDBException("bitstream EOF");
  }
  if (head_count == 0) {
    // all bits in first byte are invalid
    ++index;
    if (index >= end) {
      throw base::TSDBException("bitstream EOF");
    }
    head_count = 8;
  }
  uint8_t temp = 0;
  if (vector_mode) {
    temp = (stream[index] << (8 - (head_count)));
    ++index;
    if (head_count != 8 && index >= end) {
      throw base::TSDBException("bitstream EOF");
    }
    temp |= (stream[index] >> (head_count));
  } else {
    temp = (stream_ptr[index] << (8 - (head_count)));
    ++index;
    if (head_count != 8 && index >= end) {
      throw base::TSDBException("bitstream EOF");
    }
    temp |= (stream_ptr[index] >> (head_count));
  }
  return temp;
}

uint64_t BitStream::read_bits(int num) {
  // std::cout << "N " << num << " ";
  uint64_t result = 0;
  while (num >= 8) {
    uint8_t temp = read_byte();
    num -= 8;
    result = (result << 8) | (temp);
  }
  if (vector_mode) {
    if (head_count < num) {
      result =
          (result << head_count) | (stream[index] & ((1 << head_count) - 1));
      num -= head_count;
      ++index;
      if (index >= end) {
        throw base::TSDBException("bitstream EOF");
      }
      head_count = 8;
    }
    result = (result << num) |
             ((stream[index] >> (head_count - num)) & ((1 << num) - 1));
    head_count -= num;
  } else {
    if (head_count < num) {
      result = (result << head_count) |
               (stream_ptr[index] & ((1 << head_count) - 1));
      num -= head_count;
      ++index;
      if (index >= end) {
        throw base::TSDBException("bitstream EOF");
      }
      head_count = 8;
    }
    result = (result << num) |
             ((stream_ptr[index] >> (head_count - num)) & ((1 << num) - 1));
    head_count -= num;
  }
  return result;
}

uint64_t BitStream::read_unsigned_varint() {
  uint64_t decoded_value = 0;
  int shift_amount = 0;
  uint8_t current = 0;

  do {
    current = read_byte();
    decoded_value |= static_cast<uint64_t>(current & 0x7F) << shift_amount;
    shift_amount += 7;
  } while ((current & 0x80) != 0);

  return decoded_value;
}

int64_t BitStream::read_signed_varint() {
  uint64_t unsigned_value = read_unsigned_varint();
  return static_cast<int64_t>(unsigned_value & 1 ? ~(unsigned_value >> 1)
                                                 : (unsigned_value >> 1));
}

bool BitStream::read_bit(int pos) {
  int index_ = pos >> 3;
  // uint8_t head_count_ = 8 - (pos % 8);
  uint8_t head_count_ = pos % 8;
  if (index_ >= end) {
    throw base::TSDBException("bitstream EOF");
  }
  // if(head_count_ == 0){
  //     // all bits in first byte are invalid
  //     // stream.pop_front();
  //     ++index_;
  //     if(index_ >= end){
  //         throw base::TSDBException("bitstream EOF");
  //     }
  //     head_count_ = 8;
  // }

  // May not use reinterpret_cast
  // In case of invalidation of address when vector expanding
  if (vector_mode) {
    // return ((stream[index_] << (8 - (head_count_))) & 0x80) == 0x80;
    return ((stream[index_] << head_count_) & 0x80) == 0x80;
  } else {
    // return ((stream_ptr[index_] << (8 - (head_count_))) & 0x80) == 0x80;
    return ((stream_ptr[index_] << head_count_) & 0x80) == 0x80;
  }
}
uint8_t BitStream::read_byte(int pos) {
  int index_ = pos >> 3;
  // uint8_t head_count_ = 8 - (pos % 8);
  uint8_t head_count_ = pos % 8;
  if (index_ >= end) {
    throw base::TSDBException("bitstream EOF");
  }
  // if(head_count_ == 0){
  //     // all bits in first byte are invalid
  //     ++index_;
  //     if(index_ >= end){
  //         throw base::TSDBException("bitstream EOF");
  //     }
  //     head_count_ = 8;
  // }
  uint8_t temp = 0;
  if (vector_mode) {
    // temp = (stream[index_] << (8 - (head_count_)));
    temp = stream[index_] << head_count_;
    ++index_;
    // if(head_count_ != 8 && index_ >= end){
    if (head_count_ != 0 && index_ >= end) {
      throw base::TSDBException("bitstream EOF");
    }
    // temp |= (stream[index_] >> (head_count_));
    temp |= stream[index_] >> (8 - head_count_);
  } else {
    // temp = (stream_ptr[index_] << (8 - (head_count_)));
    temp = stream_ptr[index_] << head_count_;
    ++index_;
    // if(head_count_ != 8 && index_ >= end){
    if (head_count_ != 0 && index_ >= end) {
      throw base::TSDBException("bitstream EOF");
    }
    // temp |= (stream_ptr[index_] >> (head_count_));
    temp |= stream_ptr[index_] >> (8 - head_count_);
  }
  return temp;
}
uint8_t BitStream::read_byte_helper(int &index_, uint8_t &head_count_) {
  if (index_ >= end) {
    throw base::TSDBException("bitstream EOF");
  }
  if (head_count_ == 0) {
    // all bits in first byte are invalid
    ++index_;
    if (index_ >= end) {
      throw base::TSDBException("bitstream EOF");
    }
    head_count_ = 8;
  }
  uint8_t temp = 0;
  if (vector_mode) {
    temp = (stream[index_] << (8 - (head_count_)));
    ++index_;
    if (head_count_ != 8 && index_ >= end) {
      throw base::TSDBException("bitstream EOF");
    }
    temp |= (stream[index_] >> (head_count_));
  } else {
    temp = (stream_ptr[index_] << (8 - (head_count_)));
    ++index_;
    if (head_count_ != 8 && index_ >= end) {
      throw base::TSDBException("bitstream EOF");
    }
    temp |= (stream_ptr[index_] >> (head_count_));
  }
  return temp;
}
int64_t BitStream::read_bytes_be(int pos, int num) {
  int index_ = pos >> 3;
  uint8_t head_count_ = 8 - (pos % 8);
  int64_t result = 0;
  int i = num;
  while (i >= 1) {
    i -= 1;
    result = (result << 8) | read_byte_helper(index_, head_count_);
  }
  if ((result >> ((num << 3) - 1)) == 1)  // Negetive.
    result |= base::bytes64pretable[num - 1];
  return result;
}
uint64_t BitStream::read_bits(int pos, int num) {
  int index_ = pos >> 3;
  uint8_t head_count_ = 8 - (pos % 8);
  uint64_t result = 0;
  while (num >= 8) {
    num -= 8;
    result = (result << 8) | read_byte_helper(index_, head_count_);
  }
  if (vector_mode) {
    if (head_count_ < num) {
      result =
          (result << head_count_) | (stream[index_] & ((1 << head_count_) - 1));
      num -= head_count_;
      ++index_;
      if (index_ >= end) {
        throw base::TSDBException("bitstream EOF");
      }
      head_count_ = 8;
      // std::cout << "R " << result;
    }
    result = (result << num) |
             ((stream[index_] >> (head_count_ - num)) & ((1 << num) - 1));
    // std::cout << " T " << (stream.front() >> (head_count - num));
    // head_count_ -= num;
  } else {
    if (head_count_ < num) {
      result = (result << head_count_) |
               (stream_ptr[index_] & ((1 << head_count_) - 1));
      num -= head_count_;
      ++index_;
      if (index_ >= end) {
        throw base::TSDBException("bitstream EOF");
      }
      head_count_ = 8;
      // std::cout << "R " << result;
    }
    result = (result << num) |
             ((stream_ptr[index_] >> (head_count_ - num)) & ((1 << num) - 1));
    // std::cout << " T " << (stream.front() >> (head_count - num));
    // head_count_ -= num;
  }
  // std::cout << " R " << result << std::endl;
  return result;
}
std::pair<uint64_t, uint8_t> BitStream::read_unsigned_varint(int pos) {
  int index_ = pos >> 3;
  uint8_t head_count_ = 8 - (pos % 8);
  uint64_t decoded_value = 0;
  int shift_amount = 0;
  uint8_t current = 0;

  uint8_t c = 0;
  do {
    current = read_byte_helper(index_, head_count_);
    decoded_value |= static_cast<uint64_t>(current & 0x7F) << shift_amount;
    shift_amount += 7;
    c += 8;
  } while ((current & 0x80) != 0);

  return {decoded_value, c};
}
std::pair<int64_t, uint8_t> BitStream::read_signed_varint(int pos) {
  auto unsigned_value = read_unsigned_varint(pos);
  return {static_cast<int64_t>(unsigned_value.first & 1
                                   ? ~(unsigned_value.first >> 1)
                                   : (unsigned_value.first >> 1)),
          unsigned_value.second};
}

// Only called when vector mode
std::vector<uint8_t> *BitStream::bytes() { return &stream; }

const uint8_t *BitStream::bytes_ptr() const {
  if (vector_mode)
    return &(stream[0]);
  else
    return stream_ptr;
}

// Use in read mode
void BitStream::pop_front() {
  // stream.pop_front();
  ++index;
}

std::vector<uint8_t> &BitStream::get_stream() { return stream; }

int BitStream::size() {
  if (vector_mode)
    return stream.size();
  else
    return end;
}

int BitStream::write_pos() {
  if (tail_count == 0)
    return size() << 3;
  else
    return ((size() - 1) << 3) + tail_count;
}

BitStreamV2::BitStreamV2(uint8_t *ptr)
    : stream(ptr), head_count(0), tail_count(0), index(0), end(0) {}

// Write mode, tail_count
BitStreamV2::BitStreamV2(uint8_t *ptr, int size)
    : stream(ptr), head_count(0), tail_count(0), index(0), end(size) {
  for (int i = 0; i < size; i++) ptr[i] = 0;
}

BitStreamV2::BitStreamV2(uint8_t *stream_ptr, int size, bool)
    : stream(stream_ptr),
      head_count(size >= 1 ? 8 : 0),
      tail_count(0),
      index(0),
      end(size) {}

void BitStreamV2::write_bit(bool bit) {
  if (tail_count == 0) {
    stream[end] = 0;
    ++end;
  }
  ++tail_count;
  if (bit) stream[end - 1] |= (bit << (8 - tail_count));
  tail_count &= 0x07;
}

void BitStreamV2::write_byte(uint8_t byte) {
  if (tail_count == 0) {
    stream[end] = 0;
    ++end;
  }
  stream[end - 1] |= (byte >> tail_count);
  if (tail_count != 0) {
    stream[end] = (byte << (8 - tail_count)) & 0xff;
    ++end;
  }
}

void BitStreamV2::write_bits(uint64_t bits, int num) {
  bits <<= (64 - num);
  while (num >= 8) {
    write_byte(static_cast<uint8_t>(0xff & (bits >> 56)));
    num -= 8;
    bits <<= 8;
  }
  while (num > 0) {
    write_bit((bits >> 63) == 1);
    bits <<= 1;
    --num;
  }
}

bool BitStreamV2::read_bit() {
  if (index >= end) {
    throw base::TSDBException("BitStreamV2 EOF");
  }
  if (head_count == 0) {
    // all bits in first byte are invalid
    ++index;
    if (index >= end) {
      throw base::TSDBException("BitStreamV2 EOF");
    }
    head_count = 8;
  }

  // May not use reinterpret_cast
  // In case of invalidation of address when vector expanding
  return ((stream[index] << (8 - (head_count--))) & 0x80) == 0x80;
}

uint8_t BitStreamV2::read_byte() {
  if (index >= end) {
    throw base::TSDBException("BitStreamV2 EOF");
  }
  if (head_count == 0) {
    // all bits in first byte are invalid
    ++index;
    if (index >= end) {
      throw base::TSDBException("BitStreamV2 EOF");
    }
    head_count = 8;
  }

  uint8_t temp = (stream[index] << (8 - (head_count)));
  ++index;
  if (head_count != 8 && index >= end) {
    throw base::TSDBException("BitStreamV2 EOF");
  }
  temp |= (stream[index] >> (head_count));

  return temp;
}

uint64_t BitStreamV2::read_bits(int num) {
  uint64_t result = 0;
  while (num >= 8) {
    uint8_t temp = read_byte();
    num -= 8;
    result = (result << 8) | (temp);
  }

  if (head_count < num) {
    result = (result << head_count) | (stream[index] & ((1 << head_count) - 1));
    num -= head_count;
    ++index;
    if (index >= end) {
      throw base::TSDBException("BitStreamV2 EOF");
    }
    head_count = 8;
  }
  result = (result << num) |
           ((stream[index] >> (head_count - num)) & ((1 << num) - 1));
  head_count -= num;

  return result;
}

uint64_t BitStreamV2::read_unsigned_varint() {
  uint64_t decoded_value = 0;
  int shift_amount = 0;
  uint8_t current = 0;

  do {
    current = read_byte();
    decoded_value |= static_cast<uint64_t>(current & 0x7F) << shift_amount;
    shift_amount += 7;
  } while ((current & 0x80) != 0);

  return decoded_value;
}

int64_t BitStreamV2::read_signed_varint() {
  uint64_t unsigned_value = read_unsigned_varint();
  return static_cast<int64_t>(unsigned_value & 1 ? ~(unsigned_value >> 1)
                                                 : (unsigned_value >> 1));
}

bool BitStreamV2::read_bit(int pos) {
  int index_ = pos >> 3;
  uint8_t head_count_ = pos % 8;
  if (index_ >= end) {
    throw base::TSDBException("BitStreamV2 EOF");
  }

  // May not use reinterpret_cast
  // In case of invalidation of address when vector expanding
  return ((stream[index_] << head_count_) & 0x80) == 0x80;
}

uint8_t BitStreamV2::read_byte(int pos) {
  int index_ = pos >> 3;
  uint8_t head_count_ = pos % 8;
  if (index_ >= end) {
    throw base::TSDBException("BitStreamV2 EOF");
  }

  uint8_t temp = stream[index_] << head_count_;
  ++index_;
  if (head_count_ != 0 && index_ >= end) {
    throw base::TSDBException("BitStreamV2 EOF");
  }
  temp |= stream[index_] >> (8 - head_count_);

  return temp;
}

uint8_t BitStreamV2::read_byte_helper(int &index_, uint8_t &head_count_) {
  if (index_ >= end) {
    throw base::TSDBException("BitStreamV2 EOF");
  }
  if (head_count_ == 0) {
    // all bits in first byte are invalid
    ++index_;
    if (index_ >= end) {
      throw base::TSDBException("BitStreamV2 EOF");
    }
    head_count_ = 8;
  }

  uint8_t temp = (stream[index_] << (8 - (head_count_)));
  ++index_;
  if (head_count_ != 8 && index_ >= end) {
    throw base::TSDBException("BitStreamV2 EOF");
  }
  temp |= (stream[index_] >> (head_count_));

  return temp;
}

uint64_t BitStreamV2::read_bits(int pos, int num) {
  int index_ = pos >> 3;
  uint8_t head_count_ = 8 - (pos % 8);
  uint64_t result = 0;
  while (num >= 8) {
    num -= 8;
    result = (result << 8) | read_byte_helper(index_, head_count_);
  }

  if (head_count_ < num) {
    result =
        (result << head_count_) | (stream[index_] & ((1 << head_count_) - 1));
    num -= head_count_;
    ++index_;
    if (index_ >= end) {
      throw base::TSDBException("BitStreamV2 EOF");
    }
    head_count_ = 8;
  }
  result = (result << num) |
           ((stream[index_] >> (head_count_ - num)) & ((1 << num) - 1));

  return result;
}

std::pair<uint64_t, uint8_t> BitStreamV2::read_unsigned_varint(int pos) {
  int index_ = pos >> 3;
  uint8_t head_count_ = 8 - (pos % 8);
  uint64_t decoded_value = 0;
  int shift_amount = 0;
  uint8_t current = 0;

  uint8_t c = 0;
  do {
    current = read_byte_helper(index_, head_count_);
    decoded_value |= static_cast<uint64_t>(current & 0x7F) << shift_amount;
    shift_amount += 7;
    c += 8;
  } while ((current & 0x80) != 0);

  return {decoded_value, c};
}

std::pair<int64_t, uint8_t> BitStreamV2::read_signed_varint(int pos) {
  auto unsigned_value = read_unsigned_varint(pos);
  return {static_cast<int64_t>(unsigned_value.first & 1
                                   ? ~(unsigned_value.first >> 1)
                                   : (unsigned_value.first >> 1)),
          unsigned_value.second};
}

uint8_t *BitStreamV2::bytes() { return stream; }

const uint8_t *BitStreamV2::bytes_ptr() const { return stream; }

// Use in read mode
void BitStreamV2::pop_front() { ++index; }

int BitStreamV2::size() { return end; }

}  // namespace chunk
}  // namespace tsdb