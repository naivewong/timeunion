// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_reader.h"

#include <cstdio>

#include "leveldb/env.h"

#include "util/coding.h"
#include "util/crc32c.h"

#include "base/Endian.hpp"

namespace leveldb {
namespace log {

bool series(const Slice& s, std::vector<RefSeries>* v) {
  if (s.data()[0] != kSeries) return false;
  int i = 1;
  uint32_t size;
  Slice result;
  while (i < s.size()) {
    v->emplace_back();
    v->back().ref = DecodeFixed64BE(s.data() + i);
    i += 8;
    Slice tmps(s.data() + i, s.size() - i);
    GetVarint32(&tmps, &size);
    i = s.size() - tmps.size();

    for (uint32_t j = 0; j < size; j++) {
      tmps = Slice(s.data() + i, s.size() - i);
      GetLengthPrefixedSlice(&tmps, &result);
      i = s.size() - tmps.size();
      std::string label;
      label.append(result.data(), result.size());

      tmps = Slice(s.data() + i, s.size() - i);
      GetLengthPrefixedSlice(&tmps, &result);
      i = s.size() - tmps.size();
      std::string value;
      value.append(result.data(), result.size());
      v->back().lset.emplace_back(label, value);
    }
  }
  return true;
}

bool group(const Slice& s, RefGroup* g) {
  if (s.data()[0] != kGroup) return false;
  int i = 1;
  uint32_t size;
  Slice result;

  g->ref = DecodeFixed64BE(s.data() + i);
  i += 8;
  Slice tmps(s.data() + i, s.size() - i);
  GetVarint32(&tmps, &size);
  i = s.size() - tmps.size();

  for (uint32_t j = 0; j < size; j++) {
    tmps = Slice(s.data() + i, s.size() - i);
    GetLengthPrefixedSlice(&tmps, &result);
    i = s.size() - tmps.size();
    std::string label;
    label.append(result.data(), result.size());

    tmps = Slice(s.data() + i, s.size() - i);
    GetLengthPrefixedSlice(&tmps, &result);
    i = s.size() - tmps.size();
    std::string value;
    value.append(result.data(), result.size());
    g->group_lset.emplace_back(label, value);
  }

  uint32_t num_ts;
  tmps = Slice(s.data() + i, s.size() - i);
  GetVarint32(&tmps, &num_ts);
  i = s.size() - tmps.size();
  for (uint32_t j = 0; j < num_ts; j++) {
    tmps = Slice(s.data() + i, s.size() - i);
    GetVarint32(&tmps, &size);
    i = s.size() - tmps.size();

    g->individual_lsets.emplace_back();
    for (uint32_t j = 0; j < size; j++) {
      tmps = Slice(s.data() + i, s.size() - i);
      GetLengthPrefixedSlice(&tmps, &result);
      i = s.size() - tmps.size();
      std::string label;
      label.append(result.data(), result.size());

      tmps = Slice(s.data() + i, s.size() - i);
      GetLengthPrefixedSlice(&tmps, &result);
      i = s.size() - tmps.size();
      std::string value;
      value.append(result.data(), result.size());
      g->individual_lsets.back().emplace_back(label, value);
    }
  }
  return true;
}

bool samples(const Slice& s, std::vector<RefSample>* v) {
  if (s.data()[0] != kSample) return false;

  int i = 1;
  uint64_t tmp;
  v->emplace_back();
  v->back().ref = DecodeFixed64BE(s.data() + i);
  i += 8;
  v->back().t = DecodeFixed64BE(s.data() + i);
  i += 8;
  v->back().v = ::tsdb::base::decode_double(DecodeFixed64BE(s.data() + i));
  i += 8;
  v->back().txn = DecodeFixed64BE(s.data() + i);
  i += 8;
  while (i < s.size()) {
    v->emplace_back();

    Slice tmps(s.data() + i, s.size() - i);
    GetVarint64(&tmps, &tmp);
    v->back().ref = (uint64_t)((int64_t)(v->at(v->size() - 2).ref) +
                               ::tsdb::base::decode_signed_varint(tmp));
    i = s.size() - tmps.size();

    tmps = Slice(s.data() + i, s.size() - i);
    GetVarint64(&tmps, &tmp);
    v->back().t =
        v->at(v->size() - 2).t + ::tsdb::base::decode_signed_varint(tmp);
    i = s.size() - tmps.size();

    v->back().v = ::tsdb::base::decode_double(DecodeFixed64BE(s.data() + i));
    i += 8;

    tmps = Slice(s.data() + i, s.size() - i);
    GetVarint64(&tmps, &tmp);
    v->back().txn =
        v->at(v->size() - 2).txn + ::tsdb::base::decode_signed_varint(tmp);
    i = s.size() - tmps.size();
  }
  return true;
}

bool group_sample(const Slice& s, RefGroupSample* g) {
  if (s.data()[0] != kGroupSample) return false;

  int i = 1;
  uint32_t num_slots, slot;
  uint64_t tmp;
  g->ref = DecodeFixed64BE(s.data() + i);
  i += 8;

  if (*(s.data() + i++) == 1) {
    Slice tmps(s.data() + i, s.size() - i);
    GetVarint32(&tmps, &num_slots);
    i = s.size() - tmps.size();
    if ((num_slots >> 31) == 0) {
      for (uint32_t j = 0; j < num_slots; j++) {
        tmps = Slice(s.data() + i, s.size() - i);
        GetVarint32(&tmps, &slot);
        i = s.size() - tmps.size();
        g->slots.push_back(slot);
      }
    }
  } else {
    uint32_t num_ts, size;
    Slice result;
    Slice tmps(s.data() + i, s.size() - i);
    GetVarint32(&tmps, &num_ts);
    num_slots = num_ts;
    i = s.size() - tmps.size();
    for (uint32_t j = 0; j < num_ts; j++) {
      tmps = Slice(s.data() + i, s.size() - i);
      GetVarint32(&tmps, &size);
      i = s.size() - tmps.size();

      g->individual_lsets.emplace_back();
      for (uint32_t j = 0; j < size; j++) {
        tmps = Slice(s.data() + i, s.size() - i);
        GetLengthPrefixedSlice(&tmps, &result);
        i = s.size() - tmps.size();
        std::string label;
        label.append(result.data(), result.size());

        tmps = Slice(s.data() + i, s.size() - i);
        GetLengthPrefixedSlice(&tmps, &result);
        i = s.size() - tmps.size();
        std::string value;
        value.append(result.data(), result.size());
        g->individual_lsets.back().emplace_back(label, value);
      }
    }
  }

  g->t = DecodeFixed64BE(s.data() + i);
  i += 8;

  for (uint32_t j = 0; j < (num_slots & 0x7fffffff); j++) {
    g->v.push_back(::tsdb::base::decode_double(DecodeFixed64BE(s.data() + i)));
    i += 8;
  }

  g->txn = DecodeFixed64BE(s.data() + i);

  return true;
}

bool flush(const Slice& s, RefFlush* f) {
  if (s.data()[0] != kFlush) return false;

  f->ref = DecodeFixed64BE(s.data() + 1);
  f->txn = DecodeFixed64BE(s.data() + 9);
  return true;
}

Reader::Reporter::~Reporter() = default;

Reader::Reader(SequentialFile* file, Reporter* reporter, bool checksum,
               uint64_t initial_offset)
    : file_(file),
      reporter_(reporter),
      checksum_(checksum),
      backing_store_(new char[kBlockSize]),
      buffer_(),
      eof_(false),
      last_record_offset_(0),
      end_of_buffer_offset_(0),
      initial_offset_(initial_offset),
      resyncing_(initial_offset > 0) {}

Reader::~Reader() { delete[] backing_store_; }

bool Reader::SkipToInitialBlock() {
  const size_t offset_in_block = initial_offset_ % kBlockSize;
  uint64_t block_start_location = initial_offset_ - offset_in_block;

  // Don't search a block if we'd be in the trailer
  if (offset_in_block > kBlockSize - 6) {
    block_start_location += kBlockSize;
  }

  end_of_buffer_offset_ = block_start_location;

  // Skip to start of first block that can contain the initial record
  if (block_start_location > 0) {
    Status skip_status = file_->Skip(block_start_location);
    if (!skip_status.ok()) {
      ReportDrop(block_start_location, skip_status);
      return false;
    }
  }

  return true;
}

bool Reader::ReadRecord(Slice* record, std::string* scratch) {
  if (last_record_offset_ < initial_offset_) {
    if (!SkipToInitialBlock()) {
      return false;
    }
  }

  scratch->clear();
  record->clear();
  bool in_fragmented_record = false;
  // Record offset of the logical record that we're reading
  // 0 is a dummy value to make compilers happy
  uint64_t prospective_record_offset = 0;

  Slice fragment;
  while (true) {
    const unsigned int record_type = ReadPhysicalRecord(&fragment);

    // ReadPhysicalRecord may have only had an empty trailer remaining in its
    // internal buffer. Calculate the offset of the next physical record now
    // that it has returned, properly accounting for its header size.
    uint64_t physical_record_offset =
        end_of_buffer_offset_ - buffer_.size() - kHeaderSize - fragment.size();

    if (resyncing_) {
      if (record_type == kMiddleType) {
        continue;
      } else if (record_type == kLastType) {
        resyncing_ = false;
        continue;
      } else {
        resyncing_ = false;
      }
    }

    switch (record_type) {
      case kFullType:
        if (in_fragmented_record) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          if (!scratch->empty()) {
            ReportCorruption(scratch->size(), "partial record without end(1)");
          }
        }
        prospective_record_offset = physical_record_offset;
        scratch->clear();
        *record = fragment;
        last_record_offset_ = prospective_record_offset;
        return true;

      case kFirstType:
        if (in_fragmented_record) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          if (!scratch->empty()) {
            ReportCorruption(scratch->size(), "partial record without end(2)");
          }
        }
        prospective_record_offset = physical_record_offset;
        scratch->assign(fragment.data(), fragment.size());
        in_fragmented_record = true;
        break;

      case kMiddleType:
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(1)");
        } else {
          scratch->append(fragment.data(), fragment.size());
        }
        break;

      case kLastType:
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(2)");
        } else {
          scratch->append(fragment.data(), fragment.size());
          *record = Slice(*scratch);
          last_record_offset_ = prospective_record_offset;
          return true;
        }
        break;

      case kEof:
        if (in_fragmented_record) {
          // This can be caused by the writer dying immediately after
          // writing a physical record but before completing the next; don't
          // treat it as a corruption, just ignore the entire logical record.
          scratch->clear();
        }
        return false;

      case kBadRecord:
        if (in_fragmented_record) {
          ReportCorruption(scratch->size(), "error in middle of record");
          in_fragmented_record = false;
          scratch->clear();
        }
        break;

      default: {
        char buf[40];
        std::snprintf(buf, sizeof(buf), "unknown record type %u", record_type);
        ReportCorruption(
            (fragment.size() + (in_fragmented_record ? scratch->size() : 0)),
            buf);
        in_fragmented_record = false;
        scratch->clear();
        break;
      }
    }
  }
  return false;
}

uint64_t Reader::LastRecordOffset() { return last_record_offset_; }

void Reader::ReportCorruption(uint64_t bytes, const char* reason) {
  ReportDrop(bytes, Status::Corruption(reason));
}

void Reader::ReportDrop(uint64_t bytes, const Status& reason) {
  if (reporter_ != nullptr &&
      end_of_buffer_offset_ - buffer_.size() - bytes >= initial_offset_) {
    reporter_->Corruption(static_cast<size_t>(bytes), reason);
  }
}

unsigned int Reader::ReadPhysicalRecord(Slice* result) {
  while (true) {
    if (buffer_.size() < kHeaderSize) {
      if (!eof_) {
        // Last read was a full read, so this is a trailer to skip
        buffer_.clear();
        Status status = file_->Read(kBlockSize, &buffer_, backing_store_);
        end_of_buffer_offset_ += buffer_.size();
        if (!status.ok()) {
          buffer_.clear();
          ReportDrop(kBlockSize, status);
          eof_ = true;
          return kEof;
        } else if (buffer_.size() < kBlockSize) {
          eof_ = true;
        }
        continue;
      } else {
        // Note that if buffer_ is non-empty, we have a truncated header at the
        // end of the file, which can be caused by the writer crashing in the
        // middle of writing the header. Instead of considering this an error,
        // just report EOF.
        buffer_.clear();
        return kEof;
      }
    }

    // Parse the header
    const char* header = buffer_.data();
    const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
    const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
    const unsigned int type = header[6];
    const uint32_t length = a | (b << 8);
    if (kHeaderSize + length > buffer_.size()) {
      size_t drop_size = buffer_.size();
      buffer_.clear();
      if (!eof_) {
        ReportCorruption(drop_size, "bad record length");
        return kBadRecord;
      }
      // If the end of the file has been reached without reading |length| bytes
      // of payload, assume the writer died in the middle of writing the record.
      // Don't report a corruption.
      return kEof;
    }

    if (type == kZeroType && length == 0) {
      // Skip zero length record without reporting any drops since
      // such records are produced by the mmap based writing code in
      // env_posix.cc that preallocates file regions.
      buffer_.clear();
      return kBadRecord;
    }

    // Check crc
    if (checksum_) {
      uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(header));
      uint32_t actual_crc = crc32c::Value(header + 6, 1 + length);
      if (actual_crc != expected_crc) {
        // Drop the rest of the buffer since "length" itself may have
        // been corrupted and if we trust it, we could find some
        // fragment of a real log record that just happens to look
        // like a valid log record.
        size_t drop_size = buffer_.size();
        buffer_.clear();
        ReportCorruption(drop_size, "checksum mismatch");
        return kBadRecord;
      }
    }

    buffer_.remove_prefix(kHeaderSize + length);

    // Skip physical record that started before initial_offset_
    if (end_of_buffer_offset_ - buffer_.size() - kHeaderSize - length <
        initial_offset_) {
      result->clear();
      return kBadRecord;
    }

    *result = Slice(header + kHeaderSize, length);
    return type;
  }
}

}  // namespace log
}  // namespace leveldb
