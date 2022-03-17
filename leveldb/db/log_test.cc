// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_reader.h"
#include "db/log_writer.h"
#include <boost/filesystem.hpp>

#include "leveldb/env.h"

#include "util/coding.h"
#include "util/crc32c.h"
#include "util/random.h"

#include "gtest/gtest.h"

namespace leveldb {
namespace log {

// Construct a string of the specified length made out of the supplied
// partial string.
static std::string BigString(const std::string& partial_string, size_t n) {
  std::string result;
  while (result.size() < n) {
    result.append(partial_string);
  }
  result.resize(n);
  return result;
}

// Construct a string from a number
static std::string NumberString(int n) {
  char buf[50];
  std::snprintf(buf, sizeof(buf), "%d.", n);
  return std::string(buf);
}

// Return a skewed potentially long string
static std::string RandomSkewedString(int i, Random* rnd) {
  return BigString(NumberString(i), rnd->Skewed(17));
}

class LogTest : public testing::Test {
 public:
  LogTest()
      : reading_(false),
        writer_(new Writer(&dest_)),
        reader_(new Reader(&source_, &report_, true /*checksum*/,
                           0 /*initial_offset*/)) {}

  ~LogTest() {
    delete writer_;
    delete reader_;
  }

  void ReopenForAppend() {
    delete writer_;
    writer_ = new Writer(&dest_, dest_.contents_.size());
  }

  void Write(const std::string& msg) {
    ASSERT_TRUE(!reading_) << "Write() after starting to read";
    writer_->AddRecord(Slice(msg));
  }

  size_t WrittenBytes() const { return dest_.contents_.size(); }

  std::string Read() {
    if (!reading_) {
      reading_ = true;
      source_.contents_ = Slice(dest_.contents_);
    }
    std::string scratch;
    Slice record;
    if (reader_->ReadRecord(&record, &scratch)) {
      return record.ToString();
    } else {
      return "EOF";
    }
  }

  void IncrementByte(int offset, int delta) {
    dest_.contents_[offset] += delta;
  }

  void SetByte(int offset, char new_byte) {
    dest_.contents_[offset] = new_byte;
  }

  void ShrinkSize(int bytes) {
    dest_.contents_.resize(dest_.contents_.size() - bytes);
  }

  void FixChecksum(int header_offset, int len) {
    // Compute crc of type/len/data
    uint32_t crc = crc32c::Value(&dest_.contents_[header_offset + 6], 1 + len);
    crc = crc32c::Mask(crc);
    EncodeFixed32(&dest_.contents_[header_offset], crc);
  }

  void ForceError() { source_.force_error_ = true; }

  size_t DroppedBytes() const { return report_.dropped_bytes_; }

  std::string ReportMessage() const { return report_.message_; }

  // Returns OK iff recorded error message contains "msg"
  std::string MatchError(const std::string& msg) const {
    if (report_.message_.find(msg) == std::string::npos) {
      return report_.message_;
    } else {
      return "OK";
    }
  }

  void WriteInitialOffsetLog() {
    for (int i = 0; i < num_initial_offset_records_; i++) {
      std::string record(initial_offset_record_sizes_[i],
                         static_cast<char>('a' + i));
      Write(record);
    }
  }

  void StartReadingAt(uint64_t initial_offset) {
    delete reader_;
    reader_ = new Reader(&source_, &report_, true /*checksum*/, initial_offset);
  }

  void CheckOffsetPastEndReturnsNoRecords(uint64_t offset_past_end) {
    WriteInitialOffsetLog();
    reading_ = true;
    source_.contents_ = Slice(dest_.contents_);
    Reader* offset_reader = new Reader(&source_, &report_, true /*checksum*/,
                                       WrittenBytes() + offset_past_end);
    Slice record;
    std::string scratch;
    ASSERT_TRUE(!offset_reader->ReadRecord(&record, &scratch));
    delete offset_reader;
  }

  void CheckInitialOffsetRecord(uint64_t initial_offset,
                                int expected_record_offset) {
    WriteInitialOffsetLog();
    reading_ = true;
    source_.contents_ = Slice(dest_.contents_);
    Reader* offset_reader =
        new Reader(&source_, &report_, true /*checksum*/, initial_offset);

    // Read all records from expected_record_offset through the last one.
    ASSERT_LT(expected_record_offset, num_initial_offset_records_);
    for (; expected_record_offset < num_initial_offset_records_;
         ++expected_record_offset) {
      Slice record;
      std::string scratch;
      ASSERT_TRUE(offset_reader->ReadRecord(&record, &scratch));
      ASSERT_EQ(initial_offset_record_sizes_[expected_record_offset],
                record.size());
      ASSERT_EQ(initial_offset_last_record_offsets_[expected_record_offset],
                offset_reader->LastRecordOffset());
      ASSERT_EQ((char)('a' + expected_record_offset), record.data()[0]);
    }
    delete offset_reader;
  }

 private:
  class StringDest : public WritableFile {
   public:
    Status Close() override { return Status::OK(); }
    Status Flush() override { return Status::OK(); }
    Status Sync() override { return Status::OK(); }
    Status Append(const Slice& slice) override {
      contents_.append(slice.data(), slice.size());
      return Status::OK();
    }

    std::string contents_;
  };

  class StringSource : public SequentialFile {
   public:
    StringSource() : force_error_(false), returned_partial_(false) {}

    Status Read(size_t n, Slice* result, char* scratch) override {
      EXPECT_TRUE(!returned_partial_) << "must not Read() after eof/error";

      if (force_error_) {
        force_error_ = false;
        returned_partial_ = true;
        return Status::Corruption("read error");
      }

      if (contents_.size() < n) {
        n = contents_.size();
        returned_partial_ = true;
      }
      *result = Slice(contents_.data(), n);
      contents_.remove_prefix(n);
      return Status::OK();
    }

    Status Skip(uint64_t n) override {
      if (n > contents_.size()) {
        contents_.clear();
        return Status::NotFound("in-memory file skipped past end");
      }

      contents_.remove_prefix(n);

      return Status::OK();
    }

    Slice contents_;
    bool force_error_;
    bool returned_partial_;
  };

  class ReportCollector : public Reader::Reporter {
   public:
    ReportCollector() : dropped_bytes_(0) {}
    void Corruption(size_t bytes, const Status& status) override {
      dropped_bytes_ += bytes;
      message_.append(status.ToString());
    }

    size_t dropped_bytes_;
    std::string message_;
  };

  // Record metadata for testing initial offset functionality
  static size_t initial_offset_record_sizes_[];
  static uint64_t initial_offset_last_record_offsets_[];
  static int num_initial_offset_records_;

  StringDest dest_;
  StringSource source_;
  ReportCollector report_;
  bool reading_;
  Writer* writer_;
  Reader* reader_;
};

size_t LogTest::initial_offset_record_sizes_[] = {
    10000,  // Two sizable records in first block
    10000,
    2 * log::kBlockSize - 1000,  // Span three blocks
    1,
    13716,                          // Consume all but two bytes of block 3.
    log::kBlockSize - kHeaderSize,  // Consume the entirety of block 4.
};

uint64_t LogTest::initial_offset_last_record_offsets_[] = {
    0,
    kHeaderSize + 10000,
    2 * (kHeaderSize + 10000),
    2 * (kHeaderSize + 10000) + (2 * log::kBlockSize - 1000) + 3 * kHeaderSize,
    2 * (kHeaderSize + 10000) + (2 * log::kBlockSize - 1000) + 3 * kHeaderSize +
        kHeaderSize + 1,
    3 * log::kBlockSize,
};

// LogTest::initial_offset_last_record_offsets_ must be defined before this.
int LogTest::num_initial_offset_records_ =
    sizeof(LogTest::initial_offset_last_record_offsets_) / sizeof(uint64_t);

TEST_F(LogTest, Empty) { ASSERT_EQ("EOF", Read()); }

TEST_F(LogTest, ReadWrite) {
  Write("foo");
  Write("bar");
  Write("");
  Write("xxxx");
  ASSERT_EQ("foo", Read());
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("", Read());
  ASSERT_EQ("xxxx", Read());
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ("EOF", Read());  // Make sure reads at eof work
}

TEST_F(LogTest, ManyBlocks) {
  for (int i = 0; i < 100000; i++) {
    Write(NumberString(i));
  }
  for (int i = 0; i < 100000; i++) {
    ASSERT_EQ(NumberString(i), Read());
  }
  ASSERT_EQ("EOF", Read());
}

TEST_F(LogTest, Fragmentation) {
  Write("small");
  Write(BigString("medium", 50000));
  Write(BigString("large", 100000));
  ASSERT_EQ("small", Read());
  ASSERT_EQ(BigString("medium", 50000), Read());
  ASSERT_EQ(BigString("large", 100000), Read());
  ASSERT_EQ("EOF", Read());
}

TEST_F(LogTest, MarginalTrailer) {
  // Make a trailer that is exactly the same length as an empty record.
  const int n = kBlockSize - 2 * kHeaderSize;
  Write(BigString("foo", n));
  ASSERT_EQ(kBlockSize - kHeaderSize, WrittenBytes());
  Write("");
  Write("bar");
  ASSERT_EQ(BigString("foo", n), Read());
  ASSERT_EQ("", Read());
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("EOF", Read());
}

TEST_F(LogTest, MarginalTrailer2) {
  // Make a trailer that is exactly the same length as an empty record.
  const int n = kBlockSize - 2 * kHeaderSize;
  Write(BigString("foo", n));
  ASSERT_EQ(kBlockSize - kHeaderSize, WrittenBytes());
  Write("bar");
  ASSERT_EQ(BigString("foo", n), Read());
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(0, DroppedBytes());
  ASSERT_EQ("", ReportMessage());
}

TEST_F(LogTest, ShortTrailer) {
  const int n = kBlockSize - 2 * kHeaderSize + 4;
  Write(BigString("foo", n));
  ASSERT_EQ(kBlockSize - kHeaderSize + 4, WrittenBytes());
  Write("");
  Write("bar");
  ASSERT_EQ(BigString("foo", n), Read());
  ASSERT_EQ("", Read());
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("EOF", Read());
}

TEST_F(LogTest, AlignedEof) {
  const int n = kBlockSize - 2 * kHeaderSize + 4;
  Write(BigString("foo", n));
  ASSERT_EQ(kBlockSize - kHeaderSize + 4, WrittenBytes());
  ASSERT_EQ(BigString("foo", n), Read());
  ASSERT_EQ("EOF", Read());
}

TEST_F(LogTest, OpenForAppend) {
  Write("hello");
  ReopenForAppend();
  Write("world");
  ASSERT_EQ("hello", Read());
  ASSERT_EQ("world", Read());
  ASSERT_EQ("EOF", Read());
}

TEST_F(LogTest, RandomRead) {
  const int N = 500;
  Random write_rnd(301);
  for (int i = 0; i < N; i++) {
    Write(RandomSkewedString(i, &write_rnd));
  }
  Random read_rnd(301);
  for (int i = 0; i < N; i++) {
    ASSERT_EQ(RandomSkewedString(i, &read_rnd), Read());
  }
  ASSERT_EQ("EOF", Read());
}

// Tests of all the error paths in log_reader.cc follow:

TEST_F(LogTest, ReadError) {
  Write("foo");
  ForceError();
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(kBlockSize, DroppedBytes());
  ASSERT_EQ("OK", MatchError("read error"));
}

TEST_F(LogTest, BadRecordType) {
  Write("foo");
  // Type is stored in header[6]
  IncrementByte(6, 100);
  FixChecksum(0, 3);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(3, DroppedBytes());
  ASSERT_EQ("OK", MatchError("unknown record type"));
}

TEST_F(LogTest, TruncatedTrailingRecordIsIgnored) {
  Write("foo");
  ShrinkSize(4);  // Drop all payload as well as a header byte
  ASSERT_EQ("EOF", Read());
  // Truncated last record is ignored, not treated as an error.
  ASSERT_EQ(0, DroppedBytes());
  ASSERT_EQ("", ReportMessage());
}

TEST_F(LogTest, BadLength) {
  const int kPayloadSize = kBlockSize - kHeaderSize;
  Write(BigString("bar", kPayloadSize));
  Write("foo");
  // Least significant size byte is stored in header[4].
  IncrementByte(4, 1);
  ASSERT_EQ("foo", Read());
  ASSERT_EQ(kBlockSize, DroppedBytes());
  ASSERT_EQ("OK", MatchError("bad record length"));
}

TEST_F(LogTest, BadLengthAtEndIsIgnored) {
  Write("foo");
  ShrinkSize(1);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(0, DroppedBytes());
  ASSERT_EQ("", ReportMessage());
}

TEST_F(LogTest, ChecksumMismatch) {
  Write("foo");
  IncrementByte(0, 10);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(10, DroppedBytes());
  ASSERT_EQ("OK", MatchError("checksum mismatch"));
}

TEST_F(LogTest, UnexpectedMiddleType) {
  Write("foo");
  SetByte(6, kMiddleType);
  FixChecksum(0, 3);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(3, DroppedBytes());
  ASSERT_EQ("OK", MatchError("missing start"));
}

TEST_F(LogTest, UnexpectedLastType) {
  Write("foo");
  SetByte(6, kLastType);
  FixChecksum(0, 3);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(3, DroppedBytes());
  ASSERT_EQ("OK", MatchError("missing start"));
}

TEST_F(LogTest, UnexpectedFullType) {
  Write("foo");
  Write("bar");
  SetByte(6, kFirstType);
  FixChecksum(0, 3);
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(3, DroppedBytes());
  ASSERT_EQ("OK", MatchError("partial record without end"));
}

TEST_F(LogTest, UnexpectedFirstType) {
  Write("foo");
  Write(BigString("bar", 100000));
  SetByte(6, kFirstType);
  FixChecksum(0, 3);
  ASSERT_EQ(BigString("bar", 100000), Read());
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(3, DroppedBytes());
  ASSERT_EQ("OK", MatchError("partial record without end"));
}

TEST_F(LogTest, MissingLastIsIgnored) {
  Write(BigString("bar", kBlockSize));
  // Remove the LAST block, including header.
  ShrinkSize(14);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ("", ReportMessage());
  ASSERT_EQ(0, DroppedBytes());
}

TEST_F(LogTest, PartialLastIsIgnored) {
  Write(BigString("bar", kBlockSize));
  // Cause a bad record length in the LAST block.
  ShrinkSize(1);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ("", ReportMessage());
  ASSERT_EQ(0, DroppedBytes());
}

TEST_F(LogTest, SkipIntoMultiRecord) {
  // Consider a fragmented record:
  //    first(R1), middle(R1), last(R1), first(R2)
  // If initial_offset points to a record after first(R1) but before first(R2)
  // incomplete fragment errors are not actual errors, and must be suppressed
  // until a new first or full record is encountered.
  Write(BigString("foo", 3 * kBlockSize));
  Write("correct");
  StartReadingAt(kBlockSize);

  ASSERT_EQ("correct", Read());
  ASSERT_EQ("", ReportMessage());
  ASSERT_EQ(0, DroppedBytes());
  ASSERT_EQ("EOF", Read());
}

TEST_F(LogTest, ErrorJoinsRecords) {
  // Consider two fragmented records:
  //    first(R1) last(R1) first(R2) last(R2)
  // where the middle two fragments disappear.  We do not want
  // first(R1),last(R2) to get joined and returned as a valid record.

  // Write records that span two blocks
  Write(BigString("foo", kBlockSize));
  Write(BigString("bar", kBlockSize));
  Write("correct");

  // Wipe the middle block
  for (int offset = kBlockSize; offset < 2 * kBlockSize; offset++) {
    SetByte(offset, 'x');
  }

  ASSERT_EQ("correct", Read());
  ASSERT_EQ("EOF", Read());
  const size_t dropped = DroppedBytes();
  ASSERT_LE(dropped, 2 * kBlockSize + 100);
  ASSERT_GE(dropped, 2 * kBlockSize);
}

TEST_F(LogTest, ReadStart) { CheckInitialOffsetRecord(0, 0); }

TEST_F(LogTest, ReadSecondOneOff) { CheckInitialOffsetRecord(1, 1); }

TEST_F(LogTest, ReadSecondTenThousand) { CheckInitialOffsetRecord(10000, 1); }

TEST_F(LogTest, ReadSecondStart) { CheckInitialOffsetRecord(10007, 1); }

TEST_F(LogTest, ReadThirdOneOff) { CheckInitialOffsetRecord(10008, 2); }

TEST_F(LogTest, ReadThirdStart) { CheckInitialOffsetRecord(20014, 2); }

TEST_F(LogTest, ReadFourthOneOff) { CheckInitialOffsetRecord(20015, 3); }

TEST_F(LogTest, ReadFourthFirstBlockTrailer) {
  CheckInitialOffsetRecord(log::kBlockSize - 4, 3);
}

TEST_F(LogTest, ReadFourthMiddleBlock) {
  CheckInitialOffsetRecord(log::kBlockSize + 1, 3);
}

TEST_F(LogTest, ReadFourthLastBlock) {
  CheckInitialOffsetRecord(2 * log::kBlockSize + 1, 3);
}

TEST_F(LogTest, ReadFourthStart) {
  CheckInitialOffsetRecord(
      2 * (kHeaderSize + 1000) + (2 * log::kBlockSize - 1000) + 3 * kHeaderSize,
      3);
}

TEST_F(LogTest, ReadInitialOffsetIntoBlockPadding) {
  CheckInitialOffsetRecord(3 * log::kBlockSize - 3, 5);
}

TEST_F(LogTest, ReadEnd) { CheckOffsetPastEndReturnsNoRecords(0); }

TEST_F(LogTest, ReadPastEnd) { CheckOffsetPastEndReturnsNoRecords(5); }

class LogRecordTest : public testing::Test {
 public:
  std::string name;
};

TEST_F(LogRecordTest, Test1) {
  Env* env = Env::Default();
  name = "head.log";
  boost::filesystem::remove(name);
  {
    WritableFile* f;
    ASSERT_TRUE(env->NewAppendableFile(name, &f).ok());
    Writer w(f);
    ::tsdb::label::Labels lset;
    for (int i = 0; i < 100; i++) {
      lset.emplace_back(
          "labellabellabellabellabellabellabellabellabellabellabellabellabellab"
          "el" +
              std::to_string(i),
          "valuevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevalueval"
          "ue" +
              std::to_string(i));
    }
    for (int i = 0; i < 1000; i++) {
      RefSeries r;
      r.ref = i;
      r.lset = lset;
      std::string record = series({r});
      ASSERT_TRUE(w.AddRecord(record).ok());
    }
    delete f;

    SequentialFile* sf;
    ASSERT_TRUE(env->NewSequentialFile(name, &sf).ok());
    Reader r(sf, nullptr, false, 0);
    Slice record;
    std::string scratch;
    int i = 0;
    while (r.ReadRecord(&record, &scratch)) {
      std::vector<RefSeries> rs;
      ASSERT_TRUE(series(record, &rs));
      ASSERT_EQ(1, rs.size());
      ASSERT_EQ(i, rs[0].ref);
      ASSERT_EQ(lset, rs[0].lset);
      i++;
    }
    ASSERT_EQ(1000, i);
    delete sf;
  }

  // Reopen and append.
  {
    WritableFile* f;
    ASSERT_TRUE(env->NewAppendableFile(name, &f).ok());
    uint64_t fsize;
    env->GetFileSize(name, &fsize);
    Writer w(f, fsize);
    ::tsdb::label::Labels lset;
    for (int i = 0; i < 100; i++) {
      lset.emplace_back(
          "labellabellabellabellabellabellabellabellabellabellabellabellabellab"
          "el" +
              std::to_string(i),
          "valuevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevalueval"
          "ue" +
              std::to_string(i));
    }
    for (int i = 1000; i < 2000; i++) {
      RefSeries r;
      r.ref = i;
      r.lset = lset;
      std::string record = series({r});
      ASSERT_TRUE(w.AddRecord(record).ok());
    }
    delete f;

    SequentialFile* sf;
    ASSERT_TRUE(env->NewSequentialFile(name, &sf).ok());
    Reader r(sf, nullptr, false, 0);
    Slice record;
    std::string scratch;
    int i = 0;
    while (r.ReadRecord(&record, &scratch)) {
      std::vector<RefSeries> rs;
      ASSERT_TRUE(series(record, &rs));
      ASSERT_EQ(1, rs.size());
      ASSERT_EQ(i, rs[0].ref);
      ASSERT_EQ(lset, rs[0].lset);
      i++;
    }
    ASSERT_EQ(2000, i);
    delete sf;
  }
}

TEST_F(LogRecordTest, Test2) {
  Env* env = Env::Default();
  name = "head.log";
  boost::filesystem::remove(name);

  std::vector<RefSample> g_samples;
  {
    WritableFile* f;
    ASSERT_TRUE(env->NewAppendableFile(name, &f).ok());
    Writer w(f);
    for (int i = 0; i < 303; i += 3) {
      std::vector<RefSample> v;
      v.emplace_back(i, rand() % 100000, (double)(rand()) / (double)(RAND_MAX),
                     i);
      g_samples.push_back(v.back());
      v.emplace_back(i + 1, rand() % 100000,
                     (double)(rand()) / (double)(RAND_MAX), i + 1);
      g_samples.push_back(v.back());
      v.emplace_back(i + 2, rand() % 100000,
                     (double)(rand()) / (double)(RAND_MAX), i + 2);
      g_samples.push_back(v.back());

      std::string record = samples(v);
      ASSERT_TRUE(w.AddRecord(record).ok());
      if (i == 102) {
        RefFlush f(293, 214857);
        record = flush(f);
        ASSERT_TRUE(w.AddRecord(record).ok());
      }
    }
    delete f;

    SequentialFile* sf;
    ASSERT_TRUE(env->NewSequentialFile(name, &sf).ok());
    Reader r(sf, nullptr, false, 0);
    Slice record;
    std::string scratch;
    int i = 0;
    while (r.ReadRecord(&record, &scratch)) {
      std::vector<RefSample> rs;
      ASSERT_TRUE(samples(record, &rs));
      ASSERT_EQ(3, rs.size());
      ASSERT_EQ(g_samples[i].ref, rs[0].ref);
      ASSERT_EQ(g_samples[i].t, rs[0].t);
      ASSERT_EQ(g_samples[i].v, rs[0].v);
      ASSERT_EQ(g_samples[i].txn, rs[0].txn);
      ASSERT_EQ(g_samples[i + 1].ref, rs[1].ref);
      ASSERT_EQ(g_samples[i + 1].t, rs[1].t);
      ASSERT_EQ(g_samples[i + 1].v, rs[1].v);
      ASSERT_EQ(g_samples[i + 1].txn, rs[1].txn);
      ASSERT_EQ(g_samples[i + 2].ref, rs[2].ref);
      ASSERT_EQ(g_samples[i + 2].t, rs[2].t);
      ASSERT_EQ(g_samples[i + 2].v, rs[2].v);
      ASSERT_EQ(g_samples[i + 2].txn, rs[2].txn);
      if (i == 102) {
        ASSERT_TRUE(r.ReadRecord(&record, &scratch));
        RefFlush f;
        ASSERT_TRUE(flush(record, &f));
      }
      i += 3;
    }
    ASSERT_EQ(303, i);
    delete sf;
  }

  {
    WritableFile* f;
    ASSERT_TRUE(env->NewAppendableFile(name, &f).ok());
    uint64_t fsize;
    env->GetFileSize(name, &fsize);
    Writer w(f, fsize);
    for (int i = 0; i < 303; i += 3) {
      std::vector<RefSample> v;
      v.emplace_back(i, rand() % 100000, (double)(rand()) / (double)(RAND_MAX),
                     i);
      g_samples.push_back(v.back());
      v.emplace_back(i + 1, rand() % 100000,
                     (double)(rand()) / (double)(RAND_MAX), i + 1);
      g_samples.push_back(v.back());
      v.emplace_back(i + 2, rand() % 100000,
                     (double)(rand()) / (double)(RAND_MAX), i + 2);
      g_samples.push_back(v.back());

      std::string record = samples(v);
      ASSERT_TRUE(w.AddRecord(record).ok());
      if (i == 102) {
        RefFlush f(293, 214857);
        record = flush(f);
        ASSERT_TRUE(w.AddRecord(record).ok());
      }
    }
    delete f;

    SequentialFile* sf;
    ASSERT_TRUE(env->NewSequentialFile(name, &sf).ok());
    Reader r(sf, nullptr, false, 0);
    Slice record;
    std::string scratch;
    int i = 0;
    while (r.ReadRecord(&record, &scratch)) {
      std::vector<RefSample> rs;
      ASSERT_TRUE(samples(record, &rs));
      ASSERT_EQ(3, rs.size());
      ASSERT_EQ(g_samples[i].ref, rs[0].ref);
      ASSERT_EQ(g_samples[i].t, rs[0].t);
      ASSERT_EQ(g_samples[i].v, rs[0].v);
      ASSERT_EQ(g_samples[i].txn, rs[0].txn);
      ASSERT_EQ(g_samples[i + 1].ref, rs[1].ref);
      ASSERT_EQ(g_samples[i + 1].t, rs[1].t);
      ASSERT_EQ(g_samples[i + 1].v, rs[1].v);
      ASSERT_EQ(g_samples[i + 1].txn, rs[1].txn);
      ASSERT_EQ(g_samples[i + 2].ref, rs[2].ref);
      ASSERT_EQ(g_samples[i + 2].t, rs[2].t);
      ASSERT_EQ(g_samples[i + 2].v, rs[2].v);
      ASSERT_EQ(g_samples[i + 2].txn, rs[2].txn);
      if (i == 102 || i == 303 + 102) {
        ASSERT_TRUE(r.ReadRecord(&record, &scratch));
        RefFlush f;
        ASSERT_TRUE(flush(record, &f));
        ASSERT_EQ(293, f.ref);
        ASSERT_EQ(214857, f.txn);
      }
      i += 3;
    }
    ASSERT_EQ(606, i);
    delete sf;
  }
}

TEST_F(LogRecordTest, Test3) {
  Env* env = Env::Default();
  name = "head.log";
  boost::filesystem::remove(name);
  {
    WritableFile* f;
    ASSERT_TRUE(env->NewAppendableFile(name, &f).ok());
    Writer w(f);
    ::tsdb::label::Labels glset({{"a", "b"}, {"c", "d"}});
    ::tsdb::label::Labels lset;
    for (int i = 0; i < 100; i++) {
      lset.emplace_back(
          "labellabellabellabellabellabellabellabellabellabellabellabellabellab"
          "el" +
              std::to_string(i),
          "valuevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevalueval"
          "ue" +
              std::to_string(i));
    }
    for (int i = 0; i < 100; i++) {
      RefGroup r;
      r.ref = i;
      r.group_lset = glset;
      r.individual_lsets = {lset, lset, lset};
      std::string record = group({r});
      ASSERT_TRUE(w.AddRecord(record).ok());
    }
    delete f;

    SequentialFile* sf;
    ASSERT_TRUE(env->NewSequentialFile(name, &sf).ok());
    Reader r(sf, nullptr, false, 0);
    Slice record;
    std::string scratch;
    int i = 0;
    while (r.ReadRecord(&record, &scratch)) {
      RefGroup rs;
      ASSERT_TRUE(group(record, &rs));
      ASSERT_EQ(i, rs.ref);
      ASSERT_EQ(glset, rs.group_lset);
      ASSERT_EQ(std::vector<::tsdb::label::Labels>({lset, lset, lset}),
                rs.individual_lsets);
      i++;
    }
    ASSERT_EQ(100, i);
    delete sf;
  }

  // Reopen and append.
  {
    WritableFile* f;
    ASSERT_TRUE(env->NewAppendableFile(name, &f).ok());
    uint64_t fsize;
    env->GetFileSize(name, &fsize);
    Writer w(f, fsize);
    ::tsdb::label::Labels glset({{"a", "b"}, {"c", "d"}});
    ::tsdb::label::Labels lset;
    for (int i = 0; i < 100; i++) {
      lset.emplace_back(
          "labellabellabellabellabellabellabellabellabellabellabellabellabellab"
          "el" +
              std::to_string(i),
          "valuevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevalueval"
          "ue" +
              std::to_string(i));
    }
    for (int i = 100; i < 200; i++) {
      RefGroup r;
      r.ref = i;
      r.group_lset = glset;
      r.individual_lsets = {lset, lset, lset};
      std::string record = group({r});
      ASSERT_TRUE(w.AddRecord(record).ok());
    }
    delete f;

    SequentialFile* sf;
    ASSERT_TRUE(env->NewSequentialFile(name, &sf).ok());
    Reader r(sf, nullptr, false, 0);
    Slice record;
    std::string scratch;
    int i = 0;
    while (r.ReadRecord(&record, &scratch)) {
      RefGroup rs;
      ASSERT_TRUE(group(record, &rs));
      ASSERT_EQ(i, rs.ref);
      ASSERT_EQ(glset, rs.group_lset);
      ASSERT_EQ(std::vector<::tsdb::label::Labels>({lset, lset, lset}),
                rs.individual_lsets);
      i++;
    }
    ASSERT_EQ(200, i);
    delete sf;
  }
}

TEST_F(LogRecordTest, Test4) {
  Env* env = Env::Default();
  name = "head.log";
  boost::filesystem::remove(name);

  std::vector<RefGroupSample> g_samples;
  ::tsdb::label::Labels lset;
  for (int i = 0; i < 100; i++) {
    lset.emplace_back(
        "labellabellabellabellabellabellabellabellabellabellabellabellabellabe"
        "l" +
            std::to_string(i),
        "valuevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevalu"
        "e" +
            std::to_string(i));
  }
  {
    WritableFile* f;
    ASSERT_TRUE(env->NewAppendableFile(name, &f).ok());
    Writer w(f);
    for (int i = 0; i < 100; i++) {
      RefGroupSample s(i, {lset, lset, lset}, rand() % 100000,
                       {(double)(rand()) / (double)(RAND_MAX),
                        (double)(rand()) / (double)(RAND_MAX),
                        (double)(rand()) / (double)(RAND_MAX)},
                       i);

      std::string record = group_sample(s);
      ASSERT_TRUE(w.AddRecord(record).ok());
      if (i == 50) {
        RefFlush f(293, 214857);
        record = flush(f);
        ASSERT_TRUE(w.AddRecord(record).ok());
      }
      g_samples.push_back(s);
    }
    for (int i = 100; i < 200; i++) {
      RefGroupSample s(i, rand() % 100000,
                       {(double)(rand()) / (double)(RAND_MAX),
                        (double)(rand()) / (double)(RAND_MAX),
                        (double)(rand()) / (double)(RAND_MAX)},
                       i);

      std::string record = group_sample(s);
      ASSERT_TRUE(w.AddRecord(record).ok());
      if (i == 150) {
        RefFlush f(293, 214857);
        record = flush(f);
        ASSERT_TRUE(w.AddRecord(record).ok());
      }
      g_samples.push_back(s);
    }
    for (int i = 200; i < 300; i++) {
      RefGroupSample s(i, {0, 1, 2}, rand() % 100000,
                       {(double)(rand()) / (double)(RAND_MAX),
                        (double)(rand()) / (double)(RAND_MAX),
                        (double)(rand()) / (double)(RAND_MAX)},
                       i);

      std::string record = group_sample(s);
      ASSERT_TRUE(w.AddRecord(record).ok());
      if (i == 250) {
        RefFlush f(293, 214857);
        record = flush(f);
        ASSERT_TRUE(w.AddRecord(record).ok());
      }
      g_samples.push_back(s);
    }
    delete f;

    SequentialFile* sf;
    ASSERT_TRUE(env->NewSequentialFile(name, &sf).ok());
    Reader r(sf, nullptr, false, 0);
    Slice record;
    std::string scratch;
    int i = 0;
    while (r.ReadRecord(&record, &scratch)) {
      RefGroupSample rs;
      ASSERT_TRUE(group_sample(record, &rs));
      ASSERT_EQ(g_samples[i].ref, rs.ref);
      ASSERT_EQ(g_samples[i].t, rs.t);
      ASSERT_EQ(g_samples[i].v, rs.v);
      if (i < 100) {
        ASSERT_EQ(3, rs.individual_lsets.size());
        ASSERT_EQ(0, rs.slots.size());
        ASSERT_EQ(std::vector<::tsdb::label::Labels>({lset, lset, lset}),
                  rs.individual_lsets);
      } else if (i < 200) {
        ASSERT_EQ(0, rs.individual_lsets.size());
        ASSERT_EQ(0, rs.slots.size());
      } else {
        ASSERT_EQ(0, rs.individual_lsets.size());
        ASSERT_EQ(3, rs.slots.size());
        ASSERT_EQ(g_samples[i].slots, rs.slots);
      }
      if (i == 50 || i == 150 || i == 250) {
        ASSERT_TRUE(r.ReadRecord(&record, &scratch));
        RefFlush f;
        ASSERT_TRUE(flush(record, &f));
        ASSERT_EQ(293, f.ref);
        ASSERT_EQ(214857, f.txn);
      }
      i++;
    }
    ASSERT_EQ(300, i);
    delete sf;
  }

  {
    WritableFile* f;
    ASSERT_TRUE(env->NewAppendableFile(name, &f).ok());
    uint64_t fsize;
    env->GetFileSize(name, &fsize);
    Writer w(f, fsize);
    for (int i = 0; i < 100; i++) {
      RefGroupSample s(i, {lset, lset, lset}, rand() % 100000,
                       {(double)(rand()) / (double)(RAND_MAX),
                        (double)(rand()) / (double)(RAND_MAX),
                        (double)(rand()) / (double)(RAND_MAX)},
                       i);

      std::string record = group_sample(s);
      ASSERT_TRUE(w.AddRecord(record).ok());
      if (i == 50) {
        RefFlush f(293, 214857);
        record = flush(f);
        ASSERT_TRUE(w.AddRecord(record).ok());
      }
      g_samples.push_back(s);
    }
    for (int i = 100; i < 200; i++) {
      RefGroupSample s(i, rand() % 100000,
                       {(double)(rand()) / (double)(RAND_MAX),
                        (double)(rand()) / (double)(RAND_MAX),
                        (double)(rand()) / (double)(RAND_MAX)},
                       i);

      std::string record = group_sample(s);
      ASSERT_TRUE(w.AddRecord(record).ok());
      if (i == 150) {
        RefFlush f(293, 214857);
        record = flush(f);
        ASSERT_TRUE(w.AddRecord(record).ok());
      }
      g_samples.push_back(s);
    }
    for (int i = 200; i < 300; i++) {
      RefGroupSample s(i, {0, 1, 2}, rand() % 100000,
                       {(double)(rand()) / (double)(RAND_MAX),
                        (double)(rand()) / (double)(RAND_MAX),
                        (double)(rand()) / (double)(RAND_MAX)},
                       i);

      std::string record = group_sample(s);
      ASSERT_TRUE(w.AddRecord(record).ok());
      if (i == 250) {
        RefFlush f(293, 214857);
        record = flush(f);
        ASSERT_TRUE(w.AddRecord(record).ok());
      }
      g_samples.push_back(s);
    }
    delete f;

    SequentialFile* sf;
    ASSERT_TRUE(env->NewSequentialFile(name, &sf).ok());
    Reader r(sf, nullptr, false, 0);
    Slice record;
    std::string scratch;
    int i = 0;
    while (r.ReadRecord(&record, &scratch)) {
      RefGroupSample rs;
      ASSERT_TRUE(group_sample(record, &rs));
      ASSERT_EQ(g_samples[i].ref, rs.ref);
      ASSERT_EQ(g_samples[i].t, rs.t);
      ASSERT_EQ(g_samples[i].v, rs.v);
      if (i < 100) {
        ASSERT_EQ(3, rs.individual_lsets.size());
        ASSERT_EQ(0, rs.slots.size());
        ASSERT_EQ(std::vector<::tsdb::label::Labels>({lset, lset, lset}),
                  rs.individual_lsets);
      } else if (i < 200) {
        ASSERT_EQ(0, rs.individual_lsets.size());
        ASSERT_EQ(0, rs.slots.size());
      } else if (i < 300) {
        ASSERT_EQ(0, rs.individual_lsets.size());
        ASSERT_EQ(3, rs.slots.size());
        ASSERT_EQ(g_samples[i].slots, rs.slots);
      } else if (i < 400) {
        ASSERT_EQ(3, rs.individual_lsets.size());
        ASSERT_EQ(0, rs.slots.size());
        ASSERT_EQ(std::vector<::tsdb::label::Labels>({lset, lset, lset}),
                  rs.individual_lsets);
      } else if (i < 500) {
        ASSERT_EQ(0, rs.individual_lsets.size());
        ASSERT_EQ(0, rs.slots.size());
      } else if (i < 600) {
        ASSERT_EQ(0, rs.individual_lsets.size());
        ASSERT_EQ(3, rs.slots.size());
        ASSERT_EQ(g_samples[i].slots, rs.slots);
      }
      if (i == 50 || i == 150 || i == 250 || i == 350 || i == 450 || i == 550) {
        ASSERT_TRUE(r.ReadRecord(&record, &scratch));
        RefFlush f;
        ASSERT_TRUE(flush(record, &f));
        ASSERT_EQ(293, f.ref);
        ASSERT_EQ(214857, f.txn);
      }
      i++;
    }
    ASSERT_EQ(600, i);
    delete sf;
  }
}

}  // namespace log
}  // namespace leveldb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
