#include "db/partition_index.h"

#include <boost/filesystem.hpp>
#include <ctime>
#include <iostream>
#include <stdlib.h>
#include <unistd.h>

#include "chunk/XORChunk.hpp"
#include "gtest/gtest.h"
#include "label/Label.hpp"
#include "tsdbutil/ThreadPool.hpp"

namespace leveldb {

class PartitionIndexTest : public testing::Test {
 public:
  PartitionMemIndex mem_index_;

  void mem_add(uint64_t id, const ::tsdb::label::Labels& lset, int64_t time) {
    mem_index_.add(id, lset, time);
  }

  void mem_add(uint64_t id, int64_t time) { mem_index_.add(id, time); }
};

TEST_F(PartitionIndexTest, MemIndexTest) {
  int num_ts = 10000;
  int num_labels = 10;
  int num_tuples = 1000;

  std::unordered_set<std::string> syms;

  for (int i = 0; i < num_ts; i++) {
    ::tsdb::label::Labels lset;
    for (int j = 0; j < num_labels; j++) {
      lset.emplace_back("label" + std::to_string(j),
                        "label" + std::to_string(j) + "_" + std::to_string(i));
      syms.insert("label" + std::to_string(j));
      syms.insert("label" + std::to_string(j) + "_" + std::to_string(i));
    }
    mem_add(i, lset, 0);
  }

  for (int t = 1; t < num_tuples; t++) {
    for (int i = 0; i < num_ts; i++) mem_add(i, t * 1000);
  }

  // Validate symbols.
  ASSERT_EQ(mem_index_.symbols(), syms);

  // Validate postings.
  std::pair<std::unique_ptr<::tsdb::index::PostingsInterface>, bool> pair =
      mem_index_.postings("label1", "label1_0");
  ASSERT_TRUE(pair.second);
  std::vector<uint64_t> result_ids;
  while (pair.first->next()) result_ids.push_back(pair.first->at());
  ASSERT_EQ(result_ids, std::vector<uint64_t>({0}));
  pair = mem_index_.postings("label1", "label2_0");
  ASSERT_FALSE(pair.second);

  // Validate label values.
  std::unique_ptr<::tsdb::tsdbutil::StringTuplesInterface> lvals_ptr =
      mem_index_.label_values("label0");
  ASSERT_EQ(num_ts, lvals_ptr->len());
  std::vector<std::string> lvals;
  for (int i = 0; i < num_ts; i++)
    lvals.push_back("label0_" + std::to_string(i));
  std::sort(lvals.begin(), lvals.end());
  for (int i = 0; i < num_ts; i++) ASSERT_EQ(lvals[i], lvals_ptr->at(i));

  // Validate label names.
  std::vector<std::string> result_lnames = mem_index_.label_names();
  std::sort(result_lnames.begin(), result_lnames.end());
  std::vector<std::string> lnames;
  for (int i = 0; i < num_labels; i++)
    lnames.push_back("label" + std::to_string(i));
  std::sort(lnames.begin(), lnames.end());
  ASSERT_EQ(lnames, result_lnames);

  // Validate series.
  std::vector<int64_t> result_times;
  ::tsdb::label::Labels result_lset;
  ASSERT_TRUE(mem_index_.series(0, result_lset, result_times));
  ::tsdb::label::Labels lset;
  for (int i = 0; i < num_labels; i++)
    lset.emplace_back("label" + std::to_string(i),
                      "label" + std::to_string(i) + "_" + std::to_string(0));
  std::vector<int64_t> times;
  for (int i = 0; i < num_tuples; i++) times.push_back(i * 1000);
  ASSERT_EQ(lset, result_lset);
  ASSERT_EQ(times, result_times);
}

void mem_index_test_helper(PartitionMemIndex* mem_index, int num_ts,
                           int num_labels, int num_tuples) {
  // Validate series.
  for (int ts = 0; ts < num_ts; ts++) {
    std::vector<int64_t> result_times;
    ::tsdb::label::Labels result_lset;
    if (!mem_index->series(ts, result_lset, result_times)) {
      std::cout << "series not found\n";
      return;
    }
    ::tsdb::label::Labels lset;
    for (int i = 0; i < num_labels; i++)
      lset.emplace_back("label" + std::to_string(i),
                        "label" + std::to_string(i) + "_" + std::to_string(ts));
    std::vector<int64_t> times;
    for (int i = 0; i < num_tuples; i++) times.push_back(i * 1000);
    if (::tsdb::label::lbs_compare(lset, result_lset)) {
      std::cout << "label not matched\n";
      return;
    }
    if (times.size() != result_times.size()) {
      std::cout << "times size not matched\n";
      return;
    }
    for (int i = 0; i < times.size(); i++) {
      if (times[i] != result_times[i]) {
        std::cout << "time not matched\n";
        return;
      }
    }
  }
  std::cout << "Validation Finished\n";
}

TEST_F(PartitionIndexTest, MemIndexTestMultiThread) {
  int num_ts = 10000;
  int num_labels = 10;
  int num_tuples = 1000;

  for (int i = 0; i < num_ts; i++) {
    ::tsdb::label::Labels lset;
    for (int j = 0; j < num_labels; j++) {
      lset.emplace_back("label" + std::to_string(j),
                        "label" + std::to_string(j) + "_" + std::to_string(i));
    }
    mem_add(i, lset, 0);
  }

  for (int t = 1; t < num_tuples; t++) {
    for (int i = 0; i < num_ts; i++) mem_add(i, t * 1000);
  }

  ::tsdb::tsdbutil::TestThreadPool pool(8);

  for (int i = 0; i < 8; i++) {
    pool.enqueue(std::bind(mem_index_test_helper, &mem_index_, num_ts,
                           num_labels, num_tuples));
  }
  sleep(10);
}

class PartitionDiskIndexWriterTest : public testing::Test {
 public:
  PartitionMemIndex mem_index_;
  std::unique_ptr<PartitionDiskIndexWriter> writer_;

  void mem_add(uint64_t id, const ::tsdb::label::Labels& lset, int64_t time) {
    mem_index_.add(id, lset, time);
  }

  void mem_add(uint64_t id, int64_t time) { mem_index_.add(id, time); }

  void mem_flush(const std::vector<uint32_t>* ssts = nullptr,
                 const std::vector<uint64_t>* tsids = nullptr) {
    writer_->write(mem_index_, ssts, tsids);
    mem_index_.clear();
  }

  void write_timeseries(int64_t time, uint64_t tsid,
                        const std::vector<int64_t>& timestamps,
                        const std::vector<double>& values) {
    Env* env = Env::Default();
    WritableFile* f;
    Status s = env->NewWritableFile(
        std::to_string(time) + "/" + std::to_string(tsid), &f);
    if (!s.ok()) {
      std::cout << s.ToString() << std::endl;
      return;
    }

    std::string data;

    // Write header.
    PutFixed32(&data, timestamps.size());
    for (size_t i = 0; i < timestamps.size(); i += CLOUD_TUPLE_SIZE) {
      PutFixed64(&data, timestamps[i]);
      PutFixed32(&data, 0);  // Uninitialized offset.
    }
    uint32_t off = data.size();

    // Write data.
    int tuple_idx = 0;
    for (size_t i = 0; i < timestamps.size(); i += CLOUD_TUPLE_SIZE) {
      tsdb::chunk::XORChunk c;
      auto app = c.appender();
      size_t end = i + CLOUD_TUPLE_SIZE;
      if (end > timestamps.size()) end = timestamps.size();
      for (size_t j = i; j < end; j++) app->append(timestamps[j], values[j]);
      // auto v = c.bytes_vector();
      // for (size_t j = 0; j < v->size(); j++) {
      //   data.push_back(v->at(j));
      // }
      data.append(reinterpret_cast<const char*>(c.bytes() + 2), c.size() - 2);
      EncodeFixed32(&data[12 + (tuple_idx++) * 12], off);
      off += c.size() - 2;
    }

    s = f->Append(data);
    if (!s.ok()) {
      std::cout << s.ToString() << std::endl;
      return;
    }
    s = f->Sync();
    if (!s.ok()) {
      std::cout << s.ToString() << std::endl;
      return;
    }
    s = f->Close();
    if (!s.ok()) {
      std::cout << s.ToString() << std::endl;
      return;
    }
    delete f;
  }

  void setup_mem_index(int64_t time) { mem_index_.set_time_boundary(time); }

  void setup_writer(int64_t time, int64_t interval) {
    writer_.reset(
        new PartitionDiskIndexWriter(std::to_string(time), interval, 16));
  }
};

void reader_test_helper(PartitionDiskIndexReader* reader, int num_ts,
                        int num_labels, int num_tuples) {
  // Validate series.
  for (int ts = 0; ts < 100; ts++) {
    std::vector<int64_t> result_times;
    ::tsdb::label::Labels result_lset;
    std::pair<std::unique_ptr<::tsdb::index::PostingsInterface>, bool> pair =
        reader->postings_and_tsids("label1", "label1_" + std::to_string(ts));
    ASSERT_TRUE(pair.second);
    pair.first->next();
    uint64_t ref, id;
    pair.first->at(&ref, &id);
    if (id != ts) {
      std::cout << "tsid not matched "
                << "exp:" << ts << " got:" << id << "\n";
      return;
    }
    if (!reader->series(ref, result_lset, &id)) {
      std::cout << "series not found\n";
      return;
    }
    if (id != ts) {
      std::cout << "tsid not matched "
                << "exp:" << ts << " got:" << id << "\n";
      return;
    }
    ::tsdb::label::Labels lset;
    for (int i = 0; i < num_labels; i++)
      lset.emplace_back("label" + std::to_string(i),
                        "label" + std::to_string(i) + "_" + std::to_string(ts));
    std::vector<int64_t> times;
    for (int i = 0; i < num_tuples; i++) times.push_back(i * 1000);
    if (::tsdb::label::lbs_compare(lset, result_lset)) {
      std::cout << "label not matched\n";
      return;
    }

    auto p = reader->timestamps(ts);
    if (!p) {
      std::cout << "error get timestamps\n";
      return;
    }
    while (p->next()) result_times.push_back(p->at());
    if (times.size() != result_times.size()) {
      std::cout << "times size not matched\n";
      return;
    }
    for (int i = 0; i < times.size(); i++) {
      if (times[i] != result_times[i]) {
        std::cout << "time not matched\n";
        return;
      }
    }
  }
  std::cout << "Validation Finished\n";
}

TEST_F(PartitionDiskIndexWriterTest, SinglePartition) {
  boost::filesystem::remove_all("0");
  int num_ts = 10000;
  int num_labels = 10;
  int num_tuples = 1000;
  setup_mem_index(0);
  std::set<std::string> syms;

  for (int i = 0; i < num_ts; i++) {
    ::tsdb::label::Labels lset;
    for (int j = 0; j < num_labels; j++) {
      lset.emplace_back("label" + std::to_string(j),
                        "label" + std::to_string(j) + "_" + std::to_string(i));
      syms.insert("label" + std::to_string(j));
      syms.insert("label" + std::to_string(j) + "_" + std::to_string(i));
    }
    mem_add(i, lset, 0);
  }

  for (int t = 1; t < num_tuples; t++) {
    for (int i = 0; i < num_ts; i++) mem_add(i, t * 1000);
  }

  std::vector<uint32_t> ssts({10, 20, 30});
  std::vector<uint64_t> sst_tsids({100, 200, 300});
  setup_writer(0, num_tuples * 1000);
  mem_flush(&ssts, &sst_tsids);

  // Validate reader.
  PartitionDiskIndexReader reader("0", num_tuples * 1000, 16);
  ASSERT_TRUE(reader.status().ok());

  // Validate symbols.
  ASSERT_EQ(reader.symbols(), syms);

  // Validate postings.
  std::pair<std::unique_ptr<::tsdb::index::PostingsInterface>, bool> pair =
      reader.postings_and_tsids("label1", "label1_2");
  ASSERT_TRUE(pair.second);
  pair.first->next();
  uint64_t ref, id;
  pair.first->at(&ref, &id);
  pair = reader.postings_and_tsids("label1", "label2_0");
  ASSERT_FALSE(pair.second);

  // Validate label values.
  std::unique_ptr<::tsdb::tsdbutil::StringTuplesInterface> lvals_ptr =
      reader.label_values({"label0"});
  ASSERT_EQ(num_ts, lvals_ptr->len());
  std::vector<std::string> lvals;
  for (int i = 0; i < num_ts; i++)
    lvals.push_back("label0_" + std::to_string(i));
  std::sort(lvals.begin(), lvals.end());
  for (int i = 0; i < num_ts; i++) ASSERT_EQ(lvals[i], lvals_ptr->at(i));

  // Validate label names.
  std::deque<std::string> result_lnames = reader.label_names();
  std::sort(result_lnames.begin(), result_lnames.end());
  std::deque<std::string> lnames;
  for (int i = 0; i < num_labels; i++)
    lnames.push_back("label" + std::to_string(i));
  std::sort(lnames.begin(), lnames.end());
  ASSERT_EQ(lnames, result_lnames);

  // Validate series.
  ::tsdb::label::Labels result_lset;
  ASSERT_TRUE(reader.series(ref, result_lset, &id));
  ASSERT_EQ(2, id);
  ::tsdb::label::Labels lset;
  for (int i = 0; i < num_labels; i++)
    lset.emplace_back("label" + std::to_string(i),
                      "label" + std::to_string(i) + "_" + std::to_string(2));
  ASSERT_EQ(lset, result_lset);
  std::vector<int64_t> times;
  std::vector<int64_t> result_times;
  auto p = reader.timestamps(id);
  while (p->next()) result_times.push_back(p->at());
  for (int i = 0; i < num_tuples; i++) times.push_back(i * 1000);
  ASSERT_EQ(times.size(), result_times.size());
  ASSERT_EQ(times, result_times);

  // Validate ssts.
  ASSERT_EQ(ssts, reader.get_ssts());
  ASSERT_EQ(sst_tsids, reader.get_sst_tsids());
  ASSERT_EQ(10, reader.get_sst(89));
  ASSERT_EQ(10, reader.get_sst(100));
  ASSERT_EQ(20, reader.get_sst(101));
  ASSERT_EQ(20, reader.get_sst(200));
  ASSERT_EQ(30, reader.get_sst(250));

  // Multi thread.
  ::tsdb::tsdbutil::TestThreadPool pool(8);
  for (int i = 0; i < 8; i++) {
    pool.enqueue(
        std::bind(reader_test_helper, &reader, num_ts, num_labels, num_tuples));
  }
  sleep(10);
}

// TEST_F(PartitionDiskIndexWriterTest, WriteSeriesTest) {
//   std::vector<int64_t> timestamps;
//   std::vector<double> values;
//   for (int i = 848; i < 856; i++) {
//     timestamps.push_back(i);
//     values.push_back(i);
//   }

//   std::string data;
//   tsdb::chunk::XORChunk c;
//   auto app = c.appender();
//   for (size_t j = 0; j < timestamps.size(); j++)
//     app->append(timestamps[j], values[j]);
//   std::cout << "c.size():" << c.size() << std::endl;
//   auto v = c.bytes_vector();
//   for (size_t j = 0; j < v->size(); j++) {
//     std::cout << j << std::endl;
//     std::cout << (int)(v->at(j)) << std::endl;
//     data.push_back(v->at(j));
//   }
// }

TEST_F(PartitionDiskIndexWriterTest, SinglePartitionCloudReader) {
  boost::filesystem::remove_all("0");
  int num_ts = 10000;
  int num_labels = 10;
  int num_tuples = 1000;
  setup_mem_index(0);
  std::set<std::string> syms;

  for (int i = 0; i < num_ts; i++) {
    ::tsdb::label::Labels lset;
    for (int j = 0; j < num_labels; j++) {
      lset.emplace_back("label" + std::to_string(j),
                        "label" + std::to_string(j) + "_" + std::to_string(i));
      syms.insert("label" + std::to_string(j));
      syms.insert("label" + std::to_string(j) + "_" + std::to_string(i));
    }
    mem_add(i, lset, 0);
  }

  for (int t = 1; t < num_tuples; t++) {
    for (int i = 0; i < num_ts; i++) mem_add(i, t * 1000);
  }

  std::vector<uint32_t> ssts({10, 20, 30});
  std::vector<uint64_t> sst_tsids({100, 200, 300});
  setup_writer(0, num_tuples * 1000);
  mem_flush(&ssts, &sst_tsids);

  // Write 10 timeseries.
  std::vector<int64_t> timestamps;
  std::vector<double> values;
  for (int i = 0; i < 1000; i++) {
    timestamps.push_back(i);
    values.push_back(i);
  }
  for (int i = 0; i < 10; i++) write_timeseries(0, i, timestamps, values);

  // Validate reader.
  PartitionCloudIndexReader reader("0", num_tuples * 1000);
  ASSERT_TRUE(reader.status().ok());

  // Validate symbols.
  ASSERT_EQ(reader.symbols(), syms);

  // Validate postings.
  std::pair<std::unique_ptr<::tsdb::index::PostingsInterface>, bool> pair =
      reader.postings_and_tsids("label1", "label1_2");
  ASSERT_TRUE(pair.second);
  pair.first->next();
  uint64_t ref, id;
  pair.first->at(&ref, &id);
  pair = reader.postings_and_tsids("label1", "label2_0");
  ASSERT_FALSE(pair.second);

  // Validate label values.
  std::unique_ptr<::tsdb::tsdbutil::StringTuplesInterface> lvals_ptr =
      reader.label_values({"label0"});
  ASSERT_EQ(num_ts, lvals_ptr->len());
  std::vector<std::string> lvals;
  for (int i = 0; i < num_ts; i++)
    lvals.push_back("label0_" + std::to_string(i));
  std::sort(lvals.begin(), lvals.end());
  for (int i = 0; i < num_ts; i++) ASSERT_EQ(lvals[i], lvals_ptr->at(i));

  // Validate label names.
  std::deque<std::string> result_lnames = reader.label_names();
  std::sort(result_lnames.begin(), result_lnames.end());
  std::deque<std::string> lnames;
  for (int i = 0; i < num_labels; i++)
    lnames.push_back("label" + std::to_string(i));
  std::sort(lnames.begin(), lnames.end());
  ASSERT_EQ(lnames, result_lnames);

  // Validate series.
  ::tsdb::label::Labels result_lset;
  ASSERT_TRUE(reader.series(ref, result_lset, &id));
  ASSERT_EQ(2, id);
  ::tsdb::label::Labels lset;
  for (int i = 0; i < num_labels; i++)
    lset.emplace_back("label" + std::to_string(i),
                      "label" + std::to_string(i) + "_" + std::to_string(2));
  ASSERT_EQ(lset, result_lset);

  std::vector<int64_t> times;
  std::vector<int64_t> result_times = reader.timestamps(0);
  for (int i = 0; i < 1000; i += CLOUD_TUPLE_SIZE) times.push_back(i);
  ASSERT_EQ(times.size(), result_times.size());
  ASSERT_EQ(times, result_times);

  // Validate iterator.
  std::cout << "before validating iterator" << std::endl;
  CloudTimeseriesFileIterator* it = reader.iterator(9, 222, 888);
  int i = 222;
  while (it->next()) {
    auto p = it->at();
    ASSERT_EQ((int64_t)(i), p.first);
    ASSERT_EQ((double)(i), p.second);
    i++;
  }
  ASSERT_EQ(889, i);

  delete it;
  it = reader.iterator(9, 222, 888);
  it->seek(444);
  i = 444;
  auto p = it->at();
  ASSERT_EQ((int64_t)(i), p.first);
  ASSERT_EQ((double)(i), p.second);
  i++;
  while (it->next()) {
    p = it->at();
    ASSERT_EQ((int64_t)(i), p.first);
    ASSERT_EQ((double)(i), p.second);
    i++;
  }
  ASSERT_EQ(889, i);

  delete it;
  it = reader.iterator(9, -1, 1200);
  i = 0;
  while (it->next()) {
    p = it->at();
    ASSERT_EQ((int64_t)(i), p.first);
    ASSERT_EQ((double)(i), p.second);
    i++;
  }
  ASSERT_EQ(1000, i);

  delete it;
  it = reader.iterator(9, -1, 1200);
  it->seek(444);
  i = 444;
  p = it->at();
  ASSERT_EQ((int64_t)(i), p.first);
  ASSERT_EQ((double)(i), p.second);
  i++;
  while (it->next()) {
    p = it->at();
    ASSERT_EQ((int64_t)(i), p.first);
    ASSERT_EQ((double)(i), p.second);
    i++;
  }
  ASSERT_EQ(1000, i);

  delete it;
  it = reader.iterator(9, -10, 500);
  i = 0;
  while (it->next()) {
    p = it->at();
    ASSERT_EQ((int64_t)(i), p.first);
    ASSERT_EQ((double)(i), p.second);
    i++;
  }
  ASSERT_EQ(501, i);

  delete it;
  it = reader.iterator(9, 10, 1300);
  i = 10;
  while (it->next()) {
    p = it->at();
    ASSERT_EQ((int64_t)(i), p.first);
    ASSERT_EQ((double)(i), p.second);
    i++;
  }
  ASSERT_EQ(1000, i);
}

// TEST_F(PartitionDiskIndexWriterTest, SinglePartitionSize) {
//   boost::filesystem::remove_all("0");
//   int num_ts = 100000;
//   int num_labels = 20;
//   int num_tuples = 1440;
//   setup_mem_index(0);
//   std::set<std::string> syms;

//   for (int i = 0; i < num_ts; i++) {
//     ::tsdb::label::Labels lset;
//     for (int j = 0; j < num_labels; j++) {
//       lset.emplace_back("label" + std::to_string(j), "label" +
//       std::to_string(j) + "_" + std::to_string(i)); syms.insert("label" +
//       std::to_string(j)); syms.insert("label" + std::to_string(j) + "_" +
//       std::to_string(i));
//     }
//     mem_add(i, lset, 0);
//   }

//   srand((unsigned) time(0));
//   for (int t = 1; t < num_tuples; t++) {
//     for (int i = 0; i < num_ts; i++)
//       mem_add(i, t * 1000 + (rand() % 100));
//   }

//   setup_writer(0, num_tuples * 1000);
//   mem_flush();
// }

TEST_F(PartitionDiskIndexWriterTest, MultiPartitions) {
  int num_ts = 10000;
  int num_labels = 10;
  int num_tuples = 1000;
  int num_partitions = 3;
  std::set<std::string> syms;

  std::vector<int64_t> boundaries;
  for (int partition = 0; partition < num_partitions; partition++) {
    boost::filesystem::remove_all(
        std::to_string(partition * num_tuples * 1000));
    setup_mem_index(partition * num_tuples * 1000);
    boundaries.push_back(partition * num_tuples * 1000);

    for (int i = 0; i < num_ts; i++) {
      ::tsdb::label::Labels lset;
      for (int j = 0; j < num_labels; j++) {
        lset.emplace_back(
            "label" + std::to_string(j),
            "label" + std::to_string(j) + "_" + std::to_string(i));
        syms.insert("label" + std::to_string(j));
        syms.insert("label" + std::to_string(j) + "_" + std::to_string(i));
      }
      mem_add(i, lset, partition * num_tuples * 1000);
    }

    for (int t = 1; t < num_tuples; t++) {
      for (int i = 0; i < num_ts; i++)
        mem_add(i, t * 1000 + partition * num_tuples * 1000);
    }

    setup_writer(partition * num_tuples * 1000, num_tuples * 1000);
    mem_flush();
  }

  // Validate merge.
  PartitionDiskIndexMerger merger(".", boundaries, num_tuples * 1000, 16);
  ASSERT_TRUE(merger.status().ok());
  Status s = merger.merge(true);
  std::cout << s.ToString() << std::endl;
  ASSERT_TRUE(s.ok());

  // Validate reader.
  PartitionDiskIndexReader reader("0", num_partitions * num_tuples * 1000, 16);

  // Validate symbols.
  ASSERT_EQ(reader.symbols(), syms);

  // Validate postings.
  std::pair<std::unique_ptr<::tsdb::index::PostingsInterface>, bool> pair =
      reader.postings_and_tsids("label1", "label1_2");
  ASSERT_TRUE(pair.second);
  pair.first->next();
  uint64_t ref, id;
  pair.first->at(&ref, &id);
  pair = reader.postings_and_tsids("label1", "label2_0");
  ASSERT_FALSE(pair.second);

  // Validate label values.
  std::unique_ptr<::tsdb::tsdbutil::StringTuplesInterface> lvals_ptr =
      reader.label_values({"label0"});
  ASSERT_EQ(num_ts, lvals_ptr->len());
  std::vector<std::string> lvals;
  for (int i = 0; i < num_ts; i++)
    lvals.push_back("label0_" + std::to_string(i));
  std::sort(lvals.begin(), lvals.end());
  for (int i = 0; i < num_ts; i++) ASSERT_EQ(lvals[i], lvals_ptr->at(i));

  // Validate label names.
  std::deque<std::string> result_lnames = reader.label_names();
  std::sort(result_lnames.begin(), result_lnames.end());
  std::deque<std::string> lnames;
  for (int i = 0; i < num_labels; i++)
    lnames.push_back("label" + std::to_string(i));
  std::sort(lnames.begin(), lnames.end());
  ASSERT_EQ(lnames, result_lnames);

  // Validate series.
  ::tsdb::label::Labels result_lset;
  ASSERT_TRUE(reader.series(ref, result_lset, &id));
  ASSERT_EQ(2, id);
  ::tsdb::label::Labels lset;
  for (int i = 0; i < num_labels; i++)
    lset.emplace_back("label" + std::to_string(i),
                      "label" + std::to_string(i) + "_" + std::to_string(2));
  ASSERT_EQ(lset, result_lset);
  std::vector<int64_t> times;
  std::vector<int64_t> result_times;
  auto p = reader.timestamps(id);
  while (p->next()) result_times.push_back(p->at());
  for (int i = 0; i < num_tuples * num_partitions; i++)
    times.push_back(i * 1000);
  ASSERT_EQ(times.size(), result_times.size());
  ASSERT_EQ(times, result_times);

  // Multi thread.
  ::tsdb::tsdbutil::TestThreadPool pool(8);
  for (int i = 0; i < 8; i++) {
    pool.enqueue(std::bind(reader_test_helper, &reader, num_ts, num_labels,
                           num_tuples * num_partitions));
  }
  sleep(10);
}

}  // namespace leveldb.

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  ::testing::GTEST_FLAG(filter) =
      "PartitionDiskIndexWriterTest.SinglePartitionCloudReader";
  return RUN_ALL_TESTS();
}
