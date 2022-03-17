#include "cloud/cloud_cache.h"

#include "leveldb/cache.h"
#include "leveldb/cloud/db_cloud.h"
#include "leveldb/env.h"

#include "third_party/thread_pool.h"
#include "util/random.h"
#include "util/testutil.h"

#include "cloud/aws/aws_env.h"
#include "cloud/aws/aws_file.h"

int CLOUD_CACHE_TEST_THREAD_NUM = 4;

namespace leveldb {

class FragmentMapTest : public testing::Test {
 public:
  FragmentMapTest() : m(1024 * 1024) {}

 protected:
  FragmentMap m;
};

TEST_F(FragmentMapTest, Test1) {
  m.insert(129, 32, 1);
  m.insert(1, 32, 2);
  m.insert(199, 32, 1);
  m.insert(1028, 32, 2);
  m.insert(512, 32, 1);

  ASSERT_EQ(1, m.lookup(129));
  ASSERT_EQ(2, m.lookup(1));
  ASSERT_EQ(1, m.lookup(199));
  ASSERT_EQ(2, m.lookup(1028));
  ASSERT_EQ(1, m.lookup(512));
  ASSERT_EQ(0, m.lookup(128));
}

class PersistentCacheTest : public testing::Test {
 public:
  PersistentCacheTest() : env(Env::Default()), pc(nullptr) {}
  ~PersistentCacheTest() {
    if (pc) delete pc;
  }

  void Init(uint64_t cache_size, uint64_t block_size) {
    if (pc) delete pc;
    pc = new PersistentCacheManager(env, cache_size, block_size);
  }

 protected:
  Env* env;
  PersistentCacheManager* pc;
};

TEST_F(PersistentCacheTest, SingleThread) {
  Init(32, 16);

  for (int i = 0; i < 8; i++) {
    char buf[16];
    sprintf(buf, "%04d", i);
    ASSERT_EQ(true, pc->insert_page(i, buf, 4).ok());
  }
  for (int i = 0; i < 8; i++) {
    char buf[16];
    sprintf(buf, "%04d", i);
    char read_buf[16];
    ASSERT_EQ(true, pc->get_page(i, read_buf, 4).ok());
    ASSERT_EQ(std::string(buf, 4), std::string(read_buf, 4));
  }

  ASSERT_EQ(Status::NotFound().code(), pc->get_page(9, nullptr, 4).code());

  for (int i = 8; i < 11; i++) {
    char buf[16];
    sprintf(buf, "%04d", i);
    ASSERT_EQ(true, pc->insert_page(i, buf, 4).ok());
  }
  for (int i = 0; i < 11; i++) {
    char buf[16];
    sprintf(buf, "%04d", i);
    char read_buf[16];
    ASSERT_EQ(true, pc->get_page(i, read_buf, 4).ok());
    ASSERT_EQ(std::string(buf, 4), std::string(read_buf, 4));
  }
  for (int i = 11; i < 15; i++) {
    char buf[16];
    sprintf(buf, "%04d", i);
    ASSERT_EQ(true, pc->insert_page(i, buf, 4).ok());
  }
  for (int i = 0; i < 4; i++) {
    ASSERT_EQ(Status::NotFound().code(), pc->get_page(i, nullptr, 4).code());
  }
  for (int i = 4; i < 15; i++) {
    char buf[16];
    sprintf(buf, "%04d", i);
    char read_buf[16];
    ASSERT_EQ(true, pc->get_page(i, read_buf, 4).ok());
    ASSERT_EQ(std::string(buf, 4), std::string(read_buf, 4));
  }
}

TEST_F(PersistentCacheTest, MultiThread1) {
  Init(16 * 1024, 512);

  ThreadPool pool(CLOUD_CACHE_TEST_THREAD_NUM);
  std::atomic<int> p(0);

  for (int i = 0; i < CLOUD_CACHE_TEST_THREAD_NUM; i++)
    pool.enqueue(std::bind(
        [](PersistentCacheManager* pc_, std::atomic<int>* p_) {
          int pid = p_->fetch_add(1);
          while (pid < 4096) {
            char buf[16];
            sprintf(buf, "%04d", pid);
            ASSERT_EQ(true, pc_->insert_page(pid, buf, 4).ok());
            pid = p_->fetch_add(1);
          }
        },
        pc, &p));
  pool.wait_barrier();

  p.store(0);
  for (int i = 0; i < CLOUD_CACHE_TEST_THREAD_NUM; i++)
    pool.enqueue(std::bind(
        [](PersistentCacheManager* pc_, std::atomic<int>* p_) {
          int pid = p_->fetch_add(1);
          while (pid < 4096) {
            char buf[16];
            sprintf(buf, "%04d", pid);
            char read_buf[16];
            ASSERT_EQ(true, pc_->get_page(pid, read_buf, 4).ok());
            ASSERT_EQ(std::string(buf, 4), std::string(read_buf, 4));
            pid = p_->fetch_add(1);
          }
        },
        pc, &p));
  pool.wait_barrier();
}

// Read and write at the same time.
TEST_F(PersistentCacheTest, MultiThread2) {
  Init(1024, 512);

  ThreadPool pool(16);
  std::atomic<int> p(0);

  for (int i = 0; i < 8; i++)
    pool.enqueue(std::bind(
        [](PersistentCacheManager* pc_, int seed) {
          Random r(seed);
          for (int j = 0; j < 10000; j++) {
            int pid = r.Next() % 9999;
            char buf[16];
            sprintf(buf, "%04d", pid);
            ASSERT_EQ(true, pc_->insert_page(pid, buf, 4).ok());
          }
        },
        pc, i));

  for (int i = 0; i < 8; i++)
    pool.enqueue(std::bind(
        [](PersistentCacheManager* pc_, int seed) {
          Random r(seed);
          for (int j = 0; j < 10000; j++) {
            int pid = r.Next() % 9999;
            char buf[16];
            sprintf(buf, "%04d", pid);
            char read_buf[16];
            pc_->get_page(pid, read_buf, 4);
          }
        },
        pc, (i + 10) * (i + 10)));
  pool.wait_barrier();
}

class CloudCacheTest : public testing::Test {
 public:
  CloudCacheTest() : c(nullptr) {}
  ~CloudCacheTest() {
    if (c) delete c;
  }

  void Init(const std::shared_ptr<Cache>& cache,
            uint64_t cache_size = 1 * 1024 * 1024 * 1024,
            uint64_t block_size = 1 * 1024 * 1024, Env* env = nullptr) {
    c = new CloudCache(cache, cache_size, block_size, env);
  }

 protected:
  CloudCache* c;
};

TEST_F(CloudCacheTest, TestClean) {
  Init(std::shared_ptr<Cache>(NewLRUCache(2048 * 4)));
  c->add_file("file", 2048 * 8);
  c->insert("file", 0, 4, (void*)("hell"), 4, [](const Slice&, void* value) {});
  c->print_summary(true);
  c->clean();
  c->print_summary(true);
  c->add_file("file", 2048 * 8);
  c->insert("file", 0, 4, (void*)("hell"), 4, [](const Slice&, void* value) {});
  c->print_summary(true);
}

TEST_F(CloudCacheTest, MultiThread1) {
  // Without persistent cache.
  Init(std::shared_ptr<Cache>(NewLRUCache(2048 * 4)));

  c->add_file("file", 2048 * 8);
  ThreadPool pool(CLOUD_CACHE_TEST_THREAD_NUM);

  std::atomic<int> page_id(0);
  for (int i = 0; i < CLOUD_CACHE_TEST_THREAD_NUM; i++) {
    pool.enqueue(std::bind(
        [](CloudCache* cache, std::atomic<int>* pid) {
          int page = pid->fetch_add(1);
          while (page < 2048) {
            char* buf = new char[16];
            sprintf(buf, "%04d", page);
            cache->insert(
                "file", page * 4, 4, buf, 4,
                [](const Slice&, void* value) { delete (char*)(value); });
            page = pid->fetch_add(1);
          }
        },
        c, &page_id));
  }
  pool.wait_barrier();

  for (int i = 0; i < CLOUD_CACHE_TEST_THREAD_NUM; i++) {
    pool.enqueue(std::bind(
        [](CloudCache* cache) {
          for (int j = 0; j < 2048; j++) {
            char buf[16];
            sprintf(buf, "%04d", j);
            Cache::Handle* h = cache->lookup("file", j * 4, 4);
            ASSERT_EQ(std::string(buf, 4),
                      std::string((char*)(cache->value(h)), 4));
            cache->release(h);
          }
        },
        c));
  }
  pool.wait_barrier();
}

TEST_F(CloudCacheTest, MultiThread2) {
  // Without persistent cache, read/write mixed.
  Init(std::shared_ptr<Cache>(NewLRUCache(2048 * 4)));

  c->add_file("file", 2048 * 8);
  ThreadPool pool(CLOUD_CACHE_TEST_THREAD_NUM);

  std::atomic<int> page_id(0);
  for (int i = 0; i < CLOUD_CACHE_TEST_THREAD_NUM; i++) {
    pool.enqueue(std::bind(
        [](CloudCache* cache, std::atomic<int>* pid) {
          int page = pid->fetch_add(1);
          std::vector<int> inserted_pages;
          while (page < 2048) {
            inserted_pages.push_back(page);
            char* buf = new char[16];
            sprintf(buf, "%04d", page);
            cache->insert(
                "file", page * 4, 4, buf, 4,
                [](const Slice&, void* value) { delete (char*)(value); });
            page = pid->fetch_add(1);

            if ((int)(inserted_pages.size()) % 16 == 0) {
              for (int j : inserted_pages) {
                char bu2[16];
                sprintf(bu2, "%04d", j);
                Cache::Handle* h = cache->lookup("file", j * 4, 4);
                if (h == nullptr) {
                  printf("nullptr page:%d\n", j);
                }
                ASSERT_EQ(std::string(bu2, 4),
                          std::string((char*)(cache->value(h)), 4));
                cache->release(h);
              }
              inserted_pages.clear();
            }
          }
        },
        c, &page_id));
  }
  pool.wait_barrier();
}

TEST_F(CloudCacheTest, MultiThread3) {
  // With persistent cache.
  Init(std::shared_ptr<Cache>(NewLRUCache(2048 * 4)), 16 * 1024, 512,
       Env::Default());

  c->add_file("file", 2048 * 8);
  ThreadPool pool(CLOUD_CACHE_TEST_THREAD_NUM);

  std::atomic<int> page_id(0);
  for (int i = 0; i < CLOUD_CACHE_TEST_THREAD_NUM; i++) {
    pool.enqueue(std::bind(
        [](CloudCache* cache, std::atomic<int>* pid) {
          int page = pid->fetch_add(1);
          while (page < 2048 * 2) {
            char* buf = new char[16];
            sprintf(buf, "%04d", page);
            cache->insert(
                "file", page * 4, 4, buf, 4,
                [](const Slice&, void* value) { delete (char*)(value); });
            page = pid->fetch_add(1);
          }
        },
        c, &page_id));
  }
  pool.wait_barrier();
  std::cout << "Finish insert" << std::endl;

  for (int i = 0; i < CLOUD_CACHE_TEST_THREAD_NUM; i++) {
    pool.enqueue(std::bind(
        [](CloudCache* cache) {
          for (int j = 0; j < 2048 * 2; j++) {
            char buf[16];
            sprintf(buf, "%04d", j);
            Cache::Handle* h = cache->lookup("file", j * 4, 4);
            ASSERT_EQ(std::string(buf, 4),
                      std::string((char*)(cache->value(h)), 4));
            cache->release(h);
          }
        },
        c));
  }
  pool.wait_barrier();
  std::cout << "Finish lookup" << std::endl;
}

TEST_F(CloudCacheTest, MultiThread4) {
  // With persistent cache, read/write mixed.
  Init(std::shared_ptr<Cache>(NewLRUCache(2048 * 4)), 16 * 1024, 512,
       Env::Default());

  c->add_file("file", 2048 * 8);
  ThreadPool pool(CLOUD_CACHE_TEST_THREAD_NUM);

  std::atomic<int> page_id(0);
  for (int i = 0; i < CLOUD_CACHE_TEST_THREAD_NUM; i++) {
    pool.enqueue(std::bind(
        [](CloudCache* cache, std::atomic<int>* pid) {
          int page = pid->fetch_add(1);
          std::vector<int> inserted_pages;
          while (page < 2048 * 2) {
            inserted_pages.push_back(page);
            char* buf = new char[16];
            sprintf(buf, "%04d", page);
            cache->insert(
                "file", page * 4, 4, buf, 4,
                [](const Slice&, void* value) { delete (char*)(value); });
            page = pid->fetch_add(1);

            if ((int)(inserted_pages.size()) % 16 == 0) {
              for (int j : inserted_pages) {
                char buf2[16];
                sprintf(buf2, "%04d", j);
                Cache::Handle* h = cache->lookup("file", j * 4, 4);
                if (h == nullptr) {
                  printf("nullptr page:%d\n", j);
                }
                ASSERT_EQ(std::string(buf2, 4),
                          std::string((char*)(cache->value(h)), 4));
                cache->release(h);
              }
              inserted_pages.clear();
            }
          }
        },
        c, &page_id));
  }
  pool.wait_barrier();
}

// Without persistent cache, concurrent read/write.
TEST_F(CloudCacheTest, MultiThread5) {
  Init(std::shared_ptr<Cache>(NewLRUCache(2048 * 4)));

  c->add_file("file", 2048 * 8);
  ThreadPool pool(16);

  for (int i = 0; i < 8; i++) {
    pool.enqueue(std::bind(
        [](CloudCache* cache, int seed) {
          Random r(seed);
          for (int j = 0; j < 10000; j++) {
            int pid = r.Next() % 4096;
            char* buf = new char[16];
            sprintf(buf, "%04d", pid);
            cache->insert(
                "file", pid * 4, 4, buf, 4,
                [](const Slice&, void* value) { delete (char*)(value); });
          }
        },
        c, i));
  }
  for (int i = 0; i < 8; i++) {
    pool.enqueue(std::bind(
        [](CloudCache* cache, int seed) {
          Random r(seed);
          for (int j = 0; j < 10000; j++) {
            int pid = r.Next() % 4096;
            Cache::Handle* h = cache->lookup("file", pid * 4, 4);
            if (h != nullptr) {
              cache->release(h);
            }
          }
        },
        c, (i + 10) * (i + 10)));
  }
  pool.wait_barrier();
}

// With persistent cache, concurrent read/write.
TEST_F(CloudCacheTest, MultiThread6) {
  Init(std::shared_ptr<Cache>(NewLRUCache(2048 * 4)), 16 * 1024, 512,
       Env::Default());

  c->add_file("file", 2048 * 8);
  ThreadPool pool(16);

  for (int i = 0; i < 8; i++) {
    pool.enqueue(std::bind(
        [](CloudCache* cache, int seed) {
          Random r(seed);
          for (int j = 0; j < 10000; j++) {
            int pid = r.Next() % 4096;
            char* buf = new char[16];
            sprintf(buf, "%04d", pid);
            cache->insert(
                "file", pid * 4, 4, buf, 4,
                [](const Slice&, void* value) { delete (char*)(value); });
          }
        },
        c, i));
  }
  for (int i = 0; i < 8; i++) {
    pool.enqueue(std::bind(
        [](CloudCache* cache, int seed) {
          Random r(seed);
          for (int j = 0; j < 10000; j++) {
            int pid = r.Next() % 4096;
            Cache::Handle* h = cache->lookup("file", pid * 4, 4);
            if (h != nullptr) {
              cache->release(h);
            }
          }
        },
        c, (i + 10) * (i + 10)));
  }
  pool.wait_barrier();
}

class S3CloudCacheTest : public testing::Test {
 public:
  S3CloudCacheTest()
      : dbpath("/tmp"),
        region("ap-northeast-1"),
        bucket_prefix("rockset."),
        bucket_suffix("cloud-db-examples.alec") {
    cloud_env_options.src_bucket.SetBucketName(bucket_suffix, bucket_prefix);
    cloud_env_options.dest_bucket.SetBucketName(bucket_suffix, bucket_prefix);
    cloud_env_options.keep_local_sst_files = false;
    cloud_env_options.keep_sst_levels = 2;
    const std::string bucketName = bucket_suffix + bucket_prefix;
    Status s = CloudEnv::TEST_NewAwsEnv(Env::Default(), bucket_suffix, dbpath,
                                        region, bucket_suffix, dbpath, region,
                                        cloud_env_options, nullptr, &cenv);
    if (!s.ok()) {
      printf("cannot create CloudEnv:%s\n", s.ToString().c_str());
      exit(-1);
    }
    cloud_env.reset(cenv);

    cloud_env->DeletePathInS3(cloud_env->GetDestBucketName(),
                              "/tmp/000001.sst");
    std::cout << cloud_env_options.dest_bucket.GetObjectPath() << std::endl;
    std::cout << cloud_env->GetDestObjectPath() << std::endl;
    std::cout << cloud_env->GetDestBucketName() << std::endl;

    FIRST_LEVEL_GRANULARITY = 2048 * 8;
    SECOND_LEVEL_GRANULARITY = 2048;
    THIRD_LEVEL_GRANULARITY = 256;
    MUTABLE_PERSISTENT_BLOCK_NUM = 4;
    PAGE_SIZE = 8;
    env_options.aws_use_cloud_cache = true;
    env_options.ccache = std::shared_ptr<CloudCache>(new CloudCache(
        std::shared_ptr<Cache>(NewLRUCache(512)), 128 /*pcache size*/,
        64 /*block size*/, cloud_env->GetBaseEnv()));

    // Fill a file.
    S3WritableFile f(
        cloud_env.get(), "/tmp/000001.sst", cloud_env->GetDestBucketName(),
        cloud_env_options.dest_bucket.GetObjectPath() + "/tmp/000001.sst",
        env_options, cloud_env_options);
    s = f.status();
    if (!s.ok()) {
      printf("cannot open file to write:%s\n", s.ToString().c_str());
      exit(-1);
    }
    char buf[9];
    for (int i = 0; i < 256; i++) {
      uint8_t tmp = i;
      sprintf(buf, "%c%c%c%c%c%c%c%c", tmp, tmp, tmp, tmp, tmp, tmp, tmp, tmp);
      s = f.Append(Slice(buf, 8));
      if (!s.ok()) {
        printf("append file error\n");
        exit(-1);
      }
    }
  }

  ~S3CloudCacheTest() {
    PAGE_SIZE = 2048;
    MUTABLE_PERSISTENT_BLOCK_NUM = 4;
    FIRST_LEVEL_GRANULARITY = 8 * 1024 * 1024;
    SECOND_LEVEL_GRANULARITY = 1 * 1024 * 1024;
    THIRD_LEVEL_GRANULARITY = 128 * 1024;

    cloud_env->DeletePathInS3(cloud_env->GetDestBucketName(),
                              "/tmp/000001.sst");
  }

  void get_result(uint64_t start_off, size_t n, char* scratch) {
    int idx = 0;
    for (uint64_t i = start_off; i < start_off + n; i++) {
      uint8_t c = i / 8;
      scratch[idx++] = c;
    }
  }

 protected:
  std::string dbpath;
  std::string region;
  std::string bucket_prefix;
  std::string bucket_suffix;
  CloudEnvOptions cloud_env_options;
  AwsEnv* cenv;
  std::unique_ptr<AwsEnv> cloud_env;
  EnvOptions env_options;
};

TEST_F(S3CloudCacheTest, Test1) {
  S3ReadableFile f(
      cloud_env.get(), cloud_env->GetDestBucketName(),
      cloud_env_options.dest_bucket.GetObjectPath() + "/tmp/000001.sst", 2048,
      env_options);

  Slice result;
  char scratch1[9];
  char scratch2[9];
  for (int i = 0; i < 256; i++) {
    ASSERT_TRUE(
        (f.Read((uint64_t)(i)*PAGE_SIZE, PAGE_SIZE, &result, scratch1)).ok());
    uint8_t tmp = i;
    sprintf(scratch2, "%c%c%c%c%c%c%c%c", tmp, tmp, tmp, tmp, tmp, tmp, tmp,
            tmp);
    ASSERT_EQ(0, strncmp(scratch2, result.data(), PAGE_SIZE));
  }
  printf("cache_size:%lu\n",
         env_options.ccache->cached_size("/tmp/000001.sst"));
  ASSERT_TRUE(env_options.ccache->cached_size("/tmp/000001.sst") >= 640 &&
              env_options.ccache->cached_size("/tmp/000001.sst") <= 896);
  env_options.ccache->print_summary(true);

  // Sequential read.
  for (int i = 0; i < 256; i++) {
    ASSERT_TRUE(
        (f.Read((uint64_t)(i)*PAGE_SIZE, PAGE_SIZE, &result, scratch1)).ok());
    uint8_t tmp = i;
    sprintf(scratch2, "%c%c%c%c%c%c%c%c", tmp, tmp, tmp, tmp, tmp, tmp, tmp,
            tmp);
    ASSERT_EQ(0, strncmp(scratch2, result.data(), PAGE_SIZE));
  }

  // Random read.
  Random rnd(101);
  for (int i = 0; i < 100; i++) {
    uint32_t page = rnd.Next() % 256;
    ASSERT_TRUE(
        (f.Read((uint64_t)(page)*PAGE_SIZE, PAGE_SIZE, &result, scratch1))
            .ok());
    uint8_t tmp = page;
    sprintf(scratch2, "%c%c%c%c%c%c%c%c", tmp, tmp, tmp, tmp, tmp, tmp, tmp,
            tmp);
    ASSERT_EQ(0, strncmp(scratch2, result.data(), PAGE_SIZE));
  }
}

TEST_F(S3CloudCacheTest, Test2) {
  S3ReadableFile f(
      cloud_env.get(), cloud_env->GetDestBucketName(),
      cloud_env_options.dest_bucket.GetObjectPath() + "/tmp/000001.sst", 2048,
      env_options);

  Random rnd(101);
  Slice result;
  char scratch1[2048];
  char scratch2[2048];
  for (int i = 0; i < 500; i++) {
    uint32_t start_off = (rnd.Next() % 512) * 4;
    ASSERT_TRUE((f.Read(start_off, 4, &result, scratch1)).ok());
    get_result(start_off, 4, scratch2);
    ASSERT_EQ(0, strncmp(scratch2, result.data(), 4));
  }
}

class S3RWTest : public testing::Test {
 public:
  S3RWTest()
      : dbpath("/tmp"),
        region("ap-northeast-1"),
        bucket_prefix("rockset."),
        bucket_suffix("cloud-db-examples.alec") {
    cloud_env_options.src_bucket.SetBucketName(bucket_suffix, bucket_prefix);
    cloud_env_options.dest_bucket.SetBucketName(bucket_suffix, bucket_prefix);
    cloud_env_options.keep_local_sst_files = false;
    cloud_env_options.keep_sst_levels = 2;
    const std::string bucketName = bucket_suffix + bucket_prefix;
    Status s = CloudEnv::TEST_NewAwsEnv(Env::Default(), bucket_suffix, dbpath,
                                        region, bucket_suffix, dbpath, region,
                                        cloud_env_options, nullptr, &cenv);
    if (!s.ok()) {
      printf("cannot create CloudEnv:%s\n", s.ToString().c_str());
      exit(-1);
    }
    cloud_env.reset(cenv);
  }

 protected:
  std::string dbpath;
  std::string region;
  std::string bucket_prefix;
  std::string bucket_suffix;
  CloudEnvOptions cloud_env_options;
  AwsEnv* cenv;
  std::unique_ptr<AwsEnv> cloud_env;
  EnvOptions env_options;
};

TEST_F(S3RWTest, Test1) {
  S3WritableFile f(
      cloud_env.get(), "/tmp/000001.ldb", cloud_env->GetDestBucketName(),
      cloud_env_options.dest_bucket.GetObjectPath() + "/tmp/000001.ldb",
      env_options, cloud_env_options);
  ASSERT_TRUE(f.status().ok());
  ASSERT_TRUE(f.Append("helloworld").ok());
}

TEST_F(S3RWTest, Test2) {
  ASSERT_TRUE(
      cloud_env
          ->PutObjectWithString(
              "wdnmd", cloud_env->GetDestBucketName(),
              cloud_env_options.dest_bucket.GetObjectPath() + "/tmp/wdnmd")
          .ok());
}

TEST_F(S3RWTest, Test3) {
  S3StringWritableFile f(
      cloud_env.get(), cloud_env->GetDestBucketName(),
      cloud_env_options.dest_bucket.GetObjectPath() + "/tmp/wdnmd1",
      env_options, cloud_env_options);
  ASSERT_TRUE(f.Append("wdnmd").ok());
}

TEST_F(S3RWTest, Test4) {
  {
    S3StringWritableFile f(
        cloud_env.get(), cloud_env->GetDestBucketName(),
        cloud_env_options.dest_bucket.GetObjectPath() + "/tmp/wdnmd1",
        env_options, cloud_env_options);
    Timer t;
    t.start();
    ASSERT_TRUE(f.status().ok());
    std::string s(64 * 1024, 'w');
    for (size_t i = 0; i < 1024; i++) ASSERT_TRUE(f.Append(s).ok());
    std::cout << t.since_start_nano() / 1000000 << std::endl;
    t.start();
    f.Close();
    std::cout << t.since_start_nano() / 1000000 << std::endl;
  }
  {
    S3StringWritableFile f(
        cloud_env.get(), cloud_env->GetDestBucketName(),
        cloud_env_options.dest_bucket.GetObjectPath() + "/tmp/wdnmd2",
        env_options, cloud_env_options);
    Timer t;
    t.start();
    ASSERT_TRUE(f.status().ok());
    for (size_t i = 0; i < 64 * 1024 * 1024; i++)
      ASSERT_TRUE(f.Append("w").ok());
    std::cout << t.since_start_nano() / 1000000 << std::endl;
    t.start();
    f.Close();
    std::cout << t.since_start_nano() / 1000000 << std::endl;
  }
}

}  // namespace leveldb

int main(int argc, char** argv) {
  CLOUD_CACHE_TEST_THREAD_NUM = 4;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}