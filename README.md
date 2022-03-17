# TimeUnion

[TimeUnion](https://doi.org/10.1145/3514221.3526175) is intended to be used as a library your application code. You can check `leveldb/db/dbtest.cc` for various examples.  
Besides, we also implement a simple http client/server which wrap TimeUnion for experiments. You can check `db/db_test.cc` for the usage.

## Try The Test Cases

### Prerequisite
The code is tested under Ubuntu 20.04. If you want to use AWS S3, please follow this [link](https://docs.aws.amazon.com/sdk-for-cpp/v1/developer-guide/setup-linux.html) to install AWS C++ SDK and then set the environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` to your own values.  
Dependencies: boost, tcmalloc, protobuf, snappy

### Example
`leveldb/db/dbtest.cc` shows a large part of our test/bench codes. Before running it, please download the [timeseries tags file](https://drive.google.com/file/d/1L2SEp8H-wQg3xl3LvpY8Ok45xi4CSav_/view?usp=sharing) (containing 10M generated timeseries from TSBS), and place it under the folder `test`.  

Compilation
```
$ mkdir build
$ cmake ..
$ make dbtest
```
In the following, we provide some code segments to show the usage.  

Configuration
```c++
  std::string dbpath = "/tmp/tsdb_big";
  std::string region = "ap-northeast-1";                // Replace your value.
  std::string bucket_prefix = "rockset.";               // Replace your value.
  std::string bucket_suffix = "cloud-db-examples.alec"; // Replace your value.

  CloudEnvOptions cloud_env_options;
  std::unique_ptr<CloudEnv> cloud_env;
  cloud_env_options.src_bucket.SetBucketName(bucket_suffix,bucket_prefix);
  cloud_env_options.dest_bucket.SetBucketName(bucket_suffix,bucket_prefix);
  cloud_env_options.keep_local_sst_files = true;
  cloud_env_options.keep_sst_levels = 2;
  CloudEnv* cenv;
  const std::string bucketName = bucket_suffix + bucket_prefix;
  Status s = CloudEnv::NewAwsEnv(Env::Default(),
        bucket_suffix, dbpath, region,
        bucket_suffix, dbpath, region,
        cloud_env_options, nullptr, &cenv);
        // NewLRUCache(256*1024*1024));
  if (!s.ok()) {
    std::cout << "Cannot create AwsEnv" << std::endl;
    exit(-1);
  }
  cloud_env.reset(cenv);

  Options options;
  options.create_if_missing = true;
  options.use_log = false; // Disable the log in LevelDB, use the log in TimeUnion.
  options.env = cloud_env.get();
  options.aws_use_cloud_cache = true;
  options.ccache = std::shared_ptr<CloudCache>(new CloudCache(
      std::shared_ptr<Cache>(NewLRUCache(64 * 1024 * 1024)),
      64 * 1024 * 1024/*pcache size*/, 128 * 1024/*block size*/,
      cloud_env->GetBaseEnv()));
  options.max_imm_num = 3;
  options.write_buffer_size = 64 * 1024 * 1024;
```
Create the DB instances
```c++
  // This is to manage the persistent files in EBS and S3.
  DBCloud* ldb;
  if (!DBCloud::Open(options, dbpath, &ldb).ok()) {
    std::cout << "Cannot open DB" << std::endl;
    exit(-1);
  }

  // This is to manage the in-memory timeseries objects and indexes.
  std::unique_ptr<::tsdb::head::MMapHeadWithTrie> head(
    new ::tsdb::head::MMapHeadWithTrie(0, dbpath, dbpath, ldb)
  );
```
Insert individual timeseries
```c++
  std::unique_ptr<tsdb::db::AppenderInterface> app = head_->appender(false);
  tsdb::label::Labels lset = {{"label1", "value1"}, {"label2", "value2"}};
  std::pair<uint64_t, leveldb::Status> p = app->add(lset, 0, 0.15);
  assert(p.second.ok());

  // Fast path insertion with TSID.
  leveldb::Status st = app->add_fast(p.first, 10, 1.15);
  assert(st.ok());

  // Commit.
  st = app->commit();
  assert(st.ok());
```
Insert group timeseries
```c++
  // In the following, we insert three members for a group one by one.
  uint64_t gid;
  std::vector<int> slots;
  tsdb::label::Labels lset;
  for (int j = 0; j < num_labels; j++)
    lset.emplace_back("label_" + std::to_string(j), "group_value_0");
  app->add({{"group", "group_0"}}, {lset}, 0, {1.1}, &gid, &slots);
  assert(slots.size() == 1);
  assert(slots[0] == 0);

  lset.clear();
  slots.clear();
  for (int j = 0; j < num_labels; j++)
    lset.emplace_back("label_" + std::to_string(j), "group_value_1");
  // It will fill NULL value for the first member.
  app->add({{"group", "group_" + std::to_string(i)}}, {lset}, 1, {1.1}, &gid, &slots);
  assert(slots.size() == 1);
  assert(slots[0] == 1);

  lset.clear();
  slots.clear();
  for (int j = 0; j < num_labels; j++)
    lset.emplace_back("label_" + std::to_string(j), "group_value_2");
  // It will fill NULL value for the first two members.
  app->add({{"group", "group_" + std::to_string(i)}}, {lset}, 2, {1.1}, &gid, &slots);
  assert(slots.size() == 1);
  assert(slots[0] == 2);

  // Insert by group id.
  app->add(gid, 3, {2.2, 2.2, 2.2});

  // Insert by group id and slots
  // (which members you want to insert).
  app->add(gid, {1}, 4, {3.3});
```