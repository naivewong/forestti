# ForestTI

ForestTI is intended to be used as a library your application code. You can check `leveldb/db/dbtest.cc` for various examples.  
Besides, we also implement a simple http client/server which wrap ForestTI for experiments. You can check `db/db_test.cc` for the usage.

## Try The Test Cases

### Prerequisite
The code is tested under Ubuntu 20.04.    
Dependencies: cmake, gcc, boost, tcmalloc, jemalloc, protobuf, protobuf-compiler, profiler, snappy
```
sudo apt-get install cmake build-essential libboost-all-dev google-perftools libprotobuf-dev protobuf-compiler libgoogle-perftools-dev libsnappy-dev
git clone https://github.com/jemalloc/jemalloc.git
cd jemalloc
autoconf
./configure
make
sudo make install
```

### Example
`leveldb/db/dbtest.cc` shows a large part of our test/bench codes. Before running it, please download the [timeseries tags file](https://drive.google.com/file/d/1L2SEp8H-wQg3xl3LvpY8Ok45xi4CSav_/view?usp=sharing) (containing 10M generated timeseries from TSBS), and place it under the folder `test` (you need to first create this folder, like the following).  
```
mkdir test
```

Generate files from protobuf
```
cd db
protoc DB.proto --cpp_out=.
```

Compilation (in the root directory of the repo)
```
mkdir build
cd build
cmake ..
make db_test
```

Run the generated db_test with 1000 timeseries
```
./db_test 1000
```

In the following, we provide some code segments to show the usage.  

Configuration
```c++
  Options options;
  options.create_if_missing = true;
  options.use_log = false; // Disable the log in LevelDB, use the log in ForestTI.
  options.max_imm_num = 3;
  options.write_buffer_size = 64 * 1024 * 1024;
```
Create the DB instances
```c++
  // This is to manage the persistent files in EBS and S3.
  DB* ldb;
  if (!DB::Open(options, dbpath, &ldb).ok()) {
    std::cout << "Cannot open DB" << std::endl;
    exit(-1);
  }

  // This is to manage the in-memory timeseries objects and indexes.
  std::unique_ptr<::tsdb::head::MMapHeadWithTrie> head(
    new ::tsdb::head::Head(dbpath, "", ldb, true)
  );

  ldb->SetHead(head.get());
```
Insert individual timeseries
```c++
  std::unique_ptr<tsdb::db::AppenderInterface> app = head->appender(false);
  tsdb::label::Labels lset = {{"label1", "value1"}, {"label2", "value2"}};
  std::pair<uint64_t, leveldb::Status> p = app->add(lset, 0, 0.15);
  assert(p.second.ok());

  // Fast path insertion with logical TSID.
  leveldb::Status st = app->add_fast(p.first, 10, 1.15);
  assert(st.ok());

  // Commit.
  st = app->commit();
  assert(st.ok());
```