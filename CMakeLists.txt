cmake_minimum_required(VERSION 3.9)

project(tsdb-unittest)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -g -w -std=c++11 -lprofiler -ltcmalloc")
message(STATUS "CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS}")

set(LEVELDB_PLATFORM_NAME LEVELDB_PLATFORM_POSIX)

option(LEVELDB_BUILD_TESTS "Build LevelDB's unit tests" ON)
option(LEVELDB_BUILD_BENCHMARKS "Build LevelDB's benchmarks" ON)
option(LEVELDB_INSTALL "Install LevelDB's header and library" ON)

include(CheckIncludeFile)
check_include_file("unistd.h" HAVE_UNISTD_H)

include(CheckLibraryExists)
check_library_exists(crc32c crc32c_value "" HAVE_CRC32C)
check_library_exists(snappy snappy_compress "" HAVE_SNAPPY)
check_library_exists(tcmalloc malloc "" HAVE_TCMALLOC)
message(STATUS "HAVE_SNAPPY ${HAVE_SNAPPY}")

FIND_PACKAGE(Boost COMPONENTS system filesystem iostreams unit_test_framework)
if (NOT Boost_FOUND)
    message(FATAL_ERROR "Fatal error: Boost (version >= 1.55) required.")
else()
    message(STATUS "Setting up BOOST")
    message(STATUS " Includes - ${Boost_INCLUDE_DIRS}")
    message(STATUS " Library  - ${Boost_LIBRARY_DIRS}")
    message(STATUS " boost_system ${Boost_SYSTEM_LIBRARY}")
    message(STATUS " boost_filesystem ${Boost_FILESYSTEM_LIBRARY}")
    message(STATUS " boost_iostreams ${Boost_IOSTREAMS_LIBRARY}")
    message(STATUS " boost_unit_test_framework ${Boost_UNIT_TEST_FRAMEWORK_LIBRARY}")
    include_directories(${Boost_INCLUDE_DIRS})
    link_directories(${Boost_LIBRARY_DIRS})
endif (NOT Boost_FOUND)

find_package(Protobuf REQUIRED)
if(PROTOBUF_FOUND)
    message(STATUS "protobuf library found")
else()
    message(FATAL_ERROR "protobuf library is needed but cant be found")
endif()
include_directories(${PROTOBUF_INCLUDE_DIRS})

find_package(Threads REQUIRED)

option(BUILD_SHARED_LIBS "Build shared libraries" ON)

find_library(TCMALLOC_LIB tcmalloc)
message(STATUS "TCMALLOC: ${TCMALLOC_LIB}")

set(LEVELDB_PUBLIC_INCLUDE_DIR "leveldb/include/leveldb")
set(LEVELDB_PORT_CONFIG_DIR "include/port")

configure_file(
  "${CMAKE_SOURCE_DIR}/leveldb/port/port_config.h.in"
  "${PROJECT_BINARY_DIR}/leveldb/${LEVELDB_PORT_CONFIG_DIR}/port_config.h"
)

include_directories(${GTEST_INCLUDE_DIRS})
include_directories(..)

include_directories(
  "${PROJECT_BINARY_DIR}/leveldb"
  "${PROJECT_BINARY_DIR}/leveldb/include"
  "."
  "${CMAKE_SOURCE_DIR}"
  "${CMAKE_SOURCE_DIR}/leveldb"
  "${CMAKE_SOURCE_DIR}/leveldb/include"
  "${CMAKE_SOURCE_DIR}/db"
  #"~/.local/include"
)

set(PROTO_SRCS "${CMAKE_SOURCE_DIR}/db/DB.pb.cc")
set(PROTO_HDRS "${CMAKE_SOURCE_DIR}/db/DB.pb.h")

set(SOURCES
    "${PROJECT_BINARY_DIR}/leveldb/${LEVELDB_PORT_CONFIG_DIR}/port_config.h"
    "${CMAKE_SOURCE_DIR}/leveldb/cloud/aws/aws_env.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/cloud/aws/aws_env.h"
    "${CMAKE_SOURCE_DIR}/leveldb/cloud/aws/aws_file.h"
    "${CMAKE_SOURCE_DIR}/leveldb/cloud/aws/aws_retry.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/cloud/aws/aws_s3.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/cloud/cloud_cache.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/cloud/cloud_cache.h"
    "${CMAKE_SOURCE_DIR}/leveldb/cloud/cloud_env.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/cloud/cloud_env_impl.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/cloud/cloud_env_impl.h"
    "${CMAKE_SOURCE_DIR}/leveldb/cloud/cloud_env_options.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/cloud/cloud_env_wrapper.h"
    "${CMAKE_SOURCE_DIR}/leveldb/cloud/cloud_log_controller.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/cloud/cloud_log_controller.h"
    "${CMAKE_SOURCE_DIR}/leveldb/cloud/cloud_manifest.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/cloud/cloud_manifest.h"
    "${CMAKE_SOURCE_DIR}/leveldb/cloud/db_cloud_impl.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/cloud/db_cloud_impl.h"
    "${CMAKE_SOURCE_DIR}/leveldb/cloud/manifest_reader.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/cloud/manifest_reader.h"
    "${CMAKE_SOURCE_DIR}/leveldb/cloud/purge.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/cloud/purge.h"
    "${CMAKE_SOURCE_DIR}/leveldb/db/builder.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/db/builder.h"
    "${CMAKE_SOURCE_DIR}/leveldb/db/c.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/db/db_impl.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/db/db_impl.h"
    "${CMAKE_SOURCE_DIR}/leveldb/db/db_iter.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/db/db_iter.h"
    "${CMAKE_SOURCE_DIR}/leveldb/db/dbformat.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/db/dbformat.h"
    "${CMAKE_SOURCE_DIR}/leveldb/db/db_querier.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/db/db_querier.h"
    "${CMAKE_SOURCE_DIR}/leveldb/db/dumpfile.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/db/filename.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/db/filename.h"
    "${CMAKE_SOURCE_DIR}/leveldb/db/file_util.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/db/file_util.h"
    "${CMAKE_SOURCE_DIR}/leveldb/db/index_reader.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/db/index_reader.h"
    "${CMAKE_SOURCE_DIR}/leveldb/db/log_format.h"
    "${CMAKE_SOURCE_DIR}/leveldb/db/log_reader.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/db/log_reader.h"
    "${CMAKE_SOURCE_DIR}/leveldb/db/log_writer.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/db/log_writer.h"
    "${CMAKE_SOURCE_DIR}/leveldb/db/memtable.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/db/memtable.h"
    "${CMAKE_SOURCE_DIR}/leveldb/db/partition_index.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/db/partition_index.h"
    "${CMAKE_SOURCE_DIR}/leveldb/db/repair.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/db/skiplist.h"
    "${CMAKE_SOURCE_DIR}/leveldb/db/snapshot.h"
    "${CMAKE_SOURCE_DIR}/leveldb/db/table_cache.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/db/table_cache.h"
    "${CMAKE_SOURCE_DIR}/leveldb/db/version_edit.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/db/version_edit.h"
    "${CMAKE_SOURCE_DIR}/leveldb/db/version_set.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/db/version_set.h"
    "${CMAKE_SOURCE_DIR}/leveldb/db/write_batch_internal.h"
    "${CMAKE_SOURCE_DIR}/leveldb/db/write_batch.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/port/port_stdcxx.h"
    "${CMAKE_SOURCE_DIR}/leveldb/port/port_stdcxx.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/port/port.h"
    "${CMAKE_SOURCE_DIR}/leveldb/port/thread_annotations.h"
    "${CMAKE_SOURCE_DIR}/leveldb/table/block_builder.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/table/block_builder.h"
    "${CMAKE_SOURCE_DIR}/leveldb/table/block.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/table/block.h"
    "${CMAKE_SOURCE_DIR}/leveldb/table/filter_block.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/table/filter_block.h"
    "${CMAKE_SOURCE_DIR}/leveldb/table/format.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/table/format.h"
    "${CMAKE_SOURCE_DIR}/leveldb/table/iterator_wrapper.h"
    "${CMAKE_SOURCE_DIR}/leveldb/table/iterator.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/table/merger.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/table/merger.h"
    "${CMAKE_SOURCE_DIR}/leveldb/table/table_builder.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/table/table.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/table/two_level_iterator.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/table/two_level_iterator.h"
    "${CMAKE_SOURCE_DIR}/leveldb/util/arena.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/util/arena.h"
    "${CMAKE_SOURCE_DIR}/leveldb/util/bloom.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/util/cache.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/util/coding.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/util/coding.h"
    "${CMAKE_SOURCE_DIR}/leveldb/util/comparator.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/util/crc32c.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/util/crc32c.h"
    "${CMAKE_SOURCE_DIR}/leveldb/util/env.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/util/filter_policy.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/util/hash.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/util/hash.h"
    "${CMAKE_SOURCE_DIR}/leveldb/util/io_posix.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/util/io_posix.h"
    "${CMAKE_SOURCE_DIR}/leveldb/util/logging.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/util/logging.h"
    "${CMAKE_SOURCE_DIR}/leveldb/util/mutexlock.h"
    "${CMAKE_SOURCE_DIR}/leveldb/util/no_destructor.h"
    "${CMAKE_SOURCE_DIR}/leveldb/util/options.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/util/random.h"
    "${CMAKE_SOURCE_DIR}/leveldb/util/status.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/util/string_util.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/util/string_util.h"
    "${CMAKE_SOURCE_DIR}/leveldb/util/xxhash.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/util/xxhash.h"

    "${CMAKE_SOURCE_DIR}/base/Checksum.cpp"
    "${CMAKE_SOURCE_DIR}/base/Endian.cpp"
    "${CMAKE_SOURCE_DIR}/base/Logging.cpp"
    "${CMAKE_SOURCE_DIR}/base/LogStream.cpp"
    "${CMAKE_SOURCE_DIR}/base/Rune.cpp"
    "${CMAKE_SOURCE_DIR}/block/BlockUtils.cpp"
    "${CMAKE_SOURCE_DIR}/chunk/BitStream.cpp"
    "${CMAKE_SOURCE_DIR}/chunk/ChunkUtils.cpp"
    "${CMAKE_SOURCE_DIR}/chunk/DeleteIterator.cpp"
    "${CMAKE_SOURCE_DIR}/chunk/XORAppender.cpp"
    "${CMAKE_SOURCE_DIR}/chunk/XORChunk.cpp"
    "${CMAKE_SOURCE_DIR}/chunk/XORIterator.cpp"
    "${CMAKE_SOURCE_DIR}/cloud/S3.cpp"
    "${CMAKE_SOURCE_DIR}/cloud/S3File.cpp"
    "${CMAKE_SOURCE_DIR}/cloud/Slice.cpp"
    "${CMAKE_SOURCE_DIR}/cloud/Status.cpp"
    "${CMAKE_SOURCE_DIR}/db/DB.cpp"
    "${CMAKE_SOURCE_DIR}/db/HttpParser.cpp"
    "${CMAKE_SOURCE_DIR}/third_party/ulid_uint128.cpp"
    "${CMAKE_SOURCE_DIR}/third_party/xxhash.cpp"
    "${CMAKE_SOURCE_DIR}/head/Head.cpp"
    "${CMAKE_SOURCE_DIR}/head/HeadWithTrie.cpp"
    "${CMAKE_SOURCE_DIR}/head/MMapHeadWithTrie.cpp"
    "${CMAKE_SOURCE_DIR}/head/HeadSeriesSet.cpp"
    "${CMAKE_SOURCE_DIR}/head/HeadUtils.cpp"
    "${CMAKE_SOURCE_DIR}/head/MemSeries.cpp"
    "${CMAKE_SOURCE_DIR}/head/SeriesHashmap.cpp"
    "${CMAKE_SOURCE_DIR}/head/StripeSeries.cpp"
    "${CMAKE_SOURCE_DIR}/index/IndexReader.cpp"
    "${CMAKE_SOURCE_DIR}/index/IndexUtils.cpp"
    "${CMAKE_SOURCE_DIR}/index/IndexWriter.cpp"
    "${CMAKE_SOURCE_DIR}/index/IntersectPostings.cpp"
    "${CMAKE_SOURCE_DIR}/index/ListPostings.cpp"
    "${CMAKE_SOURCE_DIR}/index/MemPostings.cpp"
    "${CMAKE_SOURCE_DIR}/index/SubtractionPostings.cpp"
    "${CMAKE_SOURCE_DIR}/index/TOC.cpp"
    "${CMAKE_SOURCE_DIR}/index/Uint32BEPostings.cpp"
    "${CMAKE_SOURCE_DIR}/index/UnionPostings.cpp"
    "${CMAKE_SOURCE_DIR}/index/UvarintPostings.cpp"
    "${CMAKE_SOURCE_DIR}/label/Label.cpp"
    "${CMAKE_SOURCE_DIR}/querier/ChainSeries.cpp"
    "${CMAKE_SOURCE_DIR}/querier/ChainSeriesIterator.cpp"
    "${CMAKE_SOURCE_DIR}/querier/MergedSeriesSet.cpp"
    "${CMAKE_SOURCE_DIR}/querier/QuerierUtils.cpp"
    "${CMAKE_SOURCE_DIR}/tombstone/Interval.cpp"
    "${CMAKE_SOURCE_DIR}/tombstone/TombstoneUtils.cpp"
    "${CMAKE_SOURCE_DIR}/tsdbutil/RecordDecoder.cpp"
    "${CMAKE_SOURCE_DIR}/tsdbutil/RecordEncoder.cpp"
    "${CMAKE_SOURCE_DIR}/tsdbutil/tsdbutils.cpp"

    ${PROTO_SRCS}
    ${PROTO_HDRS}
)

add_library(TSDBLIB ${SOURCES})

target_sources(TSDBLIB
  PRIVATE
    "${CMAKE_SOURCE_DIR}/leveldb/util/env_posix.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/util/posix_logger.h"
)

target_sources(TSDBLIB
  PRIVATE
    "${CMAKE_SOURCE_DIR}/leveldb/helpers/memenv/memenv.cc"
    "${CMAKE_SOURCE_DIR}/leveldb/helpers/memenv/memenv.h"
)

target_compile_definitions(TSDBLIB
  PRIVATE
    # Used by include/export.h when building shared libraries.
    LEVELDB_COMPILE_LIBRARY
    # Used by port/port.h.
    ${LEVELDB_PLATFORM_NAME}=1
)

if(HAVE_CRC32C)
  target_link_libraries(TSDBLIB crc32c)
endif(HAVE_CRC32C)
# if(HAVE_SNAPPY)
  target_link_libraries(TSDBLIB snappy)
# endif(HAVE_SNAPPY)
if(HAVE_TCMALLOC)
  target_link_libraries(TSDBLIB tcmalloc)
endif(HAVE_TCMALLOC)
find_package(Threads REQUIRED)
target_link_libraries(TSDBLIB Threads::Threads)

target_link_libraries(TSDBLIB
  aws-cpp-sdk-core
  aws-cpp-sdk-s3
  aws-cpp-sdk-transfer
  ${Boost_IOSTREAMS_LIBRARY}
  ${Boost_FILESYSTEM_LIBRARY}
  ${Boost_SYSTEM_LIBRARY}
)

#target_link_libraries(TSDBLIB ${TCMALLOC_LIB})
target_link_libraries(TSDBLIB ${PROTOBUF_LIBRARIES})
target_link_libraries(TSDBLIB
  profiler
)
target_link_libraries(TSDBLIB
  tcmalloc
)
#target_link_libraries(TSDBLIB gRPC::grpc++)

add_executable(leveldbutil
  "${CMAKE_SOURCE_DIR}/leveldb/db/leveldbutil.cc"
)
target_link_libraries(leveldbutil TSDBLIB)

enable_testing()

# Prevent overriding the parent project's compiler/linker settings on Windows.
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
set(install_gtest OFF)
set(install_gmock OFF)
set(build_gmock ON)

# This project is tested using GoogleTest.
set(BUILD_SHARED_LIBS_SAVED "${BUILD_SHARED_LIBS}")
set(BUILD_SHARED_LIBS OFF)
add_subdirectory("${CMAKE_SOURCE_DIR}/leveldb/third_party/googletest")
set(BUILD_SHARED_LIBS "${BUILD_SHARED_LIBS_SAVED}")

# This project uses Google benchmark for benchmarking.
# set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
# set(BENCHMARK_ENABLE_EXCEPTIONS OFF CACHE BOOL "" FORCE)
# add_subdirectory("${CMAKE_SOURCE_DIR}/leveldb/third_party/benchmark")

# GoogleTest triggers a missing field initializers warning.
if(LEVELDB_HAVE_NO_MISSING_FIELD_INITIALIZERS)
  set_property(TARGET gtest
      APPEND PROPERTY COMPILE_OPTIONS -Wno-missing-field-initializers)
  set_property(TARGET gmock
      APPEND PROPERTY COMPILE_OPTIONS -Wno-missing-field-initializers)
endif(LEVELDB_HAVE_NO_MISSING_FIELD_INITIALIZERS)

function(tsdb_test test_file)
  get_filename_component(test_target_name "${test_file}" NAME_WE)

  add_executable("${test_target_name}" "")
  target_sources("${test_target_name}"
    PRIVATE
      "${PROJECT_BINARY_DIR}/leveldb/${LEVELDB_PORT_CONFIG_DIR}/port_config.h"
      "${CMAKE_SOURCE_DIR}/leveldb/util/testutil.cc"
      "${CMAKE_SOURCE_DIR}/leveldb/util/testutil.h"

      "${test_file}"
  )
  target_link_libraries("${test_target_name}" TSDBLIB gmock gtest)
  target_compile_definitions("${test_target_name}"
    PRIVATE
      ${LEVELDB_PLATFORM_NAME}=1
  )

  add_test(NAME "${test_target_name}" COMMAND "${test_target_name}")
endfunction(tsdb_test)

tsdb_test("base/base_test.cc")
tsdb_test("chunk/bitstream_test.cc")
tsdb_test("chunk/chunk_test.cc")
tsdb_test("db/db_test.cc")
tsdb_test("db/http_parser_test.cc")
tsdb_test("head/head_querier_test.cc")
tsdb_test("head/head_test.cc")
tsdb_test("index/mem_postings_test.cc")
tsdb_test("leveldb/cloud/cloud_cache_test.cc")
tsdb_test("leveldb/db/dbtest.cc")
tsdb_test("leveldb/db/db_test2.cc")
tsdb_test("third_party/persistent_cedarpp_test.cc")
tsdb_test("querier/querier_test.cc")