project(WIPE)
cmake_minimum_required(VERSION 3.0)
set(MKL_LINK_DIRECTORY "/opt/intel/mkl/lib/intel64")
set(MKL_INCLUDE_DIRECTORY "/opt/intel/mkl/include")

# use `cmake -DSERVER:BOOL=ON ..` when running in server
option(SERVER "Run in server" ON)
option(USE_LIBPMEM "libpmem or libvmmalloc" ON)
option(BUF_SORT "Sort buffer in KVBufer" OFF)
option(STREAMING_LOAD "Use Non-temporal Load" OFF)
option(STREAMING_STORE "Use Non-temporal Store" OFF)
option(NO_LOCK "Don't use lock" OFF)
option(BRANGE "Multi-thread expanding" ON)
option(NO_ENTRY_BUF "BEntry without KVBuffer" ON)

# use `make clean && make CXX_DEFINES="-DNAME=VALUE"` to override during compile
if(SERVER)
  set(PMEM_DIR \"/mnt/pmem1/lbl/\")
  set(CLEVEL_PMEM_FILE_SIZE "(1024*1024*1024*32UL)")
  set(CLEVEL_PMEM_FILE \"/mnt/pmem1/lbl/letree-clevel-\")
  set(BLEVEL_PMEM_FILE \"/mnt/pmem1/lbl/letree-blevel-\")
  set(ALEVEL_PMEM_FILE \"/mnt/pmem1/lbl/letree-alevel-\")
  set(PGM_INDEX_PMEM_FILE \"/mnt/pmem1/lbl/letree-pgmindex-\")
  set(COMMON_PMEM_FILE \"/mnt/pmem1/lbl/common-alloctor\")
else()
  set(PMEM_DIR \"/mnt/pmem1/lbl/\")
  set(CLEVEL_PMEM_FILE_SIZE "(1024*1024*512UL)")
  set(CLEVEL_PMEM_FILE \"/mnt/pmem1/lbl/letree-clevel-\")
  set(BLEVEL_PMEM_FILE \"/mnt/pmem1/lbl/letree-blevel-\")
  set(ALEVEL_PMEM_FILE \"/mnt/pmem1/lbl/letree-alevel-\")
  set(PGM_INDEX_PMEM_FILE \"/mnt/pmem1/lbl/letree-pgmindex-\")
  set(COMMON_PMEM_FILE \"/mnt/pmem1/lbl/common-alloctor\")
endif(SERVER)

if(BRANGE)
  set(EXPAND_THREADS 4)
endif(BRANGE)

if(NO_ENTRY_BUF)
  set(BLEVEL_EXPAND_BUF_KEY 4)
else()
  set(BLEVEL_EXPAND_BUF_KEY 6)
endif(NO_ENTRY_BUF)

set(EXPANSION_FACTOR 2)
set(DEFAULT_SPAN 2)
set(PMEMKV_THRESHOLD 10000)
set(ENTRY_SIZE_FACTOR 1.2)

configure_file(
  "${PROJECT_SOURCE_DIR}/src/letree_config.h.in"
  "${PROJECT_SOURCE_DIR}/src/letree_config.h"
)

# -march=native used to detect CPU flags and use corrosponding clflush instruction
set(CMAKE_CXX_FLAGS "-march=native -Wall -Wno-reorder -O3")

# Require C++17
set(CMAKE_CXX_STANDARD_REQUIRED on)
set(CMAKE_CXX_STANDARD 17)

FIND_PACKAGE(OpenMP REQUIRED)

if(OPENMP_FOUND)
  message("OPENMP FOUND")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${OpenMP_EXE_LINKER_FLAGS}")
endif()

add_definitions(-DNDEBUG -DNDEBUGGING)

# MK for RMI
link_directories(${MKL_LINK_DIRECTORY})
include_directories(${MKL_INCLUDE_DIRECTORY})

# shared library
include_directories(include)

set(LETREE_SRC
  src/alevel.cc
  src/enviroment.cc
  src/pgm_index.cc
  src/rmi_index.cc
  src/learn_index.cc
  src/blevel.cc
  src/bentry.cc
  src/clevel.cc
  src/pmemkv.cc
)

add_library(letree SHARED ${LETREE_SRC})
target_link_libraries(letree pmem pmemobj mkl_rt pthread)

# Unit Test
enable_testing()
include_directories(src)

add_executable(example test/example.cc)
target_link_libraries(example letree)
add_test(example example)
