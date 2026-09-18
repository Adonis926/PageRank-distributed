cmake_minimum_required(VERSION 3.16)

project(mr_distributed C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS ON)

option(MR_ENABLE_CXLFORK "Build the restricted CXLfork worker integration" OFF)
set(CXLFORK_UAPI_INCLUDE_DIR "" CACHE PATH
    "headers_install include directory containing linux/cxlfork.h")

set(HYDRARPC_ROOT "${CMAKE_CURRENT_LIST_DIR}/../../HydraRPC" CACHE PATH "HydraRPC source tree")
if(NOT EXISTS "${HYDRARPC_ROOT}/CMakeLists.txt")
  message(FATAL_ERROR "HydraRPC source tree not found: ${HYDRARPC_ROOT}")
endif()

add_subdirectory("${HYDRARPC_ROOT}" "${CMAKE_BINARY_DIR}/_deps/HydraRPC" EXCLUDE_FROM_ALL)
find_package(Threads REQUIRED)

add_executable(mr_hydra
  src/mapreduce.c
  src/mr_cxlfork.c
  src/mr_hydra_transport.c
  src/mr_kv.c
  src/mr_porter.c
  src/mr_util.c
  src/mr_wire.c
  src/wordcount.c
)

target_include_directories(mr_hydra PRIVATE include src)
if(MR_ENABLE_CXLFORK)
  if(NOT EXISTS "${CXLFORK_UAPI_INCLUDE_DIR}/linux/cxlfork.h")
    message(FATAL_ERROR
      "MR_ENABLE_CXLFORK requires CXLFORK_UAPI_INCLUDE_DIR with linux/cxlfork.h")
  endif()
  target_include_directories(mr_hydra SYSTEM PRIVATE "${CXLFORK_UAPI_INCLUDE_DIR}")
  target_compile_definitions(mr_hydra PRIVATE MR_ENABLE_CXLFORK=1)
else()
  target_compile_definitions(mr_hydra PRIVATE MR_ENABLE_CXLFORK=0)
endif()
target_link_libraries(mr_hydra PRIVATE hydrarpc Threads::Threads)
target_compile_options(mr_hydra PRIVATE -Wall -Wextra -Wpedantic)

# PageRank is intentionally a separate executable.  It shares the audited
# HydraRPC and restricted CXLfork wrappers, but none of the old MapReduce
# task/file-shuffle lifecycle is on its measured path.
add_executable(pagerank_hydra
  pagerank/pagerank_hydra.c
  pagerank/pr_cxlfork.c
  pagerank/pr_graph.c
  src/mr_cxlfork.c
  src/mr_util.c
)
target_include_directories(pagerank_hydra PRIVATE include src pagerank)
if(MR_ENABLE_CXLFORK)
  target_include_directories(pagerank_hydra SYSTEM PRIVATE
    "${CXLFORK_UAPI_INCLUDE_DIR}")
  target_compile_definitions(pagerank_hydra PRIVATE MR_ENABLE_CXLFORK=1)
else()
  target_compile_definitions(pagerank_hydra PRIVATE MR_ENABLE_CXLFORK=0)
endif()
target_link_libraries(pagerank_hydra PRIVATE hydrarpc Threads::Threads m)
target_compile_options(pagerank_hydra PRIVATE -O3 -Wall -Wextra -Wpedantic)
