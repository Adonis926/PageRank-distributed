cmake_minimum_required(VERSION 3.16)

project(mr_distributed C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS ON)

set(HYDRARPC_ROOT "${CMAKE_CURRENT_LIST_DIR}/../../HydraRPC" CACHE PATH "HydraRPC source tree")
if(NOT EXISTS "${HYDRARPC_ROOT}/CMakeLists.txt")
  message(FATAL_ERROR "HydraRPC source tree not found: ${HYDRARPC_ROOT}")
endif()

add_subdirectory("${HYDRARPC_ROOT}" "${CMAKE_BINARY_DIR}/_deps/HydraRPC" EXCLUDE_FROM_ALL)
find_package(Threads REQUIRED)

add_executable(mr_hydra
  src/mapreduce.c
  src/mr_hydra_transport.c
  src/mr_kv.c
  src/mr_util.c
  src/mr_wire.c
  src/wordcount.c
)

target_include_directories(mr_hydra PRIVATE include src)
target_link_libraries(mr_hydra PRIVATE hydrarpc Threads::Threads)
target_compile_options(mr_hydra PRIVATE -Wall -Wextra -Wpedantic)
