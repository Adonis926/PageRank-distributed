cmake_minimum_required(VERSION 3.16)

project(mr_distributed C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS ON)

set(HYDRARPC_ROOT "/home/liulingling/GVM/HydraRPC" CACHE PATH "HydraRPC source tree")

add_library(hydrarpc STATIC
  "${HYDRARPC_ROOT}/src/hydra.c"
)

target_include_directories(hydrarpc
  PUBLIC
    "${HYDRARPC_ROOT}/include"
  PRIVATE
    "${HYDRARPC_ROOT}/src"
)

target_compile_options(hydrarpc PRIVATE -Wall -Wextra -Wpedantic)

add_executable(mr_hydra
  src/mr_hydra.c
)

target_link_libraries(mr_hydra PRIVATE hydrarpc)
target_compile_options(mr_hydra PRIVATE -Wall -Wextra -Wpedantic)
