cmake_minimum_required(VERSION 2.8.9)

project(dj)

set(ENV{OMPI_CXX} "clang++")
set(CMAKE_CXX_COMPILER             mpicxx)
set(CMAKE_CXX_FLAGS                "-Wall -std=c++11")
set(CMAKE_CXX_FLAGS_DEBUG          "-g")
set(CMAKE_CXX_FLAGS_MINSIZEREL     "-Os -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE        "-O4 -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g")

set(CMAKE_LINKER  "/usr/bin/llvm-ld")


set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/bin/)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${PROJECT_BINARY_DIR}/modules/)
set(LIBRARY_OUTPUT_PATH ${dj_SOURCE_DIR}/lib)

# SOURCES
add_subdirectory(src)
