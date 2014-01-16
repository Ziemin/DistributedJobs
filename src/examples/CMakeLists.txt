cmake_minimum_required(VERSION 3.8.9)

project(dj_examples)

set(ENV{OMPI_CXX} "clang++")
set(CMAKE_CXX_COMPILER             mpicxx)
set(CMAKE_CXX_FLAGS                "-Wall -std=c++11")
set(CMAKE_CXX_FLAGS_DEBUG          "-g")
set(CMAKE_CXX_FLAGS_MINSIZEREL     "-Os -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE        "-O4 -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g")

set(CMAKE_LINKER  "/usr/bin/llvm-ld")

find_package(Boost COMPONENTS mpi thread system date_time serialization unit_test_framework REQUIRED)
find_package(Threads REQUIRED)

ADD_DEFINITIONS(-DBOOST_ALL_DYN_LINK)
ADD_DEFINITIONS(-DBOOST_TEST_DYN_LINK)

link_directories( ${Boost_LIBRARY_DIRS} )
include_directories( ${Boost_INCLUDE_DIRS} )

SET(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${dj_examples_SOURCE_DIR}/bin)
