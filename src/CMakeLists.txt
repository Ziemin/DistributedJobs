link_directories( ${Boost_LIBRARY_DIRS} )
include_directories( ${Boost_INCLUDE_DIRS} ${BOTAN_INCLUDE_DIR} ${PROTOBUF_INCLUDE_DIR})
find_package(Boost COMPONENTS filesystem system thread date_time serialization unit_test_framework program_options regex mpi REQUIRED)
find_package(Threads REQUIRED)

ADD_DEFINITIONS(-DBOOST_ALL_DYN_LINK)

add_library(dj STATIC
    executor.cpp
)

target_link_libraries (dj ${Boost_LIBRARIES})
target_link_libraries (dj ${Boost_SYSTEM_LIBRARY})
target_link_libraries (dj ${CMAKE_THREAD_LIBS_INIT})

ADD_CUSTOM_TARGET(run COMMAND nodeClient DEPENDS ${nodeClient})

# tests
add_subdirectory(tests)
