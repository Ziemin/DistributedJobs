find_package(Boost COMPONENTS mpi thread system date_time serialization unit_test_framework REQUIRED)
find_package(Threads REQUIRED)

ADD_DEFINITIONS(-DBOOST_ALL_DYN_LINK)
ADD_DEFINITIONS(-DBOOST_TEST_DYN_LINK)

ENABLE_TESTING()

link_directories( ${Boost_LIBRARY_DIRS} )
include_directories( ${Boost_INCLUDE_DIRS} )

SET(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${dj_SOURCE_DIR}/bin/tests)

SET(TEST_PROPS -log_level=all)

message("-- Adding test files:")
file(GLOB TEST_FILES "*.cpp")
set(TEST_NAMES "")
foreach(file ${TEST_FILES})

    get_filename_component(base_name ${file} NAME_WE)
    get_filename_component(full_name ${file} NAME)

    add_executable(${base_name} ${full_name})

    target_link_libraries (${base_name} ${Boost_LIBRARIES})
    target_link_libraries (${base_name} ${Boost_SYSTEM_LIBRARY})
    target_link_libraries (${base_name} ${CMAKE_THREAD_LIBS_INIT})

    # link my library ... 

    add_test(NAME ${base_name} COMMAND ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${base_name} ${TEST_PROPS})
    set_tests_properties(${base_name} PROPERTIES WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/resources)

    LIST(APPEND TEST_NAMES ${base_name})
endforeach()

message(${LIBRARY_OUTPUT_PATH})
message("--   Tests found:")
foreach(test ${TEST_NAMES})
    message("--    "${test})
endforeach()

ADD_CUSTOM_TARGET(check COMMAND ${CMAKE_CTEST_COMMAND} DEPENDS ${TEST_NAMES})
