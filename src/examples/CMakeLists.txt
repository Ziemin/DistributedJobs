MACRO(SUBDIRLIST result curdir)
  FILE(GLOB children RELATIVE ${curdir} ${curdir}/*)
  SET(dirlist "")
  FOREACH(child ${children})
    IF(IS_DIRECTORY ${curdir}/${child})
        SET(dirlist ${dirlist} ${child})
    ENDIF()
  ENDFOREACH()
  SET(${result} ${dirlist})
ENDMACRO()

find_package(Boost COMPONENTS mpi thread system date_time serialization unit_test_framework REQUIRED)
find_package(Threads REQUIRED)

link_directories( ${Boost_LIBRARY_DIRS} )
include_directories( ${Boost_INCLUDE_DIRS} )

SET(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${dj_SOURCE_DIR}/bin/examples)

SET(TEST_PROPS -log_level=all)
ADD_DEFINITIONS(-DBOOST_ALL_DYN_LINK)

message("-- Adding examples:")

set(EXAMPLE_NAMES "")

file(GLOB_RECURSE EXAMPLE_FILES "*.cpp")
foreach(file ${EXAMPLE_FILES})

    get_filename_component(base_name ${file} NAME_WE)
    get_filename_component(full_name ${file} NAME)

    add_executable(${base_name} ${file})

    target_link_libraries (${base_name} ${Boost_LIBRARIES})
    target_link_libraries (${base_name} ${Boost_SYSTEM_LIBRARY})
    target_link_libraries (${base_name} ${CMAKE_THREAD_LIBS_INIT})
    target_link_libraries (${base_name} dj)

    LIST(APPEND EXAMPLE_NAMES ${base_name})
endforeach()

message("--   Examples found:")
foreach(ex ${EXAMPLE_NAMES})
    message("--    " ${ex})
endforeach()

