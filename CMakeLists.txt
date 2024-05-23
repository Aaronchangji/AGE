cmake_minimum_required(VERSION 3.20.0)

project(AGE)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    message(STATUS "Detect GCC version: ${CMAKE_CXX_COMPILER_VERSION}")
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS "8.0")
        message(FATAL_ERROR "Should use GCC with version >= 8.0")
    endif()
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    message(STATUS "Detect Clang version: ${CMAKE_CXX_COMPILER_VERSION}")
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS "5")
        message(FATAL_ERROR "Should use clang with version >= 5.0")
    endif()
endif()

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin/)
set(TEST_DATA_PATH ${PROJECT_SOURCE_DIR}/graphdata/)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -lpthread")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -lrt")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-result")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-variable")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-sign-compare")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-but-set-variable")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-reorder")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_AGE_EXE_DIR_=${EXECUTABLE_OUTPUT_PATH}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_AGE_TEST_DATA_DIR_=${TEST_DATA_PATH}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_AGE_ROOT_=${PROJECT_SOURCE_DIR}")

### Options
option(BUILD_TESTS "Build tests" ON)

# third-party installed by user
include(cmake/third-party.cmake)

# deps installed by git submodule
include(cmake/deps.cmake)

# Debug mode
if(CMAKE_BUILD_TYPE MATCHES "Debug")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDEBUG")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DNDEBUG")
endif()


include_directories(${PROJECT_SOURCE_DIR}/src ${PROJECT_SOURCE_DIR})

add_subdirectory(src)
