# deps installed by git submodule

# brpc
message(STATUS "Finding IB Verbs for bRPC's RDMA support")
find_path(RDMA_INCLUDE_PATH NAMES infiniband/verbs.h)
find_library(RDMA_LIB NAMES ibverbs)
if((NOT RDMA_INCLUDE_PATH) OR (NOT RDMA_LIB))
    message(STATUS "IB Verbs NOT found, building bRPC without RDMA")
    set(WITH_RDMA OFF CACHE BOOL "ENABLE RDMA" FORCE)
else()
    message(STATUS "Found IB Verbs, building bRPC with RDMA:")
    message(STATUS "  (Headers)       ${RDMA_INCLUDE_PATH}")
    message(STATUS "  (Library)       ${RDMA_LIB}")
    set(WITH_RDMA ON CACHE BOOL "ENABLE RDMA" FORCE)
endif()

set(WITH_GLOG ON CACHE BOOL "ENABLE GLOG" FORCE)
set(BUILD_SHARED_LIBS ON)
add_subdirectory(${PROJECT_SOURCE_DIR}/deps/incubator-brpc)
list(APPEND DEPS_LIBS brpc-shared)
list(APPEND DEPS_INCLUDES ${PROJECT_SOURCE_DIR}/build/deps/incubator-brpc/output/include)
list(APPEND CMAKE_INSTALL_RPATH brpc-shared)

# tbb
add_subdirectory(${PROJECT_SOURCE_DIR}/deps/tbb)
list(APPEND DEPS_LIBS tbb)
list(APPEND DEPS_INCLUDES ${PROJECT_SOURCE_DIR}/deps/tbb/include)
list(APPEND CMAKE_INSTALL_RPATH tbb)

#libcypher-parser
set(CYPHER_PARSER_ROOT "${PROJECT_SOURCE_DIR}/deps/libcypher-parser")
# built libcypher-parser in auto-build.sh
find_library(CYPHER_PARSER_LIBRARY
    NAMES libcypher-parser.a
    PATHS ${CYPHER_PARSER_ROOT}/lib/src/.libs
    NO_DEFAULT_PATH
)
list(APPEND DEPS_INCLUDES ${CYPHER_PARSER_ROOT}/lib/src)
list(APPEND DEPS_LIBS ${CYPHER_PARSER_LIBRARY})

include_directories(${DEPS_INCLUDES})
