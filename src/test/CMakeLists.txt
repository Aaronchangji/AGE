#gtest
find_path(GTEST_INCLUDE_DIR NAMES gtest/gtest.h)
find_library(GTEST_LIB NAMES gtest)
if (GTEST_INCLUDE_DIR AND GTEST_LIB)
    message(STATUS "Found gtest:")
    message(STATUS "  (Headers)       ${GTEST_INCLUDE_DIR}")
    message(STATUS "  (Library)       ${GTEST_LIB}")
else()
    message(FATAL_ERROR "gtest not found")
endif()


include_directories(${GTEST_INCLUDE_DIR})
include_directories(./)

# AGE unit test
file(GLOB unit_test_files
    test_tool.cc
    test_tbb.cc
    test_query.cc
    ../plan/test/test_cypher.cc
    ../plan/test/test_index.cc
    ../storage/test/test_storage.cc
    ../storage/test/test_b_tree.cc
    ../storage/test/test_red_black_tree.cc
    ../base/test/test_expr.cc
    ../base/test/test_type.cc
    ../base/test/test_filter.cc
    ../base/test/test_intervals.cc
    ../base/memory_manager.cc
    ../base/test/test_mem_manager.cc
    ../execution/test/test_message.cc
    ../execution/test/test_mailbox.cc
    ../operator/test/test_expand.cc
    ../operator/test/test_property_op.cc
    ../operator/test/test_expand_into.cc
    ../operator/test/test_vertex_scan_op.cc
    ../operator/test/test_subquery_op.cc
    ../operator/test/test_optional_match.cc
    ../operator/test/test_aggregation.cc
    ../operator/test/test_projection.cc
    ../execution/test/test_standalone_executor.cc
)

add_executable(${PROJECT_NAME}_unit_tests run_tests_main.cc ${unit_test_files})
target_link_libraries(${PROJECT_NAME}_unit_tests AGE ${DEPS_LIBS} ${THIRD_PARTY_LIBS} ${GTEST_LIB})

file(GLOB distributed_test_files
    test_tool.cc
)

add_executable(${PROJECT_NAME}_distributed_tests test_distributed_query.cc ${distributed_test_files})
target_link_libraries(${PROJECT_NAME}_distributed_tests AGE ${DEPS_LIBS} ${THIRD_PARTY_LIBS} ${GTEST_LIB})

file(GLOB throughput_test_async_files
    client.cc
)
add_executable(${PROJECT_NAME}_throughput_test_async throughput_test_async_main.cc ${throughput_test_async_files})
target_link_libraries(${PROJECT_NAME}_throughput_test_async AGE ${DEPS_LIBS} ${THIRD_PARTY_LIBS} ${GTEST_LIB})