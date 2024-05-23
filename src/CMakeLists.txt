# generate protobuf files
foreach(_proto base master client compute_worker cache_worker)
    set(PROTO_DIR ${PROJECT_SOURCE_DIR}/src/proto)
    set(PROTO_FILE "${PROTO_DIR}/${_proto}.proto")
    add_custom_command(
        OUTPUT "${PROTO_DIR}/${_proto}.pb.h" "${PROTO_DIR}/${_proto}.pb.cc"
        COMMAND ${PROTOBUF_PROTOC}
        ARGS --cpp_out "${PROTO_DIR}"
            -I "${PROTO_DIR}"
            "${PROTO_FILE}"
        DEPENDS "${PROTO_FILE}"
    )
    list(APPEND PROTO_SRCS ${PROTO_DIR}/${_proto}.pb.cc)
endforeach()
#message(STATUS "protos_src: ${PROTO_SRCS}")
#message(STATUS "protos_src: ${PROTO_DIR}")

set(AGE_sources
    base/rdmalib/rdmaio.cpp
    base/type.cc
    base/intervals.cc
    base/aggregation.cc
    execution/execution_stage.cc
    execution/message.cc
    execution/mailbox.cc
    execution/physical_plan.cc
    execution/query_coordinator.cc
    storage/graph_loader.cc
    storage/red_black_tree.cc
    plan/ast_helper.cc
    plan/physical_plan_builder.cc
    plan/pattern_graph.cc
    plan/plan_graph.cc
    plan/logical_plan_builder.cc
    plan/logical_plan_optimizer.cc
    plan/op_params.cc
    server/console_util.cc
    storage/graph_loader.cc
    util/iniparser.c
    util/dictionary.c
    util/thpt_test_helper.cc
    operator/expand_op.cc
    ${PROTO_SRCS}
)

add_library(AGE_obj OBJECT ${AGE_sources})

add_library(AGE STATIC
    $<TARGET_OBJECTS:AGE_obj>
)

add_subdirectory(server)
add_subdirectory(storage)

# Build test
if (BUILD_TESTS)
    add_subdirectory(test)
endif()

# load_data
add_executable(graph_loader storage/load_data.cc)
target_link_libraries(graph_loader AGE ${DEPS_LIBS} ${THIRD_PARTY_LIBS})
