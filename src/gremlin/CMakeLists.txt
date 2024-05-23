# Copyright 2021 HDL
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.5.1)
set(CMAKE_CXX_STANDARD 17)

# Find Protobuf installation
# Looks for protobuf-config.cmake file installed by Protobuf's cmake installation.
set(protobuf_MODULE_COMPATIBLE TRUE)
find_package(Protobuf CONFIG REQUIRED)
message(STATUS "Using protobuf ${Protobuf_VERSION}")

set(_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
set(_REFLECTION gRPC::grpc++_reflection)
if(CMAKE_CROSSCOMPILING)
  find_program(_PROTOBUF_PROTOC protoc)
else()
  set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)
endif()

# Find gRPC installation
# Looks for gRPCConfig.cmake file installed by gRPC's cmake installation.
find_package(gRPC CONFIG REQUIRED)
message(STATUS "Using gRPC ${gRPC_VERSION}")

set(_GRPC_GRPCPP gRPC::grpc++)
if(CMAKE_CROSSCOMPILING)
  find_program(_GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)
else()
  set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
endif()

# Protobuf generate
# message(STATUS ${CMAKE_CURRENT_SOURCE_DIR})
set(proto_dir ${CMAKE_CURRENT_SOURCE_DIR}/proto)
set(gremlin_proto_files
    ${proto_dir}/*.proto
)
#set(gremlin_proto_files 
#    "${proto_dir}/common.proto"
#    "${proto_dir}/gremlin_result.proto"
#    "${proto_dir}/gremlin.proto"
#    "${proto_dir}/job_service.proto"
#)
set(gremlin_proto_hdrs "")
set(gremlin_proto_srcs "")

foreach(_proto
    common gremlin gremlin_result job_service)
    set(_prefix "${CMAKE_CURRENT_BINARY_DIR}/${_proto}")
    set(_proto_files "${proto_dir}/${_proto}.proto")
    add_custom_command(
        OUTPUT "${_prefix}.pb.h" "${_prefix}.pb.cc" "${_prefix}.grpc.pb.cc" "${_prefix}.grpc.pb.h"
        COMMAND ${_PROTOBUF_PROTOC}
        ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
            --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"   
            -I "${proto_dir}"
            --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
            "${_proto_files}"
        DEPENDS "${_proto_files}"
    )

    list(APPEND gremlin_proto_hdrs "${_prefix}.pb.h")
    list(APPEND gremlin_proto_hdrs "${_prefix}.grpc.pb.h")
    list(APPEND gremlin_proto_srcs "${_prefix}.pb.cc")
    list(APPEND gremlin_proto_srcs "${_prefix}.grpc.pb.cc")
endforeach()

# Include *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")

add_library(gremlin_proto
    ${gremlin_proto_srcs}
    ${gremlin_proto_hdrs}
)
target_link_libraries(gremlin_proto
    ${_REFLECTION}
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF}
)

foreach(_target 
    rpc_server rpc_tester)
    add_executable(${_target} "${_target}.cc")
    target_link_libraries(${_target}
        gremlin_proto
        ${_REFLECTION}
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIBPROTOBUF}
        AGE
        ${AGE_EXTERNAL_LIBRARIES}
    )
endforeach()
