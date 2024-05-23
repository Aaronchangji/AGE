// Copyright 2021 HDL
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "gremlin/rpc_server.h"
#include <google/protobuf/util/json_util.h>
#include <gremlin.pb.h>
#include <grpcpp/grpcpp.h>
#include <job_service.grpc.pb.h>
#include <cstdio>
#include <iostream>
#include <string>

int main(int argc, char *argv[]) {
    if (argc < 2) {
        printf("[GraphDB] missing arguments for input graph\n");
        return 0;
    }

    int numThreads = 1;
    if (argc >= 3) numThreads = atoi(argv[2]);

    AGE::RpcServer rpcServer(argv[1], numThreads, "localhost:1234");
    rpcServer.start();
    return 0;
}
