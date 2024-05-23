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

#include <google/protobuf/util/json_util.h>
#include <gremlin.pb.h>
#include <grpcpp/grpcpp.h>
#include <job_service.grpc.pb.h>
#include <cstdio>
#include <iostream>
#include <string>

using google::protobuf::util::MessageToJsonString;

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerWriter;
using grpc::Status;
using protocol::JobRequest;
using protocol::JobResponse;
using protocol::JobService;

class RpcServiceImpl final : public JobService::Service {
    Status Submit(ServerContext *ctx, const JobRequest *req, ServerWriter<JobResponse> *reply) override {
        printf("JobRequest: %s\n", req->DebugString());
        printf("%d %d %d %d\n", req->has_conf(), req->has_source(), req->has_plan(), req->has_sink());

        std::string reqStr;
        google::protobuf::util::JsonPrintOptions options;
        options.add_whitespace = true;
        // options.always_print_primitive_fields = true;
        MessageToJsonString(*req, &reqStr, options);

        printf("req json str: %s\n", reqStr.c_str());

        const protocol::TaskPlan &plan = req->plan();

        gremlin::GremlinStep source;

        source.ParseFromString(req->source().resource());
        std::string stepStr;
        MessageToJsonString(source, &stepStr, options);
        printf("source: %s\n", stepStr.c_str());
        for (int i = 0; i < plan.plan_size(); i++) {
            const protocol::OperatorDef op = plan.plan(i);
            if (op.has_flat_map()) {
                printf("!!\n");
            }
        }
        return Status::OK;
    }
};

void RunServer() {
    std::string server_address("localhost:1234");
    RpcServiceImpl service;
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;

    server->Wait();
}

int main(int argc, char **argv) {
    RunServer();
    return 0;
}
