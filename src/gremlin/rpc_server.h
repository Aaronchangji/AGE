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
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <memory>
#include <string>
#include "./gremlin.pb.h"
#include "./gremlin_result.pb.h"
#include "./job_service.grpc.pb.h"
#include "execution/mailbox.h"
#include "execution/result_collector.h"
#include "execution/standalone_executor.h"
#include "gremlin/plan_translator.h"
#include "plan/planner.h"
#include "server/console_util.h"
#include "server/standalone.h"
#include "storage/graph.h"

using std::cin;
using std::cout;
using std::string;

using google::protobuf::util::MessageToJsonString;

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerWriter;
using grpc::Status;
using protocol::JobRequest;
using protocol::JobResponse;
using protocol::JobService;

namespace AGE {

class RpcServiceImpl final : public JobService::Service {
   public:
    RpcServiceImpl(Standalone* queryServer, Graph* g) : queryServer(queryServer), planTranslator(g->getStrMap(), g) {}

    bool checkResult(const Result& reply, ItemType& type) {
        type = T_NULL;
        for (const Row& r : reply.result) {
            if (r.size() > 1) return false;
            if (r.size() == 0) continue;
            if (type == T_NULL)
                type = r[0].type;
            else if (type != r[0].type)
                return false;
        }
        return true;
    }

    void toValue(const Item& item, common::Value* val) {
        switch (item.type) {
        case T_INTEGER:
            val->set_i64(item.integerVal);
            break;
        case T_FLOAT:
            val->set_f64(item.floatVal);
            break;
        case T_STRING:
            val->set_str(item.stringVal);
            break;
        case T_STRINGVIEW:
            val->set_str(item.stringView);
            break;
        case T_BOOL:
            val->set_boolean(item.boolVal);
            break;
        default:
            assert("RpcServiceImpl::toValue()" && false);
        }
    }

    protocol::JobResponse toResponse(const Result& reply) {
        protocol::JobResponse resp;
        protobuf::Result result;
        ItemType type;
        resp.set_job_id(static_cast<u64>(-reply.qid));
        if (!checkResult(reply, type)) return resp;

        if (type == T_VERTEX || type == T_EDGE) {
            // GraphElementArray.
            protobuf::GraphElementArray* elements = result.mutable_elements();
            for (const Row& r : reply.result) {
                protobuf::GraphElement* element = elements->add_item();
                if (type == T_VERTEX) {
                    protobuf::Vertex* v = element->mutable_vertex();
                    v->set_id(r[0].vertex.id);
                    v->mutable_label()->set_name_id(r[0].vertex.label);
                } else {
                    protobuf::Edge* e = element->mutable_edge();
                    e->set_id(std::to_string(r[0].edge.id));
                    e->mutable_label()->set_name_id(r[0].edge.label);
                }
            }
        } else if (reply.result.size() == 1) {
            // Value.
            toValue(reply.result[0][0], result.mutable_value());
        } else {
            // ValueArray.
            protobuf::ValueArray* valList = result.mutable_value_list();
            for (const Row& r : reply.result) {
                common::Value* val = valList->add_item();
                toValue(r[0], val);
            }
        }

        std::string resultStr;
        result.SerializeToString(&resultStr);
        resp.set_data(resultStr);
        return resp;
    }

    Status Submit(ServerContext* ctx, const JobRequest* req, ServerWriter<JobResponse>* writer) override {
        std::string reqStr;
        google::protobuf::util::JsonPrintOptions options;
        options.add_whitespace = true;
        MessageToJsonString(*req, &reqStr, options);

        printf("has_sink: %d\n", req->has_sink());
        const protocol::Sink& sink = req->sink();
        printf("sink: %d %d %d\n", sink.has_resource(), sink.has_fold(), sink.has_group());

        printf("req json str: %s\n", reqStr.c_str());
        printf("before translate\n");
        PhysicalPlan* plan = planTranslator.translate(req);
        printf("before runQuery\n");
        Result reply = queryServer->RunQuery(plan);
        printf("before toResponse\n");
        protocol::JobResponse resp = toResponse(reply);
        writer->Write(resp);

        return Status::OK;
    }

   private:
    Standalone* queryServer;
    PlanTranslator planTranslator;
};

class RpcServer {
   public:
    explicit RpcServer(string graphDir, int numThread, std::string address)
        : queryServer(graphDir, numThread), address(address) {
        g = queryServer.getGraph();
    }

    void start() {
        RpcServiceImpl service(&queryServer, g);
        ServerBuilder builder;
        builder.AddListeningPort(address, grpc::InsecureServerCredentials());
        builder.RegisterService(&service);
        std::unique_ptr<Server> server(builder.BuildAndStart());
        std::cout << "Server listening on " << address << std::endl;
        server->Wait();
    }

   private:
    Graph* g;
    Standalone queryServer;
    std::string address;
};
}  // namespace AGE
