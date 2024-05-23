// Copyright 2022 HDL
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

#pragma once

#include <shared_mutex>
#include <string>
#include <vector>
#include "brpc/closure_guard.h"
#include "execution/result_collector.h"
#include "proto/client.pb.h"
#include "util/tool.h"

using std::string;
namespace AGE {
namespace ProtoBuf {
namespace Client {

class ClientRpcServer : public Service {
   public:
    explicit ClientRpcServer(Result **result) : result(result) {}
    ~ClientRpcServer() {}

    void SendResult(google::protobuf::RpcController *cntlBase, const SendResultRequest *req, SendResultResponse *resp,
                    google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);
        // brpc::Controller *cntl = static_cast<brpc::Controller *>(cntlBase);

        size_t resultPos = 0;
        Result *res = new Result();
        res->FromString(req->result(), resultPos);

        *result = res;
        resp->set_success(true);
    }

   private:
    Result **result;
};
}  // namespace Client
}  // namespace ProtoBuf
}  // namespace AGE
