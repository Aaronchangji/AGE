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

#pragma once

#include <brpc/channel.h>
#include <glog/logging.h>
#include <stdio.h>
#include <memory>
#include <string>

#include "util/config.h"

namespace AGE {
using std::flush;
using std::string;

class BRPCHelper {
   public:
    static std::shared_ptr<brpc::Channel> build_channel(int32_t timeout_ms, int max_retry, string ip_addr, int port) {
        string full_addr = ip_addr + ":" + std::to_string(port);
        return build_channel(timeout_ms, max_retry, full_addr);
    }

    /**
     * @param send_to_server: Client is assumed NOT to support RDMA connection
     * @return: a pointer to channle. If it's nullptr, then channel build failed
     */
    static std::shared_ptr<brpc::Channel> build_channel(int32_t timeout_ms, int max_retry, string addr,
                                                        bool send_to_server = true) {
        brpc::ChannelOptions options;
        options.use_rdma = send_to_server && Config::GetInstance()->brpc_use_rdma_;
        options.timeout_ms = timeout_ms;
        options.max_retry = max_retry;

        std::shared_ptr<brpc::Channel> channel(new brpc::Channel());
        if (channel->Init(addr.c_str(), &options) != 0) {
            LOG(ERROR) << "Fail to initialize channel to " << addr << flush;
            exit(-1);
        }
        return channel;
    }

    // Log after a rpc call
    static bool LogRpcCallStatus(brpc::Controller& cntl, bool logSuccess = true) {
        if (!cntl.Failed()) {
            if (logSuccess) {
                bool long_latency = cntl.latency_us() > 3000;
                LOG_IF(INFO, Config::GetInstance()->verbose_ && long_latency)
                    << "Received response from " << cntl.remote_side() << ", latency=" << cntl.latency_us() << "us"
                    << flush;
            }
        } else {
            LOG(WARNING) << cntl.ErrorText() << flush;
        }
        return !cntl.Failed();
    }
};
}  // namespace AGE
