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

#include <glog/logging.h>
#include <algorithm>
#include <fstream>
#include <memory>
#include <regex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "base/item.h"
#include "base/node.h"
#include "base/serializer.h"
#include "execution/compute_executor.h"
#include "execution/mailbox.h"
#include "execution/monitor.h"
#include "execution/result_collector.h"
#include "execution/thpt_handler.h"
#include "model/rl_action.h"
#include "plan/planner.h"
#include "proto/compute_worker.pb.h"
#include "storage/graph.h"
#include "storage/unique_namer.h"
#include "util/config.h"
#include "util/tool.h"

using std::string;
using std::vector;

namespace AGE {
namespace ProtoBuf {
namespace ComputeWorker {

class RpcServer : public Service {
   public:
    RpcServer(const Node &self_node, const Nodes &cache_nodes, Mailbox &mailbox, ComputeExecutor &executor,
              ResultCollector &rc, Planner *planner, std::shared_ptr<WorkerThptHandler> thpt_handler,
              std::shared_ptr<Monitor<thpt_result_data_t>> thpt_result_monitor,
              std::shared_ptr<Monitor<thpt_result_data_t>> thpt_result_timeout_monitor,
              std::shared_ptr<Monitor<thpt_input_data_t>> thpt_input_monitor)
        : config(Config::GetInstance()),
          self_node(self_node),
          cache_nodes(cache_nodes),
          mailbox(mailbox),
          executor(executor),
          rc(rc),
          planner(planner),
          thpt_handler(thpt_handler),
          thpt_result_monitor(thpt_result_monitor),
          thpt_result_timeout_monitor(thpt_result_timeout_monitor),
          thpt_input_monitor(thpt_input_monitor) {}
    ~RpcServer() {}

    // CacheWorker invoke this function to send message to compute worker
    void SendMsg(google::protobuf::RpcController *cntlBase, const SendMsgRequest *req, SendMsgResponse *resp,
                 google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);
        mailbox.send_local(req->msg().data(), req->msg().header());
        resp->set_success(true);
    }

    // Client invoke this function to send query to the compute worker
    void SendQuery(google::protobuf::RpcController *cntlBase, const SendQueryRequest *req, SendQueryResponse *resp,
                   google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);
        brpc::Controller *cntl = static_cast<brpc::Controller *>(cntlBase);

        string query = req->query(), clientHost;
        butil::ip2hostname(cntl->remote_side().ip, &clientHost);
        string clientHostPort = clientHost + ":" + to_string(req->client_port());
        mailbox.add_query(std::move(query), std::move(clientHostPort), Tool::getTimeNs() / 1000, req->timeout(),
                          req->is_throughput_test(), req->query_template_idx());
        // thpt_input_data_t d;
        // thpt_input_monitor->record_single_data(d);
        resp->set_success(true);
    }

    void SendMultiQuery(google::protobuf::RpcController *cntlBase, const SendMultiQueryReq *req,
                        SendMultiQueryResp *resp, google::protobuf::Closure *done) {
        // Only thpt test will use this rpc
        // temporarily not used
        brpc::ClosureGuard doneGuard(done);
        brpc::Controller *cntl = static_cast<brpc::Controller *>(cntlBase);

        auto cur_time = Tool::getTimeNs() / 1000;
        for (int i = 0; i < req->queries_size(); i++) {
            string client = "";
            string q = req->queries(i);
            mailbox.add_query(std::move(q), std::move(client), cur_time, req->timeout(), true, -1);
        }
        resp->set_success(true);
    }

    void SyncIndex(google::protobuf::RpcController *cntlBase, const SyncIndexReq *req, SyncIndexResp *resp,
                   google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);
        planner->UpdateIndexEnableSet(req->label(), req->prop(), req->iscreate());
        resp->set_success(true);
    }

    void CheckThpt(google::protobuf::RpcController *cntlBase, const CheckThptReq *req, CheckThptResp *resp,
                   google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);
        uint32_t sent_num_queries = req->num_queries();
        auto num_queries = thpt_result_monitor->get_all_data_size() + thpt_result_timeout_monitor->get_all_data_size();
        if (sent_num_queries > num_queries) {
            LOG(INFO) << std::to_string(sent_num_queries - num_queries) << " queries are not finished";
            resp->set_finish(false);
        } else {
            resp->set_finish(true);
        }
        resp->set_num_finished_queries(num_queries);
    }

    void GetThptMetrics(google::protobuf::RpcController *cntlBase, const GetThptMetricsReq *req,
                        GetThptMetricsResp *resp, google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);
        string ofname = req->ofname();
        bool enable_timeout = Config::GetInstance()->enable_timeout_;
        bool is_stream = req->is_stream();

        if (!is_stream) thpt_handler->disable_monitors();

        thpt_handler->collect_thpt_metrics(ofname, req->num_sent_queries(), /*us*/ req->max_lat() * 1E6,
                                           req->duration(), req->with_idxed_lat(), req->with_idxed_width());
        if (req->with_model_state()) {
            thpt_handler->collect_rl_state(req->duration());
            thpt_handler->StateToString(resp->mutable_model_state());
        }

        // Set the required part
        resp->set_duration(thpt_handler->get_thpt_duration());
        resp->set_num_queries(thpt_handler->get_num_queries());
        InnerMetrics &inner_metrics = thpt_handler->get_metrics();
        inner_metrics.ToString(resp->mutable_metrics(), enable_timeout);

        if (req->with_idxed_lat()) {
            unordered_map<int, double> idxed_p50, idxed_p99, idxed_qos, idxed_width;
            thpt_handler->get_idxed_data(idxed_p50, idxed_p99, idxed_qos, idxed_width);

            CHECK_EQ(idxed_p50.size(), idxed_p99.size());
            if (enable_timeout) CHECK_EQ(idxed_p50.size(), idxed_qos.size());

            for (auto itr = idxed_p50.begin(); itr != idxed_p50.end(); itr++) {
                ProtoBuf::ComputeWorker::GetThptMetricsResp::idxed_lat *lat = resp->add_lats();
                CHECK(idxed_p99.find(itr->first) != idxed_p99.end());
                lat->set_qt_idx(itr->first);
                lat->set_p50(itr->second);
                lat->set_p99(idxed_p99.at(itr->first));
                if (enable_timeout) {
                    CHECK(idxed_qos.find(itr->first) != idxed_qos.end());
                    lat->set_qos(idxed_qos.at(itr->first));
                }
                if (req->with_idxed_width()) {
                    CHECK(idxed_width.find(itr->first) != idxed_width.end());
                    lat->set_p99_width(idxed_width.at(itr->first));
                }
            }
        }

        resp->set_success(true);
    }

    void GetInterData(google::protobuf::RpcController *cntlBase, const GetInterDataReq *req,
                      GetInterDataResp *resp, google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);
        vector<string> data;
        thpt_handler->collect_inter_data(data);
        for (auto &d : data) {
            resp->add_data(d);
        }
        resp->set_success(true);
    }

    void ClearThptMonitor(google::protobuf::RpcController *cntlBase, const AnnounceReq *req, AnnounceResp *resp,
                          google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);
        LOG(INFO) << "Monitor Cleared";
        thpt_handler->reset();
        resp->set_success(true);
    }

    void ThptStart(google::protobuf::RpcController *cntlBase, const AnnounceReq *req, AnnounceResp *resp,
                   google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);
        LOG(INFO) << "Start thpt test";
        thpt_handler->thpt_start();
        resp->set_success(true);
    }

    void ThptSendEnd(google::protobuf::RpcController *cntlBase, const AnnounceReq *req, AnnounceResp *resp,
                     google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);
        LOG(INFO) << "Send end thpt test";
        thpt_handler->thpt_send_end();
        resp->set_success(true);
    }

    void GetRLState(google::protobuf::RpcController *cntlBase, const GetRLStateReq *req, GetRLStateResp *resp,
                    google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);
        thpt_handler->StateToString(resp->mutable_state());
        resp->set_success(true);
    }

    void ApplyRLAction(google::protobuf::RpcController *cntlBase, const ApplyRLActionReq *req, ApplyRLActionResp *resp,
                       google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);
        MLModel::RLAction action;
        size_t pos = 0;
        action.FromString(req->action(), pos);
        LOG(INFO) << "Apply Batch Size: " << action.get_batch_size();
        LOG(INFO) << "Apply Message Size: " << action.get_message_size();
        config->set_batch_size(action.get_batch_size());
        config->set_message_size(action.get_message_size());
        resp->set_success(true);
    }

   private:
    Config *config;
    const Node &self_node;
    const Nodes &cache_nodes;
    Mailbox &mailbox;
    ComputeExecutor &executor;
    ResultCollector &rc;
    Planner *planner;

    std::shared_ptr<WorkerThptHandler> thpt_handler;
    std::shared_ptr<Monitor<thpt_result_data_t>> thpt_result_monitor;
    std::shared_ptr<Monitor<thpt_result_data_t>> thpt_result_timeout_monitor;
    std::shared_ptr<Monitor<thpt_input_data_t>> thpt_input_monitor;
};
}  // namespace ComputeWorker
}  // namespace ProtoBuf
}  // namespace AGE
