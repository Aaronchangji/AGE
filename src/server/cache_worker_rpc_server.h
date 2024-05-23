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
#include <netinet/in.h>
#include <memory>
#include <regex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "base/graph_entity.h"
#include "base/item.h"
#include "base/node.h"
#include "base/serializer.h"
#include "base/type.h"
#include "base/thpt_metrics.h"
#include "execution/cache_executor.h"
#include "execution/mailbox.h"
#include "execution/result_collector.h"
#include "model/rl_action.h"
#include "proto/cache_worker.pb.h"
#include "proto/compute_worker.pb.h"
#include "storage/general_histogram.h"
#include "storage/graph.h"
#include "storage/meta_store.h"
#include "util/config.h"
#include "util/thpt_test_config.h"
#include "util/thpt_test_helper.h"

using std::string;
using std::vector;

namespace AGE {
namespace ProtoBuf {
namespace CacheWorker {

class RpcServer : public Service {
   public:
    RpcServer(u8 rank, Mailbox &mailbox, CacheExecutor &executor, Graph &g, MetaStore *meta_store)
        : rank(rank), mailbox(mailbox), g(g), meta_store(meta_store) {
        config = Config::GetInstance();
    }
    ~RpcServer() {}

    void SendMsg(google::protobuf::RpcController *cntlBase, const SendMsgRequest *req, SendMsgResponse *resp,
                 google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);
        brpc::Controller *cntl = static_cast<brpc::Controller *>(cntlBase);
        mailbox.send_local(req->msg().data(), req->msg().header(), req->plan().data(),
                           ntohl(cntl->remote_side().ip.s_addr));
        resp->set_success(true);
    }

    void GetThptTestQueries(google::protobuf::RpcController *cntlBase, const ThptTestQueryReq *req,
                            ThptTestQueryResp *resp, google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);
        ThptTestConfig conf;
        size_t pos = 0;
        conf.FromString(req->config(), pos);

        u32 queryPropId = g.getPropId(conf.queryTemplates[0].place_holder);
        u32 label = g.getLabelId(conf.queryTemplates[0].place_holder_label);
        MetaStore::MetaStoreKey degree_filter_key = GetEdgeFilter(conf, label);

        LOG(INFO) << "Query PropId " << conf.queryTemplates[0].place_holder << ": " << queryPropId << ", " << label;
        LOG(INFO) << "Seeds filtered with histogram: " << degree_filter_key.DebugString();

        vector<Item> labelVtx;
        if (label == ALL_LABEL) {
            if (allVtx.size() == 0) g.getAllVtx(allVtx);
        } else {
            g.getLabelVtx(label, labelVtx, conf.kSeedPerWorker * 100);
        }
        vector<Item> *candidates = label == ALL_LABEL ? &allVtx : &labelVtx;

        std::default_random_engine rand_generator(time(nullptr));
        auto startT = Tool::getTimeMs();
        for (int j = 0; j < conf.kSeedPerWorker; j++) {
            string result = "";
            while (strlen(result.c_str()) == 0) {
                Item &candidate = candidates->at(rand_generator() % (candidates->size() - 1));
                if (!VertexDegreeFilter(conf, degree_filter_key, candidate)) continue;
                Item randResult = g.getProp(candidate, queryPropId);

                switch (randResult.type) {
                case T_STRING:
                case T_STRINGVIEW:
                    result = std::string(randResult.stringVal);
                    break;
                case T_FLOAT:
                    result = std::to_string(randResult.floatVal);
                    break;
                case T_INTEGER:
                    result = std::to_string(randResult.integerVal);
                    break;
                case T_BOOL:
                    result = randResult.boolVal ? "true" : "false";
                    break;
                default:
                    break;
                }
            }
            resp->add_seeds(result);
        }
        auto duration = Tool::getTimeMs() - startT;
        LOG(INFO) << "Duration of query generation: " << duration << " ms" << std::flush;

        resp->set_success(true);
    }

    void GetHistogram(google::protobuf::RpcController *cntlBase, const GetHistogramReq *req, GetHistogramResp *resp,
                      google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);

        MetaStore::MetaStoreKey key;
        LabelId lid = g.getLabelId(req->label()), elid;
        PropId pid;
        u16 granularity = req->granularity();
        DirectionType dir = static_cast<DirectionType>(req->direction());
        if (dir == 255) {
            pid = g.getPropId(req->prop());
            key = MetaStore::MetaStoreKey(lid, pid);
        } else {
            elid = g.getLabelId(req->prop());
            key = MetaStore::MetaStoreKey(lid, elid, dir);
        }

        if (!meta_store->FindHistogram(key, granularity)) {
            resp->set_success(false);
            return;
        }

        meta_store->GetHistogram(key).ToString(resp->mutable_hist());
        resp->set_success(true);
    }

    void ApplyRLAction(google::protobuf::RpcController *cntlBase, const ApplyRLActionReq *req, ApplyRLActionResp *resp,
                       google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);
        MLModel::RLAction action;
        size_t pos = 0;
        action.FromString(req->action(), pos);
        // config->set_batch_size(action.get_batch_size());
        config->set_message_size(action.get_message_size());
        LOG(INFO) << "Change Message Size to " << action.get_message_size();
        resp->set_success(true);
    }

    void GetThptMetrics(google::protobuf::RpcController *cntlBase, const GetThptMetricsReq *req, GetThptMetricsResp *resp,
                        google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);
        std::shared_ptr<NetworkMonitors> net_monitor = mailbox.get_network_monitors();
        uint64_t duration_ns = req->duration();

        uint64_t msgs, bytes, heavy_msgs, heavy_bytes;
        net_monitor->get(msgs, heavy_msgs, bytes, heavy_bytes);
        double msg_thpt = (double)msgs * 1E9 / duration_ns;
        double heavy_msg_thpt = (double)heavy_msgs * 1E9 / duration_ns;
        double byte_thpt = (double)bytes * 1E9 / duration_ns;
        double heavy_byte_thpt = (double)heavy_bytes * 1E9 / duration_ns;
        LOG(INFO) << "Network thpt: " << msg_thpt << " msgs/s, " << byte_thpt << " bytes/s, " << heavy_msg_thpt
                  << " heavy msgs/s, " << heavy_byte_thpt << " heavy bytes/s";
        InnerMetrics m; 
        m.collect_request_info(msg_thpt, heavy_msg_thpt, byte_thpt, heavy_byte_thpt, true);
        m.ToString(resp->mutable_metrics(), false);
        resp->set_success(true);
    }

   private:
    MetaStore::MetaStoreKey GetEdgeFilter(const ThptTestConfig &config, LabelId vlabel) {
        CHECK(config.queryTemplates.size());
        const ThptTestConfig::QueryTemplate &tmpl = config.queryTemplates.back();
        // Check if filter is required, return an empty key if not
        if (tmpl.edge_dir > DirectionType_OUT) return MetaStore::MetaStoreKey();
        DirectionType dir = static_cast<DirectionType>(tmpl.edge_dir);
        // Check if edge label is specified
        LabelId elabel = ALL_LABEL;
        if (tmpl.edge_label.size()) elabel = g.getLabelId(tmpl.edge_label);
        return MetaStore::MetaStoreKey(vlabel, elabel, dir);
    }

    bool VertexDegreeFilter(const ThptTestConfig &config, MetaStore::MetaStoreKey &key, const Item &candidate) {
        if (key.type == MetaStore::kNone || !meta_store->FindHistogram(key)) return true;

        float min_percent = config.minVertexDegree / 100.0, max_percent = config.maxVertexDegree / 100.0;
        i64 candi_degree = static_cast<i64>(g.getNeighborCnt(candidate, key.GetEdgeLabel(), key.GetEdgeDir()));
        auto [low_percent, high_percent] = meta_store->QueryHistogram(key, Item(T_INTEGER, candi_degree));
        // LOG(INFO) << candidate.DebugString() << ": " << candi_degree << " " << low_percent << " " << high_percent;
        // 1-to-1 edges, e.g., isLocatedIn
        if (low_percent == 0.0 && high_percent == 1.0) return true;
        // 1-to-n edges, e.g., knows
        if (low_percent >= min_percent && high_percent <= max_percent) return true;
        return false;
    }

    u8 rank;
    Mailbox &mailbox;
    Graph &g;
    MetaStore *meta_store;
    Config *config;

    vector<Item> allVtx;
    vector<string> queryTemplate;
};
}  // namespace CacheWorker
}  // namespace ProtoBuf
}  // namespace AGE
