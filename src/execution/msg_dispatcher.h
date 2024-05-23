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

#include <glog/logging.h>
#include <deque>
#include <string>
#include <utility>
#include <vector>
#include "base/item.h"
#include "base/node.h"
#include "base/row.h"
#include "base/type.h"
#include "execution/message.h"
#include "execution/physical_plan.h"
#include "operator/op_type.h"
#include "operator/ops.h"
#include "storage/id_mapper.h"
#include "util/config.h"

using std::deque;

namespace AGE {

class MsgDispatcher {
   public:
    MsgDispatcher(const Node& self_node, const Nodes& cache_nodes)
        : self_node_(self_node), target_cluster_(cache_nodes), id_mapper_(cache_nodes) {}

    void dispatch(vector<Message>& msgs) {
        vector<Message> ret;
        for (Message& m : msgs) dispatch(m, ret);
        LOG_IF(INFO, Config::GetInstance()->verbose_ >= 2)
            << "MsgDispatcher dispatches " << msgs.size() << " messages to " << ret.size() << " messages" << std::flush;
        msgs.swap(ret);
    }

    void split_messages(vector<Message>& msgs) {
        vector<Message> ret;
        for (auto& m : msgs) {
            OpType optype = m.plan->getStage(m.header.currentStageIdx)->getOpType(m.header.currentOpIdx);
            if (OpType_IsBarrier(optype)) {
                continue;
            }
            Message::SplitMessage(m, ret);
        }
        LOG_IF(INFO, Config::GetInstance()->verbose_ >= 2)
            << "MsgDispatcher splits " << msgs.size() << " messages to " << ret.size() << " messages" << std::flush;
        msgs.swap(ret);
    }

   private:
    void dispatch(Message& msg, vector<Message>& ret) {
        if (MessageHeader::NoSplit(msg.header.type)) {
            ret.emplace_back(msg);
            return;
        }

        vector<Message> new_msgs;
        CHECK(msg.plan != nullptr) << "MsgDispatcher dispatch message without plan";
        OpType cur_op_type = msg.plan->getStage(msg.header.currentStageIdx)->getOpType(msg.header.currentOpIdx);

        // Recording ret size before dispatch (i.e., cannot use Message::SplitMessage)
        size_t originalReturnSize = ret.size();

        if (OpType_NeedRequestData(cur_op_type)) {
            msg.header.senderPort = self_node_.port;
            if (OpType_IsScan(cur_op_type) || OpType_IsIndex(cur_op_type)) {
                dispatch_by_cluster_size(msg, new_msgs);
            } else {
                dispatch_by_storage(msg, new_msgs);
            }
        } else if (OpType_IsSubquery(cur_op_type)) {
            // Need to check whether the message is sent to subquery entry ops
            dispatch_to_subquery_entry(msg, new_msgs);
        } else {
            // TODO(cjli): whether split the message
            new_msgs.emplace_back(std::move(msg));
        }

        for (auto& m : new_msgs) {
            uint32_t compressBeginIdx = m.GetCompressBeginIdx();
            do {
                ret.emplace_back(m.split(compressBeginIdx));
            } while (m.data.size() > 0);
        }

        // Recording split number
        uint32_t splitSize = ret.size() - originalReturnSize;
        if (splitSize <= 1) return;
        for (size_t i = originalReturnSize; i < ret.size(); i++) {
            ret[i].header.splitHistory += "\t" + std::to_string(splitSize);
            ret[i].header.opPerfInfos.emplace_back();
        }
    }

    void dispatch_by_cluster_size(Message& msg, vector<Message>& output) {
        // create all messages
        for (size_t node_idx = 0; node_idx < target_cluster_.getWorldSize(); node_idx++) {
            const Node& target_node = target_cluster_.get(node_idx);
            Tool::string2ip(target_node.host, msg.header.recverNode);
            msg.header.recverPort = target_node.port;
            output.emplace_back(msg);
        }
    }

    void dispatch_by_storage(Message& msg, vector<Message>& output) {
        if (msg.data.size() == 0) {
            return;
        }

        u32 compressBeginIdx = msg.GetCompressBeginIdx();
        ColIndex storageFetchCol =
            msg.plan->getStage(msg.header.currentStageIdx)->ops_[msg.header.currentOpIdx]->storageFetchCol;

        ItemType type = touch_item_type(msg, storageFetchCol);
        CHECK(type == T_EDGE || type == T_VERTEX) << "Wrong Item type for Dispatcher: " << ItemType_DebugString(type);

        // Dispatch data into different nodes according to idMapper
        vector<vector<Row>> newData(target_cluster_.getWorldSize());
        int invalid_rank_counter = 0;
        if (storageFetchCol == COLINDEX_COMPRESS) {
            for (Row& r : msg.data) {
                uint32_t rowCount = r.count();
                // Only create new row when first appear in a node
                deque<bool> isEmpty(target_cluster_.getWorldSize(), true);
                for (size_t i = compressBeginIdx; i < r.size(); i++) {
                    int nid = id_mapper_.GetRank(r[i]);
                    if (nid == INVALID_RANK) {
                        // Randomly choose a node
                        nid = invalid_rank_counter++ % target_cluster_.getWorldSize();
                    }
                    if (isEmpty[nid]) {
                        newData[nid].emplace_back(r, 0, compressBeginIdx);
                        isEmpty[nid] = false;
                    }
                    newData[nid].back().emplace_back(std::move(r[i]));
                    if (newData[nid].back().size() == 1ull) newData[nid].back().setCount(rowCount);
                }
            }
        } else {
            for (Row& r : msg.data) {
                int nid = id_mapper_.GetRank(r[storageFetchCol]);
                if (nid == INVALID_RANK) {
                    // Randomly choose a node
                    nid = invalid_rank_counter++ % target_cluster_.getWorldSize();
                }
                newData[nid].emplace_back(std::move(r));
            }
        }
        msg.data.clear();

        // Complete message header and create all messages
        fill_basic_info(msg);
        for (u8 i = 0; i < target_cluster_.getWorldSize(); i++) {
            if (newData[i].empty()) continue;

            Message m(msg, false);
            const Node& target_node = target_cluster_.get(i);
            Tool::string2ip(target_node.host, m.header.recverNode);
            m.header.recverPort = target_node.port;
            m.data.swap(newData[i]);
            output.emplace_back(std::move(m));
        }
    }

    void dispatch_to_subquery_entry(Message& msg, vector<Message>& opt) {
        size_t subquery_depth = msg.header.subQueryInfos.size();
        if (subquery_depth > 0) {
            if (msg.header.subQueryInfos.back().subquery_entry_pos ==
                msg.header.currentStageIdx) {  // subquery send back
                msg.header.type = MessageType::SUBQUERY;
            }
        }
        opt.emplace_back(std::move(msg));
    }

    void fill_basic_info(Message& msg) {
        Tool::string2ip(self_node_.host, msg.header.senderNode);
        msg.header.senderPort = self_node_.port;
    }

    ItemType touch_item_type(Message& msg, ColIndex col) {
        // If data is empty, is ok to return T_VERTEX
        uint32_t compressBeginIdx = msg.GetCompressBeginIdx();
        if (col == COLINDEX_COMPRESS) {
            for (const Row& r : msg.data) {
                for (u64 col = compressBeginIdx; col < r.size(); col++) {
                    if (r[col].type == T_NULL) continue;
                    return r[col].type;
                }
            }
        } else {
            for (const Row& r : msg.data) {
                if (r[col].type == T_NULL) continue;
                return r[col].type;
            }
        }
        return T_VERTEX;
    }

    const Node& self_node_;
    const Nodes& target_cluster_;  // CacheCluster Topo for ComputeNode
    IdMapper id_mapper_;
};

}  // namespace AGE
