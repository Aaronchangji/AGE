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

#include "execution/mailbox.h"
#include <memory>
#include "base/type.h"
#include "execution/message_header.h"

namespace AGE {

bool Mailbox::compute_try_recv(int tid, Message& msg) {
    InMsgT message_struct;
    bool success = in_msg_queues_[tid].wait_dequeue_timed(message_struct, 100);
    if (!success) return false;
    bool from_raw_data = message_struct.GetComputeMessage(msg);
    in_msg_queue_scheduler_.SubLoad(tid, from_raw_data ? 1 : msg.data.size());
    return true;
}

bool Mailbox::compute_try_steal(int tid, Message& msg) {
    int target_thread_id = tid;
    while (target_thread_id == tid) {
        target_thread_id = rand_generator_() % num_exe_threads_;
    }
    return compute_try_recv(target_thread_id, msg);
}

bool Mailbox::cache_try_recv(int tid, Message& msg, Graph& g) {
    InMsgT message_struct;
    bool success = in_msg_queues_[tid].wait_dequeue_timed(message_struct, 100);
    if (!success) return false;
    message_struct.GetCacheMessage(msg, g);
    in_msg_queue_scheduler_.SubLoad(tid, msg.data.size());
    return true;
}

bool Mailbox::cache_try_steal(int tid, Message& msg, Graph& g) {
    int target_thread_id = tid;
    while (target_thread_id == tid) {
        target_thread_id = rand_generator_() % num_exe_threads_;
    }
    return cache_try_recv(target_thread_id, msg, g);
}

void Mailbox::send_single_msg(Message&& msg, ClusterRole target_cluster) {
    string addr;
    Tool::ip2string(msg.header.recverNode, addr);
    addr += ":" + std::to_string(msg.header.recverPort);
    brpc::Channel* channel = get_channel(addr);
    if (channel == nullptr) {
        LOG(ERROR) << "brpc channel build fails" << std::flush;
        exit(-1);
    }

    DLOG_IF(INFO, Config::GetInstance()->verbose_ >= 2) << "Send message to Cluster " << RoleString(target_cluster)
                                                        << ", Node " << addr << ":" << msg.header.recverPort;

    if (target_cluster == ClusterRole::CACHE) {
        ProtoBuf::CacheWorker::Service_Stub stub(channel);
        /* Prepare rpc request message */
        ProtoBuf::CacheWorker::SendMsgRequest req;
        // LOG(INFO) << "Send to CACHE with batchIdx: " << msg.header.batchIdx;

        bool is_heavy = msg.plan->is_heavy;
        auto st = Tool::getTimeMs();
        msg.header.ToString(req.mutable_msg()->mutable_header(), target_cluster);
        msg.ToString(req.mutable_msg()->mutable_data(), target_cluster);
        auto et = Tool::getTimeMs() - st, msg_size = req.mutable_msg()->ByteSizeLong();
        LOG_IF(INFO, Config::GetInstance()->verbose_ >= 2)
            << "Serialize msg of size " << msg_size << " in: " << et << " ms" << std::flush;
        network_monitors_->record(msg_size, is_heavy);

        /* We currently only send one operator to cache worker for processing */
        PhysicalPlan::SingleStageToString(msg.plan, req.mutable_plan()->mutable_data(), msg.header.currentStageIdx);
        brpc::Controller cntl;
        ProtoBuf::CacheWorker::SendMsgResponse resp;

        st = Tool::getTimeMs();
        stub.SendMsg(&cntl, &req, &resp, nullptr);
        et = Tool::getTimeMs() - st;
        LOG_IF(INFO, Config::GetInstance()->verbose_ >= 2) << "SendMsg time: " << et << " ms" << std::flush;
        // LOG(INFO) << "SendMsg time: " << et << " ms to Node " << addr << ":" << msg.header.recverPort;
        // if (et > 500) {
        //     LOG(INFO) << "This heavy message is " << msg.header.DebugString();
        // }

        BRPCHelper::LogRpcCallStatus(cntl);
    } else if (target_cluster == ClusterRole::COMPUTE) {
        ProtoBuf::ComputeWorker::Service_Stub stub(channel);
        /* Prepare rpc request message */
        ProtoBuf::ComputeWorker::SendMsgRequest req;

        bool is_heavy = msg.plan->is_heavy;
        auto st = Tool::getTimeMs();
        msg.header.ToString(req.mutable_msg()->mutable_header(), target_cluster);
        msg.ToString(req.mutable_msg()->mutable_data(), target_cluster);
        auto et = Tool::getTimeMs() - st, msg_size = req.mutable_msg()->ByteSizeLong();
        LOG_IF(INFO, Config::GetInstance()->verbose_ >= 2)
            << "Serialize msg of size " << msg_size << " in: " << et << " ms" << std::flush;
        network_monitors_->record(msg_size, is_heavy);

        brpc::Controller cntl;
        ProtoBuf::ComputeWorker::SendMsgResponse resp;

        st = Tool::getTimeMs();
        stub.SendMsg(&cntl, &req, &resp, nullptr);
        et = Tool::getTimeMs() - st;
        LOG_IF(INFO, Config::GetInstance()->verbose_ >= 2) << "SendMsg time: " << et << " ms" << std::flush;
        // LOG(INFO) << "SendMsg time: " << et << " ms to Node " << addr << ":" << msg.header.recverPort;
        // if (et > 500) {
        //     LOG(INFO) << "This heavy message is " << msg.header.DebugString();
        // }

        BRPCHelper::LogRpcCallStatus(cntl);
    } else {
        LOG(ERROR) << "Unexpected Role: " << RoleString(target_cluster) << std::flush;
        return;
    }
}

void Mailbox::rdma_send_single_msg(int tid, Message&& msg, ClusterRole target_cluster) {
    string addr;
    Tool::ip2string(msg.header.recverNode, addr);
    int dst_nid = rdma_mailbox_->get_nid(addr);
    if (dst_nid == -1) {
        LOG(ERROR) << "Cannot find the nid for " << addr << std::flush;
        exit(-1);
    }

    // prepare data
    bool is_heavy = msg.plan->is_heavy;
    string data;
    msg.header.ToString(&data, target_cluster);
    msg.ToString(&data, target_cluster);
    if (target_cluster == ClusterRole::CACHE) {
        PhysicalPlan::SingleStageToString(msg.plan, &data, msg.header.currentStageIdx);
    }

    size_t msg_size = data.size();
    network_monitors_->record(msg_size, is_heavy);
    rdma_mailbox_->send_data(tid, dst_nid, tid, data);
}

bool Mailbox::rdma_try_recv(int tid, ClusterRole target_cluster, bool placeholder) {
    string data;
    string remote_addr;
    CHECK_LT(tid, num_mailbox_threads_) << "tid: " << tid << ", num_mailbox_threads_: " << num_mailbox_threads_;
    if (rdma_mailbox_->try_recv(tid, data, remote_addr)) {
        // Got the data, insert it into the in_msg_queue_
        if (target_cluster == ClusterRole::CACHE) {
            uint32_t remote_ip;
            Tool::string2ip(remote_addr, remote_ip);
            send_local(data, remote_ip);
        } else if (target_cluster == ClusterRole::COMPUTE) {
            send_local(data);
        } else {
            LOG(ERROR) << "Unexpected Role: " << RoleString(target_cluster) << std::flush;
            return false;
        }
    }
    return true;
}

void Mailbox::try_send(int tid) {
    vector<OutMsgT> msg_vec(100);
    CHECK_LT(tid, num_mailbox_threads_) << "tid: " << tid << ", num_mailbox_threads_: " << num_mailbox_threads_;
    u64 msg_cnt = out_msg_queues_[tid].wait_dequeue_bulk(msg_vec.begin(), msg_vec.size());
    for (u64 i = 0; i < msg_cnt; i++) {
        out_msg_queue_scheduler_.SubLoad(tid, msg_vec[i].msg_.data.size());
        if (Config::GetInstance()->use_rdma_mailbox_) {
            rdma_send_single_msg(tid, std::move(msg_vec[i].msg_), msg_vec[i].target_cluster_);
        } else {
            send_single_msg(std::move(msg_vec[i].msg_), msg_vec[i].target_cluster_);
        }
    }
}

}  // namespace AGE
