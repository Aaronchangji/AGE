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

#include <thread>
#include "execution/mailbox.h"
#include "execution/physical_plan.h"
#include "gtest/gtest.h"
#include "operator/vertex_scan_op.h"

namespace AGE {
constexpr uint64_t kOfThreads = 16;
constexpr uint64_t kOfMailboxThreads = 16;

TEST(test_mail_box, test_send_receive) {
    Config::GetInstance()->num_threads_ = kOfThreads;
    Config::GetInstance()->num_mailbox_threads_ = kOfMailboxThreads;
    Mailbox mailbox(kOfThreads, kOfMailboxThreads);
    shared_ptr<PhysicalPlan> plan = std::make_shared<PhysicalPlan>(PhysicalPlan());
    plan->stages.emplace_back(new ExecutionStage());
    plan->stages[0]->appendOp(new VertexScanOp(0, 0, 2));
    for (uint64_t i = 0; i < kOfThreads; i++) {
        Message sendMsg;
        sendMsg.header.qid = i;
        sendMsg.header.currentStageIdx = 0;
        sendMsg.header.currentOpIdx = 0;
        sendMsg.header.recverNode = 0;
        sendMsg.plan = plan;
        mailbox.send_local(std::move(sendMsg));
        Message recvMsg;
        EXPECT_TRUE(mailbox.compute_try_recv(i, recvMsg));
        EXPECT_EQ(i, recvMsg.header.qid);
    }
}

TEST(test_mail_box, test_concurrent_send_receive) {
    Config::GetInstance()->num_threads_ = kOfThreads;
    Config::GetInstance()->num_mailbox_threads_ = kOfMailboxThreads;
    Mailbox mailbox(kOfThreads, kOfMailboxThreads);
    shared_ptr<PhysicalPlan> plan = std::make_shared<PhysicalPlan>(PhysicalPlan());
    plan->stages.emplace_back(new ExecutionStage());
    plan->stages[0]->appendOp(new VertexScanOp(0, 0, 2));
    for (uint64_t i = 0; i < kOfThreads; i++) {
        Message sendMsg;
        sendMsg.header.qid = i;
        sendMsg.header.currentStageIdx = 0;
        sendMsg.header.currentOpIdx = 0;
        sendMsg.header.recverNode = 0;
        sendMsg.plan = plan;
        mailbox.send_local(std::move(sendMsg));
    }
    std::vector<std::thread> threads;
    Message messages[kOfThreads];

    for (size_t i = 0; i < kOfThreads; i++) {
        threads.emplace_back(std::thread(&Mailbox::compute_try_recv, &mailbox, i, std::ref(messages[i])));
    }

    for (auto &t : threads) {
        t.join();
    }

    for (size_t i = 0; i < kOfThreads; i++) {
        EXPECT_EQ(i, messages[i].header.qid);
    }
}
}  // namespace AGE
