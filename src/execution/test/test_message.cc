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

#include "execution/message.h"
#include "execution/physical_plan.h"
#include "gtest/gtest.h"
#include "operator/ops.h"

namespace AGE {

#define ceiling_div(x, y) (((x) + (y)-1) / (y))

void testSingleSplit(u64 rowNum, u64 rowLen, u32 compressBeginIdx, u64 maxDataSize) {
    MessageHeader header = MessageHeader::InitHeader(0);
    header.maxDataSize = maxDataSize;
    Message m(std::move(header));

    for (u32 i = 0; i < rowNum; i++) {
        m.data.emplace_back(0);
        for (u32 j = 0; j < rowLen; j++) {
            m.data.back().emplace_back(Item(T_INTEGER, i64(i * rowLen + j)));
        }
    }

    // LOG(INFO) << "Before split: " << m.DebugString();
    Message n = m.split(compressBeginIdx);
    // LOG(INFO) << "After split m: " << m.DebugString();
    // LOG(INFO) << "After split n: " << n.DebugString();
    EXPECT_EQ(static_cast<u64>(n.data.size()), (maxDataSize + rowLen - compressBeginIdx - 1) / rowLen);
    EXPECT_EQ(static_cast<u64>(m.data.size()), rowNum - maxDataSize / rowLen);

    bool splitCompressCol = maxDataSize % rowLen > compressBeginIdx;

    i64 nTotalGT = splitCompressCol ? maxDataSize : maxDataSize / rowLen * rowLen,
        mTotalGT = splitCompressCol ? rowNum * rowLen - nTotalGT + compressBeginIdx : rowNum * rowLen - nTotalGT;

    int cur = 0;

    // Check m
    for (const Row& r : m.data)
        for (const Item& item : r) EXPECT_EQ(item, Item(T_INTEGER, cur++));
    EXPECT_EQ(cur, mTotalGT);

    // LOG(INFO) << "cur: " << cur;
    // Check n
    if (splitCompressCol) {
        u32 splitRowIdx = rowNum - maxDataSize / rowLen - 1;
        u32 beg = rowLen * splitRowIdx;
        for (u32 i = 0; i < compressBeginIdx; i++) EXPECT_EQ(n.data[0][i], Item(T_INTEGER, i64(beg + i)));
        for (u32 i = compressBeginIdx; i < maxDataSize % rowLen; i++) EXPECT_EQ(n.data[0][i], Item(T_INTEGER, cur++));

        for (u64 i = 1; i < n.data.size(); i++)
            for (const Item& item : n.data[i]) EXPECT_EQ(item, Item(T_INTEGER, cur++));
    } else {
        for (const Row& r : n.data)
            for (const Item& item : r) EXPECT_EQ(item, Item(T_INTEGER, cur++));
    }

    EXPECT_EQ(u64(cur), rowNum * rowLen);
}

void testSplitToMessages(u64 rowNum, u64 rowLen, u32 compressBeginIdx, u64 maxDataSize) {
    MessageHeader header = MessageHeader::InitHeader(0);
    header.maxDataSize = maxDataSize;
    Message m(std::move(header));
    m.header.currentOpIdx = 0;

    // plan only work as {compressBeginIndx} provider
    m.plan = std::make_shared<PhysicalPlan>(PhysicalPlan());
    m.plan->AppendStage(new ExecutionStage());
    m.plan->stages[0]->appendOp(new VertexScanOp(0, 0, compressBeginIdx));

    for (u32 i = 0; i < rowNum; i++) {
        m.data.emplace_back(0);
        for (u32 j = 0; j < rowLen; j++) {
            m.data.back().emplace_back(Item(T_INTEGER, i64(i * rowLen + j)));
        }
    }
    // LOG(INFO) << "before split " << m.DebugString();

    vector<Message> result;
    Message::SplitMessage(m, result);

    int cur = 0;
    for (size_t i = 0; i < result.size(); i++) {
        // LOG(INFO) << "After split, result[" << i << "]: " << result[i].DebugString();
        // Check split compress
        if (i > 0 && result[i - 1].data.back().size() < rowLen) {
            // split compress
            u32 lastBeg = result[i - 1].data.back()[0].integerVal;
            cur = result[i - 1].data.back().back().integerVal + 1;

            for (size_t j = 0; j < compressBeginIdx; j++)
                EXPECT_EQ(result[i].data[0][j], Item(T_INTEGER, i64(lastBeg + j)));
            for (size_t j = compressBeginIdx; j < result[i].data[0].size(); j++)
                EXPECT_EQ(result[i].data[0][j], Item(T_INTEGER, cur++));

            // LOG(INFO) << cur << ", cbi: " << compressBeginIdx;

            for (size_t j = 1; j < result[i].data.size(); j++)
                for (const Item& item : result[i].data[j]) EXPECT_EQ(item, Item(T_INTEGER, cur++));
            continue;
        }

        for (const Row& r : result[i].data)
            for (const Item& item : r) EXPECT_EQ(item, Item(T_INTEGER, cur++));
    }

    EXPECT_EQ((u32)cur, rowLen * rowNum);
}

TEST(test_message, test_split_single) {
    testSingleSplit(3, 5, 2, 8);
    testSingleSplit(5, 6, 0, 20);
    testSingleSplit(5, 6, 2, 20);
}

TEST(test_message, test_split_multiple) {
    testSplitToMessages(5, 5, 2, 8);
    testSplitToMessages(10, 20, 3, 20);
    testSplitToMessages(10, 20, 0, 20);
}

#undef ceiling_div

}  // namespace AGE
