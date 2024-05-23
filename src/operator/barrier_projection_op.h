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
// limitations under the License.#pragma once

#pragma once
#include <glog/logging.h>
#include <cassert>
#include <cstdio>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "base/aggregation.h"
#include "base/expr_util.h"
#include "base/expression.h"
#include "base/proj_util.h"
#include "base/row.h"
#include "base/serializer.h"
#include "base/type.h"
#include "execution/physical_plan.h"
#include "operator/abstract_op.h"
#include "operator/base_barrier_op.h"
#include "operator/distinct/global_distinctor.h"
#include "operator/op_type.h"
#include "plan/op_params.h"

using std::set;
using std::vector;
namespace AGE {
// BPType: Used to indicate the functional type of barrier_projection_op .
// ie: Need Order, Need Distinct & Need both
typedef enum : u8 {
    BP_ORDER = (1 << 0),
    BP_DISTINCT = (1 << 1),
    BP_DISTINCT_ORDER = (1 << 2),
    BP_UNDECIDED = (1 << 3),
    BP_LIMIT = (1 << 4)
} BPType;

/**
 * @brief This class is used to store the generated data in barrier_projection and process them on
 * the fly according to the functional type of the barrier_projection_op. Each barrier_projection_op
 * will have only one BarrierProjSet, which is used to simplify the process logic of the op and avoid
 * double copying. We use the optimal storing strategy for different functional use.
 *  Distinction Only : globalDistincotr (STL unordered_set)
 *  Order Only : STL map
 *  Both : STL set
 *  Limit : trivial FIFO vector
 */
class BarrierProjSet {
   public:
    BarrierProjSet() : type(BP_UNDECIDED), globalDistinctor(nullptr) {}
    BarrierProjSet(bool needDistinct, bool needOrder, vector<pair<Expression, isASC>> &orderByExps, int limitNum) {
        if (needDistinct && needOrder) {
            type = BP_DISTINCT_ORDER;
            fusionSet = new set<pair<Row, Row>, OrderUtil::DistinctSort>(OrderUtil::DistinctSort(orderByExps));
        } else if (needDistinct) {
            type = BP_DISTINCT;
            globalDistinctor = new GlobalDistinctor(16, limitNum);
        } else if (needOrder) {
            type = BP_ORDER;
            orderByMap = new OrderUtil::OrderbyMap(OrderUtil::OrderSort(orderByExps));
        } else if (limitNum != LimitUtil::WITHOUT_LIMIT) {
            type = BP_LIMIT;
            originData = new std::vector<Row>;
        } else {
            LOG(FATAL) << "Invalid BarrierProjection function type\n";
        }
    }

    BarrierProjSet(BarrierProjSet &&aSet) { *this = std::move(aSet); }
    BarrierProjSet &operator=(BarrierProjSet &&aSet) {
        type = aSet.type;
        aSet.type = BP_UNDECIDED;
        if (type == BP_UNDECIDED) {
            globalDistinctor = nullptr;
        } else if (type == BP_DISTINCT) {
            globalDistinctor = aSet.globalDistinctor;
            aSet.globalDistinctor = nullptr;
        } else if (type == BP_ORDER) {
            orderByMap = aSet.orderByMap;
            aSet.orderByMap = nullptr;
        } else if (type == BP_DISTINCT_ORDER) {
            fusionSet = aSet.fusionSet;
            aSet.fusionSet = nullptr;
        } else if (type == BP_LIMIT) {
            originData = aSet.originData;
            aSet.originData = nullptr;
        }

        return *this;
    }

    ~BarrierProjSet() { clear(); }

    void clear() {
        switch (type) {
        case BP_UNDECIDED:
            break;
        case BP_DISTINCT:
            delete globalDistinctor;
            break;
        case BP_ORDER:
            delete orderByMap;
            break;
        case BP_DISTINCT_ORDER:
            delete fusionSet;
            break;
        case BP_LIMIT:
            delete originData;
            break;
        }
        type = BP_UNDECIDED;
    }

    bool insert(Row &&aRow, Row &&orderKey) {
        switch (type) {
        case BP_ORDER:
            orderByMap->emplace(std::move(orderKey), std::move(aRow));
            return true;
        case BP_DISTINCT_ORDER:
            return fusionSet->emplace(std::move(orderKey), std::move(aRow)).second;
        default:
            LOG(FATAL) << "Invalid BarrierProjSet type\n";
            return false;
        }
    }
    // Only valid for BP_DISTINCT || BP_LIMIT
    bool insert(Row &&aRow) {
        switch (type) {
        case BP_DISTINCT:
            return globalDistinctor->insert(std::move(aRow));
        case BP_LIMIT:
            originData->emplace_back(std::move(aRow));
            return true;
        default:
            LOG(FATAL) << "Invalid BarrierProjSet type\n";
            return false;
        }
    }

    // Retrieve the data from storage and store data in Message
    void getResult(Message &m, int limitNum) {
        vector<Row> newData;
        LimitUtil limit(limitNum);
        switch (type) {
        case BP_DISTINCT:
            // Limit is implemented inside the distinctor
            m.data = globalDistinctor->getResult();
            break;
        case BP_ORDER:
            for (auto &[orderbyKey, row] : (*orderByMap)) {
                if (!limit.checkCnt()) break;
                newData.emplace_back(std::move(row));
                if (!limit.updateCnt(newData.back())) break;
            }
            m.data.swap(newData);
            break;
        case BP_DISTINCT_ORDER:
            for (auto &[orderbyKey, row] : (*fusionSet)) {
                if (!limit.checkCnt()) break;
                newData.emplace_back(std::move(row));
                if (!limit.updateCnt(newData.back())) break;
            }
            m.data.swap(newData);
            break;
        case BP_LIMIT: {
            for (auto &itr : (*originData)) {
                if (!limit.checkCnt()) break;
                newData.emplace_back(std::move(itr));
                if (!limit.updateCnt(newData.back())) break;
            }
            m.data.swap(newData);
            break;
        }
        default:
            LOG(FATAL) << "BarrierProjSet is not initialized correctly.\n";
        }
    }

    BPType getType() const { return type; }

   private:
    // Functional type of the set.
    BPType type;
    // Storage set for specific functional use.
    union {
        OrderUtil::OrderbyMap *orderByMap;
        GlobalDistinctor *globalDistinctor;
        std::set<pair<Row, Row>, OrderUtil::DistinctSort> *fusionSet;
        std::vector<Row> *originData;
    };
};

class BarrierProjectOp : public BaseBarrierOp {
   public:
    explicit BarrierProjectOp(OpType type, u32 compressBeginIdx = COMPRESS_BEGIN_IDX_UNDECIDED)
        : BaseBarrierOp(type, compressBeginIdx), limitNum(LimitUtil::WITHOUT_LIMIT), hasDistinct(false), init(false) {
        // Must call FromPhysicalOpParams to complete initialization
    }
    BarrierProjectOp(OpType type, const vector<pair<Expression, ColIndex>> &exps, u32 compressBeginIdx,
                     u32 newCompressBeginIdx_, vector<pair<Expression, isASC>> *orderbyExps = nullptr,
                     bool hasDistinct = false, int limitNum_ = LimitUtil::WITHOUT_LIMIT)
        : BaseBarrierOp(type, compressBeginIdx), limitNum(limitNum_), hasDistinct(hasDistinct), init(false) {
        newCompressBeginIdx = newCompressBeginIdx_;
        Init(&exps, orderbyExps);
    }

    string DebugString(int depth = 0) const override {
        char buf[256];
        snprintf(buf, sizeof(buf), "%s hasOrder: %d, isDistinct: %d, limitNum: %d",
                 AbstractOp::DebugString(depth).c_str(), hasOrder, hasDistinct, limitNum);
        return buf;
    }

    void FromString(const string &s, size_t &pos) override {
        AbstractOp::FromString(s, pos);
        newCompressBeginIdx = Serializer::readU32(s, pos);
        limitNum = Serializer::readI32(s, pos);
        hasDistinct = Serializer::readBool(s, pos);

        u8 expsLen = Serializer::readU8(s, pos);
        while (expsLen--) {
            Expression exp = ExprUtil::CreateFromString(s, pos);
            ColIndex col = Serializer::readI8(s, pos);
            AppendProject(std::make_pair(exp, col));
        }

        vector<pair<Expression, isASC>> orderbyExps;
        u8 orderExpsLen = Serializer::readU8(s, pos);
        while (orderExpsLen--) {
            Expression exp = ExprUtil::CreateFromString(s, pos);
            isASC asc = Serializer::readBool(s, pos);
            orderbyExps.emplace_back(std::make_pair(exp, asc));
        }
        Init(nullptr, &orderbyExps);
    }

    void FromPhysicalOpParams(const PhysicalOpParams &params) override {
        AbstractOp::FromPhysicalOpParams(params);

        const string &s = params.params;
        size_t pos = 0;

        u8 expsLen = Serializer::readU8(s, pos);
        Serializer::readU8(s, pos);  // Useless in barrier project, useful in aggregate op
        u8 orderExpsLen = Serializer::readU8(s, pos);
        assert(expsLen && (expsLen + orderExpsLen == params.exprs.size()));

        vector<pair<Expression, ColIndex>> exps;
        vector<pair<Expression, isASC>> orderExps;
        for (u8 i = 0; i < expsLen; i++) exps.emplace_back(std::make_pair(params.exprs[i], params.cols[i]));
        for (u8 i = 0; i < orderExpsLen; i++) {
            isASC asc = Serializer::readBool(s, pos);
            orderExps.emplace_back(std::make_pair(params.exprs[i + expsLen], asc));
        }

        limitNum = Serializer::readI32(s, pos);
        hasDistinct = Serializer::readBool(s, pos);
        newCompressBeginIdx = Serializer::readU32(s, pos);
        Init(&exps, &orderExps);
    }

    void ToString(string *s) const override {
        AbstractOp::ToString(s);
        Serializer::appendU32(s, newCompressBeginIdx);
        Serializer::appendI32(s, limitNum);
        Serializer::appendBool(s, hasDistinct);
        Serializer::appendU8(s, exps.size());
        for (auto &p : exps) {
            ExprUtil::ToString(s, &p.first);
            Serializer::appendI8(s, p.second);
        }
        Serializer::appendU8(s, orderbyExps.size());
        for (auto &p : orderbyExps) {
            ExprUtil::ToString(s, &p.first);
            Serializer::appendBool(s, p.second);
        }
    }

    ~BarrierProjectOp() { storageSet.clear(); }

    SingleProcStat::ColStat ExtractColStat(const Message &msg) const override {
        return std::make_tuple(HasCompress(msg), true, 2);
    }

    bool do_work(Message &m, vector<Message> &output, bool isReady) override {
        assert(init);
        Graph *g = m.plan->g;

        bool hasCompress = HasCompress(m);
        bool early_stop = false;

        if (hasCompress) {
            for (Row r : m.data) {
                for (u32 curIndex = compressBeginIdx; curIndex < r.size(); curIndex++) {
                    Row newRow(newCompressBeginIdx);
                    for (u32 projIndex = 0; projIndex < exps.size(); projIndex++) {
                        Item result = exps[projIndex].first.Eval(&r, g, &r[curIndex]);
                        if (exps[projIndex].second == COLINDEX_COMPRESS)
                            newRow.emplace_back(std::move(result));
                        else
                            newRow[exps[projIndex].second] = std::move(result);
                    }
                    if (hasOrder) {
                        Row orderKey(orderbyExps.size());
                        for (u32 i = 0; i < orderbyExps.size(); i++) {
                            CHECK(!orderbyExps[i].first.HasAggregate()) << "Should not have aggregations in ORDER BY\n";
                            orderKey[i] = orderbyExps[i].first.Eval(&r, g, &r[curIndex]);
                        }
                        storageSet.insert(std::move(newRow), std::move(orderKey));
                    } else {
                        int row_cnt = newRow.count();
                        if (storageSet.insert(std::move(newRow)) && LimitReached(row_cnt)) {
                            early_stop = true;
                            break;
                        }
                    }
                }
                if (early_stop) break;
            }
        } else {
            for (Row r : m.data) {
                Row newRow(newCompressBeginIdx);
                for (u32 projIndex = 0; projIndex < exps.size(); projIndex++) {
                    Item result = exps[projIndex].first.Eval(&r, g);
                    if (exps[projIndex].second == COLINDEX_COMPRESS)
                        newRow.emplace_back(std::move(result));
                    else
                        newRow[exps[projIndex].second] = std::move(result);
                }
                if (hasOrder) {
                    Row orderKey(orderbyExps.size());
                    for (u32 i = 0; i < orderbyExps.size(); i++) {
                        CHECK(!orderbyExps[i].first.HasAggregate()) << "Should not have aggregations in ORDER BY\n";
                        orderKey[i] = orderbyExps[i].first.Eval(&r, g);
                    }
                    storageSet.insert(std::move(newRow), std::move(orderKey));
                } else {
                    int row_cnt = newRow.count();
                    if (storageSet.insert(std::move(newRow)) && LimitReached(row_cnt)) {
                        early_stop = true;
                        break;
                    }
                }
            }
        }

        if (early_stop) {
            // modify the message header s.t. it's treated as the result message
            DLOG_IF(INFO, Config::GetInstance()->verbose_) << "Invoke EarlyStop";
            Message::EarlyStop(m, get_end_history_size(m));
        }

        isReady |= early_stop;
        postprocess(m, output, isReady);
        return isReady;
    }

    inline bool is_early_stoppable() { return checkLimitBeforeReady; }
    int get_limit_num() { return limitNum; }

   protected:
    // Different implementation for local and global
    virtual void postprocess(Message &m, vector<Message> &output, bool isReady) {
        if (isReady) {
            storageSet.getResult(m, limitNum);
            output.emplace_back(std::move(m));
        }
    }

    vector<pair<Expression, ColIndex>> exps;
    vector<pair<Expression, isASC>> orderbyExps;
    int limitNum = LimitUtil::WITHOUT_LIMIT;
    bool hasDistinct = false, hasOrder = false;

    BarrierProjSet storageSet;
    u32 newCompressBeginIdx;

    LimitUtil
        earlyLimitChecker;  // used to check the limit condition before isReady, only effect when limit without order
    bool checkLimitBeforeReady = false;

   protected:
    bool init;
    void AppendProject(const pair<Expression, ColIndex> &p) { exps.emplace_back(p); }

    inline bool LimitReached(int cnt) {
        if (!checkLimitBeforeReady) return false;
        if (!earlyLimitChecker.updateCnt(cnt)) {
            ack_finish();
            return true;
        }
        return false;
    }

    void Init(const vector<pair<Expression, ColIndex>> *exps = nullptr,
              const vector<pair<Expression, isASC>> *orderbyExps_ = nullptr) {
        assert(!init);

        if (exps != nullptr) {
            for (auto &p : *exps) AppendProject(p);
        }
        if (orderbyExps_ != nullptr) {
            orderbyExps = *orderbyExps_;
            hasOrder = orderbyExps.size() > 0;
        }
        storageSet = BarrierProjSet(hasDistinct, hasOrder, orderbyExps, limitNum);
        if (storageSet.getType() == BPType::BP_LIMIT ||
            (storageSet.getType() == BPType::BP_DISTINCT && limitNum != LimitUtil::WITHOUT_LIMIT)) {
            earlyLimitChecker.setLimitNum(limitNum);
            checkLimitBeforeReady = true;
        }

        assert((hasDistinct || hasOrder || limitNum != LimitUtil::WITHOUT_LIMIT) &&
               "No need to use BarrierProjection\n");
        init = true;
    }
};

}  // namespace AGE
