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
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "base/item.h"
#include "base/row.h"
#include "base/type.h"
#include "operator/distinct/local_distinctor.h"
#include "storage/graph.h"

namespace AGE {
struct AggCtx {
    Item content;
    u64 cnt;
    LocalDistinctor *distinctor;
    AggCtx() : content(T_UNKNOWN), distinctor(nullptr) {}
    ~AggCtx() {
        if (distinctor != nullptr) delete (distinctor);
    }
    string DebugString() { return content.DebugString(); }
};

typedef enum : u8 { AGG_SUM, AGG_AVG, AGG_CNT, AGG_MAX, AGG_MIN } AggType;
class AggFunc {
   public:
    AggType type;
    bool isDistinct;
    int ctx;
    explicit AggFunc(AggType type) : type(type), isDistinct(false) {}
    /**
     * @brief Construct a new Agg Func object. Only AggFunc with type AGG_SUM, AGG_CNT and AGG_AVG will
     * be allowed to have distinctor.
     * @param type the type of the AggFunc
     * @param distinct indicate if the AggFunc need to do distinction.Since MAX and MIN does not
     * need to do distinction even is required, we do not set the indicator to be true.
     */
    AggFunc(AggType type, bool distinct) : type(type) { isDistinct = distinct && RegisteredDistinctAgg(type); }

    string DebugString() const {
        std::stringstream ss;
        ss << " type: " << static_cast<u32>(type);
        ss << " distinct: " << isDistinct;
        return ss.str();
    }

    void Update(const Item &val, AggCtx *ctxArr, size_t cnt) {
        // if (cnt == 0) return;
        switch (type) {
        case AGG_SUM:
            UpdateSum(val, ctxArr[ctx]);
            break;
        case AGG_AVG:
            UpdateAvg(val, ctxArr[ctx], cnt);
            break;
        case AGG_CNT:
            UpdateCnt(cnt, ctxArr[ctx]);
            break;
        case AGG_MAX:
            UpdateMax(val, ctxArr[ctx]);
            break;
        case AGG_MIN:
            UpdateMin(val, ctxArr[ctx]);
            break;
        }
    }

    Item GetResult(AggCtx *ctxArr) {
        switch (type) {
        case AGG_SUM:
        case AGG_CNT:
        case AGG_MAX:
        case AGG_MIN:
            return ctxArr[ctx].content;
        case AGG_AVG:
            return Item(T_FLOAT, ctxArr[ctx].content.floatVal / ctxArr[ctx].cnt);
        default:
            assert(false);
        }
    }
    bool NeedDistinct() const { return isDistinct; }

   private:
    void UpdateMax(const Item &val, AggCtx &aggCtx) {
        if (aggCtx.content.type == T_UNKNOWN) aggCtx.content = val;
        assert(val.type == aggCtx.content.type);
        aggCtx.content = (aggCtx.content >= val) ? aggCtx.content : val;
    }

    void UpdateMin(const Item &val, AggCtx &aggCtx) {
        if (aggCtx.content.type == T_UNKNOWN) aggCtx.content = val;
        assert(val.type == aggCtx.content.type);
        aggCtx.content = (aggCtx.content <= val) ? aggCtx.content : val;
    }

    void UpdateCnt(int num, AggCtx &aggCtx) {
        if (aggCtx.content.type == T_UNKNOWN) aggCtx.content = Item(T_INTEGER, 0);
        aggCtx.content.integerVal += num;
    }

    void UpdateSum(const Item &val, AggCtx &aggCtx) {
        if (aggCtx.content.type == T_UNKNOWN)
            aggCtx.content = val;
        else
            aggCtx.content = aggCtx.content + val;
    }

    void UpdateAvg(const Item &val, AggCtx &aggCtx, u64 cnt) {
        if (aggCtx.content.type == T_UNKNOWN) {
            aggCtx.content = Item(T_FLOAT, 0);
            aggCtx.cnt = 0;
        }
        aggCtx.cnt += cnt;
        assert(val.type & (T_FLOAT | T_INTEGER));
        aggCtx.content.floatVal += val.type == T_INTEGER ? val.integerVal : val.floatVal;
    }

    /**
     * @brief Indicate if the given AggType is a valid type for distinction. Currently we support
     * SUM,CNT and AVG
     *
     * @param type the AggType that is going to be tested.
     */
    inline bool RegisteredDistinctAgg(const AggType &type) {
        return type == AGG_SUM || type == AGG_CNT || type == AGG_AVG;
    }
};

using AggMap = std::unordered_map<Row, AggCtx *, Row::HashFunc, Row::EqualFunc>;

class Expression;
namespace Aggregation {
void UpdateCountOnlyAgg(Expression &exp, const Row *r, const Graph *g, AggCtx *ctxArr, size_t cnt,
                        const Item *curItem = nullptr);
/**
 * @param isGlobalAggregation Indicate if the aggregation is global aggregation.
 *                            If it is, we will use intermediate result (result of local aggregation) to update
 */
void UpdateAgg(Expression &exp, const Row *r, const Graph *g, AggCtx *ctxArr, const Item *curItem = nullptr,
               bool isGlobalAggregation = false);
Item GetResult(Expression &exp, AggCtx *ctxArr);
void AssignAggCtx(Expression &exp, u32 &nAggFunc);
void AppendResult(vector<pair<Expression, ColIndex>> &exps, vector<Row> &newData, AggCtx *AggCtx, const Row &key,
                  u32 nKey, u32 nAggFunc);
}  // namespace Aggregation.
}  // namespace AGE
