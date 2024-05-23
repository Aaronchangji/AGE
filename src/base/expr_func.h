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

#include <map>
#include <string>
#include "base/aggregation.h"
#include "util/tool.h"

using std::map;
using std::string;

namespace AGE {

// Map function calls in cypher(e.g. max(), min()) to Expression member
class ExprFunc {
   public:
    static ExprFunc* GetInstance() {
        static ExprFunc expFunc;
        return &expFunc;
    }

    bool isAggFunc(const string& func) {
        const string _lower_func = Tool::toLower(func);
        auto itr = fMap.find(_lower_func);
        if (itr == fMap.end()) return false;
        return itr->second.aggregate;
    }

    AggType getAggType(const string& func) {
        const string _lower_func = Tool::toLower(func);
        assert(fMap.find(_lower_func) != fMap.end());
        return fMap[_lower_func].aggType;
    }

   private:
    ExprFunc() { registerAggFunc(); }
    struct FuncMeta {
        bool aggregate;
        AggType aggType;
        FuncMeta() : aggregate(false) {}
        FuncMeta(bool aggregate, AggType aggType) : aggregate(aggregate), aggType(aggType) {}
    };
    map<string, FuncMeta> fMap;

    void registerAggFunc() {
        fMap["max"] = FuncMeta(true, AGG_MAX);
        fMap["min"] = FuncMeta(true, AGG_MIN);
        fMap["avg"] = FuncMeta(true, AGG_AVG);
        fMap["sum"] = FuncMeta(true, AGG_SUM);
        fMap["count"] = FuncMeta(true, AGG_CNT);
    }
};
}  // namespace AGE
