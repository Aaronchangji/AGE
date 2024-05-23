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
#include <base/expression.h>
#include <common.pb.h>
#include <execution/physical_plan.h>
#include <google/protobuf/util/json_util.h>
#include <gremlin.pb.h>
#include <grpcpp/grpcpp.h>
#include <job_service.grpc.pb.h>
#include <operator/ops.h>
#include <plan/logical_plan.h>
#include <cstdio>
#include <iostream>
#include <string>
#include <utility>

using std::pair;
using std::string;
using std::vector;

namespace AGE {

class FilterTranslator {
   public:
    explicit FilterTranslator(const StrMap* strMap) : strMap(strMap) {}
    Expression* translateFilterChain(const gremlin::FilterChain& filterChain, int curVar) {
        int node_size = filterChain.node_size();
        if (node_size == 0) return nullptr;
        if (node_size == 1) return translateFilterNode(filterChain.node(0), curVar);

        const gremlin::Connect chainType = filterChain.node(0).next();
        Expression* rt = new Expression(chainType == gremlin::Connect::AND ? EXP_AND : EXP_OR);

        for (int i = 0; i < node_size; i++) {
            const gremlin::FilterNode& filterNode = filterChain.node(i);
            if (i < node_size - 1 && filterNode.next() != chainType) printf("[ERROR] filter chain type not unified\n");
            rt->AddChild(translateFilterNode(filterNode, curVar));
        }

        return rt;
    }

    Expression* translateFilterNode(const gremlin::FilterNode& filterNode, int curVar) {
        if (filterNode.has_chain()) {
            gremlin::FilterChain filterChain;
            filterChain.ParseFromString(filterNode.chain());
            return translateFilterChain(filterChain, curVar);
        } else {
            const gremlin::FilterExp& filterExp = filterNode.single();
            Expression* rt = new Expression(translateCompare(filterExp.cmp()));
            rt->AddChild(translateKey(filterExp.left(), curVar));
            rt->AddChild(translateValue(filterExp.right()));
            return rt;
        }
        return nullptr;
    }

    ExpType translateCompare(const gremlin::Compare& cmp) {
        switch (cmp) {
        case gremlin::EQ:
            return EXP_EQ;
        case gremlin::NE:
            return EXP_EQ;
        case gremlin::LT:
            return EXP_LT;
        case gremlin::LE:
            return EXP_LE;
        case gremlin::GT:
            return EXP_GT;
        case gremlin::GE:
            return EXP_GE;
        case gremlin::WITHIN:
        case gremlin::WITHOUT:
        default:
            // TODO(ycli): support whithin/without.
            printf("[Not support] compare type within/without");
            return EXP_EQ;
        }
    }

    Expression* translateKey(const common::Key& key, int curVar) {
        Expression* cur = new Expression(EXP_VARIABLE, curVar);
        if (key.has_name()) {
            string name = key.name();
            auto itr = strMap->strPropMap.find(name);
            int keyId = itr != strMap->strPropMap.end() ? itr->second : INVALID_PROP;
            Expression* prop = new Expression(EXP_PROP, keyId);
            prop->AddChild(cur);
            return prop;
        } else if (key.has_name_id()) {
            Expression* prop = new Expression(EXP_PROP, key.name_id());
            prop->AddChild(cur);
            return prop;
        } else if (key.has_id()) {
            return cur;
        } else if (key.has_label()) {
            Expression* label = new Expression(EXP_PROP, VAR_KEY_LABEL);
            label->AddChild(cur);
            return label;
        }
        return nullptr;
    }

    Expression* translateValue(const common::Value& value) {
        Item item(T_NULL);
        if (value.has_boolean()) {
            item = Item(T_BOOL, value.boolean());
        } else if (value.has_i32()) {
            item = Item(T_INTEGER, value.i32());
        } else if (value.has_i64()) {
            item = Item(T_INTEGER, value.i64());
        } else if (value.has_str()) {
            item = Item(T_STRING, value.str());
        } else if (value.has_blob()) {
            item = Item(T_STRING, value.blob());
        } else if (value.has_f64()) {
            item = Item(T_FLOAT, value.f64());
        } else {
            // TODO(ycli): support this.
            printf("[Not support] i32_array/i64_array/f64_array/str_array");
        }

        if (item.type != T_NULL) {
            return new Expression(EXP_CONSTANT, item);
        } else {
            return nullptr;
        }
    }

   private:
    const StrMap* strMap;
};

}  // namespace AGE
