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
#include <algorithm>
#include <atomic>
#include <exception>
#include <map>
#include <queue>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base/expression.h"
#include "base/node.h"
#include "base/type.h"
#include "execution/physical_plan.h"
#include "operator/ops.h"
#include "plan/ast.h"
#include "plan/ast_helper.h"
#include "plan/logical_plan.h"
#include "plan/logical_plan_builder.h"
#include "plan/logical_plan_optimizer.h"
#include "plan/pattern_graph.h"
#include "plan/physical_plan_builder.h"
#include "plan/physical_plan_optimizer.h"
#include "plan/plan_graph.h"
#include "plan/plan_graph_optimizer.h"
#include "plan/planner_exception.h"
#include "plan/var_map.h"
#include "storage/str_map.h"
#include "util/config.h"
#include "util/tool.h"

namespace AGE {
#define BEG first
#define END second

using std::pair;
using std::string;
using std::vector;

// Input query string, output PhysicalPlan
class Planner {
   public:
    explicit Planner(const StrMap &strMap, u8 rank)
        : rank(rank), config(Config::GetInstance()), queryCnt(0), strMap(strMap) {}
    ~Planner() {}

    pair<vector<string>, PhysicalPlan *> process(const string &query, string *error = nullptr) {
        // LOG(INFO) << "Planner::process(\"" << query << "\")";
        try {
            AST ast(query, error);
            if (ast.success()) {
                ExecutionType execType = AstHelper::GetExecutionType(ast.GetRoot());

                // Index create/drop plan
                if (execType == EXECUTION_TYPE_INDEX_CREATE || execType == EXECUTION_TYPE_INDEX_DROP) {
                    bool isCreate = (execType == EXECUTION_TYPE_INDEX_CREATE);
                    auto [index_pair, plan] = BuildIndexPlan(ast, isCreate);
                    vector<string> returnStr;
                    returnStr.emplace_back("INDEX," + std::to_string(index_pair.first) + "," +
                                           std::to_string(index_pair.second) + "," + (isCreate ? "1" : "0"));
                    return std::make_pair(returnStr, plan);
                }

                // Query
                vector<string> returnStrs = AstHelper::GetReturnVarStrings(ast.GetRoot());
                PhysicalPlan *plan = BuildPhysicalPlan(ast, returnStrs, error);
                return std::make_pair(std::move(returnStrs), plan);
            }
        } catch (PlannerException &plannerException) {
            if (error != nullptr)
                *error = plannerException.what();
            else
                LOG(ERROR) << plannerException.what();
        }

        return std::make_pair(vector<string>(), static_cast<PhysicalPlan *>(nullptr));
    }

    pair<pair<LabelId, PropId>, PhysicalPlan *> BuildIndexPlan(const AST &ast, bool isCreate) {
        const ASTNode *n = ast.GetRoot();

        u32 nprops =
            isCreate ? cypher_ast_create_node_props_index_nprops(n) : cypher_ast_drop_node_props_index_nprops(n);
        const char *label = isCreate ? cypher_ast_label_get_name(cypher_ast_create_node_props_index_get_label(n))
                                     : cypher_ast_label_get_name(cypher_ast_drop_node_props_index_get_label(n));

        // TODO(ycli): support multi-label index build in one query.
        CHECK_EQ(nprops, static_cast<u32>(1));
        // for (u32 i = 0; i < nprops; i++) {
        const ASTNode *propName = isCreate ? cypher_ast_create_node_props_index_get_prop_name(n, 0)
                                           : cypher_ast_drop_node_props_index_get_prop_name(n, 0);
        const char *prop = cypher_ast_prop_name_get_value(propName);
        // }

        /**
         * TODO(TBD): We currently directly enable index in planner here, rather than following the complete routine:
         *  Build complete -> update planer
         */
        LabelId label_id = strMap.GetLabelId(label);
        PropId prop_id = strMap.GetPropId(prop);
        LOG_IF(INFO, Config::GetInstance()->verbose_)
            << "Build index on " << std::to_string(label_id) << ", " << std::to_string(prop_id) << std::flush;
        UpdateIndexEnableSet(label_id, prop_id, isCreate);

        // const StrMap *strMap = g->getStrMap();
        PhysicalPlan *plan = new PhysicalPlan(getQueryId());
        // Create two stages for indexOp and Endop, specifically

        ExecutionStage *stage = new ExecutionStage();
        stage->appendOp(new IndexOp(label_id, prop_id, isCreate));
        stage->setExecutionSide();
        plan->AppendStage(stage);

        stage = new ExecutionStage();
        stage->appendOp(new EndOp(0));
        stage->setExecutionSide();
        plan->AppendStage(stage);

        return std::make_pair(std::make_pair(label_id, prop_id), plan);
    }

    PhysicalPlan *BuildPhysicalPlan(const AST &ast, const vector<string> &returnStrs, string *error = nullptr) {
        const ASTNode *rt = ast.GetRoot();

        // Build plan graph
        VarMap varMap;
        PlanGraph planGraph(rt, strMap, varMap);
        planGraph.Build();
        // LOG(INFO) << varMap.DebugString();
        // LOG(INFO) << planGraph.DebugString();
        PlanGraphOptimizer().Optimize(planGraph);
        // LOG(INFO) << planGraph.DebugString();

        // Build logical plan
        LogicalPlan logicalPlan = LogicalPlanBuilder(varMap, strMap, indexEnabledSet).Build(planGraph);
        LogicalPlanOptimizer().Optimize(logicalPlan);
        // LOG(INFO) << logicalPlan.DebugString(&varMap);

        // Build physical plan
        PhysicalPlan *plan = PhysicPlanBuilder(varMap, returnStrs, getQueryId()).Build(logicalPlan);
        PhysicalPlanOptimizer(varMap).Optimize(*plan);

        return plan;
    }

    void UpdateIndexEnableSet(LabelId label_id, PropId prop_id, bool enable) {
        // TODO(TBD): Make this thread safe
        std::pair<LabelId, PropId> index_key = std::make_pair(label_id, prop_id);
        // LOG(INFO) << (enable ? "Enable" : "Disable")  << " index: " << std::to_string(label_id) << ", " <<
        // std::to_string(prop_id) << std::flush;
        if (enable)
            indexEnabledSet.insert(index_key);
        else
            indexEnabledSet.erase(index_key);
    }

   private:
    u8 rank;
    Config *config;
    std::atomic<u32> queryCnt;

    // Data related
    const StrMap &strMap;
    IndexEnabledSet indexEnabledSet;

    QueryId getQueryId() { return rank | (queryCnt++ << 8); }
};
}  // namespace AGE
