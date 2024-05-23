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

#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>
#include "base/serializer.h"
#include "base/type.h"
#include "execution/physical_plan.h"
#include "operator/abstract_op.h"
#include "storage/b_tree.h"
#include "storage/graph.h"

namespace AGE {

using std::shared_mutex;
using std::string;
using std::vector;

// Operator to build/drop a property index
class IndexOp : public AbstractOp {
   public:
    IndexOp() : AbstractOp(OpType_INDEX, 0) {}

    IndexOp(LabelId lid, PropId pid, bool buildIndex)
        : AbstractOp(OpType_INDEX, 0), lid(lid), pid(pid), buildIndex(buildIndex) {}

    void ToString(string *s) const override {
        AbstractOp::ToString(s);
        Serializer::appendVar(s, lid);
        Serializer::appendVar(s, pid);
        Serializer::appendBool(s, buildIndex);
    }

    void FromString(const string &s, size_t &pos) override {
        AbstractOp::FromString(s, pos);
        Serializer::readVar(s, pos, &lid);
        Serializer::readVar(s, pos, &pid);
        buildIndex = Serializer::readBool(s, pos);
    }

    bool process(Message &m, std::vector<Message> &output) override {
        Graph *g = m.plan->g;
        IndexStore *indexStore = g->GetIndexStore();

        string returnMsg;

        // Do...While just for jump out.
        do {
            if (!buildIndex) {
                // Drop index.
                indexStore->DisableIndex(lid, pid);
                returnMsg = ReturnMsg("Drop", g);
                break;
            }

            // Enable index.
            if (indexStore->IsBuilt(lid, pid)) {
                // Enable built index.
                indexStore->EnableIndex(lid, pid);
                returnMsg = ReturnMsg("Enable", g);
                break;
            }

            // Build index.
            vector<Vertex> nullVec;
            BTree *bt = new BTree(g->getPropType(pid));
            for (auto cursor = g->getLabelVtxCursor(lid); !cursor.end(); ++cursor) {
                Vertex v = *cursor;
                // cout << v.DebugString() << endl;
                Item p = g->getProp(Item(T_VERTEX, v), pid);
                if (p.type == T_NULL) {
                    nullVec.emplace_back(v);
                } else {
                    bt->emplace(p, v);
                }
            }

            indexStore->SetIndex(lid, pid, bt, std::move(nullVec));
            returnMsg = ReturnMsg("Build", g);
        } while (false);

        m.data.emplace_back();
        m.data.back().emplace_back(T_STRING, returnMsg);
        output.emplace_back(std::move(m));

        return true;
    }

   private:
    LabelId lid;
    PropId pid;
    bool buildIndex;

    string ReturnMsg(string prefix, Graph *g) {
        char buf[128];
        snprintf(buf, sizeof(buf), " Index for %s on %s sucess.", g->getLabelStr(lid).c_str(),
                 g->getPropStr(pid).c_str());
        return prefix + string(buf);
    }
};

}  // namespace AGE
