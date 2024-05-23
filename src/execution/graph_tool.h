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
#include <utility>
#include <vector>
#include "base/expression.h"
#include "base/type.h"
#include "execution/physical_plan.h"
#include "operator/abstract_op.h"
#include "storage/graph.h"

namespace AGE {

// Graph tool function in execution
class GraphTool {
   public:
    // Filter out Vertex in {v} that with label other than {vLabel}
    // It's a common progress that happen in many situations (e.g. expand)
    // so we abstract it out
    static void filterVLabel(vector<Item> &v, LabelId vLabel) {
        // std::remove_if.
        size_t result = 0;
        {
            size_t first = 0, last = v.size();
            while (first != last) {
                if (checkLabel(v[first], vLabel)) {
                    if (result != first) v[result] = std::move(v[first]);
                    result++;
                }
                first++;
            }
        }

        // erase.
        v.erase(v.begin() + result, v.end());
    }

   private:
    static bool checkLabel(const Item &vtx, LabelId vLabel) {
        assert(vtx.type == T_VERTEX);
        if (vLabel == ALL_LABEL) return true;
        if (vLabel == INVALID_LABEL) return false;
        return vtx.vertex.label == vLabel;
    }
};
}  // namespace AGE
