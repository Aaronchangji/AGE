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
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>
#include "base/expression.h"
#include "base/row.h"
#include "base/type.h"
#include "plan/logical_plan.h"
#include "plan/type.h"
#include "util/tool.h"

using std::map;
using std::string;
using std::vector;

namespace AGE {

// Column assginer assign variables to columns
// by simulating logical plan step by step
// It is required that every ColAssigner only manage a linear execution process
class ColAssigner {
    using preserved = bool;
    using InputMap = map<VarId, pair<ColIndex, preserved>>;

   private:
    // Record the reference/generate history for variables
    class VarHistory {
       public:
        explicit VarHistory(const InputMap& inputMap_) : inputMap(inputMap_) {}

        static constexpr size_t npos = -1;
        void RecordRef(VarId var, size_t idx) { varRefHistoryMap[var].insert(idx); }
        void RecordGen(VarId var, size_t idx) { varGenHistoryMap[var].insert(idx); }

        // Whether to keep the variable in intermediate result
        bool Keep(VarId var, size_t curPos) const {
            if (var == kVarIdNone) return false;
            if (inputMap.count(var) && inputMap.at(var).second == true) return true;

            size_t nxtRefIdx = GetNextRef(var, curPos), nxtGenIdx = GetNextGen(var, curPos);
            // LOG(INFO) << "GetNextRef(" << var << ", " << curPos << "): " << nxtRefIdx;
            // LOG(INFO) << "GetNextGen(" << var << ", " << curPos << "): " << nxtGenIdx;

            // No need to keep when:
            // - Will not use then
            // - Will generate again before next ref
            if (nxtRefIdx == VarHistory::npos || nxtRefIdx > nxtGenIdx) return false;

            return true;
        }

       private:
        const InputMap& inputMap;

        // Record every time a variable is referencedd as input for op
        map<VarId, set<size_t>> varRefHistoryMap;

        // Record every time a variable is generated
        map<VarId, set<size_t>> varGenHistoryMap;

        size_t GetNextRef(VarId var, size_t curPos) const {
            auto mapItr = varRefHistoryMap.find(var);
            if (mapItr == varRefHistoryMap.end()) return npos;
            auto idxItr = mapItr->second.upper_bound(curPos);
            if (idxItr == mapItr->second.end()) return npos;
            return *idxItr;
        }

        size_t GetNextGen(VarId var, size_t curPos) const {
            auto mapItr = varGenHistoryMap.find(var);
            if (mapItr == varGenHistoryMap.end()) return npos;
            auto idxItr = mapItr->second.upper_bound(curPos);
            if (idxItr == mapItr->second.end()) return npos;
            return *idxItr;
        }
    };

    class SimulateData {
       public:
        size_t curPos;

        // Store existing variables when execute current op
        // mutate along with the simulating of plan execution
        // Note that #COLINDEX_COMPRESS is not in {vars} & {cols}
        map<VarId, ColIndex> vars;
        map<ColIndex, VarId> cols;

        // Idle columns
        set<ColIndex> idleCols;

        // Current compressed variable
        VarId compress;

        explicit SimulateData(const InputMap& input) : curPos(0), compress(kVarIdNone) {
            if (!input.size()) return;
            for (auto [var, p] : input) {
                ColIndex col = p.first;
                // LOG(INFO) << "input: {" << var << ", " << col << "}";
                if (col == COLINDEX_COMPRESS) {
                    compress = var;
                    continue;
                }

                assert(cols.count(col) == 0);
                vars[var] = col;
                cols[col] = var;
            }

            for (auto [var, col] : vars) {
                assert(static_cast<u64>(col) < vars.size());
            }
        }

        ColIndex GetNewCol() {
            if (idleCols.empty()) idleCols.insert(vars.size());
            ColIndex ret = *idleCols.begin();
            idleCols.erase(idleCols.begin());
            return ret;
        }

        bool ExistVar(VarId var) const {
            if (compress == var) return true;
            return vars.count(var) != 0;
        }

        string DebugString() const {
            string ret = "\nVars: ";
            for (auto [varId, colIdx] : vars) {
                assert(cols.at(colIdx) == varId);
                ret += "[" + std::to_string(varId) + ", " + std::to_string(colIdx) + "], ";
            }
            if (compress != kVarIdNone) ret += "[" + std::to_string(compress) + ", compress]";

            ret += "\nIdleCols: ";
            for (ColIndex col : idleCols) ret += std::to_string(col) + ", ";
            return ret;
        }

       private:
    };

   public:
    ColAssigner(const LogicalOp* beg, const LogicalOp* end, const InputMap& input)
        : beg(beg), end(end), inputVars(input), simData_(inputVars), maxVarCnt(0) {
        // "end is nullptr" when beg == END
        assert(beg && (end || beg->type == OpType_END));
        // LOG(INFO) << "input:";
        // if (inputVars) for (auto [var, col] : *inputVars) LOG(INFO) << "input: {" << var << ", " << col << "}";
        Build();
        // Reset();
    }

    // Reset to status at {beg}, and clear all intermediate data
    void Reset() {
        simData_ = SimulateData(inputVars);
        ops.clear();
        discard.clear();
        generate.clear();
        maxVarCnt = 0;
    }

    // For applying
    u32 GetCompressBeginIdx() const { return maxVarCnt; }
    ColIndex GetCol(VarId var) const {
        // LOG(INFO) << "ColAssigner::GetCol(" << var << ")";
        if (var == simData_.compress && simData_.compress != kVarIdNone) return COLINDEX_COMPRESS;
        auto itr = simData_.vars.find(var);
        if (itr == simData_.vars.end()) {
            // LOG(WARNING) << "ColAssigner::GetCol(" << var << ") not found\n" << simData_.DebugString() << std::flush;
            return COLINDEX_NONE;
        }
        return itr->second;
    }
    VarId GetCompress() const { return simData_.compress; }

    const InputMap& GetInputMap() { return inputVars; }

    // Return compress dst
    ColIndex ProcessNextOp() {
        size_t curPos = simData_.curPos++;
        assert(curPos < ops.size());

        VarId preCompress = GetCompress();

        for (VarId var : discard[curPos]) DiscardVar(var, simData_);
        for (auto [var, col] : generate[curPos]) {
            // LOG(INFO) << "GenerateRunVar(" << var << ", " << col << ")";
            GenerateVar(var, col, simData_);
        }

        return GetCol(preCompress);
    }

    string DebugString() const { return simData_.DebugString(); }
    string GenerateDiscardString() const {
        string ret = "\n";
        for (size_t i = 0; i < ops.size(); i++) {
            ret += "op[" + to_string(i) + "]: " + string(OpType_DebugString(ops[i]->type)) + " {";

            ret += "Generate: [";
            for (auto [var, col] : generate[i]) ret += "(" + to_string(var) + ", " + to_string(col) + "), ";
            ret += "], ";

            ret += "Discard: [";
            for (auto var : discard[i]) ret += to_string(var) + ", ";
            ret += "]}\n";
        }

        return ret;
    }

   private:
    void Build() {
        const LogicalOp* cur = beg;
        VarHistory varHistory(inputVars);
        while (cur != end) {
            size_t idx = ops.size();
            ops.emplace_back(cur);
            for (VarId var : cur->input) varHistory.RecordRef(var, idx);
            for (VarId var : cur->output) varHistory.RecordGen(var, idx);

            assert(cur->nextOp.size() == 1);
            cur = cur->nextOp[0];
        }

        Build(varHistory);
    }

    // For applying to simulateData
    void GenerateVar(VarId var, ColIndex col, SimulateData& simData) {
        // LOG(INFO) << "GenerateVar(" << var << ", " << col << ")";
        if (col == COLINDEX_COMPRESS) {
            simData.compress = var;
        } else {
            simData.vars[var] = col;
            simData.cols[col] = var;
        }
    }

    // For building
    void GenerateVar(VarId var, size_t curPos, SimulateData& simData, const VarHistory& varHistory) {
        // Strategy to assign col
        ColIndex col = COLINDEX_NONE;
        if (!varHistory.Keep(var, curPos) || simData.vars.count(var)) {
            // No need to generate this var, return
            return;
        } else if (false) {
            // TODO(ycli): implement this strategy
            // Avoid unnecessary decompress by not generating to compress
        } else {
            // Decide dst for original compress
            if (simData.compress != kVarIdNone) {
                if (varHistory.Keep(simData.compress, curPos)) {
                    ColIndex compressDst = simData.GetNewCol();
                    generate[curPos][simData.compress] = compressDst;
                    GenerateVar(simData.compress, compressDst, simData);
                } else {
                    discard[curPos].insert(simData.compress);
                }
            }
            // Default strategy: generate to compress
            col = COLINDEX_COMPRESS;
        }

        generate[curPos][var] = col;
        GenerateVar(var, col, simData);
    }

    // For applying to SimulateData
    void DiscardVar(VarId var, SimulateData& simData) {
        if (simData.compress == var) {
            simData.compress = kVarIdNone;
        } else {
            ColIndex col = simData.vars[var];
            simData.idleCols.insert(col);
            simData.vars.erase(var);
            simData.cols.erase(col);
        }
    }

    // For building
    void DiscardVar(VarId var, size_t curPos, SimulateData& simData) {
        // LOG(INFO) << "DiscardVar(" << var << ")";
        discard[curPos].insert(var);
        DiscardVar(var, simData);
    }

    void Build(const VarHistory& varHistory) {
        // SimulateData simData(nullptr);
        SimulateData simData(inputVars);
        maxVarCnt = std::max(maxVarCnt, simData.vars.size());

        generate.resize(ops.size());
        discard.resize(ops.size());
        for (size_t i = 0; i < ops.size(); i++) {
            const LogicalOp* op = ops[i];
            // LOG(INFO) << "Building: " << op->DebugString();

            // Try to discard & generate variable in this order:
            // - discard useless variable
            // - generate output variable

            // Try discard variables
            for (VarId var : op->input) {
                CHECK(simData.ExistVar(var));
                if (!varHistory.Keep(var, i)) {
                    DiscardVar(var, i, simData);
                }
            }

            // Record generate of variables
            for (VarId var : op->output) {
                GenerateVar(var, i, simData, varHistory);
            }

            maxVarCnt = std::max(maxVarCnt, simData.vars.size());
            // LOG(INFO) << "maxVarCnt: " << maxVarCnt << ", " << simData.DebugString();
            // if (simData.vars.size() > static_cast<u64>(maxVarCnt)) maxVarCnt = simData.vars.size();
        }
    }

    const LogicalOp *beg, *end;

    // Input variables
    const InputMap inputVars;

    SimulateData simData_;

    // ops[i] = i-th processed logical operator in the
    // linear execution process of this ColAssigner
    vector<const LogicalOp*> ops;

    // discard[i] = discarded variables after process of ops[i]
    vector<set<VarId>> discard;

    // generate[i] = generated {variables -> columns} after process of ops[i]
    vector<map<VarId, ColIndex>> generate;

    // Maximum number of exist variables during process
    size_t maxVarCnt;
};

}  // namespace AGE
