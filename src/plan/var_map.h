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
#include <stdlib.h>
#include <cassert>
#include <map>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "base/type.h"
#include "plan/planner_exception.h"
#include "plan/type.h"
#include "util/tool.h"

using std::map;
using std::pair;
using std::string;
using std::unordered_map;

namespace AGE {

// Map variable name(string) to integer id
// Variable is provied by cypher query like "match (n)--(m) return n"
// Then "n" and "m" is variable name, varMap will map them to an integer
class VarMap {
   public:
    VarMap() {}
    VarMap(const VarMap& rhs) = delete;

    ~VarMap() {}

    void clear() {
        nameIdMap.clear();
        idNameMap.clear();
    }

    // Merge varMap from other VarMap
    void merge(const VarMap& rhs) {
        for (auto& [varName, _] : rhs.nameIdMap) insert(varName);
    }

    // Insert variable with a new variable ID
    void insert(const VarName& varName) {
        if (nameIdMap.count(varName) != 0) return;
        VarId var_id = size();
        nameIdMap.emplace(varName, var_id);
        idNameMap.emplace(var_id, varName);
    }

    VarId at(const VarName& name) const {
        if (nameIdMap.count(name) == 0) throw PlannerException("Variable '" + name + "' is not defined!");
        return nameIdMap.at(name);
    }

    VarId operator[](const VarName& name) const { return at(name); }
    const VarName& at(VarId id) const {
        assert(idNameMap.count(id) > 0);
        return idNameMap.at(id);
    }
    const VarName& operator[](VarId id) const { return at(id); }

#define ANONYMOUS_varName_PREFIX "_anon:"
#define PROP_varName_PREFIX "_prop:"

    // anonymous variable name
    static VarName CreateAnonVarName(const ASTNode* n) {
        return ANONYMOUS_varName_PREFIX + to_string(reinterpret_cast<uint64_t>(n));
    }

    static bool IsAnonVarName(const VarName& varName) { return varName.find(ANONYMOUS_varName_PREFIX) == 0; }

    static VarName CreatePropVarName(const VarName& varName, PropId prop_id) {
        return PROP_varName_PREFIX + varName + "." + to_string(prop_id);
    }

    pair<VarId, PropId> ParsePropVarId(VarId prop_var_id) const {
        auto itr = idNameMap.find(prop_var_id);
        if (itr == idNameMap.end()) ((VarMap*)nullptr)->DebugString();
        assert(itr != idNameMap.end());
        return ParsePropVarName(itr->second);
    }

    pair<VarId, PropId> ParsePropVarName(const VarName& s) const {
        size_t pos = s.find('.'), prefix_len = sizeof(PROP_varName_PREFIX) - 1;
        VarName varName = s.substr(prefix_len, pos - prefix_len);
        string prop_key_str = s.substr(pos + 1);
        pair<VarId, PropId> p;
        p.first = this->at(varName);
        p.second = strtol(prop_key_str.c_str(), NULL, 0);
        return p;
    }

    // Noticed that ':' is a keywords in cypher so will not appear in alias name
    bool IsPropVarId(VarId id) const {
        auto itr = idNameMap.find(id);
        if (itr == idNameMap.end()) return false;
        return IsPropVarName(itr->second);
    }
    static bool IsPropVarName(const VarName& varName) { return varName.find(PROP_varName_PREFIX) == 0; }

#undef ANONYMOUS_ALIAS_PREFIX
#undef PROP_varName_PREFIX

    size_t size() const { return nameIdMap.size(); }

    string DebugString() const {
        string ret = "Variable map:\n";
        // string ret = "Variable name id map:\n";

        // nameIdMap.
        for (auto pair : nameIdMap) {
            ret += "{" + pair.first + " : " + std::to_string(pair.second) + "}\n";
            CHECK(nameIdMap.at(idNameMap.at(pair.second)) == pair.second);
        }

        // ret += "Variable id name map:\n";
        // nameIdMap.
        for (auto pair : idNameMap) {
            CHECK(nameIdMap.at(pair.second) == pair.first);
            // ret += "{" + pair.second + " : " + std::to_string(pair.first) + "}\n";
        }

        return ret;
    }

    // private:
    unordered_map<VarName, VarId> nameIdMap;
    map<VarId, VarName> idNameMap;
};
}  // namespace AGE
